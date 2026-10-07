"""One-time, audited recipient overrides for controlled Client Review pilots."""
from __future__ import annotations

import re
import uuid
from hashlib import sha256
from typing import Any
from urllib.parse import urlsplit


EMAIL = re.compile(r"[^\s@<>]+@[^\s@<>]+\.[^\s@<>]+\Z")
PURPOSE = "CONTROLLED_CLIENT_REVIEW_PILOT"


def _recipient(value: Any) -> str:
    if not isinstance(value, str) or not EMAIL.fullmatch(value):
        raise ValueError("Valid pilot recipient required")
    return value


def _form_url(value: Any) -> str:
    if not isinstance(value, str):
        raise ValueError("HTTPS Apps Script form URL required")
    parsed = urlsplit(value)
    if (parsed.scheme, parsed.netloc, parsed.query, parsed.fragment, parsed.username, parsed.password) != ("https", "script.google.com", "", "", None, None):
        raise ValueError("HTTPS Apps Script form URL required")
    if not parsed.path.endswith("/exec"):
        raise ValueError("Apps Script deployed /exec URL required")
    return value


def create_override(service, *, request_id: str, recipient: str, transaction_id: int,
                    form_url: str) -> dict[str, Any]:
    """Create the sole, immutable recipient exception for an active request."""
    recipient = _recipient(recipient)
    form_url = _form_url(form_url)
    if type(transaction_id) is not int:
        raise ValueError("Valid pilot transaction required")
    with service.ledger._atomic():
        request = service.active(request_id)
        item = service.conn.execute('''SELECT item_id,status FROM review_items
                                       WHERE request_id=? AND transaction_id=?''',
                                    (request_id, transaction_id)).fetchone()
        if item is None or item[1] != "PENDING":
            raise ValueError("Pilot transaction must be pending in the active request")
        if recipient == request["recipient"]:
            raise ValueError("Pilot recipient must be an explicit exception")
        if service.conn.execute("SELECT 1 FROM review_pilot_delivery_overrides WHERE request_id=?", (request_id,)).fetchone():
            raise ValueError("Pilot delivery override already exists for this request")
        override_id = uuid.uuid4().hex
        now = service.now()
        service.conn.execute('''INSERT INTO review_pilot_delivery_overrides
            (override_id,request_id,recipient,transaction_id,scope,purpose,status,created_at)
            VALUES (?,?,?,?,? ,?,?,?)''',
            (override_id, request_id, recipient, transaction_id, "ONE_TIME", PURPOSE, "ACTIVE", now))
        form_hash = sha256(form_url.encode("utf-8")).hexdigest()
        service.conn.execute('''INSERT INTO review_pilot_form_links
            (override_id,form_url,form_url_sha256,created_at) VALUES (?,?,?,?)''',
            (override_id, form_url, form_hash, now))
        evidence = dict(override_id=override_id, request_id=request_id,
                        configured_recipient=request["recipient"], pilot_recipient=recipient,
                        transaction_id=transaction_id, item_id=item[0], scope="ONE_TIME", purpose=PURPOSE,
                        form_url_sha256=form_hash)
        service.ledger._audit("PILOT_DELIVERY_OVERRIDE_CREATED", evidence)
        return evidence


def begin_dispatch(service, *, request_id: str, override_id: str) -> dict[str, Any]:
    """Atomically reserve one override before handing a message to a provider."""
    row = service.conn.execute('''SELECT o.recipient,o.transaction_id,o.scope,o.purpose,o.status,l.form_url,l.form_url_sha256
                                  FROM review_pilot_delivery_overrides o
                                  JOIN review_pilot_form_links l ON l.override_id=o.override_id
                                  WHERE o.override_id=? AND o.request_id=?''',
                               (override_id, request_id)).fetchone()
    if row is None:
        raise ValueError("Pilot delivery override unavailable")
    recipient, transaction_id, scope, purpose, status, form_url, form_hash = row
    if status != "ACTIVE" or scope != "ONE_TIME" or purpose != PURPOSE:
        raise ValueError("Pilot delivery override is not available")
    changed = service.conn.execute("""UPDATE review_pilot_delivery_overrides
                                      SET status='SENDING'
                                      WHERE override_id=? AND request_id=? AND status='ACTIVE'""",
                                   (override_id, request_id)).rowcount
    if changed != 1:
        raise ValueError("Pilot delivery override is not available")
    evidence = dict(override_id=override_id, request_id=request_id, pilot_recipient=recipient,
                    transaction_id=transaction_id, scope=scope, purpose=purpose,
                    form_url=form_url, form_url_sha256=form_hash)
    service.ledger._audit("PILOT_DELIVERY_OVERRIDE_DISPATCHING", evidence)
    return evidence


def finish_dispatch(service, *, request_id: str, override_id: str, outcome: str) -> None:
    if outcome not in {"CONSUMED", "UNCERTAIN"}:
        raise ValueError("Invalid pilot delivery outcome")
    changed = service.conn.execute("""UPDATE review_pilot_delivery_overrides
                                      SET status=?,consumed_at=?
                                      WHERE override_id=? AND request_id=? AND status='SENDING'""",
                                   (outcome, service.now(), override_id, request_id)).rowcount
    if changed != 1:
        raise ValueError("Pilot delivery override dispatch state changed")
    service.ledger._audit("PILOT_DELIVERY_OVERRIDE_" + outcome,
                          dict(override_id=override_id, request_id=request_id, outcome=outcome))


def create_recovery_override(service, *, original_override_id: str) -> dict[str, Any]:
    """Authorize one explicit recovery only after a confirmed non-delivery."""
    with service.ledger._atomic():
        row = service.conn.execute('''SELECT request_id,recipient,transaction_id,status
                                      FROM review_pilot_delivery_overrides WHERE override_id=?''',
                                   (original_override_id,)).fetchone()
        if row is None or row[3] != "UNCERTAIN":
            raise ValueError("Only an uncertain pilot override can be recovered")
        request_id, recipient, transaction_id, _ = row
        if service.conn.execute('SELECT 1 FROM review_pilot_delivery_recoveries WHERE original_override_id=?',
                                (original_override_id,)).fetchone():
            raise ValueError("Pilot delivery recovery already exists")
        service.active(request_id)
        delivery = service.conn.execute('SELECT status FROM review_deliveries WHERE request_id=?', (request_id,)).fetchone()
        if delivery != ("UNCERTAIN",):
            raise ValueError("Pilot delivery is not awaiting recovery")
        recovery_id, now = uuid.uuid4().hex, service.now()
        service.conn.execute('''INSERT INTO review_pilot_delivery_recoveries
            (recovery_id,original_override_id,confirmation,status,created_at) VALUES (?,?,?,?,?)''',
            (recovery_id, original_override_id, "OPERATOR_CONFIRMED_NO_DELIVERY", "ACTIVE", now))
        evidence = dict(recovery_id=recovery_id, original_override_id=original_override_id,
                        request_id=request_id, pilot_recipient=recipient, transaction_id=transaction_id,
                        confirmation="OPERATOR_CONFIRMED_NO_DELIVERY", scope="ONE_TIME")
        service.ledger._audit("PILOT_DELIVERY_RECOVERY_CREATED", evidence)
        return evidence


def begin_recovery_dispatch(service, *, request_id: str, recovery_id: str) -> dict[str, Any]:
    row = service.conn.execute('''SELECT r.original_override_id,r.confirmation,r.status,o.recipient,o.transaction_id,
                                         l.form_url,l.form_url_sha256
                                  FROM review_pilot_delivery_recoveries r
                                  JOIN review_pilot_delivery_overrides o ON o.override_id=r.original_override_id
                                  JOIN review_pilot_form_links l ON l.override_id=o.override_id
                                  WHERE r.recovery_id=? AND o.request_id=?''', (recovery_id, request_id)).fetchone()
    if row is None:
        raise ValueError("Pilot delivery recovery unavailable")
    original_override_id, confirmation, status, recipient, transaction_id, form_url, form_hash = row
    if status != "ACTIVE" or confirmation != "OPERATOR_CONFIRMED_NO_DELIVERY":
        raise ValueError("Pilot delivery recovery is not available")
    if service.conn.execute("UPDATE review_pilot_delivery_recoveries SET status='SENDING' WHERE recovery_id=? AND status='ACTIVE'",
                            (recovery_id,)).rowcount != 1:
        raise ValueError("Pilot delivery recovery is not available")
    evidence = dict(recovery_id=recovery_id, original_override_id=original_override_id, request_id=request_id,
                    pilot_recipient=recipient, transaction_id=transaction_id, form_url=form_url,
                    form_url_sha256=form_hash, confirmation=confirmation, scope="ONE_TIME")
    service.ledger._audit("PILOT_DELIVERY_RECOVERY_DISPATCHING", evidence)
    return evidence


def finish_recovery_dispatch(service, *, request_id: str, recovery_id: str, outcome: str) -> None:
    if outcome not in {"CONSUMED", "UNCERTAIN"}:
        raise ValueError("Invalid pilot recovery outcome")
    if service.conn.execute('''UPDATE review_pilot_delivery_recoveries SET status=?,consumed_at=?
                               WHERE recovery_id=? AND status='SENDING' ''',
                            (outcome, service.now(), recovery_id)).rowcount != 1:
        raise ValueError("Pilot delivery recovery dispatch state changed")
    service.ledger._audit("PILOT_DELIVERY_RECOVERY_" + outcome,
                          dict(recovery_id=recovery_id, request_id=request_id, outcome=outcome))
