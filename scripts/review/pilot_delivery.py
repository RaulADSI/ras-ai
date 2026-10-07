"""One-time, audited recipient overrides for controlled Client Review pilots."""
from __future__ import annotations

import re
import uuid
from typing import Any


EMAIL = re.compile(r"[^\s@<>]+@[^\s@<>]+\.[^\s@<>]+\Z")
PURPOSE = "CONTROLLED_CLIENT_REVIEW_PILOT"


def _recipient(value: Any) -> str:
    if not isinstance(value, str) or not EMAIL.fullmatch(value):
        raise ValueError("Valid pilot recipient required")
    return value


def create_override(service, *, request_id: str, recipient: str, transaction_id: int) -> dict[str, Any]:
    """Create the sole, immutable recipient exception for an active request."""
    recipient = _recipient(recipient)
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
        evidence = dict(override_id=override_id, request_id=request_id,
                        configured_recipient=request["recipient"], pilot_recipient=recipient,
                        transaction_id=transaction_id, item_id=item[0], scope="ONE_TIME", purpose=PURPOSE)
        service.ledger._audit("PILOT_DELIVERY_OVERRIDE_CREATED", evidence)
        return evidence


def begin_dispatch(service, *, request_id: str, override_id: str) -> dict[str, Any]:
    """Atomically reserve one override before handing a message to a provider."""
    row = service.conn.execute('''SELECT recipient,transaction_id,scope,purpose,status
                                  FROM review_pilot_delivery_overrides
                                  WHERE override_id=? AND request_id=?''',
                               (override_id, request_id)).fetchone()
    if row is None:
        raise ValueError("Pilot delivery override unavailable")
    recipient, transaction_id, scope, purpose, status = row
    if status != "ACTIVE" or scope != "ONE_TIME" or purpose != PURPOSE:
        raise ValueError("Pilot delivery override is not available")
    changed = service.conn.execute("""UPDATE review_pilot_delivery_overrides
                                      SET status='SENDING'
                                      WHERE override_id=? AND request_id=? AND status='ACTIVE'""",
                                   (override_id, request_id)).rowcount
    if changed != 1:
        raise ValueError("Pilot delivery override is not available")
    evidence = dict(override_id=override_id, request_id=request_id, pilot_recipient=recipient,
                    transaction_id=transaction_id, scope=scope, purpose=purpose)
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
