import json

import pytest

from scripts.persistence.ledger import Ledger
from scripts.review.delivery import deliver
from scripts.review.drive_receipt import prepare_receipt
from scripts.review.pilot_delivery import create_override, create_recovery_override
from scripts.review.security import ReviewAccess
from scripts.review.service import ReviewService


SECRET = "pilot-test-" + "x" * 48
FORM_URL = "https://script.google.com/a/macros/rentify.live/s/test/exec"


def _request(service):
    service.ledger.register_transactions([dict(bank_id="AMEX", account_ref="AMEX", bank_reference="pilot-62",
        amount_str="11.02", currency="USD", original_date="2026-08-04", file_hash="a" * 64,
        sheet_name="CSV", row_number=188)])
    return service.create(creation_key="pilot-request", company="RAS", currency="USD",
        recipient="accounting@rentify.live", expires_at="2099-01-01T00:00:00+00:00",
        items=[dict(transaction_id=1, review_type="UNRESOLVED_PROPERTY", context=dict(company="RAS",
            merchant_original="USPS", classification={}, blockers=[]))])


def test_pilot_override_is_one_time_audited_and_uses_existing_request(temp_db_conn):
    service = ReviewService(temp_db_conn)
    request_id = _request(service)
    override = create_override(service, request_id=request_id, recipient="martha@rentify.live", transaction_id=1,
                               form_url=FORM_URL)
    captured = []
    result = deliver(service, ReviewAccess(service, SECRET), request_id,
                     sender="review@rentify.live",
                     send=lambda message: captured.append(message) or "pilot-provider-id",
                     pilot_override_id=override["override_id"])
    assert result == "SENT"
    assert captured[0]["To"] == "martha@rentify.live"
    assert "únicamente la transacción AMEX del piloto (ID 1)" in captured[0].get_content()
    assert "Deja los demás ítems en estado PENDING" in captured[0].get_content()
    assert FORM_URL in captured[0].get_content()
    assert "/review/" not in captured[0].get_content()
    assert service.request(request_id)["recipient"] == "accounting@rentify.live"
    assert temp_db_conn.execute("SELECT status FROM review_pilot_delivery_overrides").fetchone() == ("CONSUMED",)
    events = [row[0] for row in temp_db_conn.execute("SELECT event_type FROM audit_events").fetchall()]
    assert events[-4:] == ["PILOT_DELIVERY_OVERRIDE_CREATED", "PILOT_DELIVERY_OVERRIDE_DISPATCHING",
                           "PILOT_DELIVERY_OVERRIDE_CONSUMED", "REVIEW_EMAIL_SENT"]
    audit = json.loads(temp_db_conn.execute("SELECT payload_json FROM audit_events WHERE event_type='REVIEW_EMAIL_SENT'").fetchone()[0])
    assert audit["recipient"] == "martha@rentify.live"
    with pytest.raises(ValueError, match="already exists"):
        create_override(service, request_id=request_id, recipient="martha@rentify.live", transaction_id=1,
                        form_url=FORM_URL)


def test_pilot_override_failure_is_uncertain_and_never_reusable(temp_db_conn):
    service = ReviewService(temp_db_conn)
    request_id = _request(service)
    override = create_override(service, request_id=request_id, recipient="martha@rentify.live", transaction_id=1,
                               form_url=FORM_URL)
    with pytest.raises(RuntimeError, match="uncertain"):
        deliver(service, ReviewAccess(service, SECRET), request_id,
                sender="review@rentify.live", send=lambda message: (_ for _ in ()).throw(TimeoutError()),
                pilot_override_id=override["override_id"])
    assert temp_db_conn.execute("SELECT status FROM review_pilot_delivery_overrides").fetchone() == ("UNCERTAIN",)
    with pytest.raises(ValueError, match="uncertain"):
        deliver(service, ReviewAccess(service, SECRET), request_id,
                sender="review@rentify.live", send=lambda message: "must-not-send",
                pilot_override_id=override["override_id"])
    recovery = create_recovery_override(service, original_override_id=override["override_id"])
    captured = []
    assert deliver(service, ReviewAccess(service, SECRET), request_id, sender="review@rentify.live",
                   send=lambda message: captured.append(message) or "recovery-provider-id",
                   pilot_recovery_id=recovery["recovery_id"]) == "SENT"
    assert captured[0]["To"] == "martha@rentify.live"
    assert temp_db_conn.execute("SELECT status FROM review_pilot_delivery_recoveries").fetchone() == ("CONSUMED",)
    with pytest.raises(ValueError, match="already exists"):
        create_recovery_override(service, original_override_id=override["override_id"])


@pytest.mark.parametrize("form_url", ["https://example.test/exec", "http://script.google.com/x/exec",
                                       FORM_URL + "?request=62", "https://script.google.com/x/dev"])
def test_pilot_override_requires_exact_apps_script_deployment_url(temp_db_conn, form_url):
    service = ReviewService(temp_db_conn)
    request_id = _request(service)
    with pytest.raises(ValueError, match="Apps Script"):
        create_override(service, request_id=request_id, recipient="martha@rentify.live", transaction_id=1,
                        form_url=form_url)


def test_expired_request_rejects_apps_script_receipt_before_any_decision(temp_db_conn):
    service = ReviewService(temp_db_conn)
    request_id = _request(service)
    item_id, provenance, source_row, amount = temp_db_conn.execute('''SELECT i.item_id,p.provenance_hash,p.row_number,t.amount_cents
        FROM review_items i JOIN transaction_provenance p ON p.transaction_id=i.transaction_id
        JOIN transactions t ON t.id=i.transaction_id WHERE i.request_id=?''', (request_id,)).fetchone()
    manifest = {"request_id": request_id, "items": [dict(transaction_id=1, item_id=item_id,
        historical_provenance=provenance, source_row=source_row, amount_cents=amount, company="RAS")]}
    receipt = {"schema_version": 1, "submission_id": "expired-pilot-receipt", "received_at": "2100-01-02T00:00:00Z",
        "payload": {"report_type": "property_assignment_response", "responses": [dict(
            provenance_hash=provenance, source_row=source_row, amount_cents=amount, property="", notes="")]}}
    service.now = lambda: "2100-01-02T00:00:00+00:00"
    with pytest.raises(ValueError, match="expired"):
        prepare_receipt(service, receipt, manifest)
    assert temp_db_conn.execute("SELECT count(*) FROM review_submissions").fetchone() == (0,)
