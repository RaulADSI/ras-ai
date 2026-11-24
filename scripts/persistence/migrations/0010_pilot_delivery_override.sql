CREATE TABLE review_pilot_delivery_overrides (
    override_id TEXT PRIMARY KEY,
    request_id TEXT NOT NULL UNIQUE REFERENCES review_requests(request_id),
    recipient TEXT NOT NULL,
    transaction_id INTEGER NOT NULL REFERENCES transactions(id),
    scope TEXT NOT NULL CHECK(scope='ONE_TIME'),
    purpose TEXT NOT NULL CHECK(purpose='CONTROLLED_CLIENT_REVIEW_PILOT'),
    status TEXT NOT NULL CHECK(status IN ('ACTIVE','SENDING','CONSUMED','UNCERTAIN','REVOKED')),
    created_at TEXT NOT NULL,
    consumed_at TEXT
);
CREATE TRIGGER pilot_override_no_delete BEFORE DELETE ON review_pilot_delivery_overrides
BEGIN SELECT RAISE(ABORT, 'Pilot delivery override is append-only'); END;
CREATE TRIGGER pilot_override_fields_immutable BEFORE UPDATE OF request_id,recipient,transaction_id,scope,purpose,created_at ON review_pilot_delivery_overrides
BEGIN SELECT RAISE(ABORT, 'Pilot delivery override fields are immutable'); END;
CREATE TRIGGER pilot_override_status_transition BEFORE UPDATE OF status ON review_pilot_delivery_overrides
WHEN NOT ((OLD.status='ACTIVE' AND NEW.status IN ('SENDING','REVOKED'))
       OR (OLD.status='SENDING' AND NEW.status IN ('CONSUMED','UNCERTAIN')))
BEGIN SELECT RAISE(ABORT, 'Invalid pilot delivery override status transition'); END;
