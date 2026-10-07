CREATE TABLE review_pilot_delivery_recoveries (
    recovery_id TEXT PRIMARY KEY,
    original_override_id TEXT NOT NULL UNIQUE REFERENCES review_pilot_delivery_overrides(override_id),
    confirmation TEXT NOT NULL CHECK(confirmation='OPERATOR_CONFIRMED_NO_DELIVERY'),
    status TEXT NOT NULL CHECK(status IN ('ACTIVE','SENDING','CONSUMED','UNCERTAIN')),
    created_at TEXT NOT NULL,
    consumed_at TEXT
);
CREATE TRIGGER pilot_recovery_no_delete BEFORE DELETE ON review_pilot_delivery_recoveries
BEGIN SELECT RAISE(ABORT, 'Pilot delivery recovery is append-only'); END;
CREATE TRIGGER pilot_recovery_fields_immutable BEFORE UPDATE OF recovery_id,original_override_id,confirmation,created_at ON review_pilot_delivery_recoveries
BEGIN SELECT RAISE(ABORT, 'Pilot delivery recovery fields are immutable'); END;
CREATE TRIGGER pilot_recovery_status_transition BEFORE UPDATE OF status ON review_pilot_delivery_recoveries
WHEN NOT ((OLD.status='ACTIVE' AND NEW.status='SENDING')
       OR (OLD.status='SENDING' AND NEW.status IN ('CONSUMED','UNCERTAIN')))
BEGIN SELECT RAISE(ABORT, 'Invalid pilot recovery status transition'); END;
