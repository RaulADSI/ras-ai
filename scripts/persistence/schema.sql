-- Generado automáticamente; editar migrations/*.sql.
CREATE UNIQUE INDEX idx_active_reservation
ON export_items(allocation_id)
WHERE delivery_state IN ('RESERVED', 'EXPORTED', 'IMPORTED', 'IMPORT_FAILED_REVIEW');

CREATE UNIQUE INDEX idx_unique_bank_transaction
ON transactions(bank_id, account_ref, trim(bank_reference))
WHERE trim(bank_reference) != '';

CREATE TABLE allocations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    transaction_id INTEGER NOT NULL REFERENCES transactions(id) ON DELETE RESTRICT,
    amount_cents INTEGER NOT NULL CHECK(typeof(amount_cents) = 'integer'),
    eligibility_state TEXT NOT NULL CHECK(eligibility_state IN ('MATCHED_EXISTING', 'READY_TO_IMPORT', 'REVIEW_REQUIRED')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
, property_code TEXT NOT NULL DEFAULT '', vendor_name TEXT NOT NULL DEFAULT '', gl_account TEXT NOT NULL DEFAULT '', cash_account TEXT NOT NULL DEFAULT '', description TEXT NOT NULL DEFAULT '');

CREATE TABLE audit_events (
    event_id INTEGER PRIMARY KEY,
    event_type TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE TABLE exceptions (
    exception_id TEXT PRIMARY KEY,
    allocation_id INTEGER NOT NULL REFERENCES allocations(id) ON DELETE RESTRICT,
    version INTEGER NOT NULL DEFAULT 1,
    status TEXT NOT NULL CHECK(status IN ('PENDING', 'RESOLVED')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
, reason TEXT NOT NULL DEFAULT '', metadata_json TEXT NOT NULL DEFAULT '{}');

CREATE TABLE exceptions_decisions (
    decision_id TEXT PRIMARY KEY,
    exception_id TEXT NOT NULL REFERENCES exceptions(exception_id) ON DELETE RESTRICT,
    allocation_id INTEGER NOT NULL REFERENCES allocations(id) ON DELETE RESTRICT,
    action TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    reason TEXT NOT NULL,
    reviewed_by TEXT NOT NULL,
    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
, request_json TEXT NOT NULL DEFAULT '{}', exception_version INTEGER NOT NULL DEFAULT 1);

CREATE TABLE export_batches (
    batch_id TEXT PRIMARY KEY,
    file_hash TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
, file_path TEXT);

CREATE TABLE export_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id TEXT NOT NULL REFERENCES export_batches(batch_id) ON DELETE RESTRICT,
    allocation_id INTEGER NOT NULL REFERENCES allocations(id) ON DELETE RESTRICT,
    delivery_state TEXT NOT NULL CHECK(delivery_state IN ('RESERVED', 'EXPORTED', 'IMPORTED', 'IMPORT_FAILED_REVIEW', 'IMPORT_FAILED_CLOSED')),
    appfolio_reference TEXT
, snapshot_json TEXT);

CREATE TABLE invoice_matches (
    allocation_id INTEGER PRIMARY KEY REFERENCES allocations(id) ON DELETE RESTRICT,
    appfolio_invoice_id TEXT NOT NULL UNIQUE,
    matched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
, match_details_json TEXT NOT NULL DEFAULT '{}');

CREATE TABLE schema_migrations (
            version INTEGER PRIMARY KEY, name TEXT NOT NULL,
            hash_sha256 TEXT NOT NULL,
            applied_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
        );

CREATE TABLE transaction_provenance (
    id INTEGER PRIMARY KEY,
    transaction_id INTEGER NOT NULL REFERENCES transactions(id) ON DELETE RESTRICT,
    file_hash TEXT NOT NULL CHECK(length(file_hash) = 64),
    sheet_name TEXT NOT NULL,
    row_number INTEGER NOT NULL CHECK(typeof(row_number) = 'integer' AND row_number > 0),
    provenance_hash TEXT NOT NULL UNIQUE CHECK(length(provenance_hash) = 64),
    ingested_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    UNIQUE(file_hash, sheet_name, row_number)
);

CREATE TABLE transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    bank_id TEXT NOT NULL,
    account_ref TEXT NOT NULL,
    bank_reference TEXT NOT NULL,
    provenance_hash TEXT NOT NULL,
    amount_cents INTEGER NOT NULL CHECK(typeof(amount_cents) = 'integer'),
    currency TEXT NOT NULL,
    original_date DATE NOT NULL,
    UNIQUE(bank_id, account_ref, bank_reference, provenance_hash)
);

CREATE TRIGGER audit_events_no_delete BEFORE DELETE ON audit_events
BEGIN
    SELECT RAISE(ABORT, 'audit_events es append-only');
END;

CREATE TRIGGER audit_events_no_update BEFORE UPDATE ON audit_events
BEGIN
    SELECT RAISE(ABORT, 'audit_events es append-only');
END;

CREATE TRIGGER exceptions_decisions_no_delete BEFORE DELETE ON exceptions_decisions
BEGIN
    SELECT RAISE(ABORT, 'Las decisiones aplicadas son inmutables');
END;

CREATE TRIGGER exceptions_decisions_no_update BEFORE UPDATE ON exceptions_decisions
BEGIN
    SELECT RAISE(ABORT, 'Las decisiones aplicadas son inmutables');
END;

CREATE TRIGGER export_snapshot_no_update BEFORE UPDATE OF snapshot_json ON export_items
WHEN OLD.snapshot_json IS NOT NULL
BEGIN
    SELECT RAISE(ABORT, 'La instantánea reservada es inmutable');
END;

CREATE TRIGGER transaction_provenance_no_delete BEFORE DELETE ON transaction_provenance
BEGIN
    SELECT RAISE(ABORT, 'La procedencia es inmutable');
END;

CREATE TRIGGER transaction_provenance_no_update BEFORE UPDATE ON transaction_provenance
BEGIN
    SELECT RAISE(ABORT, 'La procedencia es inmutable');
END;
