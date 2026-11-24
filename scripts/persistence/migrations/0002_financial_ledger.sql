-- 0002_financial_ledger.sql

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

CREATE TABLE allocations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    transaction_id INTEGER NOT NULL REFERENCES transactions(id) ON DELETE RESTRICT,
    amount_cents INTEGER NOT NULL CHECK(typeof(amount_cents) = 'integer'),
    eligibility_state TEXT NOT NULL CHECK(eligibility_state IN ('MATCHED_EXISTING', 'READY_TO_IMPORT', 'REVIEW_REQUIRED')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE invoice_matches (
    allocation_id INTEGER PRIMARY KEY REFERENCES allocations(id) ON DELETE RESTRICT,
    appfolio_invoice_id TEXT NOT NULL UNIQUE,
    matched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE exceptions (
    exception_id TEXT PRIMARY KEY,
    allocation_id INTEGER NOT NULL REFERENCES allocations(id) ON DELETE RESTRICT,
    version INTEGER NOT NULL DEFAULT 1,
    status TEXT NOT NULL CHECK(status IN ('PENDING', 'RESOLVED')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE exceptions_decisions (
    decision_id TEXT PRIMARY KEY,
    exception_id TEXT NOT NULL REFERENCES exceptions(exception_id) ON DELETE RESTRICT,
    allocation_id INTEGER NOT NULL REFERENCES allocations(id) ON DELETE RESTRICT,
    action TEXT NOT NULL,
    request_hash TEXT NOT NULL, 
    reason TEXT NOT NULL,
    reviewed_by TEXT NOT NULL,
    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE export_batches (
    batch_id TEXT PRIMARY KEY,
    file_hash TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE export_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id TEXT NOT NULL REFERENCES export_batches(batch_id) ON DELETE RESTRICT,
    allocation_id INTEGER NOT NULL REFERENCES allocations(id) ON DELETE RESTRICT,
    delivery_state TEXT NOT NULL CHECK(delivery_state IN ('RESERVED', 'EXPORTED', 'IMPORTED', 'IMPORT_FAILED_REVIEW', 'IMPORT_FAILED_CLOSED')),
    appfolio_reference TEXT
);

CREATE UNIQUE INDEX idx_active_reservation 
ON export_items(allocation_id) 
WHERE delivery_state IN ('RESERVED', 'EXPORTED', 'IMPORTED', 'IMPORT_FAILED_REVIEW');