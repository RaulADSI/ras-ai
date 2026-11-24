-- La columna transactions.provenance_hash queda como dato legado obligatorio.
-- La identidad bancaria ya no depende de ella. No se inventan coordenadas para
-- registros anteriores: sus archivos originales deberán recuperarse por separado.
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
-- Si existen referencias duplicadas, aborta la migración sin fusionar eventos.
CREATE UNIQUE INDEX idx_unique_bank_transaction
ON transactions(bank_id, account_ref, trim(bank_reference))
WHERE trim(bank_reference) != '';
CREATE TRIGGER transaction_provenance_no_update BEFORE UPDATE ON transaction_provenance
BEGIN
    SELECT RAISE(ABORT, 'La procedencia es inmutable');
END;
CREATE TRIGGER transaction_provenance_no_delete BEFORE DELETE ON transaction_provenance
BEGIN
    SELECT RAISE(ABORT, 'La procedencia es inmutable');
END;
