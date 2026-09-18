import pytest
import sqlite3
from scripts.persistence.ledger import Ledger

@pytest.fixture
def seeded_ledger(temp_db_conn):
    """
    Herregni akka wal-qixxaatu gochuu (10000 = 5000 + 5000).
    """
    temp_db_conn.execute("INSERT INTO transactions (id, bank_id, account_ref, bank_reference, provenance_hash, amount_cents, currency, original_date) VALUES (1, 'AMEX', '1234', 'REF1', 'HASH1', 10000, 'USD', '2026-08-12')")
    temp_db_conn.execute("INSERT INTO allocations (id, transaction_id, amount_cents, eligibility_state) VALUES (1, 1, 5000, 'READY_TO_IMPORT')")
    temp_db_conn.execute("INSERT INTO allocations (id, transaction_id, amount_cents, eligibility_state) VALUES (2, 1, 5000, 'REVIEW_REQUIRED')")
    return Ledger(temp_db_conn), temp_db_conn

def test_reserve_batch_only_ready_to_import(seeded_ledger):
    ledger, conn = seeded_ledger
    batch_id, snapshot = ledger.reserve_batch()
    
    assert batch_id is not None
    assert len(snapshot) == 1  # Solo debe agarrar la fracción 1
    assert snapshot[0]["allocation_id"] == 1
    
    # Verificar que el estado en BD cambió
    cursor = conn.execute("SELECT delivery_state FROM export_items WHERE batch_id = ?", (batch_id,))
    assert cursor.fetchone()[0] == 'RESERVED'

def test_mark_exported_lifecycle(seeded_ledger):
    ledger, conn = seeded_ledger
    batch_id, _ = ledger.reserve_batch()
    
    ledger.mark_exported(batch_id, "dummy_hash_123")
    
    # Verificar lote y estados
    hash_val = conn.execute("SELECT file_hash FROM export_batches WHERE batch_id = ?", (batch_id,)).fetchone()[0]
    assert hash_val == "dummy_hash_123"
    
    state = conn.execute("SELECT delivery_state FROM export_items WHERE batch_id = ?", (batch_id,)).fetchone()[0]
    assert state == 'EXPORTED'

def test_confirm_import_partial_success(seeded_ledger):
    ledger, conn = seeded_ledger
    # Añadir otra fracción ready para simular lote múltiple
    conn.execute("INSERT INTO allocations (id, transaction_id, amount_cents, eligibility_state) VALUES (3, 1, 2000, 'READY_TO_IMPORT')")
    
    batch_id, _ = ledger.reserve_batch()
    ledger.mark_exported(batch_id, "hash")
    
    # Confirmar 1 como éxito, 3 como fallo
    ledger.confirm_import(batch_id, successful_refs={1: "APPFOLIO_REF_99"}, failed_allocs=[3])
    
    # Verificar éxito
    row_success = conn.execute("SELECT delivery_state, appfolio_reference FROM export_items WHERE allocation_id = 1").fetchone()
    assert row_success[0] == 'IMPORTED'
    assert row_success[1] == 'APPFOLIO_REF_99'
    
    # Verificar retención del fallo
    row_fail = conn.execute("SELECT delivery_state FROM export_items WHERE allocation_id = 3").fetchone()
    assert row_fail[0] == 'IMPORT_FAILED_REVIEW'