import pytest
import sqlite3
from scripts.persistence.ledger import Ledger

def test_amount_must_be_integer(temp_db_conn):
    with pytest.raises(sqlite3.IntegrityError, match="CHECK constraint failed"):
        temp_db_conn.execute(
            "INSERT INTO transactions (bank_id, account_ref, bank_reference, provenance_hash, amount_cents, currency, original_date) VALUES (?, ?, ?, ?, ?, ?, ?)",
            ('AMEX', '1234', 'REF1', 'HASH1', 5000.5, 'USD', '2026-08-12')
        )

def test_identity_without_bank_reference(temp_db_conn):
    temp_db_conn.execute(
        "INSERT INTO transactions (bank_id, account_ref, bank_reference, provenance_hash, amount_cents, currency, original_date) VALUES (?, ?, ?, ?, ?, ?, ?)",
        ('CITI', '5678', '', 'HASH_FILE_A_LINE_1', 1000, 'USD', '2026-08-12')
    )
    temp_db_conn.execute(
        "INSERT INTO transactions (bank_id, account_ref, bank_reference, provenance_hash, amount_cents, currency, original_date) VALUES (?, ?, ?, ?, ?, ?, ?)",
        ('CITI', '5678', '', 'HASH_FILE_B_LINE_2', 1000, 'USD', '2026-08-12')
    )
    cursor = temp_db_conn.execute("SELECT COUNT(*) FROM transactions")
    assert cursor.fetchone()[0] == 2

def test_zero_cent_strict_decomposition():
    ledger = Ledger(None)
    # Exitoso: 0 descompuesto en 0
    ledger.validate_coverage_and_signs(0, [0, 0])
    # Fallo: neteo interno oculto
    with pytest.raises(ValueError, match="solo admite fracciones cero"):
        ledger.validate_coverage_and_signs(0, [100, -100])

def test_sign_conservation():
    ledger = Ledger(None)
    with pytest.raises(ValueError, match="positiva no admite negativos"):
        ledger.validate_coverage_and_signs(8000, [10000, -2000])

def test_strict_coverage_cents():
    ledger = Ledger(None)
    with pytest.raises(ValueError, match="no coincide"):
        ledger.validate_coverage_and_signs(10000, [5000, 4999])

def test_exclusive_reservation(temp_db_conn):
    # Insertar dependencias mock
    temp_db_conn.execute(
        "INSERT INTO transactions (id, bank_id, account_ref, bank_reference, provenance_hash, amount_cents, currency, original_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (1, 'AMEX', '1234', 'REF_RESERVATION', 'HASH_RESERVATION', 5000, 'USD', '2026-08-12'),
    )
    temp_db_conn.execute("INSERT INTO allocations (id, transaction_id, amount_cents, eligibility_state) VALUES (1, 1, 5000, 'READY_TO_IMPORT')")
    temp_db_conn.execute("INSERT INTO export_batches (batch_id) VALUES ('BATCH1'), ('BATCH2')")
    
    # 1. Reserva inicial exitosa
    temp_db_conn.execute("INSERT INTO export_items (batch_id, allocation_id, delivery_state) VALUES ('BATCH1', 1, 'RESERVED')")
    
    # 2. Intento de segunda reserva falla
    with pytest.raises(sqlite3.IntegrityError, match="UNIQUE constraint failed"):
        temp_db_conn.execute("INSERT INTO export_items (batch_id, allocation_id, delivery_state) VALUES ('BATCH2', 1, 'RESERVED')")
    
    # 3. Marcar como IMPORT_FAILED_REVIEW sigue bloqueando
    temp_db_conn.execute("UPDATE export_items SET delivery_state = 'IMPORT_FAILED_REVIEW' WHERE batch_id = 'BATCH1'")
    with pytest.raises(sqlite3.IntegrityError, match="UNIQUE constraint failed"):
        temp_db_conn.execute("INSERT INTO export_items (batch_id, allocation_id, delivery_state) VALUES ('BATCH2', 1, 'RESERVED')")
        
    # 4. Autorización manual cierra el intento y libera la reserva
    temp_db_conn.execute("UPDATE export_items SET delivery_state = 'IMPORT_FAILED_CLOSED' WHERE batch_id = 'BATCH1'")
    temp_db_conn.execute("INSERT INTO export_items (batch_id, allocation_id, delivery_state) VALUES ('BATCH2', 1, 'RESERVED')") # Éxito
