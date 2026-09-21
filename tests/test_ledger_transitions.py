import csv
import hashlib
import pytest
from scripts.persistence.ledger import Ledger, connect
from scripts.output.bulk_bill_generator import publish_reserved_batch
from scripts.config import FINAL_APPFOLIO_COLUMNS


@pytest.fixture
def seeded_ledger(temp_db_conn):
    conn = temp_db_conn
    conn.execute("INSERT INTO transactions (id,bank_id,account_ref,bank_reference,provenance_hash,amount_cents,currency,original_date) VALUES (1,'AMEX','1234','REF1','HASH1',10000,'USD','2026-08-12')")
    conn.execute("INSERT INTO allocations (id,transaction_id,amount_cents,eligibility_state,property_code,vendor_name,gl_account,cash_account,description) VALUES (1,1,6001,'READY_TO_IMPORT','P1','Proveedor, ñ','6435','1150','Gasto'),(2,1,3999,'REVIEW_REQUIRED','','','','','')")
    return Ledger(conn), conn


def test_reserve_batch_only_ready_to_import(seeded_ledger):
    ledger, conn = seeded_ledger
    batch_id, snapshot = ledger.reserve_batch()
    assert snapshot[0].allocation_id == 1
    assert snapshot[0].amount_cents == 6001
    assert ledger.reserve_batch() == (None, ())
    with pytest.raises(AttributeError):
        snapshot[0].amount_cents = 10000


def test_mark_exported_lifecycle(seeded_ledger, tmp_path):
    ledger, conn = seeded_ledger
    batch_id, snapshot = ledger.reserve_batch()
    path = publish_reserved_batch(ledger, batch_id, tmp_path / 'exports')
    with path.open(encoding='utf-8-sig', newline='') as stream:
        reader = csv.DictReader(stream)
        assert reader.fieldnames == FINAL_APPFOLIO_COLUMNS
        rows = list(reader)
    assert len(rows) == 1
    assert rows[0]['Amount*'] == '60.01'
    assert rows[0]['Vendor Payee Name*'] == 'Proveedor, ñ'
    assert conn.execute('SELECT file_hash FROM export_batches').fetchone()[0] == hashlib.sha256(path.read_bytes()).hexdigest()
    ledger.mark_exported(batch_id, path, snapshot)
    assert conn.execute('SELECT delivery_state FROM export_items').fetchone()[0] == 'EXPORTED'


def test_confirm_import_partial_success(seeded_ledger, tmp_path):
    ledger, conn = seeded_ledger
    conn.execute("UPDATE allocations SET eligibility_state='READY_TO_IMPORT',property_code='P2',vendor_name='Vendor',gl_account='6435',cash_account='1150' WHERE id=2")
    batch_id, _ = ledger.reserve_batch()
    publish_reserved_batch(ledger, batch_id, tmp_path / 'exports')
    ledger.confirm_import(batch_id, {1: 'APP99'}, [2])
    ledger.confirm_import(batch_id, {1: 'APP99'}, [2])
    assert conn.execute('SELECT delivery_state FROM export_items ORDER BY allocation_id').fetchall() == [('IMPORTED',), ('IMPORT_FAILED_REVIEW',)]
    assert ledger.reserve_batch() == (None, ())
