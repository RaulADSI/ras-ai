import hashlib
import shutil
import sqlite3
from decimal import localcontext
from pathlib import Path

import pytest
from scripts.persistence.ledger import Ledger, parse_to_cents, calculate_provenance_hash, connect
from scripts.persistence.migrate import migrate, MigrationError, DEFAULT_MIGRATIONS


def record(**overrides):
    result = dict(bank_id='AMEX', account_ref='1234', bank_reference='REF1', amount_str='50.00',
                  currency='USD', original_date='2026-08-12',
                  file_hash=hashlib.sha256(b'original bytes').hexdigest(), sheet_name='CSV', row_number=2)
    result.update(overrides)
    return result


@pytest.mark.parametrize('text,expected', [('50.00',5000),('-25.00',-2500),('0',0),('-0.00',0),
    ('1.2300',123),('1e2',10000),('.01',1),('92233720368547758.07',2**63-1),('-92233720368547758.08',-2**63)])
def test_exact_cents(text, expected):
    with localcontext() as context:
        context.prec = 2
        assert parse_to_cents(text) == expected


@pytest.mark.parametrize('text', ['NaN','sNaN','Infinity','-Infinity','1.001','1e-10000',
    '1e10000','92233720368547758.08','-92233720368547758.09','1,000.00','$50','', 'abc'])
def test_invalid_amounts(text):
    with pytest.raises(ValueError):
        parse_to_cents(text)


@pytest.mark.parametrize('value', [50.0, 50, True, None])
def test_non_text_amounts(value):
    with pytest.raises(TypeError):
        parse_to_cents(value)


def test_registration_and_exact_retry(temp_db_conn):
    ledger = Ledger(temp_db_conn)
    assert ledger.register_transactions([record()])['transactions_created'] == 1
    assert ledger.register_transactions([record()])['skipped_reingestion'] == 1
    assert temp_db_conn.execute('SELECT amount_cents,eligibility_state FROM allocations').fetchall() == [(5000,'REVIEW_REQUIRED')]
    assert temp_db_conn.execute('SELECT count(*) FROM transaction_provenance').fetchone()[0] == 1


def test_overlap_links_new_provenance_not_new_event(temp_db_conn):
    ledger = Ledger(temp_db_conn)
    ledger.register_transactions([record()])
    result = ledger.register_transactions([record(file_hash='a'*64)])
    assert result['provenance_linked'] == 1
    assert result['transactions_created'] == 0
    assert temp_db_conn.execute('SELECT count(*) FROM transactions').fetchone()[0] == 1
    assert temp_db_conn.execute('SELECT count(*) FROM allocations').fetchone()[0] == 1
    assert temp_db_conn.execute('SELECT count(*) FROM transaction_provenance').fetchone()[0] == 2


def test_identical_purchases_without_reference(temp_db_conn):
    ledger = Ledger(temp_db_conn)
    rows = [record(bank_reference='',row_number=2), record(bank_reference='',row_number=3)]
    assert ledger.register_transactions(rows)['transactions_created'] == 2
    assert ledger.register_transactions(rows)['skipped_reingestion'] == 2


@pytest.mark.parametrize('change', [dict(amount_str='50.01'),dict(currency='EUR'),dict(original_date='2026-08-13')])
def test_conflict_bank_identity_rolls_back_whole_batch(temp_db_conn, change):
    ledger = Ledger(temp_db_conn)
    ledger.register_transactions([record()])
    with pytest.raises(ValueError, match='Colisión'):
        ledger.register_transactions([record(bank_reference='NEW',row_number=3), record(file_hash='b'*64,**change)])
    assert temp_db_conn.execute('SELECT count(*) FROM transactions').fetchone()[0] == 1
    assert temp_db_conn.execute('SELECT count(*) FROM transaction_provenance').fetchone()[0] == 1


def test_provenance_conflict_not_silently_skipped(temp_db_conn):
    ledger = Ledger(temp_db_conn)
    ledger.register_transactions([record()])
    with pytest.raises(ValueError,match='procedencia'):
        ledger.register_transactions([record(account_ref='other')])


def test_invalid_second_record_rolls_back_first(temp_db_conn):
    with pytest.raises(ValueError):
        Ledger(temp_db_conn).register_transactions([record(),record(row_number=3,amount_str='1.001')])
    assert temp_db_conn.execute('SELECT count(*) FROM transactions').fetchone()[0] == 0
    assert temp_db_conn.execute('SELECT count(*) FROM allocations').fetchone()[0] == 0


def test_negative_and_zero_coverage(temp_db_conn):
    Ledger(temp_db_conn).register_transactions([record(amount_str='-25',bank_reference='NEG'),record(amount_str='0',bank_reference='ZERO',row_number=3)])
    assert temp_db_conn.execute('SELECT amount_cents FROM allocations ORDER BY id').fetchall() == [(-2500,),(0,)]


def test_provenance_integrity(temp_db_conn):
    item = record()
    item['provenance_hash'] = '0'*64
    with pytest.raises(ValueError):
        Ledger(temp_db_conn).register_transactions([item])
    item['provenance_hash'] = calculate_provenance_hash(item['file_hash'],item['sheet_name'],item['row_number'])
    Ledger(temp_db_conn).register_transactions([item])
    for sql in ["DELETE FROM transaction_provenance", "UPDATE transaction_provenance SET sheet_name='changed'"]:
        with pytest.raises(sqlite3.IntegrityError):
            temp_db_conn.execute(sql)


def test_unique_bank_index_enforced(temp_db_conn):
    Ledger(temp_db_conn).register_transactions([record()])
    with pytest.raises(sqlite3.IntegrityError):
        temp_db_conn.execute("INSERT INTO transactions(bank_id,account_ref,bank_reference,provenance_hash,amount_cents,currency,original_date) VALUES ('AMEX','1234',' REF1 ','different',6000,'USD','2026-08-12')")


def test_legacy_duplicate_migration_aborts(tmp_path):
    directory = tmp_path/'migrations'
    directory.mkdir()
    for path in sorted(DEFAULT_MIGRATIONS.glob('*.sql'))[:3]:
        shutil.copy(path,directory/path.name)
    db = tmp_path/'legacy.db'
    migrate(db,directory)
    conn = connect(db)
    try:
        for provenance in ['old1','old2']:
            conn.execute("INSERT INTO transactions(bank_id,account_ref,bank_reference,provenance_hash,amount_cents,currency,original_date) VALUES ('AMEX','1234','REF1',?,5000,'USD','2026-08-12')",(provenance,))
        path = DEFAULT_MIGRATIONS/'0004_decouple_provenance_identity.sql'
        shutil.copy(path,directory/path.name)
        with pytest.raises(MigrationError):
            migrate(db,directory)
        assert conn.execute('SELECT count(*) FROM transactions').fetchone()[0] == 2
        assert conn.execute('SELECT max(version) FROM schema_migrations').fetchone()[0] == 3
        assert conn.execute("SELECT name FROM sqlite_schema WHERE name='transaction_provenance'").fetchall() == []
    finally:
        conn.close()
