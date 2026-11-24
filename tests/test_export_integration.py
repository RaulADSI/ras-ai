import sqlite3
import pytest
from scripts.persistence.ledger import Ledger, connect
from scripts.output.bulk_bill_generator import publish_reserved_batch
from test_ledger_transitions import seeded_ledger


def state(conn):
    return conn.execute('SELECT delivery_state FROM export_items').fetchone()[0]


def test_missing_or_corrupt_file_retains_reservation(seeded_ledger, tmp_path):
    ledger, conn = seeded_ledger
    batch, snapshot = ledger.reserve_batch()
    path = tmp_path / 'bad.csv'
    with pytest.raises(FileNotFoundError):
        ledger.mark_exported(batch, path, snapshot)
    path.write_text('not a valid bill')
    with pytest.raises(ValueError):
        ledger.mark_exported(batch, path, snapshot)
    assert state(conn) == 'RESERVED'
    assert conn.execute('SELECT file_hash FROM export_batches').fetchone()[0] is None


def test_equal_total_tampering_and_wrong_snapshot(seeded_ledger, tmp_path):
    ledger, conn = seeded_ledger
    batch, snapshot = ledger.reserve_batch()
    path = publish_reserved_batch(ledger, batch, tmp_path)
    path.write_bytes(path.read_bytes().replace(b'P1', b'P9'))
    with pytest.raises(ValueError):
        ledger.mark_exported(batch, path, snapshot)
    with pytest.raises(ValueError):
        ledger.mark_exported(batch, path, (snapshot[0]._replace(allocation_id=999),))
    assert state(conn) == 'EXPORTED'


def test_recover_after_publication_before_commit(seeded_ledger, tmp_path, monkeypatch):
    ledger, conn = seeded_ledger
    batch, snapshot = ledger.reserve_batch()
    original = ledger.mark_exported
    def interrupted(*args):
        raise RuntimeError('simulated interruption')
    monkeypatch.setattr(ledger, 'mark_exported', interrupted)
    with pytest.raises(RuntimeError):
        publish_reserved_batch(ledger, batch, tmp_path / 'exports')
    assert state(conn) == 'RESERVED'
    database = conn.execute('PRAGMA database_list').fetchone()[2]
    other = connect(database)
    try:
        recovered = Ledger(other)
        path = publish_reserved_batch(recovered, batch, tmp_path / 'exports')
        assert recovered.get_snapshot(batch) == snapshot
        before = path.stat().st_mtime_ns
        assert publish_reserved_batch(recovered, batch, tmp_path / 'exports') == path
        assert path.stat().st_mtime_ns == before
        assert recovered.reserve_batch() == (None, ())
    finally:
        other.close()
    assert len(list((tmp_path / 'exports').glob('*.csv'))) == 1
    assert state(conn) == 'EXPORTED'


def test_publication_failure_preserves_reservation(seeded_ledger, tmp_path, monkeypatch):
    import os
    ledger, conn = seeded_ledger
    batch, _ = ledger.reserve_batch()
    def fail(*args):
        raise OSError('disk failure')
    monkeypatch.setattr(os, 'link', fail)
    with pytest.raises(OSError):
        publish_reserved_batch(ledger, batch, tmp_path / 'exports')
    assert state(conn) == 'RESERVED'
    assert list((tmp_path / 'exports').iterdir()) == []


def test_source_changes_do_not_change_reserved_content(seeded_ledger, tmp_path):
    ledger, conn = seeded_ledger
    batch, snapshot = ledger.reserve_batch()
    conn.execute("UPDATE allocations SET vendor_name='changed' WHERE id=1")
    path = publish_reserved_batch(ledger, batch, tmp_path)
    assert b'changed' not in path.read_bytes()
    assert ledger.get_snapshot(batch) == snapshot
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute("UPDATE export_items SET snapshot_json='{}'")


def test_invalid_coverage_aborts_reservation(seeded_ledger):
    ledger, conn = seeded_ledger
    conn.execute('UPDATE allocations SET amount_cents=1 WHERE id=2')
    with pytest.raises(ValueError):
        ledger.reserve_batch()
    assert conn.execute('SELECT count(*) FROM export_batches').fetchone()[0] == 0


def test_negative_and_zero_amounts(seeded_ledger, tmp_path):
    ledger, conn = seeded_ledger
    conn.execute('UPDATE transactions SET amount_cents=-6001')
    conn.execute('UPDATE allocations SET amount_cents=CASE id WHEN 1 THEN -6001 ELSE 0 END')
    batch, snapshot = ledger.reserve_batch()
    path = publish_reserved_batch(ledger, batch, tmp_path)
    assert b'-60.01' in path.read_bytes()


def test_outer_transaction_not_rolled_back(seeded_ledger):
    ledger, conn = seeded_ledger
    conn.execute('BEGIN')
    conn.execute("UPDATE allocations SET description='pending' WHERE id=1")
    with pytest.raises(ValueError):
        ledger.reserve_batch()
    assert conn.in_transaction
    assert conn.execute('SELECT description FROM allocations WHERE id=1').fetchone()[0] == 'pending'
    conn.rollback()

def test_database_failure_rolls_back_export_state(seeded_ledger, tmp_path):
    ledger, conn = seeded_ledger
    batch, snapshot = ledger.reserve_batch()
    conn.execute("CREATE TRIGGER reject_export BEFORE UPDATE OF file_hash ON export_batches BEGIN SELECT RAISE(ABORT,'simulated database failure'); END;")
    with pytest.raises(sqlite3.IntegrityError):
        publish_reserved_batch(ledger, batch, tmp_path)
    assert state(conn) == 'RESERVED'
    assert conn.execute('SELECT file_hash FROM export_batches').fetchone()[0] is None
    conn.execute('DROP TRIGGER reject_export')
    publish_reserved_batch(ledger, batch, tmp_path)
    assert state(conn) == 'EXPORTED'


def test_incomplete_classification_and_empty_selection(seeded_ledger):
    ledger, conn = seeded_ledger
    conn.execute("UPDATE allocations SET property_code='' WHERE id=1")
    with pytest.raises(ValueError):
        ledger.reserve_batch()
    assert conn.execute('SELECT count(*) FROM export_batches').fetchone()[0] == 0
    conn.execute("UPDATE allocations SET eligibility_state='REVIEW_REQUIRED'")
    assert ledger.reserve_batch() == (None, ())


def test_zero_csv_amount(seeded_ledger, tmp_path):
    ledger, conn = seeded_ledger
    conn.execute('UPDATE transactions SET amount_cents=0')
    conn.execute('UPDATE allocations SET amount_cents=0')
    batch, _ = ledger.reserve_batch()
    path = publish_reserved_batch(ledger, batch, tmp_path)
    import csv
    with path.open(encoding='utf-8-sig', newline='') as stream:
        assert next(csv.DictReader(stream))['Amount*'] == '0.00'
