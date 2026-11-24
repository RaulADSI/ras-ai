import pytest
import sqlite3
from scripts.persistence.ledger import Ledger
from scripts.reconciliation.engine import ReconciliationEngine


def seed(conn, count=1):
    ledger = Ledger(conn)
    ledger.register_transactions([dict(bank_id='AMEX',account_ref='1234',bank_reference=f'R{i}',
        amount_str='50',currency='USD',original_date='2026-08-12',file_hash='a'*64,sheet_name='CSV',row_number=i)
        for i in range(1,count+1)])
    return ledger


def classification(**changes):
    result = dict(property_code='P1',vendor_name='Home Depot',gl_account='6435',cash_account='1150',description='Repair')
    result.update(changes)
    return result


def invoice(**changes):
    result = dict(invoice_id='I1',amount_cents=5000,currency='USD',date='2026-08-12',vendor_name='HOME DEPOT')
    result.update(changes)
    return result


def test_match_and_retry(temp_db_conn):
    ledger=seed(temp_db_conn)
    engine=ReconciliationEngine(ledger)
    assert engine.run([invoice()],{1:classification()}) == {1:'MATCHED_EXISTING'}
    assert engine.run([invoice()],{1:classification()}) == {}
    assert ledger.reserve_batch() == (None,())
    assert temp_db_conn.execute('SELECT count(*) FROM invoice_matches').fetchone()[0] == 1
    assert temp_db_conn.execute("SELECT event_type FROM audit_events").fetchone()[0] == 'MATCHED_EXISTING'


def test_new_valid_and_incomplete(temp_db_conn):
    ledger=seed(temp_db_conn,2)
    result=ReconciliationEngine(ledger).run([],{1:classification(),2:classification(property_code='REVISAR PROP: X')})
    assert result == {1:'READY_TO_IMPORT',2:'REVIEW_REQUIRED'}
    assert len(ledger.reserve_batch()[1]) == 1
    assert temp_db_conn.execute('SELECT status,reason FROM exceptions').fetchone() == ('PENDING','INCOMPLETE_CLASSIFICATION')


@pytest.mark.parametrize('change',[dict(amount_cents=-5000),dict(amount_cents=5001),dict(currency='EUR'),dict(date='2026-08-18')])
def test_strict_amount_currency_window(temp_db_conn,change):
    ledger=seed(temp_db_conn)
    assert ReconciliationEngine(ledger).run([invoice(**change)],{1:classification()}) == {1:'READY_TO_IMPORT'}


def test_multiple_invoices_review(temp_db_conn):
    ledger=seed(temp_db_conn)
    assert ReconciliationEngine(ledger).run([invoice(),invoice(invoice_id='I2')],{1:classification()}) == {1:'REVIEW_REQUIRED'}
    assert ledger.consumed_invoice_ids() == set()


def test_two_charges_one_invoice_review(temp_db_conn):
    ledger=seed(temp_db_conn,2)
    assert ReconciliationEngine(ledger).run([invoice()],{1:classification(),2:classification()}) == {1:'REVIEW_REQUIRED',2:'REVIEW_REQUIRED'}
    assert ledger.consumed_invoice_ids() == set()


def test_consumed_invoice_and_unique_rollback(temp_db_conn):
    ledger=seed(temp_db_conn,2)
    ledger.match_invoice(1,'I1',{'method':'manual_verified'})
    with pytest.raises(sqlite3.IntegrityError):
        ledger.match_invoice(2,'I1',{})
    assert temp_db_conn.execute('SELECT eligibility_state FROM allocations WHERE id=2').fetchone()[0] == 'REVIEW_REQUIRED'
    assert ReconciliationEngine(ledger).run([invoice()],{2:classification()}) == {2:'REVIEW_REQUIRED'}


def test_reference_conflict(temp_db_conn):
    ledger=seed(temp_db_conn)
    assert ReconciliationEngine(ledger).run([invoice(bank_reference='OTHER')],{1:classification()}) == {1:'REVIEW_REQUIRED'}


def test_fuzzy_requires_opt_in(temp_db_conn):
    ledger=seed(temp_db_conn)
    engine=ReconciliationEngine(ledger)
    assert engine.run([invoice(vendor_name='Home Depott')],{1:classification()}) == {1:'REVIEW_REQUIRED'}
    assert ReconciliationEngine(ledger,allow_fuzzy=True).run([invoice(vendor_name='Home Depott')],{1:classification()}) == {1:'MATCHED_EXISTING'}
    assert temp_db_conn.execute('SELECT status FROM exceptions').fetchone()[0] == 'RESOLVED'


def test_low_confidence(temp_db_conn):
    ledger=seed(temp_db_conn)
    assert ReconciliationEngine(ledger).run([],{1:classification(confidence=75)}) == {1:'REVIEW_REQUIRED'}


def test_exception_versions_and_collision(temp_db_conn):
    ledger=seed(temp_db_conn,2)
    ledger.create_exception(1,'E1','missing',{})
    ledger.create_exception(1,'E1','missing',{})
    assert temp_db_conn.execute('SELECT version FROM exceptions').fetchone()[0] == 1
    ledger.create_exception(1,'E1','changed',{'score':50})
    assert temp_db_conn.execute('SELECT version FROM exceptions').fetchone()[0] == 2
    with pytest.raises(ValueError):
        ledger.create_exception(2,'E1','wrong',{})
    ledger.classify_allocation(1,**classification())
    assert temp_db_conn.execute('SELECT status FROM exceptions').fetchone()[0] == 'RESOLVED'


def test_reserved_immutable_and_invalid_id(temp_db_conn):
    ledger=seed(temp_db_conn)
    ledger.classify_allocation(1,**classification())
    ledger.reserve_batch()
    for action in [lambda:ledger.match_invoice(1,'I1',{}),lambda:ledger.create_exception(1,'E1','reason',{}),lambda:ledger.classify_allocation(1,**classification()),lambda:ledger.match_invoice(999,'I2',{})]:
        with pytest.raises(ValueError): action()


def test_invalid_invoice_batch_no_changes(temp_db_conn):
    ledger=seed(temp_db_conn)
    with pytest.raises(ValueError):
        ReconciliationEngine(ledger).run([invoice(),invoice()],{1:classification()})
    assert ledger.consumed_invoice_ids()==set()
    assert temp_db_conn.execute('SELECT count(*) FROM audit_events').fetchone()[0]==0

def test_audit_failure_rolls_back_match(temp_db_conn):
    ledger=seed(temp_db_conn)
    temp_db_conn.execute("CREATE TRIGGER fail_audit BEFORE INSERT ON audit_events BEGIN SELECT RAISE(ABORT,'audit unavailable'); END;")
    with pytest.raises(sqlite3.IntegrityError):
        ledger.match_invoice(1,'I1',{})
    assert ledger.consumed_invoice_ids()==set()
    assert temp_db_conn.execute('SELECT eligibility_state FROM allocations').fetchone()[0]=='REVIEW_REQUIRED'


def test_ingestion_to_reconciliation_to_csv(temp_db_conn,tmp_path):
    import csv
    from scripts.output.bulk_bill_generator import publish_reserved_batch
    ledger=seed(temp_db_conn)
    ReconciliationEngine(ledger).run([],{1:classification()})
    batch,snapshot=ledger.reserve_batch()
    path=publish_reserved_batch(ledger,batch,tmp_path/'exports')
    with path.open(encoding='utf-8-sig',newline='') as stream:
        rows=list(csv.DictReader(stream))
    assert len(rows)==1 and rows[0]['Amount*']=='50.00'
    assert temp_db_conn.execute('SELECT delivery_state FROM export_items').fetchone()[0]=='EXPORTED'


def test_negative_invoice_and_inclusive_window(temp_db_conn):
    ledger=seed(temp_db_conn)
    temp_db_conn.execute('UPDATE transactions SET amount_cents=-5000')
    temp_db_conn.execute('UPDATE allocations SET amount_cents=-5000')
    result=ReconciliationEngine(ledger).run([invoice(amount_cents=-5000,date='2026-08-17')],{1:classification()})
    assert result=={1:'MATCHED_EXISTING'}


@pytest.mark.parametrize('merchant,variant', [
    ('SYKES ACE HARDWARE 0MIAMI FL', 'Sykes Ace Hardware'),
    ('ACE HDWE OF OPA LOCKA OPA LOCKA FL', 'Ace Hardware of Opa Locka'),
])
def test_explicit_ace_alias_preserves_identity(temp_db_conn, merchant, variant):
    from scripts.vendor_aliases import lookup_vendor_alias
    identity = lookup_vendor_alias(merchant)
    assert identity.merchant_raw == merchant
    assert identity.merchant_variant == variant
    ledger = seed(temp_db_conn)
    assert ReconciliationEngine(ledger).run(
        [invoice(vendor_name='Hardware, ACE')],
        {1: classification(vendor_name=merchant)},
    ) == {1: 'MATCHED_EXISTING'}
    evidence = str(temp_db_conn.execute('SELECT * FROM invoice_matches').fetchall())
    assert variant in evidence
    assert merchant in evidence
    assert 'explicit_alias' in evidence


def test_ace_alias_does_not_guess():
    from scripts.vendor_aliases import lookup_vendor_alias
    for merchant in ('ACE', 'PALACE HARDWARE', 'OTHER ACE HARDWARE', 'SYKES ACE HARDWARE UNKNOWN'):
        assert lookup_vendor_alias(merchant) is None


def test_ace_alias_multiple_invoices_stays_review(temp_db_conn):
    ledger = seed(temp_db_conn)
    assert ReconciliationEngine(ledger).run(
        [invoice(vendor_name='Hardware, ACE'), invoice(invoice_id='I2', vendor_name='Hardware, ACE')],
        {1: classification(vendor_name='SYKES ACE HARDWARE')},
    ) == {1: 'REVIEW_REQUIRED'}


def test_bill_date_matches_when_payment_is_later(temp_db_conn):
    ledger = seed(temp_db_conn)
    assert ReconciliationEngine(ledger).run(
        [invoice(date='2026-09-10', bill_date='2026-08-11', source_file_hash='x')],
        {1: classification()},
    ) == {1: 'MATCHED_EXISTING'}


def test_ledger_candidate_outside_window_blocks_bulk(temp_db_conn):
    ledger = seed(temp_db_conn)
    assert ReconciliationEngine(ledger).run(
        [invoice(date='2026-09-10', bill_date='2026-08-19', source_file_hash='x')],
        {1: classification()},
    ) == {1: 'REVIEW_REQUIRED'}
    assert ledger.reserve_batch() == (None, ())
    assert temp_db_conn.execute('SELECT reason FROM exceptions').fetchone()[0] == 'EXISTING_INVOICE_DATE_DISCREPANCY'
