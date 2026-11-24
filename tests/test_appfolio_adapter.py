import csv
from scripts.reconciliation.appfolio_adapter import read_appfolio_ledger
from scripts.reconciliation.engine import ReconciliationEngine
from test_reconciliation_engine import seed,classification,invoice


def test_group_reference_and_keep_orphans(tmp_path):
    path=tmp_path/'ledger.csv'
    fields=['Reference','Bill Date','Payee Name','Paid','Unpaid','Bank Account','Property']
    with path.open('w',newline='') as f:
        writer=csv.writer(f);writer.writerow(fields)
        writer.writerows([['R','08/12/2026','Vendor','10','5',' Amex ','P1'],['R','08/12/2026','Vendor','20','0','AMEX','P2'],['','08/12/2026','Vendor','7','0','Amex','P1'],['','08/12/2026','Vendor','7','0','Amex','P2'],['R','08/12/2026','Vendor','35','0','Operating Account','P1'],['Subtotal','','','77','0','','']])
    data=read_appfolio_ledger(path)
    assert len(data['invoices'])==3
    grouped=next(i for i in data['invoices'] if i['reference'])
    assert grouped['amount_cents']==3500 and len(grouped['source_lines'])==2
    assert len(data['other_accounts'])==1 and data['other_accounts'][0]['review_only']
    assert data['audit']['ignored_non_transaction_rows']==1


def test_other_account_is_review_only(temp_db_conn):
    ledger=seed(temp_db_conn)
    result=ReconciliationEngine(ledger).run([invoice(review_only=True,bank_account='Operating Account')],{1:classification()})
    assert result=={1:'REVIEW_REQUIRED'}
    assert ledger.consumed_invoice_ids()==set()
    assert temp_db_conn.execute('SELECT reason FROM exceptions').fetchone()[0]=='POSSIBLE_OTHER_ACCOUNT_MATCH'


def test_orphan_unique_candidate_matches(temp_db_conn):
    ledger=seed(temp_db_conn)
    result=ReconciliationEngine(ledger).run([invoice(identity_type='source_row',reference='')],{1:classification()})
    assert result=={1:'MATCHED_EXISTING'}


def test_orphan_tie_not_consumed(temp_db_conn):
    ledger=seed(temp_db_conn)
    result=ReconciliationEngine(ledger).run([invoice(identity_type='source_row'),invoice(invoice_id='I2',identity_type='source_row')],{1:classification()})
    assert result=={1:'REVIEW_REQUIRED'}
    assert not ledger.consumed_invoice_ids()


def test_paid_invoice_matches_payment_date_not_bill_date(tmp_path,temp_db_conn):
    path=tmp_path/'paid.csv'
    fields=['Reference','Bill Date','Payee Name','Paid','Unpaid','Bank Account','Property','Paid Date']
    with path.open('w',newline='') as f:
        writer=csv.writer(f);writer.writerow(fields)
        writer.writerow(['3750660W440','07/15/2026','Waste Connections of Florida','961.30','0.00','Amex','P1','07/28/2026'])
    data=read_appfolio_ledger(path)
    item=data['invoices'][0]
    assert item['date']=='2026-07-28' and item['bill_date']=='2026-07-15'
    ledger=seed(temp_db_conn)
    temp_db_conn.execute("UPDATE transactions SET original_date='2026-07-28',amount_cents=96130")
    temp_db_conn.execute('UPDATE allocations SET amount_cents=96130')
    result=ReconciliationEngine(ledger).run(data['invoices'],{1:classification(vendor_name='Waste Connections of Florida')})
    assert result=={1:'MATCHED_EXISTING'}
    assert ledger.reserve_batch()==(None,())
