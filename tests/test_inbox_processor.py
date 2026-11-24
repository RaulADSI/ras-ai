import csv
import json
import sqlite3
from pathlib import Path
import pytest
from scripts.persistence.ledger import Ledger
from scripts.reconciliation.inbox_processor import process_inbox


@pytest.fixture
def review(temp_db_conn,tmp_path):
    ledger=Ledger(temp_db_conn)
    ledger.register_transactions([dict(bank_id='AMEX',account_ref='1',bank_reference=str(i),amount_str='50',currency='USD',original_date='2026-08-12',file_hash='a'*64,sheet_name='CSV',row_number=i) for i in (1,2)])
    for i in (1,2): ledger.create_exception(i,f'E{i}','incomplete',{})
    directories=[tmp_path/name for name in ('inbox','processed','failed')]
    directories[0].mkdir()
    return ledger,temp_db_conn,directories


def decision(**changes):
    result=dict(decision_id='D1',exception_id='E1',allocation_id=1,action='CLASSIFY',
        corrected_fields=dict(property_code='P1',vendor_name='Vendor',gl_account='6435',cash_account='1150',description='Repair'),reason='Verified',reviewed_by='Analyst',exception_version=1)
    result.update(changes)
    return result


def write_csv(path,rows):
    data=[]
    for row in rows:
        row=dict(row)
        row['corrected_fields_json']=json.dumps(row.pop('corrected_fields'))
        data.append(row)
    with path.open('w',encoding='utf-8-sig',newline='') as stream:
        writer=csv.DictWriter(stream,fieldnames=list(data[0]))
        writer.writeheader(); writer.writerows(data)


def test_csv_classify_link_and_retry(review):
    ledger,conn,dirs=review
    rows=[decision(),decision(decision_id='D2',exception_id='E2',allocation_id=2,action='LINK_INVOICE',corrected_fields={'appfolio_invoice_id':'INV1'})]
    write_csv(dirs[0]/'review.csv',rows)
    result=process_inbox(ledger,*dirs)
    assert result['decisions_applied']==2 and result['files_processed']==1
    assert conn.execute('SELECT eligibility_state FROM allocations ORDER BY id').fetchall()==[('READY_TO_IMPORT',),('MATCHED_EXISTING',)]
    write_csv(dirs[0]/'review.csv',rows)
    result=process_inbox(ledger,*dirs)
    assert result['decisions_skipped']==2
    assert conn.execute('SELECT count(*) FROM exceptions_decisions').fetchone()[0]==2
    assert len(list(dirs[1].glob('*.csv')))==2


@pytest.mark.parametrize('change',[dict(reason='different'),dict(reviewed_by='Other'),dict(exception_version=2),dict(action='LINK_INVOICE',corrected_fields={'appfolio_invoice_id':'I2'})])
def test_collision(review,change):
    ledger,conn,_=review
    ledger.apply_decision(**decision())
    with pytest.raises(ValueError,match='Colisión'):
        ledger.apply_decision(**decision(**change))
    assert conn.execute('SELECT count(*) FROM exceptions_decisions').fetchone()[0]==1


@pytest.mark.parametrize('change',[dict(reason=''),dict(reviewed_by=' '),dict(exception_id='E2'),dict(exception_version=2),dict(action='DELETE'),dict(corrected_fields={'amount_cents':1})])
def test_invalid_decision_no_effects(review,change):
    ledger,conn,_=review
    with pytest.raises((ValueError,TypeError)):
        ledger.apply_decision(**decision(**change))
    assert conn.execute('SELECT count(*) FROM exceptions_decisions').fetchone()[0]==0
    assert conn.execute('SELECT status FROM exceptions WHERE exception_id="E1"').fetchone()[0]=='PENDING'


def test_invoice_consumed_rolls_back(review):
    ledger,conn,_=review
    ledger.match_invoice(2,'INV',{})
    with pytest.raises(sqlite3.IntegrityError):
        ledger.apply_decision(**decision(action='LINK_INVOICE',corrected_fields={'appfolio_invoice_id':'INV'}))
    assert conn.execute('SELECT eligibility_state FROM allocations WHERE id=1').fetchone()[0]=='REVIEW_REQUIRED'
    assert conn.execute('SELECT count(*) FROM exceptions_decisions').fetchone()[0]==0


def test_audit_failure_rolls_back_decision(review):
    ledger,conn,_=review
    conn.execute("CREATE TRIGGER fail BEFORE INSERT ON audit_events BEGIN SELECT RAISE(ABORT,'fail'); END;")
    with pytest.raises(sqlite3.IntegrityError): ledger.apply_decision(**decision())
    assert conn.execute('SELECT eligibility_state FROM allocations WHERE id=1').fetchone()[0]=='REVIEW_REQUIRED'
    assert conn.execute('SELECT count(*) FROM exceptions_decisions').fetchone()[0]==0
    assert conn.execute('SELECT status FROM exceptions WHERE exception_id="E1"').fetchone()[0]=='PENDING'


def test_partial_file_failure_and_recovery(review):
    ledger,conn,dirs=review
    rows=[decision(),decision(decision_id='D2',allocation_id=2,exception_id='E2',reason='')]
    write_csv(dirs[0]/'partial.csv',rows)
    result=process_inbox(ledger,*dirs)
    assert result['files_failed']==1 and result['decisions_applied']==1
    assert len(list(dirs[2].glob('*.csv')))==1
    rows[1]['reason']='fixed'
    write_csv(dirs[0]/'partial.csv',rows)
    result=process_inbox(ledger,*dirs)
    assert result['decisions_applied']==1 and result['decisions_skipped']==1


def test_archive_failure_retries_without_reapplying(review,monkeypatch):
    ledger,conn,dirs=review
    write_csv(dirs[0]/'review.csv',[decision()])
    original=Path.rename
    def fail(*args): raise OSError('archive unavailable')
    monkeypatch.setattr(Path,'rename',fail)
    result=process_inbox(ledger,*dirs)
    assert result['archive_errors']==1 and result['decisions_applied']==1
    assert (dirs[0]/'review.csv').exists()
    monkeypatch.setattr(Path,'rename',original)
    result=process_inbox(ledger,*dirs)
    assert result['files_processed']==1 and result['decisions_skipped']==1


def test_bad_schema_isolated_and_other_file_continues(review):
    ledger,conn,dirs=review
    (dirs[0]/'a_bad.csv').write_text('wrong\nvalue\n')
    write_csv(dirs[0]/'b_good.csv',[decision()])
    result=process_inbox(ledger,*dirs)
    assert result['files_failed']==1 and result['files_processed']==1


def test_stale_version_and_immutable_decisions(review):
    ledger,conn,_=review
    ledger.create_exception(1,'E1','new evidence',{})
    with pytest.raises(ValueError,match='obsoleta'): ledger.apply_decision(**decision())
    ledger.apply_decision(**decision(exception_version=2))
    for sql in ['DELETE FROM exceptions_decisions',"UPDATE exceptions_decisions SET reason='modified'"]:
        with pytest.raises(sqlite3.IntegrityError): conn.execute(sql)
