import csv
import hashlib
import pytest
from scripts.ingestion.amex_ledger_adapter import read_annotated_amex_csv, ANNOTATED_HEADER
from scripts.persistence.ledger import Ledger


def source(tmp_path, rows, header=ANNOTATED_HEADER):
    path = tmp_path/'amex.csv'
    with path.open('w',encoding='utf-8-sig',newline='') as stream:
        writer=csv.writer(stream); writer.writerow(header); writer.writerows(rows)
    return path


def row(amount='12.34',day='08/14/2026',merchant='Vendor'):
    return [day,merchant,'Holder',amount,'Company','GL','','note','']


def test_exact_read_and_reingestion(tmp_path,temp_db_conn):
    path=source(tmp_path,[row(),row()])
    original=path.read_bytes()
    result=read_annotated_amex_csv(path,account_ref='AMEX',currency='USD')
    assert [r['row_number'] for r in result['records']]==[2,3]
    assert result['records'][0]['file_hash']==hashlib.sha256(original).hexdigest()
    assert result['audit']['accepted_total_cents']==2468
    ledger=Ledger(temp_db_conn)
    assert ledger.register_transactions(result['records'])['transactions_created']==2
    assert ledger.register_transactions(result['records'])['skipped_reingestion']==2
    assert path.read_bytes()==original


def test_invalids_annotations_and_signed_control(tmp_path):
    path=source(tmp_path,[row('-1.01'),row('2.00','02/30/2026'),row('1.001'),['']*8+['100.00'],['']*9])
    result=read_annotated_amex_csv(path,account_ref='AMEX',currency='USD')
    assert len(result['exceptions'])==2 and len(result['annotations'])==1
    audit=result['audit']
    assert audit['accepted_total_cents']==-101 and audit['rejected_total_cents']==200
    assert audit['parsed_total_cents']==99 and audit['totals_reconciled']
    assert not audit['complete_amount_control']


def test_multiline_preserves_physical_line(tmp_path):
    path=source(tmp_path,[row(merchant='Line one\nLine two'),row('0')])
    records=read_annotated_amex_csv(path,account_ref='AMEX',currency='USD')['records']
    assert [r['row_number'] for r in records]==[2,4]


@pytest.mark.parametrize('amount,valid',[('1,234.56',True),('12,34',False),('NaN',False)])
def test_currency_format(tmp_path,amount,valid):
    result=read_annotated_amex_csv(source(tmp_path,[row(amount)]),account_ref='AMEX',currency='USD')
    assert bool(result['records'])==valid


def test_wrong_schema_rejected(tmp_path):
    path=source(tmp_path,[row()],header=['Date','Amount'])
    with pytest.raises(ValueError): read_annotated_amex_csv(path,account_ref='AMEX',currency='USD')
