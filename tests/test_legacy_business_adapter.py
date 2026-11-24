from types import SimpleNamespace
from scripts.ingestion.legacy_business_adapter import apply_legacy_business_rules


def rules(groups=None):
    return SimpleNamespace(property_groups=groups or {},vendor_to_gl={'VENDOR':'6435'},cash_accounts={'amex':'1170'},
        evaluate_row_alerts=lambda r:('OK',''),resolve_vendor=lambda m:('Vendor',100,'manual_override'),
        resolve_property=lambda *a,**k:('GROUP',100,'excel_rule'),resolve_gl=lambda v:'6435',resolve_cash_account=lambda c:'1170')


def adapted(rows):
    return dict(records=[dict(provenance_hash=str(i),row_number=i+2,amount_str=amount,original_date='2026-08-12') for i,(name,company,amount) in enumerate(rows)],
        contexts=[dict(provenance_hash=str(i),account_holder=name,company=company,gl_hint='',merchant='Store') for i,(name,company,amount) in enumerate(rows)])


def test_legacy_filter_and_signed_totals():
    data=adapted([('ARMANDO ARMAS','','10'),('RICHARD LIBUTTI','HAPPY TRAILERS','20'),('Other','RAS','-5'),('Other','Else','3')])
    result=apply_legacy_business_rules(data,rules())
    assert result['summary']['included_rows']==2
    assert result['summary']['included_total_cents']==500
    assert result['summary']['excluded_total_cents']==2300
    assert result['summary']['control_valid']


def test_proration_preserves_centavos():
    result=apply_legacy_business_rules(adapted([('ARMANDO ARMAS','','10')]),rules({'GROUP':[('P1',1),('P2',1),('P3',1)]}))
    assert [f['amount_cents'] for f in result['details'][0]['fractions']]==[333,333,334]


def test_default_gl_flag_and_netting_keeps_events():
    manager=rules(); manager.vendor_to_gl={}
    result=apply_legacy_business_rules(adapted([('ARMANDO ARMAS','','10'),('ARMANDO ARMAS','','-10')]),manager)
    assert len(result['records'])==2 and len(result['netting'])==1
    assert all('DEFAULT_GL' in d['flags'] for d in result['details'])

def test_persisted_proration_idempotent_and_rollback(temp_db_conn):
    import pytest
    from scripts.persistence.ledger import Ledger,calculate_provenance_hash
    ledger=Ledger(temp_db_conn)
    record=dict(bank_id='AMEX',account_ref='AMEX',bank_reference='',amount_str='10',currency='USD',original_date='2026-08-12',file_hash='a'*64,sheet_name='CSV',row_number=2)
    ledger.register_transactions([record])
    detail=dict(provenance_hash=calculate_provenance_hash('a'*64,'CSV',2),vendor_name='Vendor',gl_account='6435',cash_account='1170',merchant_original='Original',fractions=[dict(property_code='P1',amount_cents=1200),dict(property_code='P2',amount_cents=-200)])
    with pytest.raises(ValueError): ledger.apply_business_allocation_plan([detail])
    assert temp_db_conn.execute('SELECT amount_cents FROM allocations').fetchall()==[(1000,)]
    detail['fractions']=[dict(property_code='P1',amount_cents=333),dict(property_code='P2',amount_cents=667)]
    ledger.apply_business_allocation_plan([detail]); ledger.apply_business_allocation_plan([detail])
    assert temp_db_conn.execute('SELECT amount_cents,eligibility_state FROM allocations ORDER BY id').fetchall()==[(333,'REVIEW_REQUIRED'),(667,'REVIEW_REQUIRED')]
    detail['gl_account']='other'
    with pytest.raises(ValueError): ledger.apply_business_allocation_plan([detail])
