import csv
import json
import pytest
import scripts.pipeline_simulator as simulator
from scripts.persistence.ledger import connect


@pytest.fixture
def environment(tmp_path):
    root=simulator.initialize_simulation(tmp_path/'simulation')
    records=[dict(bank_id='AMEX',account_ref='TEST',bank_reference=str(i),amount_str='50',currency='USD',original_date='2026-08-12',file_hash='a'*64,sheet_name='CSV',row_number=i) for i in range(1,7)]
    def run(rules=None,raw=None):
        return simulator.run_pipeline_simulation(root/'ledger.db', records if raw is None else raw,[],rules or {},root/'inbox',root/'processed',root/'failed',root/'exports')
    return root,records,run


def classification():
    return dict(property_code='P1',vendor_name='Vendor',gl_account='6435',cash_account='1150',description='Test')


def write_decisions(path,ids,invalid=None):
    fields=['decision_id','exception_id','allocation_id','action','corrected_fields_json','reason','reviewed_by']
    with path.open('w',encoding='utf-8-sig',newline='') as f:
        writer=csv.DictWriter(f,fieldnames=fields); writer.writeheader()
        for i in ids:
            writer.writerow(dict(decision_id=f'D{i}',exception_id=f'reconciliation_{i}',allocation_id=i,action='CLASSIFY',corrected_fields_json=json.dumps(classification()),reason='' if i==invalid else 'verified',reviewed_by='tester'))


def test_full_cycle_repeat(environment):
    root,records,run=environment
    rules={i:classification() for i in range(1,7)}
    first=run(rules)
    assert first['coverage_audit']['valid'] and first['export']['exported_items']==6
    second=run(rules)
    assert second['ingestion']['skipped_reingestion']==6
    assert second['export']['batches']==[]
    assert len(list((root/'exports').glob('*.csv')))==1
    assert json.loads(open(second['report_path'],encoding='utf-8').read())['simulation']


def test_inbox_stops_file_continues_next(environment):
    root,records,run=environment
    write_decisions(root/'inbox'/'a.csv',range(1,6),invalid=3)
    write_decisions(root/'inbox'/'b.csv',[6])
    report=run()
    assert report['inbox']['decisions_applied']==3
    assert report['inbox']['files_failed']==1 and report['inbox']['files_processed']==1
    assert report['allocations_state']=={'READY_TO_IMPORT':3,'REVIEW_REQUIRED':3}
    assert report['export']['exported_items']==3


def test_recover_reserved_after_publication(environment,monkeypatch):
    root,records,run=environment
    original=simulator.publish_reserved_batch
    def interrupted(ledger,batch,folder):
        # Simulate failure after publication but before SQLite commit.
        from scripts.output.snapshot_csv import snapshot_csv_bytes
        (folder/f'appfolio_{batch}.csv').write_bytes(snapshot_csv_bytes(ledger.get_snapshot(batch)))
        raise OSError('interrupted')
    monkeypatch.setattr(simulator,'publish_reserved_batch',interrupted)
    with pytest.raises(OSError): run({1:classification()})
    failed=[json.loads(p.read_text(encoding='utf-8')) for p in root.glob('simulation_report_*.json')]
    assert failed[0]['status']=='FAILED'
    monkeypatch.setattr(simulator,'publish_reserved_batch',original)
    report=run({1:classification()})
    assert report['export']['batches'][0]['recovered']
    assert len(list((root/'exports').glob('*.csv')))==1


@pytest.mark.parametrize('damage',['missing','sum','sign'])
def test_coverage_aborts_export(environment,damage):
    root,records,run=environment
    run()
    conn=connect(root/'ledger.db')
    try:
        if damage=='missing':
            conn.execute("INSERT INTO transactions(bank_id,account_ref,bank_reference,provenance_hash,amount_cents,currency,original_date) VALUES ('AMEX','TEST','MISSING','missing',0,'USD','2026-08-12')")
        elif damage=='sum': conn.execute('UPDATE allocations SET amount_cents=4999 WHERE id=1')
        else:
            conn.execute('UPDATE allocations SET amount_cents=6000 WHERE id=1')
            conn.execute("INSERT INTO allocations(transaction_id,amount_cents,eligibility_state) VALUES (1,-1000,'REVIEW_REQUIRED')")
    finally: conn.close()
    with pytest.raises(ValueError,match='Cobertura'): run()
    assert list((root/'exports').glob('*.csv'))==[]


def test_invalid_ingestion_audited(environment):
    root,records,run=environment
    records[1]['amount_str']='1.001'
    with pytest.raises(ValueError): run(raw=records)
    conn=connect(root/'ledger.db')
    try: assert conn.execute('SELECT count(*) FROM transactions').fetchone()[0]==0
    finally: conn.close()
    report=json.loads(next(root.glob('simulation_report_*.json')).read_text(encoding='utf-8'))
    assert report['stage']=='ingestion' and report['status']=='FAILED'


def test_refuse_unmarked_existing_database(tmp_path):
    db=tmp_path/'ledger.db'; db.write_bytes(b'production sentinel')
    with pytest.raises(ValueError):
        simulator.run_pipeline_simulation(db,[],[],{},*(tmp_path/x for x in ['inbox','processed','failed','exports']))
    assert db.read_bytes()==b'production sentinel'


def test_refuse_external_inbox(environment,tmp_path):
    root,_,_=environment
    external=tmp_path/'external'; external.mkdir()
    with pytest.raises(ValueError):
        simulator.run_pipeline_simulation(root/'ledger.db',[],[],{},external,root/'processed',root/'failed',root/'exports')
    assert not (root/'ledger.db').exists()
