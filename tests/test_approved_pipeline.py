import json
import pytest
from scripts import approved_pipeline as pipeline


def test_repeat_reuses_batch_and_changed_input_is_blocked(tmp_path, monkeypatch):
    raw=tmp_path/'raw'; raw.mkdir()
    (raw/'amex.csv').write_text('source')
    monkeypatch.setattr(pipeline, 'AMEX_RAW_DIR', raw)
    for name in ('VENDOR_LEDGER','MAPPING_RULES','ENTITY_DICTIONARY'):
        p=tmp_path/name; p.write_text('data'); monkeypatch.setattr(pipeline,name,p)
    (tmp_path/'confirmed_amex_decisions.json').write_text('{}')
    monkeypatch.setattr(pipeline,'MASTER_DIR',tmp_path)
    calls=[]
    def execute(*args):
        calls.append(1)
        return {'export':{'batches':[{'batch_id':'same-batch'}]}}
    monkeypatch.setattr(pipeline,'_execute',execute)
    monkeypatch.setattr(pipeline,'_publish',lambda report,root:report)
    first=pipeline.run_approved_pipeline(tmp_path/'state')
    second=pipeline.run_approved_pipeline(tmp_path/'state')
    assert len(calls)==1
    assert first['export']==second['export']
    assert second['reused_existing_batch']
    (raw/'amex.csv').write_text('changed')
    with pytest.raises(ValueError,match='archivos cambiaron'):
        pipeline.run_approved_pipeline(tmp_path/'state')
    assert len(calls)==1


def test_failed_run_cannot_create_second_batch(tmp_path,monkeypatch):
    raw=tmp_path/'raw'; raw.mkdir(); (raw/'amex.csv').write_text('source')
    monkeypatch.setattr(pipeline,'AMEX_RAW_DIR',raw)
    for name in ('VENDOR_LEDGER','MAPPING_RULES','ENTITY_DICTIONARY'):
        p=tmp_path/name; p.write_text('data'); monkeypatch.setattr(pipeline,name,p)
    (tmp_path/'confirmed_amex_decisions.json').write_text('{}')
    monkeypatch.setattr(pipeline,'MASTER_DIR',tmp_path)
    def fail(*args): raise RuntimeError('interrupted')
    monkeypatch.setattr(pipeline,'_execute',fail)
    with pytest.raises(RuntimeError): pipeline.run_approved_pipeline(tmp_path/'state')
    with pytest.raises(ValueError,match='interrumpida'):
        pipeline.run_approved_pipeline(tmp_path/'state')
