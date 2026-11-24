"""Approved AMEX workflow shared by both manual entry points.

A locked, persistent run registry prevents duplicate batches. Changed input after
publication is blocked pending reconciliation with previous exports.
"""
import csv
import hashlib
import json
import os
import shutil
import sqlite3
from pathlib import Path
from scripts.config import AMEX_RAW_DIR, VENDOR_LEDGER, MAPPING_RULES, ENTITY_DICTIONARY, MASTER_DIR, CLEAN_DIR, AUDIT_DIR
from scripts.vendor_aliases import lookup_vendor_alias
from scripts.pipeline_simulator import initialize_simulation, run_pipeline_simulation
from scripts.reconciliation.appfolio_adapter import read_appfolio_ledger
from scripts.ingestion.amex_ledger_adapter import read_annotated_amex_csv
from scripts.persistence.ledger import connect


def _execute(root, source_path, confirmations, fingerprint):
    from scripts.ingestion.legacy_business_adapter import apply_legacy_business_rules
    from scripts.rules_manager import RulesManager
    raw=read_annotated_amex_csv(source_path,account_ref='AMEX',currency='USD')
    assert raw['records'] and not raw['exceptions'], raw['audit']
    plan=apply_legacy_business_rules(raw,RulesManager())
    assert plan['records']
    for amount, provenance in confirmations.items():
     if not any(d['provenance_hash']==provenance and d['amount_cents']==int(amount) for d in plan['details']):
      raise ValueError('Las decisiones confirmadas no corresponden al archivo actual; requiere revisión')

    classes={};identities={}
    for i,detail in enumerate(plan['details'],1):
     rule=dict(property_code=detail['property_resolved'],vendor_name=detail['vendor_name'],gl_account=detail['gl_account'],cash_account=detail['cash_account'],description=detail['merchant_original'],confidence=detail['vendor_score'])
     if 'DEFAULT_GL' in detail['flags']: rule['gl_account']='DEFAULT_GL'
     if len(detail['fractions'])>1: rule['property_code']='REVISAR PROP: PRORATION_PENDING'
     alias=lookup_vendor_alias(detail['merchant_original'])
     if alias:
      rule['merchant_raw']=alias.merchant_raw
      identities[i]=dict(merchant_raw=alias.merchant_raw,merchant_variant=alias.merchant_variant,location=alias.location)
     classes[i]=rule
    app=read_appfolio_ledger(VENDOR_LEDGER)
    assert not app['invalid_rows'],app['invalid_rows']
    # Explicit user confirmation: invoice 81716 is valid despite its date discrepancy.
    import csv
    approved=[(i,d) for i,d in enumerate(plan['details'],1) if d['amount_cents']==17684 and d['date']=='2026-08-11' and lookup_vendor_alias(d['merchant_original'])]
    assert len(approved)==1
    allocation_id,detail=approved[0]
    invoice=[i for i in app['invoices'] if i['reference']=='81716' and i['amount_cents']==17684 and i['vendor_name']=='Hardware, ACE']
    assert len(invoice)==1
    with (root/'inbox/ace_81716_confirmation.csv').open('w',newline='',encoding='utf-8') as f:
     writer=csv.DictWriter(f,fieldnames=['decision_id','exception_id','allocation_id','action','corrected_fields_json','reason','reviewed_by','exception_version'])
     writer.writeheader()
     writer.writerow(dict(decision_id='user-confirmed-ace-81716-20260922',exception_id=f'reconciliation_{allocation_id}',allocation_id=allocation_id,action='LINK_INVOICE',corrected_fields_json=json.dumps({'appfolio_invoice_id':invoice[0]['invoice_id']}),reason='User confirmed invoice 81716 for USD 176.84 is valid; accepted seven-day date discrepancy. Nine Ace charges total USD 953.87.',reviewed_by='user_confirmation_in_conversation',exception_version=1))
    approved_waste=[(i,d) for i,d in enumerate(plan['details'],1) if d['amount_cents']==93911 and d['date']=='2026-07-28' and d['vendor_name']=='Waste Connections of Florida']
    assert len(approved_waste)==1
    waste_id,_=approved_waste[0]
    waste_invoice=[i for i in app['invoices'] if i['reference']=='3757860W440' and i['amount_cents']==93911 and i['date']=='2026-07-28']
    assert len(waste_invoice)==1
    with (root/'inbox/waste_3757860_confirmation.csv').open('w',newline='',encoding='utf-8') as f:
     writer=csv.DictWriter(f,fieldnames=['decision_id','exception_id','allocation_id','action','corrected_fields_json','reason','reviewed_by','exception_version'])
     writer.writeheader()
     writer.writerow(dict(decision_id='user-confirmed-waste-3757860-20260922',exception_id=f'reconciliation_{waste_id}',allocation_id=waste_id,action='LINK_INVOICE',corrected_fields_json=json.dumps({'appfolio_invoice_id':waste_invoice[0]['invoice_id']}),reason='User confirmed exclusion of 930 Waste USD 939.11; invoice paid on bank transaction date 2026-07-28. Older equal-amount invoice is not this transaction.',reviewed_by='user_confirmation_in_conversation',exception_version=1))
    # User-confirmed merchant identity; property and GL are independent.
    new_vendor=[(i,d) for i,d in enumerate(plan['details'],1) if d['amount_cents']==18000 and d['date']=='2026-08-03' and 'MIAMI INF' in d['merchant_original']]
    assert len(new_vendor)==1
    new_id,new_detail=new_vendor[0]
    assert new_detail['property_hint']=='LRMM Supplies'
    correction=dict(property_code=new_detail['property_resolved'],vendor_name='AplPay IN MIAMI INF',gl_account='6461: Supplies',cash_account=new_detail['cash_account'],description=new_detail['merchant_original'])
    with (root/'inbox/miami_inf_new_vendor.csv').open('w',newline='',encoding='utf-8') as f:
     writer=csv.DictWriter(f,fieldnames=['decision_id','exception_id','allocation_id','action','corrected_fields_json','reason','reviewed_by','exception_version'])
     writer.writeheader()
     writer.writerow(dict(decision_id='user-confirmed-miami-inf-180-20260922',exception_id=f'reconciliation_{new_id}',allocation_id=new_id,action='CLASSIFY',corrected_fields_json=json.dumps(correction),reason='User confirmed merchant and inclusion with NEW_VENDOR_REQUIRED. LRMM maps to Little River Mobile Home Park; Vendor Ledger independently lists GL 6461 Supplies for this property. Vendor creation is pending, not unresolved property.',reviewed_by='user_confirmation_in_conversation',exception_version=1))
    with (root/'new_vendors_required.csv').open('w',newline='',encoding='utf-8-sig') as f:
     writer=csv.DictWriter(f,fieldnames=['status','vendor_name','merchant_raw','property','gl_account','amount','date','provenance_hash'])
     writer.writeheader();writer.writerow(dict(status='NEW_VENDOR_REQUIRED',vendor_name=correction['vendor_name'],merchant_raw=new_detail['merchant_original'],property=correction['property_code'],gl_account=correction['gl_account'],amount='180.00',date=new_detail['date'],provenance_hash=new_detail['provenance_hash']))
    report=run_pipeline_simulation(root/'ledger.db',plan['records'],app['invoices']+app['other_accounts'],classes,root/'inbox',root/'processed',root/'failed',root/'exports',approved_business_plans=[d for d in plan['details'] if d['property_hint'] in ('930 Supplies','CAST Supplies')])
    c=connect(root/'ledger.db')
    report['totals_by_state']={state:amount for state,amount in c.execute('SELECT eligibility_state,SUM(amount_cents) FROM allocations GROUP BY eligibility_state')}
    report['matches']=[json.loads(r[0]) for r in c.execute('SELECT match_details_json FROM invoice_matches')]
    report['input_fingerprint']=fingerprint
    report['merchant_identities']=identities
    assert report['coverage_audit']['valid']
    assert report['inbox']['files_failed']==0
    states=dict(c.execute('SELECT id,eligibility_state FROM allocations'))
    for i,detail in enumerate(plan['details'],1):
     detail['eligibility_state']=states[i]
    plan['matched_existing_summary']={'count':report['allocations_state']['MATCHED_EXISTING'],'amount_cents':report['totals_by_state']['MATCHED_EXISTING']}
    from scripts.output.unresolved_properties_report import build_unresolved_properties_report
    report['property_report']=build_unresolved_properties_report(plan,root/'unresolved_properties.html')
    (root/'business_plan.json').write_text(json.dumps(plan,indent=2,ensure_ascii=False),encoding='utf-8')
    c.close()
    return report


def _publish(report, root):
    """Publish copies only; retain the immutable original batch and its checksum."""
    CLEAN_DIR.mkdir(parents=True, exist_ok=True)
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    outputs = {}
    for batch in report['export']['batches']:
        source = Path(batch['file_path'])
        conn = connect(root/'ledger.db')
        try:
            expected = conn.execute('SELECT file_hash FROM export_batches WHERE batch_id=?', (batch['batch_id'],)).fetchone()[0]
        finally:
            conn.close()
        if hashlib.sha256(source.read_bytes()).hexdigest() != expected:
            raise ValueError('El CSV reservado fue modificado')
        destination = CLEAN_DIR/source.name
        if not destination.exists() or destination.read_bytes() != source.read_bytes():
            temporary = destination.with_suffix('.tmp')
            shutil.copyfile(source, temporary)
            os.replace(temporary, destination)
        outputs['bulk'] = str(destination.resolve())
    for name, destination in (
        ('new_vendors_required.csv', AUDIT_DIR/'new_vendors_required.csv'),
        ('unresolved_properties.html', AUDIT_DIR/'propiedades_sin_resolver_amex.html'),
    ):
        temporary = destination.with_suffix('.tmp')
        shutil.copyfile(root/name, temporary)
        os.replace(temporary, destination)
        outputs[name] = str(destination.resolve())
    report['outputs'] = outputs
    temporary = AUDIT_DIR/'manual_pipeline_report.tmp'
    temporary.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding='utf-8')
    os.replace(temporary, AUDIT_DIR/'manual_pipeline_report.json')
    return report


def run_approved_pipeline(state_dir=None):
    sources = sorted(AMEX_RAW_DIR.glob('*.csv'))
    if len(sources) != 1:
        raise ValueError('Se requiere exactamente un CSV AMEX para este flujo aprobado')
    decisions = MASTER_DIR/'confirmed_amex_decisions.json'
    inputs = [sources[0], VENDOR_LEDGER, MAPPING_RULES, ENTITY_DICTIONARY, decisions]
    digest = hashlib.sha256()
    for path in inputs:
        digest.update(path.name.encode())
        digest.update(hashlib.sha256(path.read_bytes()).digest())
    fingerprint = digest.hexdigest()
    confirmations = json.loads(decisions.read_text(encoding='utf-8'))
    state_dir = Path(state_dir or os.environ.get('RENTIFY_STATE_DIR') or
                     Path(os.environ['LOCALAPPDATA'])/'Rentify'/'financial_pipeline').resolve()
    state_dir.mkdir(parents=True, exist_ok=True)
    registry = sqlite3.connect(state_dir/'runs.db', isolation_level=None, timeout=30)
    try:
        registry.execute('CREATE TABLE IF NOT EXISTS runs (fingerprint TEXT PRIMARY KEY, root TEXT NOT NULL, report TEXT)')
        registry.execute('BEGIN IMMEDIATE')
        previous = registry.execute('SELECT fingerprint,root,report FROM runs').fetchone()
        if previous and previous[0] != fingerprint:
            raise ValueError('Los archivos cambiaron después de iniciar un lote. Requiere revisar el lote anterior antes de crear otra exportación; no borre el estado persistente.')
        if previous and previous[2]:
            report = json.loads(previous[2])
            report['reused_existing_batch'] = True
            result = _publish(report, Path(previous[1]))
            registry.execute('COMMIT')
            return result
        if previous:
            raise ValueError('Ejecución interrumpida: recuperar el lote existente antes de reintentar; no se genera un lote duplicado.')
        root = state_dir/fingerprint
        initialize_simulation(root)
        # Persist the attempt before execution so an interrupted run cannot be duplicated.
        registry.execute('INSERT INTO runs VALUES (?,?,NULL)', (fingerprint,str(root)))
        registry.execute('COMMIT')
        registry.execute('BEGIN IMMEDIATE')
        report = _execute(root, sources[0], confirmations, fingerprint)
        report['execution_mode'] = 'approved_manual_amex'
        report['simulation'] = False
        report['input_files'] = [{'path':str(p), 'sha256':hashlib.sha256(p.read_bytes()).hexdigest()} for p in inputs]
        registry.execute('UPDATE runs SET report=? WHERE fingerprint=?', (json.dumps(report), fingerprint))
        registry.execute('COMMIT')
        return _publish(report, root)
    finally:
        registry.close()
