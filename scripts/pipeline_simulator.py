"""Simulación persistente aislada. No invoca el orquestador productivo."""
import json
import tempfile
import uuid
from pathlib import Path

from scripts.config import DATA_DIR
from scripts.persistence.ledger import Ledger, connect
from scripts.persistence.migrate import migrate
from scripts.reconciliation.engine import ReconciliationEngine
from scripts.reconciliation.inbox_processor import process_inbox
from scripts.output.bulk_bill_generator import publish_reserved_batch

MARKER = '.rentify-simulation.json'


def initialize_simulation(root):
    """Solo inicializa una carpeta vacía; nunca adopta una base existente."""
    root = Path(root).resolve()
    production = DATA_DIR.resolve()
    if root == production or production in root.parents or root in production.parents:
        raise ValueError('La simulación debe estar fuera del árbol de datos productivos')
    root.mkdir(parents=True, exist_ok=True)
    if any(root.iterdir()):
        raise ValueError('Se requiere una carpeta vacía para inicializar la simulación')
    (root / MARKER).write_text(json.dumps({'simulation_id': uuid.uuid4().hex}), encoding='utf-8')
    for name in ('inbox', 'processed', 'failed', 'exports'):
        (root / name).mkdir()
    return root


def _verify_coverage_invariants(conn):
    """Incluye eventos sin fracciones y ceros; suma en Python sin overflow SQL."""
    problems = []
    count = 0
    for transaction_id, amount in conn.execute('SELECT id,amount_cents FROM transactions ORDER BY id'):
        count += 1
        fractions = [row[0] for row in conn.execute('SELECT amount_cents FROM allocations WHERE transaction_id=?', (transaction_id,))]
        try:
            Ledger.validate_coverage_and_signs(amount, fractions)
        except (ValueError, TypeError) as exc:
            problems.append(dict(transaction_id=transaction_id, amount_cents=amount,
                                 allocated_cents=sum(fractions), fraction_count=len(fractions), reason=str(exc)))
    return dict(valid=not problems, transactions_checked=count,
                mismatched_transactions=len(problems), details=problems)


def run_pipeline_simulation(db_path, raw_records, invoices, classification_rules,
                            inbox_dir, processed_dir, failed_dir, export_dir,
                            *, reconciliation_blockers=None, approved_business_plans=None):
    db_path = Path(db_path).resolve()
    root = db_path.parent
    production = DATA_DIR.resolve()
    if root == production or production in root.parents or root in production.parents:
        raise ValueError('Ruta productiva no permitida')
    marker = root / MARKER
    if not marker.is_file() or not json.loads(marker.read_text(encoding='utf-8')).get('simulation_id'):
        raise ValueError('Inicialice una carpeta aislada mediante initialize_simulation')
    directories = [Path(p).resolve() for p in (inbox_dir,processed_dir,failed_dir,export_dir)]
    if any(root not in p.parents for p in directories) or len(set(directories)) != 4:
        raise ValueError('Todas las carpetas deben ser distintas y estar dentro de la simulación')
    if any(a in b.parents or b in a.parents for i,a in enumerate(directories) for b in directories[i+1:]):
        raise ValueError('Las carpetas no pueden estar anidadas')
    if any(p.is_symlink() or (hasattr(p, 'is_junction') and p.is_junction()) for p in root.rglob('*')):
        raise ValueError('No se permiten enlaces en el entorno simulado')
    if db_path == marker or any(db_path == p or p in db_path.parents for p in directories):
        raise ValueError('La base debe estar separada de las carpetas de archivos')
    for p in directories:
        p.mkdir(parents=True, exist_ok=True)
    report = dict(simulation=True, status='RUNNING', stage='migration', export={'batches': [], 'exported_items': 0})
    report_path = root / f'simulation_report_{uuid.uuid4().hex}.json'
    conn = None
    try:
        migrate(db_path)
        conn = connect(db_path)
        ledger = Ledger(conn)
        report['stage'] = 'ingestion'
        report['ingestion'] = ledger.register_transactions(raw_records)
        if approved_business_plans:
            ledger.apply_business_allocation_plan(approved_business_plans)
            classification_rules = {k:dict(v) for k,v in classification_rules.items()}
            for detail in approved_business_plans:
                if detail.get('flags'):
                    raise ValueError('El plan aprobado contiene alertas pendientes')
                parent = conn.execute('SELECT transaction_id FROM transaction_provenance WHERE provenance_hash=?',
                                      (detail['provenance_hash'],)).fetchone()[0]
                for allocation in ledger.pending_allocations():
                    if allocation['transaction_id'] == parent:
                        classification_rules[allocation['id']] = {
                            k:allocation[k] for k in ('property_code','vendor_name','gl_account','cash_account','description')}
            report['approved_business_plans'] = len(approved_business_plans)
        report['stage'] = 'reconciliation'
        if reconciliation_blockers:
            report['reconciliation_blockers'] = list(reconciliation_blockers)
            report['reconciliation'] = {}
            for allocation in ledger.pending_allocations():
                ledger.create_exception(allocation['id'], f"reconciliation_{allocation['id']}",
                    'UNVERIFIED_APPFOLIO_SOURCE', {'blockers': list(reconciliation_blockers)})
                report['reconciliation'][allocation['id']] = 'REVIEW_REQUIRED'
        else:
            report['reconciliation'] = ReconciliationEngine(ledger).run(invoices, classification_rules)
        report['stage'] = 'inbox'
        report['inbox'] = process_inbox(ledger, *directories[:3])
        report['stage'] = 'coverage'
        report['coverage_audit'] = _verify_coverage_invariants(conn)
        if not report['coverage_audit']['valid']:
            raise ValueError('Cobertura o signos inválidos: exportación bloqueada')
        report['stage'] = 'export'
        reserved = [r[0] for r in conn.execute("SELECT DISTINCT batch_id FROM export_items WHERE delivery_state='RESERVED' ORDER BY batch_id")]
        for batch_id in reserved:
            path = publish_reserved_batch(ledger, batch_id, directories[3])
            report['export']['batches'].append(dict(batch_id=batch_id, file_path=str(path), recovered=True, items=len(ledger.get_snapshot(batch_id))))
        batch_id, snapshot = ledger.reserve_batch()
        if batch_id:
            path = publish_reserved_batch(ledger, batch_id, directories[3])
            report['export']['batches'].append(dict(batch_id=batch_id, file_path=str(path), recovered=False, items=len(snapshot)))
        report['export']['exported_items'] = sum(b['items'] for b in report['export']['batches'])
        report['status'] = 'COMPLETED_WITH_REVIEW' if report['inbox']['files_failed'] or report['inbox']['archive_errors'] else 'COMPLETED'
        report['stage'] = 'complete'
    except Exception as exc:
        report['status'] = 'FAILED'
        report['error'] = dict(type=type(exc).__name__, message=str(exc))
        raise
    finally:
        if conn is not None:
            try:
                report['allocations_state'] = dict(conn.execute('SELECT eligibility_state,COUNT(*) FROM allocations GROUP BY eligibility_state'))
                report['delivery_state'] = dict(conn.execute('SELECT delivery_state,COUNT(*) FROM export_items GROUP BY delivery_state'))
                if report['status'] == 'COMPLETED' and report['allocations_state'].get('REVIEW_REQUIRED', 0):
                    report['status'] = 'COMPLETED_WITH_REVIEW'
            finally:
                conn.close()
        report['report_path'] = str(report_path)
        report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding='utf-8')
    return report


def main():
    root = initialize_simulation(Path(tempfile.mkdtemp(prefix='rentify-simulation-')))
    records = [dict(bank_id='AMEX', account_ref='TEST', bank_reference='DEMO1', amount_str='50.01',
                    currency='USD', original_date='2026-08-12', file_hash='a'*64, sheet_name='CSV', row_number=1)]
    rules = {1: dict(property_code='TEST-P1', vendor_name='Test vendor', gl_account='6435', cash_account='1150', description='SIMULATION ONLY')}
    report = run_pipeline_simulation(root/'ledger.db',records,[],rules,
        root/'inbox',root/'processed',root/'failed',root/'exports')
    print(json.dumps(report, indent=2, ensure_ascii=False))


if __name__ == '__main__':
    main()
