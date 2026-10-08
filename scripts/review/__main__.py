"""Administrative entry point; web submissions use scripts.review.web.create_app."""
import argparse
import json
import os
from pathlib import Path

from scripts.persistence.ledger import connect
from scripts.persistence.migrate import migrate
from scripts.review.delivery import deliver
from scripts.review.gmail import configured_sender
from scripts.review.security import ReviewAccess
from scripts.review.service import ReviewService


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--database', type=Path, required=True)
    actions = parser.add_subparsers(dest='action', required=True)
    actions.add_parser('migrate')
    actions.add_parser('status')
    process = actions.add_parser('process')
    process.add_argument('--appfolio', type=Path, required=True, help='Verified complete AppFolio ledger CSV')
    worker = actions.add_parser('work-once', help='Deliver queued links and process submissions; never publishes batches')
    worker.add_argument('--appfolio', type=Path, required=True)
    authorize_batch = actions.add_parser('authorize-incremental-batch', help='Explicitly publish one authorized internal incremental batch')
    authorize_batch.add_argument('gate_id')
    authorize_batch.add_argument('--responsible', required=True)
    authorize_batch.add_argument('--reason', required=True)
    auth = actions.add_parser('gmail-authorize')
    auth.add_argument('--credentials', type=Path, required=True)
    auth.add_argument('--token', type=Path, required=True)
    catalog = actions.add_parser('catalog-inspect')
    catalog.add_argument('--workbook', type=Path, required=True)
    catalog_import = actions.add_parser('catalog-import')
    catalog_import.add_argument('--workbook', type=Path, default=Path('data/master/mapping_rules.xlsx'))
    catalog_import.add_argument('--company', choices=['RAS'], default='RAS')
    revoke = actions.add_parser('revoke')
    revoke.add_argument('request_id')
    send = actions.add_parser('send')
    send.add_argument('request_id')
    send.add_argument('--retry-uncertain', action='store_true')
    send.add_argument('--pilot-override', help='One-time audited pilot override ID')
    send.add_argument('--pilot-recovery', help='One-time audited recovery ID')
    send.add_argument('--pilot-second-recovery', help='One-time audited second recovery ID')
    args = parser.parse_args()
    if args.action == 'catalog-inspect':
        from scripts.review.catalog import inspect_mapping_rules
        print(json.dumps(inspect_mapping_rules(args.workbook), ensure_ascii=True))
        return
    if args.action == 'gmail-authorize':
        from scripts.review.gmail import authorize
        authorize(args.credentials, args.token)
        print('Gmail send authorization saved')
        return
    if args.action == 'migrate':
        print(json.dumps({'migrations': migrate(args.database)}))
        return
    if not args.database.is_file():
        parser.error('Database does not exist')
    conn = connect(args.database)
    try:
        service = ReviewService(conn)
        if args.action == 'catalog-import':
            from scripts.review.catalog import sync_catalog
            result = sync_catalog(service, args.workbook, company=args.company)
            print(json.dumps(result, ensure_ascii=True))
        elif args.action == 'status':
            requests = [dict(request_id=rid, status=status, count=count, total_cents=total) for rid, status, count, total in conn.execute('SELECT request_id,status,transaction_count,total_cents FROM review_requests')]
            jobs = [dict(submission_id=sid, status=status, attempts=attempts, error=error) for sid, status, attempts, error in conn.execute('SELECT submission_id,status,attempts,last_error FROM review_jobs')]
            print(json.dumps(dict(requests=requests, jobs=jobs)))
        elif args.action == 'revoke':
            service.revoke(args.request_id)
            print('REVOKED')
        elif args.action == 'authorize-incremental-batch':
            from scripts.review.batch_authorization import authorize_incremental_batch
            result = authorize_incremental_batch(conn, args.gate_id, responsible=args.responsible,
                                                  reason=args.reason, output_dir=args.database.parent / 'exports')
            print(json.dumps(result))
        elif args.action in ('process', 'work-once'):
            from scripts.reconciliation.appfolio_adapter import read_appfolio_ledger
            from scripts.review.worker import process_jobs
            appfolio = read_appfolio_ledger(args.appfolio)
            if appfolio['invalid_rows']:
                raise ValueError('AppFolio contains invalid rows; no jobs processed')
            invoices = appfolio['invoices'] + appfolio['other_accounts']
            if args.action == 'work-once':
                from scripts.review.runner import run_once
                access = ReviewAccess(service, os.environ['RENTIFY_REVIEW_SECRET'])
                sender = configured_sender()
                result = run_once(service, access, origin=os.environ['RENTIFY_REVIEW_ORIGIN'], sender=os.environ['RENTIFY_REVIEW_SENDER'],
                                  send=sender, invoices=invoices, output_dir=args.database.parent / 'exports')
            else:
                results = process_jobs(conn, invoices)
                result = dict(jobs=results)
            print(json.dumps(result))
            if any(job['status'] == 'FAILED' for job in result['jobs'].values()) or 'NEEDS_ATTENTION' in result.get('deliveries', {}).values():
                raise SystemExit(1)
        elif args.action == 'send':
            access = ReviewAccess(service, os.environ['RENTIFY_REVIEW_SECRET'])
            sender = configured_sender()
            print(deliver(service, access, args.request_id,
                          origin=None if (args.pilot_override or args.pilot_recovery or args.pilot_second_recovery) else os.environ['RENTIFY_REVIEW_ORIGIN'],
                          sender=os.environ['RENTIFY_REVIEW_SENDER'], send=sender,
                          retry_uncertain=args.retry_uncertain, pilot_override_id=args.pilot_override,
                          pilot_recovery_id=args.pilot_recovery,
                          pilot_second_recovery_id=args.pilot_second_recovery))
    finally:
        conn.close()


if __name__ == '__main__':
    main()
