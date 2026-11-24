"""Explicit stakeholder routing for unresolved AMEX items; no accounting writes."""
from collections import defaultdict

from scripts.persistence.ledger import parse_to_cents
from scripts.review.recipient_routing import resolve_recipient


RESPONSIBLES = {
    'ARMANDO ARMAS': 'Richard Libutti',
    'RICHARD LIBUTTI': 'Richard Libutti',
    'CORY S. REITER': 'Cory Reiter',
    'LINDSAY REITER': 'Lindsay Reiter',
}


def responsible_for(holder):
    key = ' '.join(holder.upper().split()) if isinstance(holder, str) else ''
    if key not in RESPONSIBLES:
        raise ValueError('Unknown cardholder; explicit stakeholder mapping required')
    return RESPONSIBLES[key]


def prepare_routing(conn, manifest, adapted, comparison):
    """Use the audited row bridge, not a new guessed transaction identity.

    The holder comes from the frozen current source. The historical plan does
    not retain holders for included rows, so no claim of historical equality
    of cardholder data is made.
    """
    if adapted['audit']['file_hash'] != comparison['source_audit']['file_hash']:
        raise ValueError('Source differs from the explicitly audited AMEX file')
    if adapted['exceptions'] or comparison['changes']:
        raise ValueError('Resolve AMEX discrepancies before stakeholder routing')
    contexts = {row['row_number']: row for row in adapted['contexts']}
    records = {row['row_number']: row for row in adapted['records']}
    items = {item['transaction_id']: item for item in manifest['items']}
    if len(items) != len(manifest['items']):
        raise ValueError('Duplicate canonical transaction in manifest')
    groups = defaultdict(list)
    pending = conn.execute('''SELECT i.transaction_id,i.item_id,t.original_date,t.amount_cents,i.snapshot_json
        FROM review_items i JOIN transactions t ON t.id=i.transaction_id
        WHERE i.request_id=? AND i.status='PENDING' ORDER BY i.transaction_id''', (manifest['request_id'],)).fetchall()
    for tx, item_id, date, amount, snapshot in pending:
        item = items[tx]
        context, record = contexts[item['source_row']], records[item['source_row']]
        provenance = conn.execute('SELECT transaction_id FROM transaction_provenance WHERE provenance_hash=?',
                                  (item['historical_provenance'],)).fetchone()
        if provenance != (tx,) or item['item_id'] != item_id or record['provenance_hash'] != item['current_provenance_candidate']:
            raise ValueError('Source-to-transaction bridge changed')
        if date != record['original_date'] or amount != item['amount_cents'] or amount != parse_to_cents(record['amount_str']):
            raise ValueError('Protected date or amount changed')
        holder = context['account_holder'].strip()
        groups[responsible_for(holder)].append(dict(transaction_id=tx,item_id=item_id,source_row=item['source_row'],
            historical_provenance=item['historical_provenance'],date=date,amount_cents=amount,
            merchant_original=context['merchant'],cardholder=holder,company=item['company'],
            status='PENDING',property=None))
    resolutions = {name: resolve_recipient(name) for name in groups}
    routing_passed = all(result.status == 'PASS' for result in resolutions.values())
    return dict(source_request_id=manifest['request_id'],source_sha256=adapted['audit']['file_hash'],
                routing_basis='Explicit user instruction; holder from frozen current AMEX CSV',
                status='ROUTING_PASS' if routing_passed else 'REVIEW_REQUIRED',
                count=len(pending),total_cents=sum(row[3] for row in pending),
                routing_evidence=[resolutions[name].evidence() for name in sorted(resolutions)],
                stakeholders=[dict(responsible=name,recipient=resolutions[name].recipient,count=len(rows),
                                   total_cents=sum(row['amount_cents'] for row in rows),transactions=rows)
                              for name,rows in sorted(groups.items())])
