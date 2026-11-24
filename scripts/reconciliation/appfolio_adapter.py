"""Reconstrucción de facturas del vendor ledger, con trazabilidad de líneas."""
import csv
import hashlib
import io
import json
from datetime import datetime
from pathlib import Path
from scripts.persistence.ledger import parse_to_cents


def _norm(value):
    return ' '.join(value.upper().split())


def read_appfolio_ledger(path, *, currency='USD', bank_account='Amex'):
    raw = Path(path).read_bytes()
    digest = hashlib.sha256(raw).hexdigest()
    reader = csv.DictReader(io.StringIO(raw.decode('utf-8-sig'), newline=''), strict=True)
    required = {'Reference','Bill Date','Payee Name','Paid','Unpaid','Bank Account','Property'}
    if not required <= set(reader.fieldnames or []):
        raise ValueError('Esquema AppFolio incompleto')
    groups, ignored, invalid = {}, [], []
    for row in reader:
        line = reader.line_num
        if not row.get('Bill Date','').strip() and not row.get('Payee Name','').strip():
            ignored.append(line)
            continue
        try:
            if None in row or any(row[k] is None for k in required):
                raise ValueError('Fila incompleta')
            day = datetime.strptime(row['Bill Date'].strip(), '%m/%d/%Y').date().isoformat()
            if not row['Payee Name'].strip():
                raise ValueError('Proveedor vacío')
            # El total original se reconstruye como pagado + pendiente, nunca solo Unpaid.
            paid = parse_to_cents(row['Paid'].strip().replace(',',''))
            unpaid = parse_to_cents(row['Unpaid'].strip().replace(',',''))
            paid_date = None
            if paid != 0 and row.get('Paid Date','').strip():
                paid_date = datetime.strptime(row['Paid Date'].strip(), '%m/%d/%Y').date().isoformat()
        except ValueError as exc:
            invalid.append(dict(line=line,reason=str(exc)))
            continue
        reference = row['Reference'].strip()
        account = _norm(row['Bank Account'])
        key = (account,_norm(row['Payee Name']),reference,day,currency)
        if not reference:
            # Fila individual, jamás agrupar todas las referencias vacías.
            key += (digest,line)
        invoice_id = 'appfolio_report_' + hashlib.sha256(json.dumps(key).encode()).hexdigest()
        item = groups.setdefault(key,dict(invoice_id=invoice_id,vendor_name=row['Payee Name'].strip(),
            date=day,currency=currency,amount_cents=0,reference=reference,bank_account=row['Bank Account'].strip(),
            identity_type='vendor_reference_date' if reference else 'source_row',
            review_only=account != _norm(bank_account),source_file_hash=digest,source_lines=[],properties=[]))
        item.setdefault('paid_cents', 0)
        item.setdefault('unpaid_cents', 0)
        item.setdefault('payment_dates', [])
        item['paid_cents'] += paid
        item['unpaid_cents'] += unpaid
        if paid != 0:
            item['payment_dates'].append(paid_date)
        item['amount_cents'] += paid + unpaid
        item['source_lines'].append(line)
        item['properties'].append(row['Property'])
    invoices = list(groups.values())
    for item in invoices:
        item['bill_date'] = item['date']
        item['date_basis'] = 'Bill Date'
        dates = set(item['payment_dates'])
        if item['unpaid_cents'] == 0 and item['paid_cents'] != 0 and len(dates) == 1 and None not in dates:
            item['date'] = next(iter(dates))
            item['date_basis'] = 'Paid Date'
    amex = [i for i in invoices if not i['review_only']]
    others = [i for i in invoices if i['review_only']]
    return dict(invoices=amex,other_accounts=others,invalid_rows=invalid,
        audit=dict(file_hash=digest,amount_basis='Paid + Unpaid',currency_assumption=currency,
            bank_account=bank_account,selected_lines=sum(len(i['source_lines']) for i in amex),
            referenced_invoices=sum(bool(i['reference']) for i in amex),
            unreferenced_lines=sum(not i['reference'] for i in amex),
            multiple_property_invoices=sum(len(set(i['properties']))>1 for i in amex),
            ignored_non_transaction_rows=len(ignored),invalid_rows=len(invalid)))
