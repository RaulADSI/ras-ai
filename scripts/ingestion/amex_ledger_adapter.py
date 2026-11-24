"""Adaptador del CSV AMEX anotado: conserva posiciones y nunca infiere cuentas."""
import csv
import hashlib
import io
import re
from datetime import datetime
from pathlib import Path

from scripts.persistence.ledger import calculate_provenance_hash, parse_to_cents


ANNOTATED_HEADER = ['', '', '', '', 'Company', 'GL', '', '', '']


def _amount(value):
    value = value.strip()
    if not re.fullmatch(r'[+-]?(?:[0-9]+|[0-9]{1,3}(?:,[0-9]{3})+)(?:\.[0-9]+)?', value):
        raise ValueError('Formato monetario inválido')
    result = value.replace(',', '')
    return result, parse_to_cents(result)


def read_annotated_amex_csv(path, *, account_ref, currency):
    """Solo acepta el formato observado, de nueve columnas, con fechas MM/DD/YYYY.

    No aplica filtros RAS ni clasifica. Las anotaciones y filas inválidas se
    devuelven separadas. Toda transacción válida entra inicialmente en revisión.
    """
    if not isinstance(account_ref, str) or not account_ref.strip():
        raise ValueError('Se requiere un alias de cuenta confirmado')
    if not isinstance(currency, str) or not re.fullmatch('[A-Z]{3}', currency):
        raise ValueError('Se requiere moneda explícita')
    path = Path(path)
    raw = path.read_bytes()
    file_hash = hashlib.sha256(raw).hexdigest()
    reader = csv.reader(io.StringIO(raw.decode('utf-8-sig'), newline=''), strict=True)
    header = next(reader, None)
    if header is None or len(header) < 9 or [cell.strip() for cell in header[:9]] != ANNOTATED_HEADER or any(cell.strip() for cell in header[9:]):
        raise ValueError('Esquema AMEX no reconocido; se esperan nueve columnas y Company/GL en posiciones 5/6')
    records, contexts, exceptions, annotations = [], [], [], []
    blank_rows = 0
    parsed_total = accepted_total = rejected_total = 0
    unparseable_amounts = 0
    data_rows = 0
    while True:
        line = reader.line_num + 1
        try:
            row = next(reader)
        except StopIteration:
            break
        data_rows += 1
        if len(row) != len(header):
            raise ValueError(f'Esquema inválido en línea {line}: {len(row)} columnas')
        if not any(cell.strip() for cell in row):
            blank_rows += 1
            continue
        origin = dict(file_name=path.name, file_hash=file_hash, sheet_name='CSV', row_number=line)
        if not any(cell.strip() for cell in row[:4]):
            annotations.append(dict(**origin, raw_fields=row))
            continue
        cents = None
        try:
            amount_str, cents = _amount(row[3])
            parsed_total += cents
            if not re.fullmatch(r'\d{1,2}/\d{1,2}/\d{4}', row[0].strip()):
                raise ValueError('Fecha debe usar MM/DD/YYYY')
            original_date = datetime.strptime(row[0].strip(), '%m/%d/%Y').date().isoformat()
            if not row[1].strip() or not row[2].strip():
                raise ValueError('Proveedor y titular son obligatorios')
        except ValueError as exc:
            if cents is None:
                unparseable_amounts += 1
            else:
                rejected_total += cents
            exceptions.append(dict(**origin, raw_fields=row, reason=str(exc), amount_cents=cents))
            continue
        prov = calculate_provenance_hash(file_hash, 'CSV', line)
        records.append(dict(bank_id='AMEX', account_ref=account_ref.strip(), bank_reference='',
            amount_str=amount_str, currency=currency, original_date=original_date,
            file_hash=file_hash, sheet_name='CSV', row_number=line, provenance_hash=prov))
        contexts.append(dict(**origin, provenance_hash=prov, merchant=row[1], account_holder=row[2],
            company=row[4], gl_hint=row[5], additional_fields=row[6:], original_date=row[0], original_amount=row[3]))
        accepted_total += cents
    return dict(records=records, contexts=contexts, exceptions=exceptions, annotations=annotations,
        audit=dict(file_name=path.name, file_hash=file_hash, account_ref=account_ref.strip(), currency=currency,
            data_rows=data_rows, accepted_rows=len(records), rejected_rows=len(exceptions),
            annotation_rows=len(annotations), blank_rows=blank_rows,
            parsed_total_cents=parsed_total, accepted_total_cents=accepted_total,
            rejected_total_cents=rejected_total, unparseable_amounts=unparseable_amounts,
            totals_reconciled=parsed_total == accepted_total + rejected_total,
            complete_amount_control=unparseable_amounts == 0))
