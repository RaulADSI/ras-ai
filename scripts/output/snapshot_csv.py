"""Serialización determinista del CSV reservado, sin recalcular reglas."""
import csv
import io
from datetime import date
from scripts.config import FINAL_APPFOLIO_COLUMNS


def snapshot_csv_bytes(snapshot):
    if not snapshot or len({x.allocation_id for x in snapshot}) != len(snapshot):
        raise ValueError('Instantánea vacía o duplicada')
    if len({x.currency for x in snapshot}) != 1:
        raise ValueError('No se pueden mezclar monedas')
    output = io.StringIO(newline='')
    writer = csv.writer(output, lineterminator='\r\n')
    writer.writerow(FINAL_APPFOLIO_COLUMNS)
    for x in snapshot:
        if type(x.amount_cents) is not int:
            raise ValueError('Importe no entero')
        if not all(str(v).strip() for v in (x.property_code, x.vendor_name, x.gl_account, x.cash_account, x.currency)) or x.property_code.startswith('REVISAR PROP:'):
            raise ValueError('Clasificación incompleta')
        date.fromisoformat(x.date)
        amount = ('-' if x.amount_cents < 0 else '') + f'{abs(x.amount_cents) // 100}.{abs(x.amount_cents) % 100:02d}'
        writer.writerow([x.property_code,x.vendor_name,amount,x.gl_account,x.date,x.date,x.date,x.description,x.cash_account])
    return output.getvalue().encode('utf-8-sig')
