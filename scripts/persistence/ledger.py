import hashlib
import json
import sqlite3
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import NamedTuple
from datetime import date
from decimal import Decimal, InvalidOperation
import re
from scripts.output.snapshot_csv import snapshot_csv_bytes


def parse_to_cents(amount_str: str) -> int:
    """Texto decimal canónico, exacto y dentro del rango INTEGER de SQLite."""
    if not isinstance(amount_str, str):
        raise TypeError('El importe debe recibirse como texto, no float')
    text = amount_str.strip()
    if not re.fullmatch(r'[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?', text):
        raise ValueError('Importe decimal inválido')
    try:
        value = Decimal(text)
    except InvalidOperation as exc:
        raise ValueError('Importe decimal inválido') from exc
    if not value.is_finite():
        raise ValueError('Importe no finito')
    # Operar sobre la tupla evita redondeos por el contexto global Decimal.
    sign, digits, exponent = value.as_tuple()
    if not any(digits):
        return 0
    shift = exponent + 2
    if shift < 0:
        places = -shift
        if places >= len(digits) or any(digits[-places:]):
            raise ValueError('El importe contiene fracciones de centavo')
        digits = digits[:-places]
        shift = 0
    if len(digits) + shift > 19:
        raise ValueError('Importe fuera del rango INTEGER de SQLite')
    cents = int(''.join(map(str, digits))) * (10 ** shift)
    cents = -cents if sign else cents
    if not -(2 ** 63) <= cents <= 2 ** 63 - 1:
        raise ValueError('Importe fuera del rango INTEGER de SQLite')
    return cents


def calculate_provenance_hash(file_hash, sheet_name, row_number):
    if not isinstance(file_hash, str) or not re.fullmatch('[0-9a-f]{64}', file_hash):
        raise ValueError('file_hash debe ser SHA-256 hexadecimal en minúsculas')
    if not isinstance(sheet_name, str) or not sheet_name.strip():
        raise ValueError('sheet_name obligatorio; usar CSV para archivos CSV')
    if type(row_number) is not int or row_number <= 0:
        raise ValueError('row_number debe ser un entero positivo')
    payload = json.dumps([file_hash, sheet_name, row_number], ensure_ascii=False, separators=(',', ':'))
    return hashlib.sha256(payload.encode('utf-8')).hexdigest()


def connect(database, timeout=5.0):
    if timeout < 0:
        raise ValueError('timeout debe ser no negativo')
    conn = sqlite3.connect(str(database), timeout=timeout, isolation_level=None)
    try:
        conn.execute('PRAGMA foreign_keys = ON')
        if conn.execute('PRAGMA foreign_keys').fetchone()[0] != 1:
            raise RuntimeError('No se pudo activar foreign_keys')
        return conn
    except BaseException:
        conn.close()
        raise


class ExportSnapshotItem(NamedTuple):
    allocation_id: int
    bank_id: str
    amount_cents: int
    currency: str
    date: str
    property_code: str
    vendor_name: str
    gl_account: str
    cash_account: str
    description: str


class Ledger:
    def __init__(self, conn):
        self.conn = conn

    def register_transactions(self, records):
        """Registra eventos y procedencias en una sola transacción; nunca fusiona sin referencia."""
        metrics = dict(registered=0, skipped_reingestion=0, transactions_created=0, provenance_linked=0)
        with self._atomic():
            for rec in records:
                amount = parse_to_cents(rec['amount_str'])
                prov = calculate_provenance_hash(rec['file_hash'], rec['sheet_name'], rec['row_number'])
                if 'provenance_hash' in rec and rec['provenance_hash'] != prov:
                    raise ValueError('provenance_hash no corresponde a sus coordenadas')
                for field in ('bank_id', 'account_ref', 'currency', 'original_date'):
                    if not isinstance(rec[field], str) or not rec[field].strip():
                        raise ValueError(f'Campo obligatorio: {field}')
                if not isinstance(rec['bank_reference'], str):
                    raise TypeError('Usar cadena vacía cuando falta referencia bancaria')
                bank, account = rec['bank_id'].strip(), rec['account_ref'].strip()
                reference = rec['bank_reference'].strip()
                currency = rec['currency'].strip()
                if not re.fullmatch('[A-Z]{3}', currency):
                    raise ValueError('Moneda debe ser un código de tres letras mayúsculas')
                original_date = rec['original_date']
                if date.fromisoformat(original_date).isoformat() != original_date:
                    raise ValueError('Fecha debe usar YYYY-MM-DD')
                identity = (bank, account, reference, amount, currency, original_date)
                existing = self.conn.execute('''SELECT t.id,t.bank_id,t.account_ref,trim(t.bank_reference),
                    t.amount_cents,t.currency,t.original_date FROM transaction_provenance p
                    JOIN transactions t ON t.id=p.transaction_id WHERE p.provenance_hash=?''', (prov,)).fetchone()
                if existing:
                    if existing[1:] != identity:
                        raise ValueError('Colisión: misma procedencia con datos distintos')
                    metrics['skipped_reingestion'] += 1
                    continue
                # No duplicar silenciosamente una fila legada cuyas coordenadas se desconocen.
                legacy = self.conn.execute('''SELECT id FROM transactions WHERE provenance_hash=?
                    AND NOT EXISTS (SELECT 1 FROM transaction_provenance p WHERE p.transaction_id=transactions.id)''', (prov,)).fetchone()
                if legacy:
                    raise ValueError('Procedencia legada sin coordenadas: requiere revisión')
                existing = None
                if reference:
                    existing = self.conn.execute('''SELECT id,amount_cents,currency,original_date
                        FROM transactions WHERE bank_id=? AND account_ref=? AND trim(bank_reference)=?''',
                        (bank, account, reference)).fetchone()
                if existing:
                    if existing[1:] != (amount, currency, original_date):
                        raise ValueError('Colisión: referencia bancaria con datos distintos')
                    transaction_id = existing[0]
                    metrics['provenance_linked'] += 1
                else:
                    transaction_id = self.conn.execute('''INSERT INTO transactions
                        (bank_id,account_ref,bank_reference,provenance_hash,amount_cents,currency,original_date)
                        VALUES (?,?,?,?,?,?,?)''', (bank, account, reference, prov, amount, currency, original_date)).lastrowid
                    self.conn.execute('''INSERT INTO allocations(transaction_id,amount_cents,eligibility_state)
                        VALUES (?,?,'REVIEW_REQUIRED')''', (transaction_id, amount))
                    metrics['transactions_created'] += 1
                self.conn.execute('''INSERT INTO transaction_provenance
                    (transaction_id,file_hash,sheet_name,row_number,provenance_hash) VALUES (?,?,?,?,?)''',
                    (transaction_id, rec['file_hash'], rec['sheet_name'], rec['row_number'], prov))
                metrics['registered'] += 1
        return metrics

    @contextmanager
    def _atomic(self):
        if self.conn.in_transaction:
            raise ValueError('La conexión ya tiene una transacción activa')
        self.conn.execute('BEGIN IMMEDIATE')
        try:
            yield
            self.conn.commit()
        except BaseException:
            self.conn.rollback()
            raise

    @staticmethod
    def validate_coverage_and_signs(transaction_amount_cents, fractions):
        if type(transaction_amount_cents) is not int or any(type(x) is not int for x in fractions):
            raise TypeError('Los centavos deben ser enteros')
        if not fractions or sum(fractions) != transaction_amount_cents:
            raise ValueError('La suma no coincide con el importe original')
        for frac in fractions:
            if transaction_amount_cents > 0 and frac < 0:
                raise ValueError('Transacción positiva no admite negativos')
            if transaction_amount_cents < 0 and frac > 0:
                raise ValueError('Transacción negativa no admite positivos')
            if transaction_amount_cents == 0 and frac != 0:
                raise ValueError('Transacción cero solo admite fracciones cero')

    @staticmethod
    def _calculate_request_hash(exception_id, allocation_id, action, corrected_fields, reason, reviewed_by):
        payload = dict(exception_id=exception_id, allocation_id=allocation_id, action=action,
                       corrected_fields=corrected_fields, reason=reason, reviewed_by=reviewed_by)
        return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(',', ':'),
                                        ensure_ascii=False, allow_nan=False).encode('utf-8')).hexdigest()

    def reserve_batch(self):
        with self._atomic():
            rows = self.conn.execute('''
                SELECT a.id,t.bank_id,a.amount_cents,t.currency,t.original_date,
                       a.property_code,a.vendor_name,a.gl_account,a.cash_account,a.description
                FROM allocations a JOIN transactions t ON t.id=a.transaction_id
                WHERE a.eligibility_state='READY_TO_IMPORT'
                AND NOT EXISTS (SELECT 1 FROM export_items e WHERE e.allocation_id=a.id
                  AND e.delivery_state IN ('RESERVED','EXPORTED','IMPORTED','IMPORT_FAILED_REVIEW'))
                ORDER BY a.id
            ''').fetchall()
            if not rows:
                return None, ()
            snapshot = tuple(ExportSnapshotItem(*row) for row in rows)
            snapshot_csv_bytes(snapshot)
            parents = set()
            for item in snapshot:
                parent, amount = self.conn.execute('''SELECT t.id,t.amount_cents FROM transactions t
                    JOIN allocations a ON a.transaction_id=t.id WHERE a.id=?''', (item.allocation_id,)).fetchone()
                if parent not in parents:
                    fractions = [r[0] for r in self.conn.execute('SELECT amount_cents FROM allocations WHERE transaction_id=?', (parent,))]
                    self.validate_coverage_and_signs(amount, fractions)
                    parents.add(parent)
            batch_id = 'batch_' + uuid.uuid4().hex
            self.conn.execute('INSERT INTO export_batches(batch_id) VALUES (?)', (batch_id,))
            for item in snapshot:
                self.conn.execute('''INSERT INTO export_items
                    (batch_id,allocation_id,delivery_state,snapshot_json) VALUES (?,?,'RESERVED',?)''',
                    (batch_id, item.allocation_id, json.dumps(item._asdict(), sort_keys=True)))
            return batch_id, snapshot

    def pending_allocations(self):
        cursor = self.conn.execute('''SELECT a.*,t.currency,t.original_date,t.bank_reference
            FROM allocations a JOIN transactions t ON t.id=a.transaction_id
            WHERE a.eligibility_state='REVIEW_REQUIRED'
            AND NOT EXISTS (SELECT 1 FROM export_items e WHERE e.allocation_id=a.id
              AND e.delivery_state IN ('RESERVED','EXPORTED','IMPORTED','IMPORT_FAILED_REVIEW'))
            ORDER BY a.id''')
        return [dict(zip([c[0] for c in cursor.description], row)) for row in cursor.fetchall()]

    def consumed_invoice_ids(self):
        return {r[0] for r in self.conn.execute('SELECT appfolio_invoice_id FROM invoice_matches')}

    def _reviewable(self, allocation_id):
        row = self.conn.execute('SELECT eligibility_state FROM allocations WHERE id=?', (allocation_id,)).fetchone()
        if row != ('REVIEW_REQUIRED',):
            raise ValueError('Fracción inexistente o no pendiente de revisión')
        if self.conn.execute('''SELECT 1 FROM export_items WHERE allocation_id=? AND
            delivery_state IN ('RESERVED','EXPORTED','IMPORTED','IMPORT_FAILED_REVIEW')''', (allocation_id,)).fetchone():
            raise ValueError('Fracción bloqueada por exportación')
        if self.conn.execute('SELECT 1 FROM invoice_matches WHERE allocation_id=?', (allocation_id,)).fetchone():
            raise ValueError('Fracción ya vinculada a una factura')

    def _audit(self, event_type, payload):
        self.conn.execute('INSERT INTO audit_events(event_type,payload_json) VALUES (?,?)',
                          (event_type, json.dumps(payload, sort_keys=True, allow_nan=False)))

    def _resolve_exceptions(self, allocation_id):
        self.conn.execute("UPDATE exceptions SET status='RESOLVED' WHERE allocation_id=? AND status='PENDING'", (allocation_id,))

    def match_invoice(self, allocation_id, appfolio_invoice_id, match_details):
        if not isinstance(appfolio_invoice_id, str) or not appfolio_invoice_id.strip():
            raise ValueError('ID de factura obligatorio')
        if not isinstance(match_details, dict):
            raise TypeError('match_details debe ser un diccionario')
        details = json.dumps(match_details, sort_keys=True, allow_nan=False)
        with self._atomic():
            self._reviewable(allocation_id)
            self.conn.execute('''INSERT INTO invoice_matches
                (allocation_id,appfolio_invoice_id,match_details_json) VALUES (?,?,?)''',
                (allocation_id, appfolio_invoice_id.strip(), details))
            self.conn.execute("UPDATE allocations SET eligibility_state='MATCHED_EXISTING' WHERE id=?", (allocation_id,))
            self._resolve_exceptions(allocation_id)
            self._audit('MATCHED_EXISTING', dict(allocation_id=allocation_id, invoice_id=appfolio_invoice_id, details=match_details))

    @staticmethod
    def validate_classification(property_code, vendor_name, gl_account, cash_account, description):
        for value in (property_code, vendor_name, gl_account, cash_account):
            if not isinstance(value, str) or not value.strip() or any(
                marker in value.upper() for marker in ('REVISAR', 'UNKNOWN', 'DEFAULT_GL')):
                raise ValueError('Clasificación incompleta o pendiente de revisión')
        if not isinstance(description, str):
            raise ValueError('Descripción debe ser texto')

    def classify_allocation(self, allocation_id, property_code, vendor_name, gl_account, cash_account, description):
        self.validate_classification(property_code, vendor_name, gl_account, cash_account, description)
        with self._atomic():
            self._reviewable(allocation_id)
            self.conn.execute('''UPDATE allocations SET property_code=?,vendor_name=?,gl_account=?,
                cash_account=?,description=?,eligibility_state='READY_TO_IMPORT' WHERE id=?''',
                (property_code.strip(),vendor_name.strip(),gl_account.strip(),cash_account.strip(),description,allocation_id))
            self._resolve_exceptions(allocation_id)
            self._audit('READY_TO_IMPORT', dict(allocation_id=allocation_id,property_code=property_code,
                vendor_name=vendor_name,gl_account=gl_account,cash_account=cash_account,description=description))

    def create_exception(self, allocation_id, exception_id, reason, metadata):
        if not isinstance(exception_id, str) or not exception_id.strip() or not isinstance(reason, str) or not reason.strip():
            raise ValueError('ID de excepción y motivo obligatorios')
        if not isinstance(metadata, dict):
            raise TypeError('metadata debe ser un diccionario')
        encoded = json.dumps(metadata, sort_keys=True, allow_nan=False)
        with self._atomic():
            self._reviewable(allocation_id)
            old = self.conn.execute('SELECT allocation_id,status,reason,metadata_json FROM exceptions WHERE exception_id=?', (exception_id,)).fetchone()
            if old:
                if old[:2] != (allocation_id, 'PENDING'):
                    raise ValueError('Excepción pertenece a otra fracción o ya está resuelta')
                if old[2:] == (reason, encoded):
                    return
                self.conn.execute('UPDATE exceptions SET reason=?,metadata_json=?,version=version+1 WHERE exception_id=?', (reason,encoded,exception_id))
            else:
                self.conn.execute('''INSERT INTO exceptions(exception_id,allocation_id,status,reason,metadata_json)
                    VALUES (?,?,'PENDING',?,?)''', (exception_id,allocation_id,reason,encoded))
            self._audit('REVIEW_REQUIRED', dict(exception_id=exception_id,allocation_id=allocation_id,reason=reason,metadata=metadata))

    def get_snapshot(self, batch_id):
        rows = self.conn.execute('SELECT snapshot_json FROM export_items WHERE batch_id=? ORDER BY allocation_id', (batch_id,)).fetchall()
        if not rows or any(row[0] is None for row in rows):
            raise ValueError('Lote inexistente o sin instantánea persistida; requiere revisión')
        return tuple(ExportSnapshotItem(**json.loads(row[0])) for row in rows)

    def apply_decision(self, decision_id, exception_id, allocation_id, action,
                       corrected_fields, reason, reviewed_by, exception_version=1):
        """Una decisión, una transacción; reintentos exactos no repiten efectos."""
        for value in (decision_id, exception_id, reason, reviewed_by):
            if not isinstance(value, str) or not value.strip():
                raise ValueError('ID, motivo y responsable son obligatorios')
        if type(allocation_id) is not int or allocation_id <= 0 or type(exception_version) is not int or exception_version <= 0:
            raise ValueError('Fracción y versión deben ser enteros positivos')
        if action not in ('CLASSIFY', 'LINK_INVOICE'):
            raise ValueError('Acción no soportada')
        if not isinstance(corrected_fields, dict):
            raise TypeError('corrected_fields debe ser un objeto JSON')
        fields = ('property_code','vendor_name','gl_account','cash_account','description')
        allowed = set(fields) if action == 'CLASSIFY' else {'appfolio_invoice_id'}
        if set(corrected_fields) - allowed:
            raise ValueError('Campo corregido no permitido; importe, moneda e identidad son inmutables')
        if action == 'CLASSIFY':
            values = {key: corrected_fields.get(key, '') for key in fields}
            self.validate_classification(**values)
        else:
            invoice_id = corrected_fields.get('appfolio_invoice_id')
            if not isinstance(invoice_id, str) or not invoice_id.strip():
                raise ValueError('ID de factura obligatorio')
        payload = dict(exception_id=exception_id, allocation_id=allocation_id, action=action,
                       corrected_fields=corrected_fields, reason=reason, reviewed_by=reviewed_by,
                       exception_version=exception_version)
        encoded = json.dumps(payload, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False)
        request_hash = hashlib.sha256(encoded.encode('utf-8')).hexdigest()
        with self._atomic():
            existing = self.conn.execute('SELECT request_hash FROM exceptions_decisions WHERE decision_id=?', (decision_id,)).fetchone()
            if existing:
                if existing[0] != request_hash:
                    raise ValueError('Colisión: decision_id reutilizado con otra solicitud')
                return 'ALREADY_APPLIED'
            exception = self.conn.execute('SELECT allocation_id,status,version FROM exceptions WHERE exception_id=?', (exception_id,)).fetchone()
            if exception != (allocation_id, 'PENDING', exception_version):
                raise ValueError('Excepción inexistente, ajena, resuelta o versión obsoleta')
            self._reviewable(allocation_id)
            if action == 'CLASSIFY':
                self.conn.execute('''UPDATE allocations SET property_code=?,vendor_name=?,gl_account=?,
                    cash_account=?,description=?,eligibility_state='READY_TO_IMPORT' WHERE id=?''',
                    tuple(values[key].strip() if key != 'description' else values[key] for key in fields) + (allocation_id,))
            else:
                self.conn.execute('''INSERT INTO invoice_matches(allocation_id,appfolio_invoice_id,match_details_json)
                    VALUES (?,?,?)''', (allocation_id, invoice_id.strip(), encoded))
                self.conn.execute("UPDATE allocations SET eligibility_state='MATCHED_EXISTING' WHERE id=?", (allocation_id,))
            self._resolve_exceptions(allocation_id)
            self.conn.execute('''INSERT INTO exceptions_decisions
                (decision_id,exception_id,allocation_id,action,request_hash,reason,reviewed_by,request_json,exception_version)
                VALUES (?,?,?,?,?,?,?,?,?)''', (decision_id,exception_id,allocation_id,action,request_hash,reason,reviewed_by,encoded,exception_version))
            self._audit('MANUAL_DECISION_APPLIED', dict(decision_id=decision_id, **payload))
            return 'APPLIED'

    def mark_exported(self, batch_id, file_path, snapshot):
        path = Path(file_path).resolve()
        with self._atomic():
            expected = self.get_snapshot(batch_id)
            if snapshot != expected:
                raise ValueError('La instantánea no corresponde al lote')
            content = path.read_bytes()
            if content != snapshot_csv_bytes(expected):
                raise ValueError('El CSV no coincide con la instantánea reservada')
            digest = hashlib.sha256(content).hexdigest()
            old_hash, old_path = self.conn.execute('SELECT file_hash,file_path FROM export_batches WHERE batch_id=?', (batch_id,)).fetchone()
            states = {r[0] for r in self.conn.execute('SELECT delivery_state FROM export_items WHERE batch_id=?', (batch_id,))}
            if old_hash:
                if (old_hash, old_path) != (digest, str(path)) or 'RESERVED' in states:
                    raise ValueError('Publicación incompatible con el lote existente')
                return
            if states != {'RESERVED'}:
                raise ValueError('El lote no está reservado')
            changed = self.conn.execute("UPDATE export_items SET delivery_state='EXPORTED' WHERE batch_id=? AND delivery_state='RESERVED'", (batch_id,))
            if changed.rowcount != len(expected):
                raise ValueError('Cantidad de fracciones inconsistente')
            self.conn.execute('UPDATE export_batches SET file_hash=?,file_path=? WHERE batch_id=?', (digest, str(path), batch_id))

    def confirm_import(self, batch_id, successful_refs, failed_allocs):
        if set(successful_refs).intersection(failed_allocs):
            raise ValueError('Éxitos y fallos deben ser disjuntos')
        if any(not isinstance(ref, str) or not ref.strip() for ref in successful_refs.values()):
            raise ValueError('Referencia vacía o inválida')
        with self._atomic():
            for alloc_id, ref in successful_refs.items():
                row = self.conn.execute('SELECT delivery_state,appfolio_reference FROM export_items WHERE batch_id=? AND allocation_id=?', (batch_id, alloc_id)).fetchone()
                if row == ('IMPORTED', ref):
                    continue
                changed = self.conn.execute("UPDATE export_items SET delivery_state='IMPORTED',appfolio_reference=? WHERE batch_id=? AND allocation_id=? AND delivery_state='EXPORTED'", (ref, batch_id, alloc_id))
                if changed.rowcount != 1:
                    raise ValueError('Fracción inexistente o transición inválida')
            for alloc_id in set(failed_allocs):
                row = self.conn.execute('SELECT delivery_state FROM export_items WHERE batch_id=? AND allocation_id=?', (batch_id, alloc_id)).fetchone()
                if row == ('IMPORT_FAILED_REVIEW',):
                    continue
                changed = self.conn.execute("UPDATE export_items SET delivery_state='IMPORT_FAILED_REVIEW' WHERE batch_id=? AND allocation_id=? AND delivery_state='EXPORTED'", (batch_id, alloc_id))
                if changed.rowcount != 1:
                    raise ValueError('Fracción inexistente o transición inválida')
