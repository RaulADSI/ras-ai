"""Conciliación de registros normalizados; no lee archivos ni cambia el orquestador."""
from collections import Counter
from datetime import date
from difflib import SequenceMatcher
import math
import sqlite3


def normalize_vendor(value):
    return ' '.join(value.upper().split())


class ReconciliationEngine:
    """La clasificación es un mapa allocation_id -> campos contables ya resueltos.

    Las facturas requieren invoice_id, amount_cents, currency, date, vendor_name.
    bank_reference es opcional y debe ser referencia bancaria comparable, no el
    número interno de factura. allow_fuzzy se habilita explícitamente.
    """
    def __init__(self, ledger, window_days=5, min_score=90, allow_fuzzy=False):
        if type(window_days) is not int or window_days < 0 or not 90 <= min_score <= 100:
            raise ValueError('Ventana o umbral inválidos')
        self.ledger = ledger
        self.window_days = window_days
        self.min_score = min_score
        self.allow_fuzzy = allow_fuzzy

    def run(self, invoices, classifications):
        invoices = [dict(i) for i in invoices]
        ids = set()
        # Validar todo el libro antes de modificar cualquier fracción.
        for invoice in invoices:
            for field in ('invoice_id','vendor_name','currency','date'):
                if not isinstance(invoice.get(field), str) or not invoice[field].strip():
                    raise ValueError(f'Factura incompleta: {field}')
            invoice['invoice_id'] = invoice['invoice_id'].strip()
            if invoice['invoice_id'] in ids or type(invoice.get('amount_cents')) is not int:
                raise ValueError('ID de factura duplicado o importe no entero')
            if len(invoice['currency']) != 3 or not invoice['currency'].isalpha() or not invoice['currency'].isupper():
                raise ValueError('Moneda inválida')
            invoice['_date'] = date.fromisoformat(invoice['date'])
            if not isinstance(invoice.get('bank_reference', ''), str):
                raise ValueError('Referencia bancaria inválida')
            ids.add(invoice['invoice_id'])
        pending = self.ledger.pending_allocations()
        consumed = self.ledger.consumed_invoice_ids()
        plans = []
        for allocation in pending:
            classification = dict(classifications.get(allocation['id'], {}))
            vendor = classification.get('vendor_name', allocation['vendor_name'])
            reason, candidates = None, []
            score = classification.get('confidence', 100)
            if not isinstance(score, (int,float)) or isinstance(score,bool) or not math.isfinite(score) or not self.min_score <= score <= 100:
                reason = 'LOW_CLASSIFICATION_CONFIDENCE'
            if not isinstance(vendor, str) or not vendor.strip() or any(x in vendor.upper() for x in ('UNKNOWN','REVISAR')):
                reason = 'MISSING_VENDOR'
            if reason is None:
                for invoice in invoices:
                    if invoice['amount_cents'] != allocation['amount_cents'] or invoice['currency'] != allocation['currency']:
                        continue
                    days = abs((invoice['_date'] - date.fromisoformat(allocation['original_date'])).days)
                    if days > self.window_days:
                        continue
                    exact = normalize_vendor(vendor) == normalize_vendor(invoice['vendor_name'])
                    similarity = 100 if exact else 100 * SequenceMatcher(None, normalize_vendor(vendor), normalize_vendor(invoice['vendor_name'])).ratio()
                    if similarity < 60:
                        continue
                    reference = invoice.get('bank_reference','').strip()
                    ref_conflict = reference and reference != allocation['bank_reference']
                    candidates.append((invoice, similarity, days, exact, bool(ref_conflict)))
            plans.append((allocation,classification,reason,candidates))
        # Evitar elegir arbitrariamente entre dos cargos que reclaman la misma factura.
        claims = Counter(c[0]['invoice_id'] for _,_,_,candidates in plans for c in candidates)
        results = {}
        fields = ('property_code','vendor_name','gl_account','cash_account','description')
        for allocation, classification, reason, candidates in plans:
            allocation_id = allocation['id']
            if reason is None and candidates:
                if len(candidates) != 1:
                    reason = 'MULTIPLE_INVOICE_CANDIDATES'
                else:
                    invoice, score, days, exact, conflict = candidates[0]
                    invoice_id = invoice['invoice_id']
                    if invoice_id in consumed:
                        reason = 'INVOICE_ALREADY_CONSUMED'
                    elif claims[invoice_id] > 1:
                        reason = 'MULTIPLE_ALLOCATION_CANDIDATES'
                    elif conflict:
                        reason = 'REFERENCE_CONFLICT'
                    elif score < self.min_score or (not exact and not self.allow_fuzzy):
                        reason = 'UNCERTAIN_VENDOR'
                    else:
                        details = dict(invoice_id=invoice_id,amount_cents=allocation['amount_cents'],
                            currency=allocation['currency'],vendor_original=invoice['vendor_name'],
                            vendor_resolved=classification.get('vendor_name',allocation['vendor_name']),
                            method='exact' if exact else 'sequence_similarity',score=score,days=days)
                        try:
                            self.ledger.match_invoice(allocation_id, invoice_id, details)
                        except sqlite3.IntegrityError:
                            # Otra conexión pudo consumir la factura después de leer el pool.
                            if invoice_id not in self.ledger.consumed_invoice_ids():
                                raise
                            reason = 'INVOICE_ALREADY_CONSUMED'
                        else:
                            consumed.add(invoice_id)
                            results[allocation_id] = 'MATCHED_EXISTING'
                            continue
            if reason is None:
                values = {key: classification.get(key, allocation[key]) for key in fields}
                try:
                    self.ledger.validate_classification(**values)
                except ValueError:
                    reason = 'INCOMPLETE_CLASSIFICATION'
                else:
                    self.ledger.classify_allocation(allocation_id, **values)
                    results[allocation_id] = 'READY_TO_IMPORT'
                    continue
            self.ledger.create_exception(allocation_id, f'reconciliation_{allocation_id}', reason,
                dict(candidate_invoice_ids=[c[0]['invoice_id'] for c in candidates], classification=classification))
            results[allocation_id] = 'REVIEW_REQUIRED'
        return results
