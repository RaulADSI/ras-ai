"""Aplica el contrato previo al CSV adaptado, con auditoría sin perder eventos."""
from dataclasses import asdict
from scripts.vendor_aliases import lookup_vendor_alias
from decimal import Decimal, ROUND_HALF_EVEN, localcontext
import hashlib
from pathlib import Path
import pandas as pd

from scripts.ingestion.normalize_amex import apply_business_rules
from scripts.persistence.ledger import parse_to_cents
from scripts.rules_manager import normalize_text


def apply_legacy_business_rules(adapted, rules):
    contexts = {c['provenance_hash']: c for c in adapted['contexts']}
    source = pd.DataFrame([dict(c, gl_account=c['gl_hint']) for c in contexts.values()])
    selected = set(apply_business_rules(source)['provenance_hash']) if not source.empty else set()
    accepted, excluded, details = [], [], []
    reverse = {normalize_text(prop): group for group, props in rules.property_groups.items() for prop,_ in props}
    for record in adapted['records']:
        context = contexts[record['provenance_hash']]
        cents = parse_to_cents(record['amount_str'])
        if record['provenance_hash'] not in selected:
            reason = 'RICHARD_HAPPY_TRAILERS' if 'RICHARD LIBUTTI' in context['account_holder'].upper() and 'HAPPY TRAILERS' in context['company'].upper() else 'OUTSIDE_LEGACY_RAS_SCOPE'
            excluded.append(dict(record=record, context=context, amount_cents=cents, reason=reason))
            continue
        accepted.append(record)
        status, note = rules.evaluate_row_alerts(dict(context, gl_account=context['gl_hint']))
        vendor, vendor_score, vendor_method = rules.resolve_vendor(context['merchant'])
        hint = context['gl_hint'] if context['gl_hint'].strip() else context['company']
        prop, prop_score, prop_method = rules.resolve_property(hint, merchant=context['merchant'], gl_account=context['gl_hint'])
        gl = rules.resolve_gl(vendor)
        cash = rules.resolve_cash_account('amex')
        flags = []
        if normalize_text(vendor) not in rules.vendor_to_gl:
            flags.append('DEFAULT_GL')
        if 'amex' not in rules.cash_accounts:
            flags.append('DEFAULT_CASH')
        if vendor_score < 90: flags.append('LOW_VENDOR_CONFIDENCE')
        if prop_score < 90 or prop.startswith('REVISAR PROP:'): flags.append('UNRESOLVED_PROPERTY')
        if status not in ('OK','ALERT'): flags.append('BUSINESS_EXCEPTION')
        group = rules.property_groups.get(normalize_text(prop)) or rules.property_groups.get(reverse.get(normalize_text(prop)))
        fractions = []
        if group:
            weights = [Decimal(str(weight)) for _,weight in group]
            if any(not w.is_finite() or w <= 0 for w in weights):
                raise ValueError('Pesos de prorrateo inválidos')
            allocated = 0
            with localcontext() as ctx:
                ctx.prec = 50
                total = sum(weights)
                for index, ((property_code,_),weight) in enumerate(zip(group,weights)):
                    amount = cents-allocated if index == len(group)-1 else int((Decimal(cents)*weight/total).to_integral_value(rounding=ROUND_HALF_EVEN))
                    allocated += amount
                    fractions.append(dict(property_code=property_code,amount_cents=amount))
        else:
            fractions = [dict(property_code=prop,amount_cents=cents)]
        if sum(f['amount_cents'] for f in fractions) != cents:
            raise ValueError('Prorrateo no conserva importe')
        details.append(dict(provenance_hash=record['provenance_hash'], row_number=record['row_number'],
            date=record['original_date'], amount_cents=cents, merchant_original=context['merchant'],
            vendor_name=vendor, vendor_score=vendor_score,vendor_method=vendor_method,
            property_hint=hint,property_resolved=prop,property_score=prop_score,property_method=prop_method,
            gl_account=gl,cash_account=cash,alert_status=status,alert_note=note,flags=flags,fractions=fractions))
        alias = lookup_vendor_alias(context['merchant'])
        if alias:
            details[-1]['merchant_identity'] = asdict(alias)
    # Detectar el neteo previo sin borrar sus eventos ni su procedencia.
    groups = {}
    for detail in details:
        key=(detail['date'],detail['merchant_original'],detail['vendor_name'],detail['property_resolved'],abs(detail['amount_cents']),detail['alert_status'])
        groups.setdefault(key,[]).append(detail)
    netting=[]
    for group in groups.values():
        if any(d['amount_cents']>0 for d in group) and any(d['amount_cents']<0 for d in group):
            netting.append(dict(provenance_hashes=[d['provenance_hash'] for d in group], net_cents=sum(d['amount_cents'] for d in group)))
            for detail in group: detail['flags'].append('NETTING_REQUIRES_LINKED_EVENTS')
    total=sum(parse_to_cents(r['amount_str']) for r in adapted['records'])
    accepted_total=sum(d['amount_cents'] for d in details)
    excluded_total=sum(d['amount_cents'] for d in excluded)
    rules_path = getattr(rules,'rules_path',None)
    return dict(records=accepted,details=details,excluded=excluded,netting=netting,
        summary=dict(source_rows=len(adapted['records']),included_rows=len(accepted),excluded_rows=len(excluded),
            source_total_cents=total,included_total_cents=accepted_total,excluded_total_cents=excluded_total,
            control_valid=total==accepted_total+excluded_total,
            proposed_allocation_rows=sum(len(d['fractions']) for d in details),
            transactions_with_proration=sum(len(d['fractions'])>1 for d in details),
            transactions_with_flags=sum(bool(d['flags']) for d in details),
            netting_groups=len(netting), rules_sha256=hashlib.sha256(Path(rules_path).read_bytes()).hexdigest() if rules_path else None))
