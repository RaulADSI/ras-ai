"""
scripts/reconciliation/dedup_appfolio.py
Conciliación y deduplicación contra el libro mayor (vendor_ledger.csv) de AppFolio.
"""

import re
import pandas as pd
from typing import Dict, Any, Set
from scripts.config import (
    VENDOR_LEDGER,
    AMEX_NORMALIZED,
    CITI_NORMALIZED,
    AMEX_NETTED,
    CITI_NETTED,
    UNMATCHED_LEDGER_REPORT
)
from scripts.rules_manager import RulesManager

AMOUNT_TOLERANCE = 0.01


def clean_tokens(text: object) -> Set[str]:
    if pd.isna(text):
        return set()
    clean = re.sub(r"[^\w\s]", " ", str(text).upper())
    tokens = set(clean.split())
    noise = {"THE", "INC", "CORP", "LLC", "CO", "AND", "DE", "LA", "LAS", "LOS", "SERVICES", "GROUP"}
    return tokens - noise


def safe_clean_currency(series: pd.Series) -> pd.Series:
    return (
        pd.to_numeric(
            series.astype(str).replace(r"[^\d\.-]", "", regex=True),
            errors="coerce"
        ).fillna(0.0).round(2)
    )


def deduplicate_card_against_ledger(card_df: pd.DataFrame, ledger_df: pd.DataFrame, vendor_key: str) -> Set[int]:
    to_remove = set()
    card_tokens = clean_tokens(vendor_key)
    if not card_tokens:
        return to_remove

    def is_vendor_match(row):
        ledger_v_tokens = clean_tokens(row["vendor"])
        ledger_d_tokens = clean_tokens(row["desc_clean"])
        return bool(card_tokens.intersection(ledger_v_tokens | ledger_d_tokens))

    bills = ledger_df[ledger_df.apply(is_vendor_match, axis=1)].copy()
    if bills.empty:
        return to_remove

    first_token = list(card_tokens)[0]
    card_vendor = card_df[card_df["vendor_resolved"].str.upper().str.contains(first_token, na=False, regex=False)].copy()
    if card_vendor.empty:
        return to_remove

    bills["reference_clean"] = bills["reference"].astype(str).str.strip()
    valid_ref_mask = (
        (bills["reference_clean"].str.len() > 1) & 
        (~bills["reference_clean"].str.lower().isin(["nan", "0", "00", "none", "null", ""]))
    )

    referenced_bills = bills[valid_ref_mask].copy()
    unreferenced_bills = bills[~valid_ref_mask].copy()

    logical_invoices = []
    if not referenced_bills.empty:
        for ref, group in referenced_bills.groupby("reference_clean"):
            logical_invoices.append({
                "amount": abs(group["unpaid_clean"].sum()),
                "original_indices": group.index.tolist()
            })

    for idx, row in unreferenced_bills.iterrows():
        logical_invoices.append({
            "amount": abs(row["unpaid_clean"]),
            "original_indices": [idx]
        })

    for inv in logical_invoices:
        inv_amount = inv["amount"]
        matches = card_vendor[
            (~card_vendor.index.isin(to_remove)) & 
            (abs(card_vendor["amount"].abs() - inv_amount) <= AMOUNT_TOLERANCE)
        ].sort_values("date")

        if not matches.empty:
            match_idx = matches.index[0]
            to_remove.add(match_idx)
            ledger_df.loc[inv["original_indices"], "matched_to_card"] = True

    return to_remove


def run(rules: RulesManager) -> Dict[str, Any]:
    """Función de entrada llamada por run_pipeline.py."""
    if not VENDOR_LEDGER.exists():
        # Si no hay ledger, transferir directamente normalizados a neteados
        for src, dst in [(AMEX_NORMALIZED, AMEX_NETTED), (CITI_NORMALIZED, CITI_NETTED)]:
            if src.exists():
                pd.read_csv(src).to_csv(dst, index=False, encoding="utf-8-sig")
        return {"amex_duplicates": 0, "citi_duplicates": 0}

    ledger = pd.read_csv(VENDOR_LEDGER)
    ledger.columns = ledger.columns.str.strip().str.lower()
    
    # Normalización de nombres de columnas del Ledger
    for col in ledger.columns:
        if "payee" in col or "vendor" in col or "name" in col:
            ledger = ledger.rename(columns={col: "vendor"})
            break

    ledger["vendor"] = ledger.get("vendor", ledger.get("description", "")).astype(str).str.strip()
    ledger["desc_clean"] = ledger.get("description", "").astype(str).str.upper()
    ledger["unpaid_clean"] = safe_clean_currency(ledger.get("unpaid", pd.Series(0, index=ledger.index)))
    ledger["reference"] = ledger.get("reference", "")
    
    # Filtrar deuda viva
    ledger = ledger[ledger["unpaid_clean"] > 0].copy()
    ledger["matched_to_card"] = False

    jobs = [
        ("amex", AMEX_NORMALIZED, AMEX_NETTED),
        ("citi", CITI_NORMALIZED, CITI_NETTED)
    ]

    metrics = {}

    for label, in_path, out_path in jobs:
        if not in_path.exists():
            continue

        card = pd.read_csv(in_path)
        if card.empty:
            card.to_csv(out_path, index=False, encoding="utf-8-sig")
            metrics[f"{label}_duplicates"] = 0
            continue

        # Resolver proveedor temporalmente con RulesManager para el cruce
        card["vendor_resolved"] = card["merchant"].apply(lambda m: rules.resolve_vendor(m)[0])

        unique_vendors = card["vendor_resolved"].dropna().unique()
        all_to_remove = set()

        for v_key in unique_vendors:
            dups = deduplicate_card_against_ledger(card, ledger, v_key)
            all_to_remove.update(dups)

        metrics[f"{label}_duplicates"] = len(all_to_remove)

        # Guardar dataset neto libre de duplicados
        net_df = card[~card.index.isin(all_to_remove)].drop(columns=["vendor_resolved"]).reset_index(drop=True)
        net_df.to_csv(out_path, index=False, encoding="utf-8-sig")

    # Guardar reporte de facturas no encontradas en AppFolio
    unmatched = ledger[~ledger["matched_to_card"]].drop(columns=["desc_clean", "unpaid_clean", "matched_to_card"], errors="ignore")
    unmatched.to_csv(UNMATCHED_LEDGER_REPORT, index=False, encoding="utf-8-sig")

    return metrics