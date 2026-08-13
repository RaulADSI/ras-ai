"""
scripts/output/bulk_bill_generator.py
Módulo generador de Bulk Bills finales y ejecutor de prorrateos ponderados.
"""

from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Tuple
import pandas as pd

from scripts.config import (
    AMEX_NETTED,
    CITI_NETTED,
    AMEX_BULK_BILL,
    CITI_BULK_BILL,
    LOGS_DIR,
    FINAL_APPFOLIO_COLUMNS
)
from scripts.rules_manager import RulesManager, normalize_text


def write_audit_log(df_errors: pd.DataFrame, log_filename: Path) -> None:
    if df_errors.empty:
        return
    df_errors = df_errors.copy()
    df_errors["processed_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    header = not log_filename.exists()
    df_errors.to_csv(log_filename, mode="a", index=False, header=header, encoding="utf-8-sig")


def split_allocations(df_netted: pd.DataFrame, property_groups: dict) -> pd.DataFrame:
    """Distribución de gastos con balanceo de centavos en la última propiedad."""
    if not property_groups or df_netted.empty:
        return df_netted

    property_to_group = {
        normalize_text(prop): grp 
        for grp, props in property_groups.items() 
        for prop, _ in props
    }

    expanded_rows = []
    for _, row in df_netted.iterrows():
        prop_str = str(row["resolved_property"]).strip()
        prop_norm = normalize_text(prop_str)
        
        target_group = property_groups.get(prop_norm) or property_groups.get(property_to_group.get(prop_norm))

        if target_group:
            total_weight = sum(w for _, w in target_group)
            if total_weight == 0:
                expanded_rows.append(row)
                continue

            total_amount = row["amount"]
            allocated_accum = 0.0

            for i, (g_prop, weight) in enumerate(target_group):
                new_row = row.copy()
                new_row["resolved_property"] = g_prop

                if i == len(target_group) - 1:
                    new_row["amount"] = round(total_amount - allocated_accum, 2)
                else:
                    split_amt = round(total_amount * (weight / total_weight), 2)
                    new_row["amount"] = split_amt
                    allocated_accum += split_amt

                expanded_rows.append(new_row)
        else:
            expanded_rows.append(row)

    return pd.DataFrame(expanded_rows)


def process_card_dataset(
    card_key: str, 
    input_file: Path, 
    output_file: Path, 
    rules: RulesManager, 
    audit_log: Path
) -> Tuple[int, int]:
    if not input_file.exists():
        return 0, 0

    df = pd.read_csv(input_file)
    if df.empty:
        pd.DataFrame(columns=FINAL_APPFOLIO_COLUMNS).to_csv(output_file, index=False, encoding="utf-8-sig")
        return 0, 0

    df.columns = df.columns.str.lower()

    # 1. Validaciones dinámicas (Alerts)
    validation_results = df.apply(rules.evaluate_row_alerts, axis=1)
    df["validation_status"] = [v[0] for v in validation_results]
    df["validation_note"] = [v[1] for v in validation_results]

    errors = df[df["validation_status"] != "OK"].copy()
    if not errors.empty:
        errors["card_type"] = card_key
        write_audit_log(errors, audit_log)

    df_valid = df[df["validation_status"].isin(["OK", "ALERT"])].copy()
    if df_valid.empty:
        pd.DataFrame(columns=FINAL_APPFOLIO_COLUMNS).to_csv(output_file, index=False, encoding="utf-8-sig")
        return 0, len(errors)

    # 2. Resolución Contextualizada de Entidades
    df_valid["resolved_vendor"] = df_valid["merchant"].apply(lambda m: rules.resolve_vendor(m)[0])
    df_valid["resolved_property"] = df_valid.apply(
        lambda r: rules.resolve_property(
            r.get("property_hint"),
            merchant=r.get("merchant"),
            gl_account=r.get("gl_account")
        )[0],
        axis=1
    )
    df_valid["abs_amount"] = df_valid["amount"].abs().round(2)

    # 3. Neteado de transacciones opuestas
    group_cols = ["date", "merchant", "resolved_vendor", "resolved_property", "abs_amount", "validation_status"]
    df_netted = df_valid.groupby(group_cols, as_index=False).agg({
        "amount": "sum",
        "validation_note": "first"
    })
    df_netted = df_netted[df_netted["amount"].round(2) != 0].copy()

    # 4. Prorrateos
    df_netted = split_allocations(df_netted, rules.property_groups)

    # 5. Ensamble Final con contrato estricto de AppFolio
    cash_account = rules.resolve_cash_account(card_key)

    final_df = pd.DataFrame({
        "Bill Property Code*": df_netted["resolved_property"],
        "Vendor Payee Name*": df_netted["resolved_vendor"],
        "Amount*": df_netted["amount"],
        "Bill Account*": df_netted["resolved_vendor"].apply(rules.resolve_gl),
        "Bill Date*": df_netted["date"],
        "Due Date*": df_netted["date"],
        "Posting Date": df_netted["date"],
        "Description": (
            card_key.upper() + " | " + 
            df_netted["merchant"].astype(str) + " | " + 
            df_netted["validation_status"].astype(str) + " " + 
            df_netted["validation_note"].fillna("").astype(str)
        ).str.strip(),
        "Cash Account": cash_account
    })

    final_df[FINAL_APPFOLIO_COLUMNS].to_csv(output_file, index=False, encoding="utf-8-sig")
    return len(final_df), len(errors)


def run_generation(rules: RulesManager, run_id: str) -> Dict[str, Any]:
    """Punto de entrada para run_pipeline.py."""
    audit_file = LOGS_DIR / f"audit_log_{datetime.now().strftime('%Y-%m')}.csv"

    amex_bills, amex_warnings = process_card_dataset("amex", AMEX_NETTED, AMEX_BULK_BILL, rules, audit_file)
    citi_bills, citi_warnings = process_card_dataset("mastercard", CITI_NETTED, CITI_BULK_BILL, rules, audit_file)

    return {
        "amex_bills": amex_bills,
        "citi_bills": citi_warnings,
        "warnings": amex_warnings + citi_warnings
    }