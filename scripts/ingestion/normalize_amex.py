"""
scripts/ingestion/normalize_amex.py
Estandariza y valida extractos crudos de American Express.
"""

import os
import glob
import pandas as pd

from scripts.config import (
    AMEX_RAW_DIR,
    AMEX_NORMALIZED,
    NORMALIZED_STATEMENT_SCHEMA
)
from scripts.rules_manager import RulesManager


CANONICAL_RAW_COLUMNS = ['date', 'merchant', 'account_holder', 'column', 'amount', 'company', 'gl_account']


def clean_currency(series: pd.Series) -> pd.Series:
    return (
        pd.to_numeric(
            series.astype(str).replace({r'\$': '', ',': '', r'\(': '-', r'\)': ''}, regex=True),
            errors='coerce'
        ).fillna(0.0).round(2)
    )


def load_amex_file(filepath: str) -> pd.DataFrame:
    ext = filepath.lower().split('.')[-1]
    raw = pd.read_excel(filepath, header=None) if ext in ["xlsx", "xls"] else pd.read_csv(filepath, header=None)

    header_row = None
    for i in range(min(15, len(raw))):
        row_str = " ".join(str(v).upper() for v in raw.iloc[i].values)
        if "DATE" in row_str and "AMOUNT" in row_str:
            header_row = i
            break

    if header_row is not None:
        df = pd.read_excel(filepath, header=header_row) if ext in ["xlsx", "xls"] else pd.read_csv(filepath, header=header_row)
    else:
        df = raw.copy()
        df.columns = CANONICAL_RAW_COLUMNS + [f"extra_{i}" for i in range(df.shape[1] - len(CANONICAL_RAW_COLUMNS))]

    df.columns = df.columns.str.lower().str.strip()
    return df


def apply_business_rules(df: pd.DataFrame) -> pd.DataFrame:
    def validate_row(row):
        acc = str(row.get('account_holder', '')).upper().strip()
        merc = str(row.get('merchant', '')).upper()
        comp = str(row.get('company', '')).upper()
        gl = str(row.get('gl_account', '')).upper()

        is_armando = "ARMANDO ARMAS" in acc or (acc in ["", "NAN"] and "ARMANDO ARMAS" in merc)
        is_richard = "RICHARD LIBUTTI" in acc or (acc in ["", "NAN"] and "RICHARD LIBUTTI" in merc)
        is_ras_marked = any(x in comp or x in gl for x in ["RAS", "REITER"])

        if is_richard and "HAPPY TRAILERS" in comp:
            return "SKIP"

        if is_ras_marked or is_armando or is_richard:
            return "KEEP"
        return "SKIP"

    df = df.copy()
    status = df.apply(validate_row, axis=1)
    return df[status == "KEEP"].copy()


def run(rules: RulesManager) -> int:
    """Función de entrada llamada por run_pipeline.py."""
    files = glob.glob(os.path.join(AMEX_RAW_DIR, "*.csv")) + glob.glob(os.path.join(AMEX_RAW_DIR, "*.xlsx"))
    if not files:
        empty_df = pd.DataFrame(columns=NORMALIZED_STATEMENT_SCHEMA)
        empty_df.to_csv(AMEX_NORMALIZED, index=False, encoding="utf-8-sig")
        return 0

    dfs = [load_amex_file(f) for f in files]
    amex = pd.concat(dfs, ignore_index=True)
    raw_rows = len(amex)

    # 1. Asegurar presencia y formato seguro de columnas requeridas
    for col in ["account_holder", "company", "gl_account", "merchant", "amount", "date"]:
        if col not in amex.columns:
            amex[col] = ""
        else:
            amex[col] = amex[col].fillna("").astype(str)

    amex["amount"] = clean_currency(amex["amount"])

    # 2. Deduplicación por transacción exacta
    key_cols = ["date", "merchant", "amount"]
    amex["occ"] = amex.groupby(key_cols).cumcount()
    amex["dedup_key"] = (
        amex["date"] + "|" +
        amex["merchant"] + "|" +
        amex["amount"].astype(str) + "|" +
        amex["occ"].astype(str)
    )
    amex = amex.drop_duplicates("dedup_key").copy()

    # 3. Filtro de negocio RAS
    amex = apply_business_rules(amex)

    # 4. Construcción canónica del schema
    amex["date"] = pd.to_datetime(amex["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    
    # Asignación contractual de property_hint
    gl_clean = amex["gl_account"].str.strip()
    amex["property_hint"] = amex["gl_account"].where(gl_clean != "", amex["company"])

    final_amex = amex[NORMALIZED_STATEMENT_SCHEMA].copy()
    final_amex.to_csv(AMEX_NORMALIZED, index=False, encoding="utf-8-sig")
    return raw_rows