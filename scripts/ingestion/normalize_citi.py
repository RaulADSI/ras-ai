"""
scripts/ingestion/normalize_citi.py
Estandariza y valida extractos de Citi Mastercard.
"""

import pandas as pd
from scripts.config import (
    CITI_RAW_INPUT,
    CITI_NORMALIZED,
    NORMALIZED_STATEMENT_SCHEMA
)
from scripts.rules_manager import RulesManager


def clean_currency(series: pd.Series) -> pd.Series:
    return (
        pd.to_numeric(
            series.astype(str).replace({r'\$': '', ',': '', r'\(': '-', r'\)': ''}, regex=True),
            errors='coerce'
        ).fillna(0.0)
    )


def run(rules: RulesManager = None) -> int:
    """Función de entrada llamada por run_pipeline.py."""
    if not CITI_RAW_INPUT.exists():
        empty_df = pd.DataFrame(columns=NORMALIZED_STATEMENT_SCHEMA)
        empty_df.to_csv(CITI_NORMALIZED, index=False, encoding="utf-8-sig")
        return 0

    df_raw = pd.read_csv(CITI_RAW_INPUT)
    raw_rows = len(df_raw)

    column_mapping = {
        'Date': 'date', 'Description': 'merchant',
        'Debit': 'debit', 'Credit': 'credit', 'Company': 'company'
    }

    df_calc = df_raw.rename(columns=column_mapping).copy()
    
    # Inicialización segura de débitos y créditos
    debit_series = df_calc['debit'] if 'debit' in df_calc.columns else pd.Series(0, index=df_calc.index)
    credit_series = df_calc['credit'] if 'credit' in df_calc.columns else pd.Series(0, index=df_calc.index)
    
    df_calc['amount'] = (clean_currency(debit_series) - clean_currency(credit_series)).round(2)

    # Filtrar solo segmento RAS
    comp_series = df_calc.get('company', pd.Series("", index=df_calc.index))
    df_ras = df_calc[comp_series.astype(str).str.upper() == "RAS"].copy()

    df_ras['date'] = pd.to_datetime(df_ras['date'], errors='coerce').dt.strftime("%Y-%m-%d")
    df_ras = df_ras[df_ras['date'].notna()].copy()

    # Columnas complementarias para cumplir el contrato canónico
    df_ras['account_holder'] = ""
    df_ras['gl_account'] = ""
    df_ras['merchant'] = df_ras.get('merchant', "").fillna("").astype(str)
    df_ras['company'] = df_ras.get('company', "").fillna("").astype(str)
    df_ras['property_hint'] = df_ras['company']

    final_citi = df_ras[NORMALIZED_STATEMENT_SCHEMA].copy()
    final_citi.to_csv(CITI_NORMALIZED, index=False, encoding="utf-8-sig")
    return raw_rows