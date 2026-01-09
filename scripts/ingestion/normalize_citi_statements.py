import pandas as pd
import sys
import os
import re

# 1. Configurar la ruta raíz del proyecto dinámicamente
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from scripts.utils.text_cleaning import normalize_vendor as normalize

def main():
    input_path = "data/raw/citi_card_statement.csv"
    if not os.path.exists(input_path):
        print(f"File not found: {input_path}")
        return

    df = pd.read_csv(input_path)

    column_mapping = {
        'Date': 'date',
        'Description': 'merchant',
        'Debit': 'amount',
        'Company': 'company'
    }

    missing = set(column_mapping) - set(df.columns)
    if missing:
        raise ValueError(f"Missing expected columns: {missing}")

    df = df.rename(columns=column_mapping)

    df = df[df['company'].astype(str).str.upper() == "RAS"].copy()

    df['amount'] = (
        df['amount']
        .astype(str)
        .replace({'\$': '', ',': '', '\(': '-', '\)': ''}, regex=True)
        .astype(float)
        .abs()
    )

    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df = df[df['date'].notna()]

    df['normalized_merchant'] = df['merchant'].fillna("").apply(normalize)
    df = df[df['normalized_merchant'] != ""]

    output_path = "data/clean/normalized_citi.csv"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)

    print(f"Citi normalized successfully: {len(df)} rows saved to {output_path}")


if __name__ == "__main__":
    main()