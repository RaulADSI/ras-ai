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
    input_path = "data/raw/citi_card_statement.csv" # Ensure this path is correct
    
    if not os.path.exists(input_path):
        print(f"File not found: {input_path}")
        return

    df = pd.read_csv(input_path)

    # 3. Citi card mapping (watch columns names)
    column_mapping = {
        'Date': 'date',
        'Description': 'merchant',
        'Debit': 'amount',
        'Company': 'company'
    }
    df = df.rename(columns=column_mapping)

    # 4. Business rules Citi: RAS only
    if 'company' in df.columns:
        df = df[df['company'].astype(str).str.upper() == "RAS"].copy()

    # 5. Normalize amount 
    if 'amount' in df.columns:
        df['amount'] = (
            df['amount']
            .astype(str)
            .replace({'\$': '', ',': '', '\(': '-', '\)': ''}, regex=True)
            .astype(float)
            .abs()
        )

    # 6. Clean and normalize Merchant
    
    df['normalized_merchant'] = df['merchant'].fillna("").apply(normalize)

    # 7. Save to clean folder
    output_path = "data/clean/normalized_citi.csv"
    df.to_csv(output_path, index=False)
    print(f"Citi normalized successfully: {len(df)} rows saved to {output_path}")

if __name__ == "__main__":
    main()