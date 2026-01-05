import pandas as pd
import sys
import os
import re
import glob

# 1. Configurar la ruta raíz del proyecto dinámicamente
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# 2. Importar utilidades
from scripts.utils.text_cleaning import normalize_vendor as normalize

# Constantes de limpieza
DEFAULT_CITIES = [
    'MIAMI', 'HIALEAH', 'OPA LOCKA', 'NORTH MIAMI', 'CORAL GABLES',
    'SUNRISE', 'DAVIE', 'FORT LAUDERDALE', 'HOLLYWOOD', 'MIAMI BEACH',
    'WESTON', 'POMPANO BEACH', 'LAUDERDALE', 'KENDALL', 'DORAL'
]
SORTED_DEFAULT_CITIES = sorted(set(c.upper() for c in DEFAULT_CITIES), key=lambda x: -len(x))

def clean_merchant(text: str) -> str:
    if not text: return ""
    t = str(text).upper()
    for city in SORTED_DEFAULT_CITIES:
        t = re.sub(rf"\b{re.escape(city)}\b", "", t)
    t = re.sub(r"\b\d{4,}\b", "", t)
    t = re.sub(r"[^\w\s]", " ", t)
    return re.sub(r"\s+", " ", t).strip()

def filter_amex_logic(df: pd.DataFrame) -> pd.DataFrame:
    """Reglas: Armando siempre pasa. Lindsay/Cory/Ricky solo si es RAS."""
    def check_row(row):
        acc = str(row.get('account_holder', '')).upper()
        comp = str(row.get('company', '')).upper()
        if "ARMANDO ARMAS" in acc: return "RAS"
        team = ["LINDSAY REITER", "CORY S REITER", "RICHARD LIBUTTI", "RICKY"]
        if any(member in acc for member in team):
            return "RAS" if "RAS" in comp else "SKIP"
        return "SKIP"

    df = df.copy()
    df['filter_status'] = df.apply(check_row, axis=1)
    return df[df['filter_status'] == "RAS"].drop(columns=['filter_status'])

def main():
    # Carpeta de entrada para múltiples archivos Amex (Lindsay/Ricky)
    input_folder = "data/raw/pending_amex/"
    files = glob.glob(os.path.join(input_folder, "*.csv"))

    if not files:
        print(f"No se encontraron archivos en {input_folder}")
        return

    mapping = {
        'Description': 'merchant', 'Merchant': 'merchant',
        'Debit': 'amount', 'Amount': 'amount', 'Charge': 'amount',
        'Date': 'date', 'Account': 'account_holder',
        'Company': 'company', 'GL': 'gl_account'
    }

    combined_list = []
    for f in files:
        temp_df = pd.read_csv(f).rename(columns=mapping)
        combined_list.append(temp_df)

    df = pd.concat(combined_list, ignore_index=True)

    # Limpieza de montos
    df['amount'] = (
        df['amount'].astype(str)
        .replace({'\$': '', ',': '', '\(': '-', '\)': ''}, regex=True)
        .astype(float).abs()
    )

    # Filtrado y Normalización
    df = filter_amex_logic(df)
    df['normalized_merchant'] = df['merchant'].apply(clean_merchant).apply(normalize)
    
    output_path = "data/clean/normalized_amex.csv"
    df.to_csv(output_path, index=False)
    print(f"Amex consolidado y normalizado: {len(df)} transacciones en {output_path}")

if __name__ == "__main__":
    main()