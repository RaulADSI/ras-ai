import pandas as pd
import sys
import os
import re
import glob

# 1. Configuración dinámica del project root
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

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
    input_folder = "data/raw/unify_all_amex/"
    files = glob.glob(os.path.join(input_folder, "*.csv")) + \
            glob.glob(os.path.join(input_folder, "*.xlsx"))

    if not files:
        print(f"No files found in {input_folder}")
        return

    mapping = {
        'Description': 'merchant', 'Merchant': 'merchant',
        'Debit': 'amount', 'Amount': 'amount', 'Charge': 'amount',
        'Date': 'date', 'Account': 'account_holder',
        'Company': 'company', 'GL': 'gl_account'
    }

    combined = []
    for f in files:
        if f.lower().endswith(".xlsx"):
            temp_df = pd.read_excel(f)
        else:
            temp_df = pd.read_csv(f)
        
        # --- IDENTIFICAR COLUMNA DE ID DE TARJETA (SIN NOMBRE) ---
        # Si la primera columna no tiene nombre, la llamamos 'card_id'
        if temp_df.columns[0].startswith('Unnamed'):
            temp_df = temp_df.rename(columns={temp_df.columns[0]: 'card_id'})
        
        temp_df = temp_df.rename(columns=mapping)
        temp_df['source_file'] = os.path.basename(f)
        combined.append(temp_df)

    df = pd.concat(combined, ignore_index=True)

    # --- PASO 1: LIMPIEZA DE MONTOS ---
    df['amount'] = (
        df['amount'].astype(str)
        .replace({'\$': '', ',': '', '\(': '-', '\)': ''}, regex=True)
        .astype(float)
    )

    # --- PASO 2: DEDUPLICACIÓN INTELIGENTE (CROSS-FILE) ---
    # Creamos una huella digital combinando fecha, monto, comercio y el ID de tarjeta (-21062, etc)
    # Si no existe card_id, usamos el account_holder como respaldo.
    id_col = 'card_id' if 'card_id' in df.columns else 'account_holder'
    df['occurrence'] = df.groupby(['source_file', 'date', 'amount', 'merchant', id_col]).cumcount()
    
    df['txn_fingerprint'] = (
        df['date'].astype(str) + 
        df['amount'].astype(str) + 
        df['merchant'].astype(str).str[:20] + 
        df[id_col].astype(str) +
        df['occurrence'].astype(str)
    )

    antes = len(df)
    # Eliminamos duplicados exactos que vienen de archivos diferentes
    df = df.drop_duplicates(subset=['txn_fingerprint'], keep='first')
    print(f"Registros duplicados por cruce de archivos eliminados: {antes - len(df)}")
    df = df.drop(columns=['txn_fingerprint', 'occurrence'])

    # --- PASO 3: FILTRADO DE NEGOCIO ---
    df = filter_amex_logic(df)

    # --- PASO 4: NORMALIZACIÓN ---
    df['normalized_merchant'] = df['merchant'].apply(clean_merchant).apply(normalize)
    
    # Limpieza final de columnas técnicas
    if 'txn_fingerprint' in df.columns:
        df = df.drop(columns=['txn_fingerprint'])

    # --- PASO 5: EXPORTACIÓN ---
    output_path = "data/clean/normalized_amex.csv"
    df.to_csv(output_path, index=False)
    
    print(f"\n--- Reporte Final ---")
    print(f"Total transacciones únicas: {len(df)}")
    print(f"Monto Neto Total: ${df['amount'].sum():,.2f}")

if __name__ == "__main__":
    main()