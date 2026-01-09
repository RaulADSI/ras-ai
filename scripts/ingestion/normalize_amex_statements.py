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

# --------------------------------------------------
# CONSTANTES Y LIMPIEZA

DEFAULT_CITIES = ['MIAMI', 'HIALEAH', 'OPA LOCKA', 'NORTH MIAMI', 'CORAL GABLES', 'SUNRISE', 'DAVIE', 'FORT LAUDERDALE', 'HOLLYWOOD', 'MIAMI BEACH', 'WESTON', 'POMPANO BEACH', 'LAUDERDALE', 'KENDALL', 'DORAL']
SORTED_DEFAULT_CITIES = sorted(set(c.upper() for c in DEFAULT_CITIES), key=lambda x: -len(x))

def clean_merchant(text: str) -> str:
    if not text: return ""
    t = str(text).upper()
    for city in SORTED_DEFAULT_CITIES:
        t = re.sub(rf"\b{re.escape(city)}\b", "", t)
    t = re.sub(r"\b\d{4,}\b", "", t)
    t = re.sub(r"[^\w\s]", " ", t)
    return re.sub(r"\s+", " ", t).strip()

# --------------------------------------------------
# NUEVA LÓGICA DE NEGOCIO (Richard Libutti / RAS / GL)
# --------------------------------------------------
def apply_business_rules(df: pd.DataFrame) -> pd.DataFrame:
    def validate_row(row):
        acc = str(row.get('account_holder', '')).upper()
        comp = str(row.get('company', '')).upper()
        gl = str(row.get('gl_account', '')).upper()

        status = "KEEP"
        notes = []

        is_armando = "ARMANDO ARMAS" in acc
        is_richard = "RICHARD LIBUTTI" in acc
        is_ras = "RAS" in comp or "RAS" in gl

        # --------------------------------------------------
        # 1. EXCEPCIÓN CRÍTICA (máxima prioridad)
        # ------------------------------------------------- 
        if is_richard and "HAPPY TRAILERS" in comp:
            status = "EXCEPTION"
            notes.append("Error: Richard Libutti no opera Happy Trailers")

        # --------------------------------------------------
        # 2. ALERTA CONTABLE (no degrada EXCEPTION)
        # --------------------------------------------------
        if status != "EXCEPTION" and "RR REITER REALTY" in comp:
            if not is_ras:
                status = "ALERT"
                notes.append("Validación requerida: RR Reiter pagado sin RAS")

        # --------------------------------------------------
        # 3. FUENTES VÁLIDAS (solo afecta KEEP → SKIP)
        # --------------------------------------------------
        is_valid_source = is_armando or is_richard or is_ras

        if status == "KEEP" and not is_valid_source:
            status = "SKIP"

        return pd.Series([status, "; ".join(notes)])

    df = df.copy()
    df[['validation_status', 'business_notes']] = df.apply(validate_row, axis=1)

    # Conservamos todo excepto SKIP
    return df[df['validation_status'] != "SKIP"]


def main():
    input_folder = "data/raw/unify_all_amex/"
    files = glob.glob(os.path.join(input_folder, "*.csv")) + \
            glob.glob(os.path.join(input_folder, "*.xlsx"))

    if not files: return

    mapping = {
        'Description': 'merchant', 'Merchant': 'merchant',
        'Debit': 'amount', 'Amount': 'amount', 'Charge': 'amount',
        'Date': 'date', 'Account': 'account_holder',
        'Company': 'company', 'GL': 'gl_account'
    }

    combined = []
    for f in files:
        temp_df = pd.read_excel(f) if f.lower().endswith(".xlsx") else pd.read_csv(f)
        if temp_df.columns[0].startswith('Unnamed'):
            temp_df = temp_df.rename(columns={temp_df.columns[0]: 'card_id'})
        temp_df = temp_df.rename(columns=mapping)
        temp_df['source_file'] = os.path.basename(f)
        combined.append(temp_df)

    df = pd.concat(combined, ignore_index=True)

    # --- PASO 1: LIMPIEZA DE MONTOS ---
    df['amount'] = df['amount'].astype(str).replace({'\$': '', ',': '', '\(': '-', '\)': ''}, regex=True).astype(float)

    # --- PASO 2: DEDUPLICACIÓN (CON CONTADOR PARA TRANSACCIONES LEGÍTIMAS) ---
    id_col = 'card_id' if 'card_id' in df.columns else 'account_holder'
    df['occurrence'] = df.groupby(['source_file', 'date', 'amount', 'merchant', id_col]).cumcount()
    df['txn_id'] = df['date'].astype(str) + df['amount'].astype(str) + df['merchant'].astype(str) + df[id_col].astype(str) + df['occurrence'].astype(str)
    
    df = df.drop_duplicates(subset=['txn_id'], keep='first').drop(columns=['txn_id', 'occurrence'])

    # --- PASO 3: APLICAR REGLAS DE NEGOCIO (RICHARD/ARMANDO/GL) ---
    df = apply_business_rules(df)

    # --- PASO 4: NORMALIZACIÓN ---
    df['normalized_merchant'] = df['merchant'].apply(clean_merchant).apply(normalize)
    
    # --- PASO 5: EXPORTACIÓN ---
    output_path = "data/clean/normalized_amex.csv"
    df.to_csv(output_path, index=False)
    
    print(f"\n--- Reporte de Validación ---")
    print(f"Excepciones halladas: {len(df[df['validation_status'] == 'EXCEPTION'])}")
    print(f"Alertas RR Reiter: {len(df[df['validation_status'] == 'ALERT'])}")
    print(f"Total procesado: {len(df)}")

if __name__ == "__main__":
    main()