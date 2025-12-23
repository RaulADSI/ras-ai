import pandas as pd
import sys
import os
import re


# Import normalize
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from scripts.utils.text_cleaning import normalize_vendor as normalize

# Constantes de limpieza
DEFAULT_CITIES = [
    'MIAMI', 'HIALEAH', 'OPA LOCKA', 'NORTH MIAMI', 'CORAL GABLES',
    'SUNRISE', 'DAVIE', 'FORT LAUDERDALE', 'HOLLYWOOD', 'MIAMI BEACH',
    'WESTON', 'POMPANO BEACH', 'LAUDERDALE', 'KENDALL', 'DORAL'
]

SORTED_DEFAULT_CITIES = sorted(
    set(c.upper() for c in DEFAULT_CITIES),
    key=lambda x: -len(x)
)

# Limpieza de merchant
def clean_merchant(text: str) -> str:
    """
    Limpieza técnica previa a la normalización:
    - Uppercase
    - Elimina ciudades conocidas
    - Elimina IDs / referencias largas
    - Limpia caracteres especiales
    """
    if not text:
        return ""

    t = str(text).upper()

    # Remover ciudades
    for city in SORTED_DEFAULT_CITIES:
        t = re.sub(rf"\b{re.escape(city)}\b", "", t)

    # Remover números largos (IDs, referencias)
    t = re.sub(r"\b\d{{4,}}\b", "", t)

    # Remover caracteres especiales
    t = re.sub(r"[^\w\s]", " ", t)

    # Normalizar espacios
    t = re.sub(r"\s+", " ", t)

    return t.strip()

# Reglas de negocio Amex

def filter_amex_transactions(df: pd.DataFrame) -> pd.DataFrame:
    """
    Reglas de oro Amex:
    1. Armando Armas -> Siempre RAS
    2. Richard Libutti -> Solo si ya viene como RAS
    3. Cory Reiter -> Solo si tiene RAS
    4. Lindsay Reiter -> Solo si tiene RAS
    5. Cualquier otro -> SKIP
    """

    def check_row(row):
        account = str(row.get('account_holder', '')).upper()
        company = str(row.get('company', '')).upper()

        if "ARMANDO ARMAS" in account:
            return "RAS"

        if "RICHARD LIBUTTI" in account:
            return "RAS" if "RAS" in company else "SKIP"
        
        if "CORY S REITER" in account:
            return "RAS" if "RAS" in company else "SKIP"
        
        if "LINDSAY REITER" in account: 
            return "RAS" if "RAS" in company else "SKIP"

        return "SKIP"

    df = df.copy()
    df['filter_status'] = df.apply(check_row, axis=1)

    df_filtered = df[df['filter_status'] == "RAS"].copy()
    return df_filtered.drop(columns=['filter_status'])

# Main

def main():
    input_path = "data/raw/amex_statement-12-22-25.csv"
    is_amex = "amex" in input_path.lower()

    df = pd.read_csv(input_path)

    # Limpiar la columna amount antes de procesar
    if 'amount' in df.columns:
        df['amount'] = (
            df['amount']
            .replace({'\$': '', ',': ''}, regex=True)
            .astype(float)
            .abs() # AppFolio requiere montos positivos
        )
    # Mapeo flexible de columnas
    column_mapping = {
        'Description': 'merchant',
        'Merchant': 'merchant',
        'Debit': 'amount',     # Común en Citi
        'Amount': 'amount',    # Común en Amex
        'Charge': 'amount',    # Otra variante de Amex
        'Date': 'date',
        'Account': 'account_holder',
        'Company': 'company',
        'GL': 'gl_account'
    }

    df = df.rename(columns=column_mapping)

    
    # Validaciones mínimas
    
    if 'merchant' not in df.columns:
        raise ValueError(
            "El archivo no contiene columnas 'Description' ni 'Merchant'"
        )

    if is_amex:
        print("Aplicando reglas de negocio Amex (Armando / Richard)...")
        df = filter_amex_transactions(df)

        if df.empty:
            print("No quedaron transacciones después del filtro Amex.")
            return

    # Limpieza y normalización
    df['raw_merchant'] = (
        df['merchant']
        .fillna("")
        .astype(str)
        .str.strip()
    )

    df['merchant_clean'] = df['raw_merchant'].apply(clean_merchant)
    df['normalized_merchant'] = df['merchant_clean'].apply(normalize)

    # Asegurar gl_account
    if 'gl_account' not in df.columns:
        df['gl_account'] = ""

    # Guardado
    output_name = "normalized_amex.csv" if is_amex else "normalized_citi.csv"
    output_path = f"data/clean/{output_name}"

    cols_to_save = [
        'date',
        'merchant',
        'amount',
        'company',
        'gl_account',
        'normalized_merchant',
        'account_holder'
    ]

    cols_existing = [c for c in cols_to_save if c in df.columns]

    df[cols_existing].to_csv(output_path, index=False)

    print(f"Proceso completado: {len(df)} transacciones guardadas en {output_path}")


if __name__ == "__main__":
    main()
