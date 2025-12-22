import pandas as pd
import sys
import os
import re

# Import normalize
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from scripts.utils.text_cleaning import normalize_vendor as normalize

# Suffixes para limpieza
DEFAULT_CITIES = [
    'MIAMI', 'HIALEAH', 'OPA LOCKA', 'NORTH MIAMI', 'CORAL GABLES',
    'SUNRISE', 'DAVIE', 'FORT LAUDERDALE', 'HOLLYWOOD', 'MIAMI BEACH',
    'WESTON', 'POMPANO BEACH', 'LAUDERDALE', 'KENDALL', 'DORAL'
]
SORTED_DEFAULT_CITIES = sorted(set(c.upper() for c in DEFAULT_CITIES), key=lambda x: -len(x))

def clean_merchant(text: str, remove_cities: list | None = None) -> str:
    if not isinstance(text, str):
        return ""
    s = text.strip()
    s = re.sub(r'\s+', ' ', s)
    s = s.rstrip(' ,-')
    up = s.upper()
    up = re.sub(r'(?:,\s*|\s+)[A-Z]{2}$', '', up).strip()
    tokens = up.split()
    if len(tokens) >= 2:
        max_rep = min(4, len(tokens)//2)
        for n in range(1, max_rep+1):
            if tokens[-2*n:-n] == tokens[-n:]:
                tokens = tokens[:-n]
                up = ' '.join(tokens)
                break
    cities_to_remove = SORTED_DEFAULT_CITIES
    if remove_cities:
        cities_to_remove = sorted(set(c.upper() for c in remove_cities), key=lambda x: -len(x))
    for city in cities_to_remove:
        if up.endswith(city):
            up = up[:-len(city)].strip(' ,-')
            break
    return up.strip()

# 1. Cargar datos
# Asegúrate de que la ruta sea la correcta para tu archivo de Citi
input_path = "data/raw/amex_statement-12-22-25.csv" 
df = pd.read_csv(input_path)

# ✨ MEJORA: Mapeo automático de columnas para evitar el KeyError
column_mapping = {
    'Description': 'merchant',
    'Debit': 'amount',
    'Date': 'date',
    'Company': 'company',
    'GL': 'gl_account'
}

# Renombrar solo las columnas que existan en el archivo
df = df.rename(columns=column_mapping)

# Verificar si 'merchant' existe después del renombramiento
if 'merchant' not in df.columns:
    print(f"Error: No se encontró la columna de descripción. Columnas disponibles: {df.columns.tolist()}")
    sys.exit()

# 2. Limpieza avanzada (Ahora sí encontrará 'merchant' y 'amount')
df['raw_merchant'] = df['merchant'].fillna("").astype(str).str.strip()
df['merchant_clean'] = df['raw_merchant'].apply(clean_merchant)
df['normalized_merchant'] = df['merchant_clean'].apply(normalize)

# --- Company y GL Account ---
df['raw_company'] = df['company'].fillna("").astype(str).str.strip()
df['normalized_company'] = df['raw_company'].apply(normalize)

# Algunos statements de Citi no traen GL, por eso usamos get()

df['raw_gl_account'] = df.get('gl_account', pd.Series([""] * len(df))).fillna("").astype(str).str.strip()
df['normalized_gl_account'] = df['raw_gl_account'].apply(normalize)

# 3. Reordenar columnas para el archivo Clean
# Usamos una lista de columnas que sabemos que existen
cols_to_save = ['date', 'merchant', 'amount', 'company', 'gl_account', 'raw_merchant', 
                'merchant_clean', 'normalized_merchant', 'raw_company', 'normalized_company']

df_final = df[cols_to_save]

# 4. Guardar resultados
output_path = "data/clean/normalized_statement_citi.csv"
df_final.to_csv(output_path, index=False)
print(f"Archivo normalizado generado en: {output_path}")