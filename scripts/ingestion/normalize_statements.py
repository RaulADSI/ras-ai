import pandas as pd
import sys
import os
import re

# Import normalize
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from scripts.utils.text_cleaning import normalize_vendor as normalize

  # Delete city suffixes 
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
    
    # ✨ IMPROVEMENT: Use the pre-sorted city list.
    cities_to_remove = SORTED_DEFAULT_CITIES
    if remove_cities: # Allow override if a custom list is provided
        cities_to_remove = sorted(set(c.upper() for c in remove_cities), key=lambda x: -len(x))

    for city in cities_to_remove:
        if up.endswith(city):
            up = up[:-len(city)].strip(' ,-')
            break

    return up.strip()
   

# load data
df = pd.read_csv("data/raw/amex_statement.csv")

# Advance cleaning
df['raw_merchant'] = df['merchant'].fillna("").astype(str).str.strip()
df['merchant_clean'] = df['raw_merchant'].apply(clean_merchant)
df['normalized_merchant'] = df['merchant_clean'].apply(normalize)

# --- Company y GL Account ---
df['raw_company'] = df['company'].fillna("").astype(str).str.strip()
df['normalized_company'] = df['raw_company'].apply(normalize)

df['raw_gl_account'] = df['gl_account'].fillna("").astype(str).str.strip()
df['normalized_gl_account'] = df['raw_gl_account'].apply(normalize)

# Reorder column 
df_final = df[['date','merchant','amount','company','gl_account',
               'raw_merchant','merchant_clean','normalized_merchant',
               'raw_company','normalized_company',
               'raw_gl_account','normalized_gl_account']]

# Save results to CSV
output_path = "data/clean/normalized_statement_amex.csv"
df_final.to_csv(output_path, index=False)
print(f"\n✅ Archivo generado en {output_path}")
