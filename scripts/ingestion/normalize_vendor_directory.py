import pandas as pd
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from scripts.utils.text_cleaning import normalize_vendor as normalize

# Load data
df = pd.read_excel(r"data/raw/rentify_entity_dictionary.xlsx", sheet_name="vendor_directory")

# Normalize text fields
df['raw_name'] = df['name'].fillna("").astype(str).str.strip()
df['normalized_name'] = df['raw_name'].apply(normalize)
df['normalized_company'] = df['company_name'].fillna("").apply(normalize)

# Reorder columns
df_final = df[['company_name', 'normalized_company', 'raw_name', 'normalized_name']]

# Save to CSV
df_final.to_csv("data/clean/normalized_vendor_directory.csv", index=False)

print("✅ Archivo generado en data/clean/normalized_vendor_directory.csv")



