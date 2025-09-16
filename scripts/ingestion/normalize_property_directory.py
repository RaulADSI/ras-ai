import pandas as pd
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from scripts.utils.text_cleaning import normalize

# Load data
df = pd.read_excel(r"data/raw/rentify_entity_dictionary.xlsx", sheet_name="property_directory")

# Normalize text fields
df['raw_property'] = df['property'].fillna("").astype(str).str.strip()
df['normalized_property'] = df['raw_property'].apply(normalize)

# Reorder columns
df_final = df[['raw_property', 'normalized_property']]

# Save to CSV
df_final.to_csv("data/clean/normalized_property_directory.csv", index=False)

print("✅ Archivo generado en data/clean/normalized_property_directory.csv")