import pandas as pd
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from scripts.utils.text_cleaning import normalize

# Load data
df = pd.read_excel(r"data/raw/rentify_entity_dictionary.xlsx", sheet_name="gl_accounts")

# Detect indentation and hierarchy
def detect_parent(row_index, df):
    current_indent = len(df.loc[row_index, 'gl_account']) - len(df.loc[row_index, 'gl_account'].lstrip())
    for i in range(row_index - 1, -1, -1):
        prev_indent = len(df.loc[i, 'gl_account']) - len(df.loc[i, 'gl_account'].lstrip())
        if prev_indent < current_indent:
            return df.loc[i, 'gl_account'].split(":")[0].strip()
    return ""

# Create columns
df['raw_name'] = df['gl_account'].apply(lambda x: x.split(":")[1].strip() if ":" in str(x) else str(x).strip())
df['gl_code'] = df['gl_account'].apply(lambda x: x.split(":")[0].strip() if ":" in str(x) else "")
df['normalized_name'] = df['raw_name'].apply(normalize)
df['parent_code'] = [detect_parent(i, df) for i in range(len(df))]

# Reorder using 'gl_type'
df_final = df[['gl_code', 'raw_name', 'normalized_name', 'gl_type', 'parent_code']]

# Save to CSV
df_final.to_csv(r"data/clean/normalized_gl_accounts.csv", index=False)

print("✅ Archivo generado en data/clean/normalized_gl_accounts.csv")
