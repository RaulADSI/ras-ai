import pandas as pd
from rapidfuzz import process, fuzz
import os
import sys

# --- Safely import normalize ---
try:
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
    from scripts.utils.text_cleaning import normalize
except (ImportError, ModuleNotFoundError):
    print("Warning: 'normalize' function not found. Proceeding with fallback.")
    def normalize(text):
        if not isinstance(text, str):
            return ""
        return text.lower().strip()

# --- Load CSVs ---
try:
    df = pd.read_csv("data/clean/normalized_statement_amex.csv")
    gl_accounts = pd.read_csv("data/clean/normalized_gl_accounts.csv")
    property_directory = pd.read_csv("data/clean/normalized_property_directory.csv")
    vendor_directory = pd.read_csv("data/clean/normalized_vendor_directory.csv")
except FileNotFoundError as e:
    print(f"Error: {e}. Please ensure all input CSV files are in the 'data/clean/' directory.")
    sys.exit()

# --- Helpers ---
def is_ambiguous(value):
    return pd.isna(value) or str(value).strip() == "" or "unknown" in str(value).lower()

def get_best_match(query, choices, score_cutoff=60):
    if is_ambiguous(query):
        return None, 0.0
    result = process.extractOne(str(query), choices, scorer=fuzz.token_set_ratio, score_cutoff=score_cutoff)
    if result is None:
        return None, 0.0
    match, score, _ = result
    return match, score

# --- Property Resolver ---
def resolve_property_code(company_name):
    if is_ambiguous(company_name):
        return company_name, 0  # keep original from statement

    # Exact match against normalized_property
    direct_match = property_directory.loc[
        property_directory["normalized_property"].str.lower() == str(company_name).lower(),
        "raw_property"
    ]
    if not direct_match.empty:
        return direct_match.values[0], 100

    # Fuzzy match against normalized_property
    choices = property_directory["normalized_property"].dropna().astype(str).tolist()
    result = process.extractOne(str(company_name), choices, scorer=fuzz.token_set_ratio)
    if result:
        match, score, _ = result
        if score >= 75:
            raw_match = property_directory.loc[
                property_directory["normalized_property"] == match,
                "raw_property"
            ].iloc[0]
            return raw_match, score
        else:
            # If score < 75, keep original
            return company_name, score

    # No match found, keep original
    return company_name, 0

# --- Vendor Resolver ---
def resolve_vendor(row):
    merchant = str(row.get("normalized_merchant", row.get("merchant", "")))
    if is_ambiguous(merchant):
        return row.get("merchant", ""), 0.0

    choices = vendor_directory["normalized_company"].dropna().astype(str).tolist()
    result = process.extractOne(merchant, choices, scorer=fuzz.token_set_ratio, score_cutoff=75)

    if result:
        match, score, _ = result
        if score >= 75:
            row_match = vendor_directory.loc[
                vendor_directory["normalized_company"] == match,
                "company_name"
            ]
            if not row_match.empty:
                return row_match.iloc[0], score
        # low score → keep original merchant
        return row.get("merchant", ""), score

    # no match found → keep original merchant
    return row.get("merchant", ""), 0.0

# --- Apply vendor resolver ---
df[["resolved_vendor", "vendor_match_score"]] = df.apply(
    lambda r: pd.Series(resolve_vendor(r)),
    axis=1
)

# --- Resolution logic ---
def resolve_transaction_mappings(row):
 
    # 1. Direct GL from statement → buscar en normalized_gl_accounts
    if not is_ambiguous(row['gl_account']):
        direct_gl = normalize(row['gl_account'])
        match_row = gl_accounts.loc[gl_accounts['normalized_name'] == direct_gl]

        if not match_row.empty:
            gl_code = match_row['gl_code'].iloc[0]
            raw_name = match_row['raw_name'].iloc[0]
            combined = f"{gl_code}: {raw_name}"
            return combined, "original", 100.0, combined, "", ""

        # si no se encuentra en catálogo, devolver como estaba
        return row['gl_account'], "original", 100.0, row['gl_account'], "", ""

    # 2. Intentar por property
    merchant = normalize(row.get('normalized_merchant', row.get('merchant', "")))
    company = normalize(row.get('normalized_company', row.get('company', "")))

    property_choices = property_directory["normalized_property"].dropna().astype(str).tolist()
    gl_choices = gl_accounts["normalized_name"].dropna().astype(str).tolist()

    property_match, property_score = get_best_match(company, property_choices)
    if property_match:
        raw_property = property_directory.loc[
            property_directory["normalized_property"] == property_match,
            "raw_property"
        ]
        if not raw_property.empty:
            combined_property = f"{raw_property.iloc[0]}: {property_match}"
            return "", "property_directory", property_score, "", combined_property, ""

    # 3. Intentar por GL Accounts
    gl_match, gl_score = get_best_match(merchant, gl_choices)
    if gl_match:
        matched_rows = gl_accounts.loc[gl_accounts['normalized_name'] == gl_match]
        if not matched_rows.empty:
            gl_code = matched_rows['gl_code'].iloc[0]
            raw_name = matched_rows['raw_name'].iloc[0]
            combined = f"{gl_code}: {raw_name}"
            return combined, "gl_accounts", gl_score, combined, "", ""

    # 4. No match
    return "", "unresolved", 0.0, "", "", ""

def resolve_cash_account(filename: str) -> str:
  # Determine the Cash Account based on the file name:

    fname = filename.lower()
    if "amex" in fname:
        return "1170: Amex"
    elif "mastercard" in fname:
        return "1180: AA Mastercard"
    return ""
# --- Apply resolution ---
print(f"🔍 Processing {len(df)} rows...")
result_columns = [
    'resolved_gl_account', 'resolution_source', 'match_score',
    'matched_value', 'matched_property', 'matched_vendor'
]
df[result_columns] = df.apply(resolve_transaction_mappings, axis=1, result_type='expand')

# --- Resolve properties ---
df[["resolved_property", "property_match_score"]] = df["company"].apply(
    lambda x: pd.Series(resolve_property_code(x))
)

# --- Build AppFolio DataFrame ---
output_columns = [
    "Bill Property Code*", "Vendor Payee Name*", "Amount*", "Bill Account*",
    "Description", "Bill Date*", "Due Date*", "Posting Date*", "Bill Reference",
    "Bill Remarks", "Memo For Check", "Purchase Order Number", "Cash Account",
    "Suggested Property Code", "Match Score", "Auto-Matched", "Matched Value",
    "Vendor Match Score"
]
new_df = pd.DataFrame(columns=output_columns)

# Detectar Cash Account según el archivo procesado
input_file = "data/clean/normalized_statement_amex.csv"  # o el que estés procesando
cash_account_value = resolve_cash_account(input_file)

# Vendor Payee Name* and score directly from df
new_df["Vendor Payee Name*"] = df["resolved_vendor"]
new_df["Vendor Match Score"] = df["vendor_match_score"]

# Amount directly from statement
new_df["Amount*"] = df["amount"]

# Resolved Bill Property Code
new_df["Bill Property Code*"] = df["resolved_property"]

# Suggested property (fuzzy)
new_df["Suggested Property Code"] = df["matched_property"]

# Resolved GL
new_df["Bill Account*"] = df["resolved_gl_account"]
new_df["Cash Account"] = cash_account_value


# Fuzzy info
new_df["Auto-Matched"] = df["resolution_source"]
new_df["Match Score"] = df["property_match_score"]  # using property score
new_df["Matched Value"] = df["matched_value"]

# Empty fields required by AppFolio format
for col in ["Description", "Bill Date*", "Due Date*", "Posting Date*", "Bill Reference",
            "Bill Remarks", "Memo For Check", "Purchase Order Number"]:
    new_df[col] = ""

# Expandir a mas casos en otro script
mask = (
    new_df["Bill Account*"].fillna("").astype(str).str.strip() == ""
) & (
    new_df["Vendor Payee Name*"].str.contains("ACE Hardware", case=False, na=False)
)

new_df.loc[mask, "Bill Account*"] = "6435: General Repairs"


# --- Save results to CSV---
output_path = "data/clean/appfolio_ready_bulk.csv"
new_df.to_csv(output_path, index=False, encoding="utf-8-sig")
print(f"File generated: {output_path}")
print("Remember to manually review 'unresolved' items or those with low confidence.")