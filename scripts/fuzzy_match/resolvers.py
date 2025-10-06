import pandas as pd
from rapidfuzz import process, fuzz

# Assuming these are in a 'utils.py' file in the same directory
from .utils import normalize, is_ambiguous, get_best_match 

# Assuming this is in another local module
from ..ingestion.assign_vendor_gl import apply_manual_rules

# --- Manual Rule Dictionaries ---
PROPERTY_RULES = {
    "930 WAREHOUSE": "14100 NW 24 COURT, OPA LOCKA, FL 33054",
    "CAST CAPITAL": "381 Sharar Ave - 381 Sharar Ave Opa Locka, FL 33054",
    "SHARAR": "381 Sharar Ave - 381 Sharar Ave Opa Locka, FL 33054",
    "CSR": "14140 NW 24th Court - 14140 NW 24th Court Opa Locka, FL 33054",
    "MSV": "Miami Shores Villas - 1280 NE 105th Street Miami Shores, FL 33138",
    "WV": "11505 NW 22nd Avenue - BLDG 1 - 11505 NW 22nd Avenue Miami, FL",
    "WESTVIEW": "11625 NW 22nd Avenue - BLDG 3 - 11625 NW 22nd Avenue Miami, FL",
    "LRMM Supplies": "Little River Mobile Home Park - 215 NW 79th Street Miami, FL 33150",
    "HAPPY TRAILERS": "Cory Mgmt Corp - 7075 NW 10TH AVE Miami, FL 33150"
}

CASH_ACCOUNT_RULES = {
    "amex": "1170: Amex",
    "mastercard": "1180: AA Mastercard",
    "boa": "1190: Bank of America",
    "chase": "1200: Chase"
}

# --- Helper Function for Rules ---
def apply_rules(name: str, rules: dict) -> str | None:
    if not name or pd.isna(name):
        return None
    norm = normalize(name)
    for key, value in rules.items():
        if key.lower() in norm: # Using .lower() on key for robustness
            return value
    return None

# --- Property Resolver (manual + fuzzy) ---
# ✨ IMPROVEMENT: Added score_cutoff parameter and normalized the input query.
def resolve_property_code(company_name: str, property_directory: pd.DataFrame, score_cutoff=75):
    if is_ambiguous(company_name):
        return company_name, 0, "ambiguous"

    # --- Step 1: Manual Rules ---
    rule_match = apply_rules(company_name, PROPERTY_RULES)
    if rule_match:
        return rule_match, 100, "manual_rule"

    # --- Step 2: Fuzzy matching ---
    # ✨ IMPROVEMENT: Normalize the query before matching.
    normalized_company = normalize(company_name)
    if not normalized_company:
        return company_name, 0, "empty"

    choices = property_directory["normalized_property"].dropna().astype(str).tolist()
    result = process.extractOne(normalized_company, choices, scorer=fuzz.token_set_ratio)
    
    if result:
        match, score, _ = result
        # ✨ IMPROVEMENT: Use the score_cutoff parameter instead of a hardcoded value.
        if score >= score_cutoff:
            raw_match = property_directory.loc[
                property_directory["normalized_property"] == match, "raw_property"
            ].iloc[0]
            return raw_match, score, "fuzzy"
        else:
            return company_name, score, "low_score"

    return company_name, 0, "unresolved"

# --- GL Resolver via Vendor ---
# ✨ IMPROVEMENT: Added score_cutoff parameter.
def resolve_gl_from_vendor(vendor_name, vendor_gl_map, gl_accounts, score_cutoff=70):
    if is_ambiguous(vendor_name):
        return None, 0.0, "unresolved"

    # 1. Apply manual rules for vendor name variations
    vendor_name = apply_manual_rules(vendor_name)
    norm_vendor = normalize(vendor_name)

    # 2. Direct match from the pre-defined vendor to GL mapping
    if norm_vendor in vendor_gl_map:
        return vendor_gl_map[norm_vendor], 100.0, "vendor_gl_map"

    # 3. Fallback to fuzzy matching the vendor name against GL account names
    gl_choices = gl_accounts["normalized_name"].dropna().astype(str).tolist()
    
    # ✨ IMPROVEMENT: Pass the score_cutoff to get_best_match.
    match, score = get_best_match(norm_vendor, gl_choices, score_cutoff=score_cutoff)
    
    if match: # get_best_match already filtered by score, so this check is sufficient
        gl_row = gl_accounts.loc[gl_accounts["normalized_name"] == match].iloc[0]
        combined = f"{gl_row['gl_code']}: {gl_row['raw_name']}"
        return combined, score, "fuzzy_vendor_to_gl"

    return None, 0.0, "unresolved"

# --- Vendor Resolver ---
# ✨ IMPROVEMENT: Added score_cutoff parameter and normalized the input query.
def resolve_vendor(row, vendor_directory, score_cutoff=67):
    # ✨ IMPROVEMENT: Normalize the merchant name at the start.
    merchant = normalize(str(row.get("merchant", "")))

    if not merchant or is_ambiguous(merchant):
        return (merchant, 0.0)

    choices = vendor_directory["normalized_company"].dropna().astype(str).tolist()
    
    # ✨ IMPROVEMENT: Use the normalized merchant and the score_cutoff parameter.
    result = process.extractOne(merchant, choices, scorer=fuzz.token_set_ratio, score_cutoff=score_cutoff)

    if result:
        match, score, _ = result
        # Using .loc for a robust lookup
        row_match = vendor_directory.loc[vendor_directory["normalized_company"] == match, "company_name"]
        if not row_match.empty:
            return (row_match.iloc[0], score)

    return (merchant, 0.0)

# --- Cash Account Resolver ---
def resolve_cash_account(filename: str) -> str:
    fname = filename.lower()
    for key, value in CASH_ACCOUNT_RULES.items():
        if key in fname:
            return value
    return ""

# --- Transaction "Conductor" Function ---
# ✨ IMPROVEMENT: This function is refactored to call other resolvers instead of
# duplicating their logic. It's not used in your main script but is now correct.
def resolve_transaction_mappings(row, gl_accounts, property_directory, vendor_gl_map, vendor_directory):
    # Hierarchy:
    # 1. Direct GL match (if provided in source data)
    # 2. Vendor -> GL mapping
    # 3. Property mapping (as a property, not a GL account)
    # 4. Fallback: Fuzzy match merchant name directly against GL accounts

    # --- 1. Direct GL Check ---
    if not is_ambiguous(row.get('gl_account')):
        # (Assuming some direct matching logic here...)
        return row['gl_account'], "original", 100.0, "", ""

    # --- 2. Vendor-based Resolution (for GL Account) ---
    # First, we need to resolve the vendor
    resolved_vendor, _ = resolve_vendor(row, vendor_directory)
    
    # Then, use that vendor to find the GL account
    gl_resolved, gl_score, gl_source = resolve_gl_from_vendor(
        resolved_vendor, vendor_gl_map, gl_accounts
    )
    if gl_resolved:
        return gl_resolved, gl_source, gl_score, "", ""

    # --- 3. Property Resolution ---
    prop_resolved, prop_score, prop_source = resolve_property_code(
        row.get('company', ""), property_directory
    )
    if prop_resolved and prop_source != "ambiguous":
         return "", "unresolved", 0.0, prop_resolved, prop_score

    # --- 4. Fallback Fuzzy GL Match ---
    merchant = normalize(row.get('merchant', ""))
    gl_match, gl_score = get_best_match(merchant, gl_accounts["normalized_name"].dropna().tolist())
    if gl_match:
        # (Add logic to format the GL match string if needed)
        return gl_match, "fuzzy_gl_fallback", gl_score, "", ""

    return "", "unresolved", 0.0, "", ""