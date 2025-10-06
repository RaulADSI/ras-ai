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

VENDOR_RULES = {
    "the home depot": "The Home Depot",
    "home depot": "The Home Depot",
    "THE HOME DEPOT      MIAMI               FL": "The Home Depot",
    "sherwin williams": "Sherwin Williams",
    "the sherwin williams": "Sherwin Williams",
    "the sherwinwilliamscleveland": "Sherwin Williams",
    "THE SHERWIN-WILLIAMSCLEVELAND           OH": "Sherwin Williams",
    "ACE HDWE OF OPA LOCKOPA LOCKA           FL": "Ace Hardware",
    "ace hdwe of opa locka": "Ace Hardware",
    "ace hdwe": "Ace Hardware",
    "ace hardware": "Ace Hardware",
    "SYKES ACE HARDWARE 0MIAMI               FL": "Ace Hardware",
    "brandsmart usa": "Brandsmart USA",
    "BRANDSMART USA      FORT LAUDERDA       FL": "Brandsmart USA",
    "7-ELEVEN 38192 00073MIAMI               FL": "7-Eleven",
    "7eleven 38192 00073": "7-Eleven",
    "7-ELEVEN 3819200073MIAMIFL": "7-Eleven",
    "USPS PO 1158810115 0MIAMI               FL": "USPS",
    "amazon": "Amazon",
    "amazon.com": "Amazon",
    "in *swiftpix real es": "Swiftpix Real Estate",
    "IN *SWIFTPIX REAL ESDAVIE               FL": "Swiftpix Real Estate",
    "shinepay laundry app": "Shinepay Laundry",
    "WINDOWS & DOORS 0000NORTH MIAMI         FL": "Windows & Doors",
    "windows doors 0000": "Windows & Doors"
}

CASH_ACCOUNT_RULES = {
    "amex": "1170: Amex",
    "mastercard": "1180: AA Mastercard",
    "boa": "1190: Bank of America",
    "chase": "1200: Chase"
}

# --- Utility Function for Applying Manual Rules ---
def apply_rules(name: str, rules: dict) -> str | None:
    
    if not name or pd.isna(name):
        return None
    
    norm_name = normalize(name)
    for key, value in rules.items():
        if key.lower() in norm_name:
            return value
            
    return None


# --- Property Code Resolver ---
def resolve_property_code(company_name: str, property_directory: pd.DataFrame, score_cutoff=75):
    if is_ambiguous(company_name):
        return company_name, 0, "ambiguous"

    rule_match = apply_rules(company_name, PROPERTY_RULES)
    if rule_match:
        return rule_match, 100, "manual_rule"

    choices = property_directory["normalized_property"].dropna().astype(str).tolist()
    match, score = get_best_match(company_name, choices, score_cutoff=score_cutoff)
    
    if match:
        raw_match = property_directory.loc[
            property_directory["normalized_property"] == match, "raw_property"
        ].iloc[0]
        return raw_match, score, "fuzzy"
    
    return company_name, 0, "unresolved"


# --- GL Account Resolver from Vendor ---
def resolve_gl_from_vendor(vendor_name, vendor_gl_map, gl_accounts, score_cutoff=70):
    if is_ambiguous(vendor_name):
        return None, 0.0, "unresolved"

    # ✨ IMPROVEMENT: Use the consolidated 'apply_rules' function for vendors.
    cleaned_vendor = apply_rules(vendor_name, VENDOR_RULES)
    # If a rule applied, use the clean name; otherwise, use the original name.
    vendor_to_process = cleaned_vendor if cleaned_vendor is not None else vendor_name

    norm_vendor = normalize(vendor_to_process)

    # 1. Direct match using the vendor-to-GL map
    if norm_vendor in vendor_gl_map:
        return vendor_gl_map[norm_vendor], 100.0, "vendor_gl_map"

    # 2. Fallback to fuzzy matching against all GL accounts
    gl_choices = gl_accounts["normalized_name"].dropna().astype(str).tolist()
    match, score = get_best_match(norm_vendor, gl_choices, score_cutoff=score_cutoff)
    
    if match:
        gl_row = gl_accounts.loc[gl_accounts["normalized_name"] == match].iloc[0]
        combined = f"{gl_row['gl_code']}: {gl_row['raw_name']}"
        return combined, score, "fuzzy_vendor_to_gl"

    return None, 0.0, "unresolved"


# --- Vendor Resolver ---
def resolve_vendor(row, vendor_directory, score_cutoff=67):
    # Use the 'merchant_clean' column first as it's the best input.
    merchant = str(row.get("merchant_clean", row.get("merchant", "")))

    # ✨ IMPROVEMENT: Apply manual vendor rules first for a cleaner match.
    cleaned_merchant = apply_rules(merchant, VENDOR_RULES)
    merchant_to_process = cleaned_merchant if cleaned_merchant is not None else merchant
    
    choices = vendor_directory["normalized_company"].dropna().astype(str).tolist()
    match, score = get_best_match(merchant_to_process, choices, score_cutoff=score_cutoff)

    if match:
        row_match = vendor_directory.loc[vendor_directory["normalized_company"] == match, "company_name"]
        if not row_match.empty:
            return (row_match.iloc[0], score)

    # If no good match, return the (potentially cleaned) merchant name with a score of 0.
    return (merchant_to_process, 0.0)


# --- Cash Account Resolver ---
def resolve_cash_account(filename: str) -> str:
    return apply_rules(filename, CASH_ACCOUNT_RULES) or ""


# --- Transaction Conductor (for future use, unchanged) ---
def resolve_transaction_mappings(row, gl_accounts, property_directory, vendor_gl_map, vendor_directory):
    # (This logic remains the same)
    pass
