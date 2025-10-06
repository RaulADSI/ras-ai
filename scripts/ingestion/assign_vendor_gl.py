import pandas as pd
from scripts.utils.text_cleaning import normalize_vendor as normalize
import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))


# --- Manual rules for problematic vendor names ---
MANUAL_VENDOR_RULES = {
    "the home depot": "The Home Depot",
    "home depot": "The Home Depot",
    "THE HOME DEPOT      MIAMI               FL": "The Home Depot",
    "the sherwin williams": "Sherwin Williams",
    "the sherwinwilliamscleveland": "Sherwin Williams",
    "THE SHERWIN-WILLIAMSCLEVELAND           OH": "Sherwin Williams",
    "sherwin williams": "Sherwin Williams",
    "ACE HDWE OF OPA LOCKOPA LOCKA           FL": "Ace Hardware",
    "ace hardware": "Ace Hardware",
    "ace hdwe of opa locka": "Ace Hardware",
    "ace hdwe": "Ace Hardware",
    "SYKES ACE HARDWARE 0MIAMI               FL": "Ace Hardware",
    "brandsmart usa": "Brandsmart USA",
    "BRANDSMART USA      FORT LAUDERDA       FL": "Brandsmart USA",
    "7-ELEVEN 38192 00073MIAMI               FL": "7-Eleven",
    "7eleven 38192 00073": "7-Eleven",
    "USPS PO 1158810115 0MIAMI               FL": "USPS",
    "amazon": "Amazon",
    "amazon.com": "Amazon",
    "in *swiftpix real es": "Swiftpix Real Estate",
    "IN *SWIFTPIX REAL ESDAVIE               FL": "Swiftpix Real Estate",
    "shinepay laundry app": "Shinepay Laundry",
    "WINDOWS & DOORS 0000NORTH MIAMI         FL": "Windows & Doors",
    "windows doors 0000": "Windows & Doors"
}

def load_vendor_gl_map(path: str) -> pd.DataFrame:
  
    if path.endswith(".xlsx"):
        df = pd.read_excel(path)
    else:
        df = pd.read_csv(path)
    
    df["vendor_norm"] = df["vendor"].fillna("").apply(normalize)
    return df

def apply_manual_rules(vendor_name: str) -> str:
    """
    Apply manual rules to map problematic vendor names to a clean version.
    """
    norm = normalize(vendor_name)
    for key, clean_name in MANUAL_VENDOR_RULES.items():
        if key in norm:
            return clean_name
    return vendor_name  

def assign_gl_account(vendor_name: str, vendor_gl_map: pd.DataFrame) -> str | None:
    """
    Try to assign a GL account to a vendor name using:
    1. Manual rules
    2. Exact normalized lookup in vendor_gl_map
    """
    # Apply manual rules
    vendor_clean = apply_manual_rules(vendor_name)
    vendor_norm = normalize(vendor_clean)

    # Look match to vendor_gl_map
    row = vendor_gl_map[vendor_gl_map["vendor_norm"] == vendor_norm]
    if not row.empty:
        return row.iloc[0]["gl_account"]

    return None