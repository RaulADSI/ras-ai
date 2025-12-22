import pandas as pd
from .utils import normalize, is_ambiguous, get_best_match 

# Diccionarios globales (vacíos ya que usamos Excel)
VENDOR_RULES = {}
PROPERTY_RULES = {}


def apply_rules(name: str, rules: dict) -> str | None:
    if not name or pd.isna(name):
        return None
    norm_name = str(name).lower()
    for key, value in rules.items():
        if key.lower() in norm_name:
            return value
    return None

def resolve_property_code(company_name: str, property_directory: pd.DataFrame, score_cutoff=75):
    """
    Busca la propiedad basándose en el código de compañía (ej. RAS, HTR).
    """
    if is_ambiguous(company_name):
        return company_name, 0, "ambiguous"

    # Intentar con reglas manuales si existieran
    rule_match = apply_rules(company_name, PROPERTY_RULES)
    if rule_match:
        return rule_match, 100, "manual_rule"

    # Fuzzy match contra el directorio de propiedades
    choices = property_directory["normalized_property"].dropna().astype(str).tolist()
    match, score = get_best_match(company_name, choices, score_cutoff=score_cutoff)
    
    if match:
        raw_match = property_directory.loc[
            property_directory["normalized_property"] == match, "raw_property"
        ].iloc[0]
        return raw_match, score, "fuzzy"
    
    return company_name, 0, "unresolved"

def resolve_vendor(row, vendor_directory, score_cutoff=67):
    merchant = str(row.get("merchant_clean", row.get("merchant", "")))
    cleaned_merchant = apply_rules(merchant, VENDOR_RULES)
    merchant_to_process = cleaned_merchant if cleaned_merchant is not None else merchant
    
    choices = vendor_directory["normalized_company"].dropna().astype(str).tolist()
    match, score = get_best_match(merchant_to_process, choices, score_cutoff=score_cutoff)

    if match:
        row_match = vendor_directory.loc[vendor_directory["normalized_company"] == match, "company_name"]
        if not row_match.empty:
            return (row_match.iloc[0], score)

    return (merchant_to_process, 0.0)

# Añade estas funciones si main.py también las importa
def resolve_gl_from_vendor(vendor_name, vendor_gl_map, gl_accounts, score_cutoff=70):
    # Lógica simplificada para evitar errores
    return None, 0.0, "unresolved"

def resolve_cash_account(filename: str) -> str:
    return "1180: AA Mastercard"