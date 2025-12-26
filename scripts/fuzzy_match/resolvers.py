import pandas as pd
from .utils import normalize, is_ambiguous, get_best_match 

def apply_rules(name: str, rules_df: pd.DataFrame, category: str) -> str | None:
    """
    Busca coincidencias de texto basadas en la categoría del Master Excel.
    """
    if not name or pd.isna(name):
        return None
    
    # Filtrar reglas por categoría (Vendor, Property, etc.)
    subset = rules_df[rules_df["Category"] == category]
    norm_name = str(name).upper().strip()
    
    for _, row in subset.iterrows():
        key = str(row["Raw_Text (Key)"]).upper().strip()
        if key in norm_name:
            return row["Mapped_Value"]
    return None

def resolve_property_code(row, property_directory, rules_df, score_cutoff=75):
    """
    Resuelve la propiedad usando el código (930, LRMM) o el merchant.
    """
    # Intentar primero con la pista del GL (ej. '930') extraída en el main
    prop_hint = str(row.get("prop_hint", "")).upper().strip()
    
    # Prioridad 1: Mapeo directo de código en Excel (ej. '930' -> Dirección larga)
    direct_match = apply_rules(prop_hint, rules_df, "Property")
    if direct_match:
        return direct_match, 100, "excel_mapping"

    # Prioridad 2: Buscar si el merchant contiene el nombre de la propiedad
    merchant = str(row.get("merchant", "")).upper()
    merchant_match = apply_rules(merchant, rules_df, "Property")
    if merchant_match:
        return merchant_match, 100, "excel_mapping"

    # Fallback: Fuzzy match contra directorio
    choices = property_directory["normalized_property"].dropna().astype(str).tolist()
    match, score = get_best_match(merchant, choices, score_cutoff=score_cutoff)
    
    if match:
        raw_match = property_directory.loc[
            property_directory["normalized_property"] == match, "raw_property"
        ].iloc[0]
        return raw_match, score, "fuzzy"
    
    return f"REVISAR PROP: {prop_hint}", 0, "unresolved"

def resolve_vendor(row, vendor_directory, rules_df, score_cutoff=67):
    """
    Resuelve el Vendor priorizando las reglas del Master Excel.
    """
    merchant = str(row.get("merchant", ""))
    
    # Prioridad 1: Reglas de Excel (Mapea 'WCI*6440' -> 'Waste Connections')
    clean_name = apply_rules(merchant, rules_df, "Vendor")
    if clean_name:
        return clean_name, 100.0

    # Prioridad 2: Fuzzy match contra el directorio
    choices = vendor_directory["normalized_company"].dropna().astype(str).tolist()
    match, score = get_best_match(merchant, choices, score_cutoff=score_cutoff)

    if match:
        row_match = vendor_directory.loc[vendor_directory["normalized_company"] == match, "company_name"]
        if not row_match.empty:
            return row_match.iloc[0], score

    return merchant, 0.0

def resolve_cash_account(card_key: str, rules_df: pd.DataFrame) -> str:
    """
    Obtiene '1170: Amex' o '1180: AA Mastercard' desde el Excel.
    """
    subset = rules_df[rules_df["Category"] == "Cash"]
    mapping = dict(subset[["Raw_Text (Key)", "Mapped_Value"]].values)
    return mapping.get(card_key, "1150: Operating")