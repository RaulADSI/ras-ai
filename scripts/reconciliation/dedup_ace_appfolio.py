import pandas as pd
import re
import os

# ============================================================
# 1. CONFIGURACIÓN
# ============================================================
AMEX_FILE = "data/clean/normalized_amex.csv"
VENDOR_LEDGER = "data/raw/appfolio/vendor_ledger.csv"
RULES_FILE = "data/master/mapping_rules.xlsx"
OUTPUT_FILE = "data/clean/amex_ras_net_of_appfolio.csv"

DATE_WINDOW_DAYS = 2
AMOUNT_TOLERANCE = 0.01

# ============================================================
# 2. CARGA DE REGLAS
# ============================================================
if os.path.exists(RULES_FILE):
    rules = pd.read_excel(RULES_FILE, sheet_name="Rules")
    rules = rules.rename(columns={
        "Raw_Text (Key)": "match_pattern",
        "Mapped_Value": "normalized_merchant",
        "Category": "vendor_class",
        "GL_Account_Hint": "gl_hint",
    })
    rules = rules.dropna(subset=["match_pattern"])
    if "priority" not in rules.columns:
        rules["priority"] = 10
    rules = rules.sort_values("priority", ascending=False)
    rules["match_pattern"] = rules["match_pattern"].astype(str).str.lower()
    print("Reglas cargadas correctamente.")
else:
    rules = None


def apply_mapping_rules(merchant_raw, rules_df):
    if rules_df is None or pd.isna(merchant_raw):
        return pd.Series({"vendor": str(merchant_raw).upper(), "class": "UNCLASSIFIED", "gl_hint": ""})

    m = str(merchant_raw).lower()
    for _, r in rules_df.iterrows():
        pattern = str(r["match_pattern"])
        if pattern == "nan": continue
        
        try:
            if re.search(pattern, m): 
                return pd.Series({"vendor": r["normalized_merchant"], "class": r["vendor_class"], "gl_hint": r.get("gl_hint", "")})
        except re.error:
            continue
            
    return pd.Series({"vendor": str(merchant_raw).upper(), "class": "UNCLASSIFIED", "gl_hint": ""})


# ============================================================
# 3. CARGA DE DATOS
# ============================================================
amex = pd.read_csv(AMEX_FILE, parse_dates=["date"])
ledger = pd.read_csv(VENDOR_LEDGER)

print(" Aplicando mapeo y clasificación...")
amex = pd.concat(
    [amex, amex["merchant"].apply(lambda x: apply_mapping_rules(x, rules))],
    axis=1
)

ledger.columns = ledger.columns.str.strip().str.lower()

def safe_clean_currency(df, col):
    if col not in df.columns:
        return pd.Series(0.0, index=df.index)
    return (
        pd.to_numeric(
            df[col].astype(str).replace(r"[^\d\.-]", "", regex=True),
            errors="coerce"
        )
        .fillna(0.0)
        .round(2)
    )

ledger["bill_date_clean"] = pd.to_datetime(ledger.get("bill date"), errors="coerce")
ledger["desc_clean"] = ledger.get("description", "").astype(str).str.upper()
ledger["amount_clean"] = safe_clean_currency(ledger, "amount")
ledger["unpaid_clean"] = safe_clean_currency(ledger, "unpaid")

if "vendor" not in ledger.columns:
    fallback = next(
        (c for c in ["payee name", "payee", "name", "vendor name"] if c in ledger.columns),
        "description"
    )
    ledger["vendor"] = ledger[fallback]
    print(f"Columna vendor asignada exitosamente desde: '{fallback}'")


# ============================================================
# 4. FUNCIÓN CORE — LEDGER COMO FUENTE DE VERDAD (DOBLE MALLA)
# ============================================================
def remove_amex_using_ledger_unpaid_exact(
    ledger_df: pd.DataFrame,
    amex_df: pd.DataFrame,
    vendor_key: str,
    amount_tolerance: float = 0.01
):
    ledger = ledger_df.copy()
    amex = amex_df.copy()

    # Preparamos la columna Reference para que sea segura de leer
    if "reference" not in ledger.columns:
        ledger["reference"] = ""
    ledger["reference_clean"] = ledger["reference"].astype(str).str.strip()

    print(f"\n--- DIAGNÓSTICO PARA '{vendor_key}' ---")

    # Aislar las facturas del proveedor
    vendor_mask = (
        ledger["vendor"].astype(str).str.upper().str.contains(vendor_key, na=False)
        | ledger["desc_clean"].str.contains(vendor_key, na=False)
    )

    bills = ledger[vendor_mask & (ledger["unpaid_clean"] > 0)].copy()

    if bills.empty:
        print(f"` No se encontró deuda pendiente para {vendor_key} en el Ledger.")
        return set(), 0.0

    amex_vendor = amex[amex["vendor"].str.upper().str.contains(vendor_key, na=False)]
    
    to_remove = set()
    matched_ledger_total = 0.0
    matched_bill_indices = set() # Aquí guardaremos las facturas de AppFolio ya procesadas

    # --------------------------------------------------
    # MALLA 1: AGRUPACIÓN POR NÚMERO DE FACTURA (REFERENCE)
    # --------------------------------------------------
    # Filtramos referencias que SÍ sean válidas (no vacías, no cero)
    valid_ref_mask = (
        (bills["reference_clean"].str.len() > 1) & 
        (~bills["reference_clean"].str.lower().isin(["nan", "0", "00", "000", "none", "null"]))
    )
    
    grouped_bills = bills[valid_ref_mask].groupby("reference_clean")

    for ref, group in grouped_bills:
        # Sumamos todos los pedacitos de esa misma factura
        group_total = group["unpaid_clean"].sum()
        
        # Buscamos si ese total exacto está en la AMEX
        potential_matches = amex_vendor[
            (~amex_vendor.index.isin(to_remove)) & 
            (abs(amex_vendor["amount"] - group_total) <= amount_tolerance)
        ].sort_values("date")
        
        if not potential_matches.empty:
            match_idx = potential_matches.index[0]
            to_remove.add(match_idx)
            matched_ledger_total += group_total
            matched_bill_indices.update(group.index.tolist())
            print(f"  [MATCH GRUPAL] Referencia '{ref}': {len(group)} facturas sumaron ${group_total:.2f} == AMEX ${amex_vendor.loc[match_idx, 'amount']:.2f}")

    # --------------------------------------------------
    # MALLA 2: EMPAREJAMIENTO 1-A-1 (EL RESTO)
    # --------------------------------------------------
    # Quitamos las facturas de AppFolio que ya se lograron agrupar arriba
    remaining_bills = bills[~bills.index.isin(matched_bill_indices)]
    unpaid_amounts = remaining_bills["unpaid_clean"].sort_values(ascending=False).tolist()

    for bill_amount in unpaid_amounts:
        potential_matches = amex_vendor[
            (~amex_vendor.index.isin(to_remove)) & 
            (abs(amex_vendor["amount"] - bill_amount) <= amount_tolerance)
        ].sort_values("date") 

        if not potential_matches.empty:
            match_idx = potential_matches.index[0]
            to_remove.add(match_idx)
            matched_ledger_total += bill_amount
            print(f"  [MATCH 1-a-1] Factura AppFolio ${bill_amount:.2f} == AMEX ${amex_vendor.loc[match_idx, 'amount']:.2f}")
        else:
            print(f"  Falló el cruce para factura de ${bill_amount:.2f}. No hay monto igual en AMEX.")

    return to_remove, matched_ledger_total

# ============================================================
# 5. DEDUPLICACIÓN ACE
# ============================================================
print("\n Ejecutando deduplicación ACE (Ledger-driven)...")

to_remove, ledger_total = remove_amex_using_ledger_unpaid_exact(
    ledger_df=ledger,
    amex_df=amex,
    vendor_key="ACE"
)

if "appfolio_duplicate" not in amex.columns:
    amex["appfolio_duplicate"] = False

amex.loc[amex.index.isin(to_remove), "appfolio_duplicate"] = True

print(f"\nLedger ACE (fuente de verdad): ${ledger_total:,.2f}")
print(f"AMEX eliminado: ${amex.loc[list(to_remove), 'amount'].sum():,.2f}")

# ============================================================
# 6. EXPORTACIÓN
# ============================================================
final_df = (
    amex[~amex["appfolio_duplicate"]]
    .sort_values("date")
    .reset_index(drop=True)
)

removed_amt = amex.loc[amex["appfolio_duplicate"], "amount"].sum()
print("\n" + "═" * 55)
print(f"║ {'REPORTE FINAL: AMEX vs APPFOLIO (ACE)':^51} ║")
print("═" * 55)
print(f"║ {'Monto ACE detectado en AMEX:':<35} ${amex[amex['vendor'].str.contains('ACE', na=False)]['amount'].sum():>11,.2f} ║")
print(f"║ {'Monto duplicado en AppFolio:':<35} ${removed_amt:>11,.2f} ║")
print("╟" + "─" * 53 + "╢")
print(f"║ {'VALOR NETO A CONTABILIZAR:':<35} ${final_df['amount'].sum():>11,.2f} ║")
print("╚" + "═" * 53 + "╝")

final_df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")
print(f"Archivo generado: {OUTPUT_FILE}")