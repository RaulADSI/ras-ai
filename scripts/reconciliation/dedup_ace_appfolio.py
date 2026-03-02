import pandas as pd
import re
import os

# ============================================================
# 1. CONFIGURACIÓN
# ============================================================
AMEX_FILE = "data/clean/normalized_amex.csv"
VENDOR_LEDGER = "data/raw/appfolio/vendor_ledger-20260116.csv"
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
        
        # Uso de re.search en lugar de pd.Series().str.contains()
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
        (c for c in ["payee", "name", "vendor name"] if c in ledger.columns),
        "description"
    )
    ledger["vendor"] = ledger[fallback]
    print(f"Columna vendor no encontrada, usando '{fallback}'")


# ============================================================
# 4. FUNCIÓN CORE — LEDGER COMO FUENTE DE VERDAD
# ============================================================
def remove_amex_using_ledger_unpaid_exact(
    ledger_df: pd.DataFrame,
    amex_df: pd.DataFrame,
    vendor_key: str,
    amount_tolerance: float = 0.01
):
    """
    Elimina cargos AMEX buscando coincidencias exactas de monto con las facturas pendientes
    en el Ledger (1-to-1 match).
    """
    ledger = ledger_df.copy()
    amex = amex_df.copy()

    # 1. Identificar facturas del proveedor en el Ledger
    vendor_mask = (
        ledger["vendor"].astype(str).str.upper().str.contains(vendor_key, na=False)
        | ledger["desc_clean"].str.contains(vendor_key, na=False)
        # Opcional: | ledger.get("gl account", "").astype(str).str.contains("6435", na=False)
    )

    bills = ledger[vendor_mask & (ledger["unpaid_clean"] > 0)]
    
    # Extraemos la lista de montos pendientes y los ordenamos (opcional, ayuda a procesar grandes primero)
    unpaid_amounts = bills["unpaid_clean"].sort_values(ascending=False).tolist()

    if not unpaid_amounts:
        print(f"No se encontró deuda pendiente para {vendor_key} en el Ledger.")
        return set(), 0.0

    # 2. Aislar transacciones AMEX del proveedor
    amex_vendor = amex[amex["vendor"].str.upper().str.contains(vendor_key, na=False)]
    
    to_remove = set()
    matched_ledger_total = 0.0

    # 3. Lógica de Emparejamiento Exacto (1-to-1)
    for bill_amount in unpaid_amounts:
        # Buscar cargos AMEX que:
        # - No hayan sido emparejados previamente (~isin(to_remove))
        # - El monto coincida con el bill_amount (+/- tolerancia)
        potential_matches = amex_vendor[
            (~amex_vendor.index.isin(to_remove)) & 
            (abs(amex_vendor["amount"] - bill_amount) <= amount_tolerance)
        ].sort_values("date") # Ordenar por fecha para tomar el cargo más antiguo en caso de empates

        if not potential_matches.empty:
            # ¡Match encontrado! Tomamos el índice del cargo AMEX más antiguo que encaja
            match_idx = potential_matches.index[0]
            to_remove.add(match_idx)
            matched_ledger_total += bill_amount

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

print(f"Ledger ACE (fuente de verdad): ${ledger_total:,.2f}")
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
