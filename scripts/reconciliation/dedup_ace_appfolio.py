import pandas as pd
from datetime import timedelta
import os

# ============================================================
# 1. CONFIGURACIÓN Y CARGA DE REGLAS
# ============================================================
AMEX_FILE = "data/clean/normalized_amex.csv"
VENDOR_LEDGER = "data/raw/appfolio/vendor_ledger-20260116.csv"
RULES_FILE = "data/master/mapping_rules.xlsx"
OUTPUT_FILE = "data/clean/amex_ras_net_of_appfolio.csv"

DATE_WINDOW_DAYS = 2 
AMOUNT_TOLERANCE = 0.01

if os.path.exists(RULES_FILE):
    rules = pd.read_excel(RULES_FILE, sheet_name="Rules")
    column_mapping = {
        'Raw_Text (Key)': 'match_pattern',
        'Mapped_Value': 'normalized_merchant',
        'Category': 'vendor_class',
        'GL_Account_Hint': 'gl_hint'
    }
    rules = rules.rename(columns=column_mapping)
    # Filtro de seguridad: eliminamos cualquier fila donde el patrón sea nulo
    rules = rules.dropna(subset=["match_pattern"])
    
    if 'priority' not in rules.columns:
        rules['priority'] = 10 
        
    rules = rules.sort_values("priority", ascending=False)
    # Aseguramos que todos los patrones sean strings
    rules["match_pattern"] = rules["match_pattern"].astype(str).str.lower()
    print("✅ Reglas cargadas correctamente.")
else:
    rules = None

def apply_mapping_rules(merchant_raw, rules_df):
    if rules_df is None: 
        return pd.Series({"vendor": str(merchant_raw).upper(), "class": "UNCLASSIFIED", "gl_hint": ""})
    
    m = str(merchant_raw).lower()
    for _, r in rules_df.iterrows():
        pattern = str(r["match_pattern"]) # Garantizamos que el patrón sea string
        if not pattern or pattern == 'nan': continue
        
        # FIX: Usamos re.escape si no quieres regex o aseguramos que el valor sea válido
        try:
            if pd.Series(m).str.contains(pattern, regex=True, na=False).iloc[0]:
                return pd.Series({
                    "vendor": r["normalized_merchant"], 
                    "class": r["vendor_class"],
                    "gl_hint": r.get("gl_hint", "")
                })
        except Exception:
            continue
            
    return pd.Series({"vendor": str(merchant_raw).upper(), "class": "UNCLASSIFIED", "gl_hint": ""})

# ============================================================
# 2. PROCESAMIENTO DE DATOS
# ============================================================
amex = pd.read_csv(AMEX_FILE, parse_dates=["date"])
ledger = pd.read_csv(VENDOR_LEDGER)

print("🔍 Aplicando mapeo y clasificación...")
amex_mapped = amex["merchant"].apply(lambda x: apply_mapping_rules(x, rules))
amex = pd.concat([amex, amex_mapped], axis=1)

def clean_currency(series):
    return pd.to_numeric(series.astype(str).replace(r"[^\d\.-]", "", regex=True), errors="coerce").fillna(0.0).round(2)

ledger.columns = ledger.columns.str.strip().str.lower()
amt_col = 'amount' if 'amount' in ledger.columns else 'unpaid'
ledger["amount_clean"] = clean_currency(ledger[amt_col])
ledger["bill_date_clean"] = pd.to_datetime(ledger["bill date"], errors="coerce")
ledger["desc_clean"] = ledger["description"].astype(str).str.upper()

# ============================================================
# 3. DEDUPLICACIÓN ACE
# ============================================================
ledger_ace = ledger[
    (ledger["amount_clean"] > 0) & 
    (ledger["desc_clean"].str.contains("ACE", na=False)) & 
    (~ledger["desc_clean"].str.contains("AMEX PAYMENT", na=False))
].copy()
ledger_ace["matched"] = False

# Usamos la columna mapeada para filtrar Ace Hardware
# ============================================================
# 3. DEDUPLICACIÓN ACE (ROBUSTA)
# ============================================================

ace_mask = (
    amex["vendor"].str.upper().isin([
        "ACE HARDWARE",
        "SYKES ACE HARDWARE"
    ])
)


print("\n🔎 DEBUG ACE DETECTION")
print("Total filas AMEX:", len(amex))
print("Filas ACE detectadas:", ace_mask.sum())
print(
    amex.loc[ace_mask, ["merchant", "vendor", "normalized_merchant"]]
    .head(10)
)

amex_ace = amex[ace_mask].copy()
amex_non_ace = amex[~ace_mask].copy()

ledger_ace = ledger[
    (ledger["amount_clean"] > 0) &
    (
        ledger["desc_clean"].str.contains("ACE", na=False) |
        ledger["gl account"].astype(str).str.contains("6435", na=False)
    ) &
    (~ledger["desc_clean"].str.contains("AMEX PAYMENT", na=False))
].copy()

ledger_ace["matched"] = False

matches = set()

for i, a in amex_ace.iterrows():
    fecha_min = a["date"] - timedelta(days=DATE_WINDOW_DAYS)
    fecha_max = a["date"] + timedelta(days=DATE_WINDOW_DAYS)

    potential = ledger_ace[
        (~ledger_ace["matched"]) &
        (ledger_ace["bill_date_clean"] >= fecha_min) &
        (ledger_ace["bill_date_clean"] <= fecha_max)
    ]

    for l_idx, l in potential.iterrows():
        if abs(a["amount"] - l["amount_clean"]) <= AMOUNT_TOLERANCE:
            matches.add(i)
            ledger_ace.loc[l_idx, "matched"] = True
            break


# ============================================================
# 4. EXPORTACIÓN
# ============================================================
amex_ace["appfolio_duplicate"] = amex_ace.index.isin(matches)
final_df = pd.concat([amex_non_ace, amex_ace[~amex_ace["appfolio_duplicate"]]]).sort_values(by="date")

removed_amt = amex_ace.loc[amex_ace["appfolio_duplicate"], "amount"].sum()

print("\n" + "═" * 55)
print(f"║ {'REPORTE FINAL: AMEX vs APPFOLIO (ACE)':^51} ║")
print("═" * 55)
print(f"║ {'Monto Ace detectado en Amex:':<35} ${amex_ace['amount'].sum():>11,.2f} ║")
print(f"║ {'Monto duplicado en AppFolio:':<35} ${removed_amt:>11,.2f} ║")
print("╟" + "─" * 53 + "╢")
print(f"║ {'VALOR NETO A CONTABILIZAR:':<35} ${final_df['amount'].sum():>11,.2f} ║")
print("╚" + "═" * 53 + "╝")

final_df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")
print(f"✅ Archivo generado: {OUTPUT_FILE}")