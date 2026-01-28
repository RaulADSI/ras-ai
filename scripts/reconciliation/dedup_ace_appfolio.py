import pandas as pd
from datetime import timedelta
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
    if rules_df is None:
        return pd.Series({
            "vendor": str(merchant_raw).upper(),
            "class": "UNCLASSIFIED",
            "gl_hint": ""
        })

    m = str(merchant_raw).lower()
    for _, r in rules_df.iterrows():
        pattern = str(r["match_pattern"])
        if not pattern or pattern == "nan":
            continue
        try:
            if pd.Series(m).str.contains(pattern, regex=True, na=False).iloc[0]:
                return pd.Series({
                    "vendor": r["normalized_merchant"],
                    "class": r["vendor_class"],
                    "gl_hint": r.get("gl_hint", "")
                })
        except Exception:
            continue

    return pd.Series({
        "vendor": str(merchant_raw).upper(),
        "class": "UNCLASSIFIED",
        "gl_hint": ""
    })


# ============================================================
# 3. CARGA DE DATOS
# ============================================================
amex = pd.read_csv(AMEX_FILE, parse_dates=["date"])
ledger = pd.read_csv(VENDOR_LEDGER)

print("🔍 Aplicando mapeo y clasificación...")
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
    print(f"⚠️ Columna vendor no encontrada, usando '{fallback}'")


# ============================================================
# 4. FUNCIÓN CORE — LEDGER COMO FUENTE DE VERDAD
# ============================================================
def remove_amex_using_ledger_unpaid(
    ledger_df: pd.DataFrame,
    amex_df: pd.DataFrame,
    vendor_key: str,
):
    """
    Elimina cargos AMEX hasta consumir exactamente el saldo unpaid del Ledger.
    Nunca elimina más de lo que el Ledger respalda.
    """

    ledger = ledger_df.copy()
    amex = amex_df.copy()

    vendor_mask = (
        ledger["vendor"].astype(str).str.upper().str.contains(vendor_key, na=False)
        | ledger["desc_clean"].str.contains(vendor_key, na=False)
        | ledger.get("gl account", "")
            .astype(str)
            .str.contains("6435", na=False)
    )

    bills = ledger[vendor_mask & (ledger["unpaid_clean"] > 0)]

    ledger_truth_total = bills["unpaid_clean"].sum()

    if ledger_truth_total <= 0:
        print(f"⚠️ No se encontró deuda para {vendor_key}")
        return set(), 0.0

    amex_vendor = (
        amex[amex["vendor"].str.upper().str.contains(vendor_key, na=False)]
        .sort_values("date")
    )

    to_remove = set()
    remaining = ledger_truth_total

    for idx, row in amex_vendor.iterrows():
        if remaining <= 0.009:
            break

        if row["amount"] <= remaining + 0.01:
            to_remove.add(idx)
            remaining -= row["amount"]

    return to_remove, ledger_truth_total


# ============================================================
# 5. DEDUPLICACIÓN ACE
# ============================================================
print("\n🔎 Ejecutando deduplicación ACE (Ledger-driven)...")

to_remove, ledger_total = remove_amex_using_ledger_unpaid(
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
