import pandas as pd
import sys
import os
import re
import glob

# ============================================================
# 1. CONFIGURACIÓN DE ENTORNO
# ============================================================
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

CANONICAL_COLUMNS = ['date', 'merchant', 'account_holder', 'column', 'amount', 'company', 'gl_account']

RULES_FILE = "data/config/mapping_rules.xlsx"

# ============================================================
# 2. UTILIDADES
# ============================================================
def clean_currency(series):
    return (
        pd.to_numeric(
            series.astype(str)
            .replace({'\$': '', ',': '', '\(': '-', '\)': ''}, regex=True),
            errors='coerce'
        ).fillna(0.0).round(2)
    )

def clean_merchant(text):
    if not text:
        return ""
    t = str(text).upper()
    t = re.sub(r"\b\d{4,}\b", "", t)
    t = re.sub(r"[^\w\s]", " ", t)
    return re.sub(r"\s+", " ", t).strip()

# ============================================================
# 3. MAPPING RULES (FUENTE DE VERDAD)
# ============================================================
def load_mapping_rules():
    rules = (
        pd.read_excel(RULES_FILE, sheet_name="merchant_rules")
        .dropna(subset=["match_pattern"])
        .sort_values("priority", ascending=False)
    )
    rules["match_pattern"] = rules["match_pattern"].str.lower()
    return rules

def apply_mapping_rules(merchant, rules_df):
    m = str(merchant).lower()
    for _, r in rules_df.iterrows():
        if pd.Series(m).str.contains(r["match_pattern"], regex=True, na=False).iloc[0]:
            return pd.Series({
                "normalized_merchant": r["normalized_merchant"],
                "vendor_class": r["vendor_class"]
            })
    return pd.Series({
        "normalized_merchant": clean_merchant(merchant),
        "vendor_class": "UNCLASSIFIED"
    })

# ============================================================
# 4. REGLAS DE NEGOCIO RAS
# ============================================================
def apply_business_rules(df):
    def validate_row(row):
        acc = str(row.get('account_holder', '')).upper().strip()
        merc = str(row.get('merchant', '')).upper()
        comp = str(row.get('company', '')).upper()
        gl = str(row.get('gl_account', '')).upper()

        is_armando = "ARMANDO ARMAS" in acc or (acc in ["", "NAN"] and "ARMANDO ARMAS" in merc)
        is_richard = "RICHARD LIBUTTI" in acc or (acc in ["", "NAN"] and "RICHARD LIBUTTI" in merc)
        is_ras_marked = any(x in comp or x in gl for x in ["RAS", "REITER"])

        if is_richard and "HAPPY TRAILERS" in comp:
            return pd.Series(["EXCEPTION", "Richard no opera Happy Trailers"])

        if is_ras_marked or is_armando or is_richard:
            return pd.Series(["KEEP", "RAS validado"])
        return pd.Series(["SKIP", "No RAS"])

    df = df.copy()
    df[["validation_status", "business_notes"]] = df.apply(validate_row, axis=1)
    return df

# ============================================================
# 5. CARGA AMEX
# ============================================================
def load_amex_file(filepath):
    ext = filepath.lower().split('.')[-1]
    raw = pd.read_excel(filepath, header=None) if ext == "xlsx" else pd.read_csv(filepath, header=None)

    header_row = None
    for i in range(min(15, len(raw))):
        row = " ".join(str(v).upper() for v in raw.iloc[i].values)
        if "DATE" in row and "AMOUNT" in row:
            header_row = i
            break

    if header_row is not None:
        df = pd.read_excel(filepath, header=header_row) if ext == "xlsx" else pd.read_csv(filepath, header=header_row)
    else:
        df = raw.copy()
        df.columns = CANONICAL_COLUMNS + [
            f"extra_{i}" for i in range(df.shape[1] - len(CANONICAL_COLUMNS))
        ]

    df.columns = df.columns.str.lower()
    return df

# ============================================================
# 6. PIPELINE PRINCIPAL
# ============================================================
def main():
    INPUT_FOLDER = "data/raw/unify_all_amex/"
    OUTPUT_PATH = "data/clean/normalized_amex.csv"

    rules = load_mapping_rules()

    files = glob.glob(os.path.join(INPUT_FOLDER, "*.csv")) + glob.glob(os.path.join(INPUT_FOLDER, "*.xlsx"))
    dfs = []

    for f in files:
        df = load_amex_file(f)
        df["source_file"] = os.path.basename(f)
        dfs.append(df)

    amex = pd.concat(dfs, ignore_index=True)
    amex["amount"] = clean_currency(amex["amount"])

    # Deduplicación
    key_cols = ["date", "merchant", "amount"]
    amex["occ"] = amex.groupby(key_cols).cumcount()
    amex["dedup_key"] = (
        amex["date"].astype(str) + "|" +
        amex["merchant"].astype(str) + "|" +
        amex["amount"].astype(str) + "|" +
        amex["occ"].astype(str)
    )
    amex = amex.drop_duplicates("dedup_key")

    # Reglas RAS
    amex = apply_business_rules(amex)
    amex = amex[amex["validation_status"] != "SKIP"].copy()

    # 🔥 NORMALIZACIÓN CENTRALIZADA
    mapped = amex["merchant"].apply(lambda x: apply_mapping_rules(x, rules))
    amex["normalized_merchant"] = mapped["normalized_merchant"]
    amex["vendor_class"] = mapped["vendor_class"]

    # Auditoría
    unclassified = (amex["vendor_class"] == "UNCLASSIFIED").sum()
    if unclassified:
        print(f"⚠️ {unclassified} merchants sin clasificar (revisar mapping_rules.xlsx)")

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    amex.to_csv(OUTPUT_PATH, index=False)
    print(f"✅ Archivo generado: {OUTPUT_PATH}")

if __name__ == "__main__":
    main()
