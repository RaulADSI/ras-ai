import sys
import os
import pandas as pd
from rapidfuzz import process, fuzz

# --- Configuración de Rutas ---
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from scripts.fuzzy_match.resolvers import resolve_vendor

GL_DEFAULT = "6435: General Repairs"

def extract_gl_info(gl_val):
    if pd.isna(gl_val) or str(gl_val).strip() == "":
        return None, None
    parts = str(gl_val).strip().split(maxsplit=1)
    return parts[0], parts[1] if len(parts) > 1 else ""

def main():
    print("🚀 Iniciando procesamiento Universal (Amex/Citi)...")
    
    # 1. DETECCIÓN DE ARCHIVO Y CARGA
    if os.path.exists("data/clean/normalized_amex.csv"):
        path = "data/clean/normalized_amex.csv"
        card_key = "amex"
    else:
        path = "data/clean/normalized_citi.csv"
        card_key = "mastercard"

    print(f"💳 Leyendo archivo: {path}")
    df = pd.read_csv(path)
    
    # Cargar Reglas y Directorios
    rules_df = pd.read_excel("data/master/mapping_rules.xlsx", sheet_name="Rules")
    gl_directory = pd.read_csv("data/clean/normalized_gl_accounts.csv")
    vendor_directory = pd.read_csv("data/clean/normalized_vendor_directory.csv")
    gl_choices = gl_directory["code_raw"].dropna().tolist()

    # 2. CONFIGURAR MAPEOS (EXCEL)
    # Cash Account (1170 o 1180)
    cash_master = {str(k).lower().strip(): v for k, v in rules_df[rules_df["Category"] == "Cash"][["Raw_Text (Key)", "Mapped_Value"]].values}
    selected_cash_account = cash_master.get(card_key, "1150: Operating")

    # Vendors y Properties
    v_master = {str(k).upper().strip(): v for k, v in rules_df[rules_df["Category"] == "Vendor"][["Raw_Text (Key)", "Mapped_Value"]].values}
    p_master = {str(k).upper().strip(): v for k, v in rules_df[rules_df["Category"] == "Property"][["Raw_Text (Key)", "Mapped_Value"]].values}
    
    # GL Hints
    gl_hints = rules_df[rules_df["Category"] == "Vendor"][["Mapped_Value", "GL_Account_Hint"]].dropna()
    gl_hints_dict = {str(k).strip(): str(v).strip() for k, v in gl_hints.values}

    # 3. FILTRADO FLEXIBLE PARA ARMANDO
    df.columns = df.columns.str.lower()
    
    # IMPORTANTE: Para Amex, confiamos en el filtro que ya hizo normalize_statements.py
    # Si es Citi, aplicamos el filtro de "company == RAS"
    if card_key == "mastercard":
        df = df[df["company"].astype(str).str.upper() == "RAS"].copy()
    
    if df.empty:
        print(f"⚠️ No hay transacciones para procesar en {path}")
        return

    # 4. RESOLUCIÓN
    df[["prop_hint", "gl_hint"]] = df["gl_account"].apply(lambda x: pd.Series(extract_gl_info(x)))

    def get_resolved_vendor(row):
        desc = str(row.get("merchant", "")).upper().strip()
        for raw_key, clean_name in v_master.items():
            if raw_key in desc: return clean_name
        res, score = resolve_vendor(row, vendor_directory)
        return res if score >= 70 else desc

    def get_resolved_property(row):
        hint = str(row.get("prop_hint", "")).upper().strip()
        if hint in p_master: return p_master[hint]
        
        desc = str(row.get("merchant", "")).upper()
        for k_key, v_name in p_master.items():
            if k_key in desc: return v_name
        return f"REVISAR PROP: {hint}"

    def resolve_final_gl(row):
        vendor = str(row["resolved_vendor"]).strip()
        if vendor in gl_hints_dict: return gl_hints_dict[vendor]
        
        gl_text = str(row.get("gl_hint", "")).strip()
        if gl_text:
            match = process.extractOne(gl_text, gl_choices, scorer=fuzz.WRatio)
            if match and match[1] >= 75: return match[0]
        return GL_DEFAULT

    print("Procesando mapeos...")
    df["resolved_vendor"] = df.apply(get_resolved_vendor, axis=1)
    df["resolved_property"] = df.apply(get_resolved_property, axis=1)
    df["resolved_gl"] = df.apply(resolve_final_gl, axis=1)

    # 5. CONSTRUIR BULK BILL
    bulk_bill = pd.DataFrame({
        "Bill Property Code*": df["resolved_property"],
        "Vendor Payee Name*": df["resolved_vendor"],
        "Amount*": df["amount"],
        "Bill Account*": df["resolved_gl"],
        "Bill Date*": df["date"],
        "Due Date*": df["date"],
        "Posting Date*": df["date"],
        "Description": f"{card_key.upper()} | " + df["merchant"].astype(str),
        "Cash Account": selected_cash_account,
        "Bill Reference": "",
        "Bill Remarks": "",
        "Memo For Check": "",
        "Purchase Order Number": ""
    })

    output_path = "data/clean/appfolio_ras_bulk_bill.csv"
    bulk_bill.to_csv(output_path, index=False, encoding="utf-8-sig")

    print(f"✅ Éxito: {len(bulk_bill)} líneas exportadas.")
    print(f"💰 Total Amount: ${bulk_bill['Amount*'].sum():.2f}")

if __name__ == "__main__":
    main()