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
    # Separamos la primera palabra (Propiedad) del resto (GL)
    parts = str(gl_val).strip().split(maxsplit=1)
    return parts[0], parts[1] if len(parts) > 1 else ""

def main():
    print("🚀 Iniciando procesamiento RAS con mapeo estricto...")

    # --- 1. Cargar Recursos ---
    df = pd.read_csv("data/clean/normalized_statement_citi.csv")
    gl_directory = pd.read_csv("data/clean/normalized_gl_accounts.csv")
    vendor_directory = pd.read_csv("data/clean/normalized_vendor_directory.csv")
    rules_df = pd.read_excel("data/master/mapping_rules.xlsx", sheet_name="Rules")

    gl_choices = gl_directory["code_raw"].dropna().tolist()

    # Normalización de Diccionarios desde Excel para evitar fallos por espacios o mayúsculas
    # Vendor: { 'RAW BANK TEXT': 'Clean Name' }
    v_master = {str(k).upper().strip(): v for k, v in rules_df[rules_df["Category"] == "Vendor"][["Raw_Text (Key)", "Mapped_Value"]].values}
    
    # Property: { 'CODE/SHORT NAME': 'Full AppFolio Name' }
    p_master = {str(k).upper().strip(): v for k, v in rules_df[rules_df["Category"] == "Property"][["Raw_Text (Key)", "Mapped_Value"]].values}
    
    # GL Hints: { 'Clean Name': '6450: Grounds' }
    gl_hints = rules_df[rules_df["Category"] == "Vendor"][["Mapped_Value", "GL_Account_Hint"]].dropna()
    gl_hints_dict = {str(k).strip(): str(v).strip() for k, v in gl_hints.values}

    # --- 2. Preparación ---
    df.columns = df.columns.str.lower()
    df = df[df["company"].astype(str).str.upper() == "RAS"].copy()

    if df.empty:
        print("⚠️ No hay transacciones RAS")
        return

    # Extraer hints de la columna gl_account del banco
    df[["prop_hint", "gl_hint"]] = df["gl_account"].apply(lambda x: pd.Series(extract_gl_info(x)))

    # --- 3. Resolver Vendor ---
    def get_resolved_vendor(row):
        desc = str(row.get("merchant", "")).upper().strip()
        # Prioridad 1: Excel Mapping
        for raw_key, clean_name in v_master.items():
            if raw_key in desc:
                return clean_name
        # Prioridad 2: Fuzzy Match
        res, score = resolve_vendor(row, vendor_directory)
        return res if score >= 70 else desc

    df["resolved_vendor"] = df.apply(get_resolved_vendor, axis=1)

    # --- 4. Resolver Property ---
    def get_resolved_property(row):
        # 1. Intentar por código extraído (ej: '930', 'LRMM')
        hint = str(row.get("prop_hint", "")).upper().strip()
        if hint in p_master:
            return p_master[hint]
        
        # 2. Intentar buscando el código dentro del merchant
        desc = str(row.get("merchant", "")).upper()
        for k_key, v_name in p_master.items():
            if k_key in desc:
                return v_name
                
        return f"REVISAR PROP: {hint}"

    df["resolved_property"] = df.apply(get_resolved_property, axis=1)

    # --- 5. Resolver GL FINAL (Prioridad Hint) ---
    def resolve_final_gl(row):
        vendor = str(row["resolved_vendor"]).strip()

        # Prioridad 1: Hint directo del Vendor en Excel (Ej: Ruggable -> 6450: Grounds)
        if vendor in gl_hints_dict:
            return gl_hints_dict[vendor]

        # Prioridad 2: Fuzzy Match desde el texto del banco
        gl_text = str(row.get("gl_hint", "")).strip()
        if gl_text:
            match = process.extractOne(gl_text, gl_choices, scorer=fuzz.WRatio)
            if match and match[1] >= 75:
                return match[0]

        return GL_DEFAULT

    df["resolved_gl"] = df.apply(resolve_final_gl, axis=1)

    # --- 6. Construir Bulk Bill con formato AppFolio ---
    bulk_bill = pd.DataFrame({
        "Bill Property Code*": df["resolved_property"],
        "Vendor Payee Name*": df["resolved_vendor"],
        "Amount*": df["amount"],
        "Bill Account*": df["resolved_gl"],
        "Bill Date*": df["date"],
        "Due Date*": df["date"],
        "Posting Date*": df["date"],
        "Description": "Citi 1180 | " + df["merchant"].astype(str),
        "Cash Account": "1180: AA Mastercard",
        "Bill Reference": "",
        "Bill Remarks": "",
        "Memo For Check": "",
        "Purchase Order Number": ""
    })

    # --- 7. Exportar ---
    output_path = "data/clean/appfolio_ras_bulk_bill.csv"
    bulk_bill.to_csv(output_path, index=False, encoding="utf-8-sig")

    print(f"✅ Éxito: {len(bulk_bill)} líneas exportadas.")
    print(f"📂 Archivo generado: {output_path}")

if __name__ == "__main__":
    main()