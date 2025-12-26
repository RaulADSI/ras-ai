import sys
import os
import pandas as pd

# --- Configuración de Rutas ---
# Aseguramos que el proyecto sea visible para las importaciones
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Importaciones desde resolvers
from scripts.fuzzy_match.resolvers import resolve_vendor, resolve_property_code, resolve_cash_account

def extract_gl_info(gl_val):
    if pd.isna(gl_val) or str(gl_val).strip() == "":
        return None, None
    parts = str(gl_val).strip().split(maxsplit=1)
    return parts[0], parts[1] if len(parts) > 1 else ""

def main():
    print("🚀 Iniciando procesamiento Universal (Amex/Citi)")

    # 1. DETECCIÓN DE ARCHIVO
    if os.path.exists("data/clean/normalized_amex.csv"):
        path = "data/clean/normalized_amex.csv"
        card_key = "amex"
    else:
        path = "data/clean/normalized_citi.csv"
        card_key = "mastercard"

    print(f"💳 Leyendo archivo: {path}")
    df = pd.read_csv(path)

    # 2. CARGA DE DIRECTORIOS Y REGLAS
    rules_df = pd.read_excel("data/master/mapping_rules.xlsx", sheet_name="Rules")
    gl_directory = pd.read_csv("data/clean/normalized_gl_accounts.csv")
    vendor_directory = pd.read_csv("data/clean/normalized_vendor_directory.csv")
    
    # IMPORTANTE: Usamos el gl_directory como property_directory para resolver códigos como 930/LRMM
    property_directory = gl_directory.copy() 
    property_directory = property_directory.rename(columns={'code_raw': 'normalized_property', 'account_name': 'raw_property'})

    # 3. CONFIGURACIÓN DE CUENTA DE EFECTIVO
    selected_cash_account = resolve_cash_account(card_key, rules_df)
    if not selected_cash_account:
        raise ValueError(f"❌ No Cash Account configurado para {card_key}")

    # 4. FILTRADO (Solo para Mastercard/Citi)
    df.columns = df.columns.str.lower()
    if card_key == "mastercard":
        df = df[df["company"].astype(str).str.upper() == "RAS"].copy()

    if df.empty:
        print("⚠️ No hay transacciones para procesar.")
        return

    # 5. RESOLUCIÓN DE DATOS
    # Extraer códigos de propiedad de la columna gl_account del banco
    df[["prop_hint", "gl_hint"]] = df["gl_account"].apply(
        lambda x: pd.Series(extract_gl_info(x))
    )

    def get_resolved_vendor(row):
        res, score = resolve_vendor(row, vendor_directory, rules_df)
        return res

    def get_resolved_property(row):
        # Resolvemos usando el código (930, LRMM) contra el Master Excel
        res, score, method = resolve_property_code(row, property_directory, rules_df)
        return res

    def get_resolved_gl(row):
        # Buscamos el GL_Account_Hint en el Excel para el vendor ya resuelto
        vendor_res = row["resolved_vendor"]
        hint = rules_df[(rules_df["Category"] == "Vendor") & 
                        (rules_df["Mapped_Value"] == vendor_res)]["GL_Account_Hint"]
        
        if not hint.dropna().empty:
            return str(hint.iloc[0]).strip()
        
        return "6435: General Repairs" # Default

    print("🔍 Aplicando mapeos y resolvers...")
    df["resolved_vendor"] = df.apply(get_resolved_vendor, axis=1)
    df["resolved_property"] = df.apply(get_resolved_property, axis=1)
    df["resolved_gl"] = df.apply(get_resolved_gl, axis=1)

    # 6. EXPORTACIÓN A FORMATO APPFOLIO (BULK BILL)
    bulk_bill = pd.DataFrame({
        "Bill Property Code*": df["resolved_property"],
        "Vendor Payee Name*": df["resolved_vendor"],
        "Amount*": df["amount"],
        "Bill Account*": df["resolved_gl"],
        "Bill Date*": df["date"],
        "Due Date*": df["date"],
        "Posting Date*": df["date"],
        "Description": (f"AMEX Payment" if card_key == "amex" else "CITI Payment"),
        "Cash Account": selected_cash_account,
        "Bill Reference": "",
        "Bill Remarks": "",
        "Memo For Check": "",
        "Purchase Order Number": ""
    })

    output_path = "data/clean/appfolio_ras_bulk_bill.csv"
    bulk_bill.to_csv(output_path, index=False, encoding="utf-8-sig")

    print(f"✅ Éxito: Exportadas {len(bulk_bill)} líneas.")
    print(f"💰 Total Amount: ${bulk_bill['Amount*'].sum():.2f}")

if __name__ == "__main__":
    main()