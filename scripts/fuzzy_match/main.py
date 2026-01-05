import sys
import os
import pandas as pd

# --- Path Configuration ---
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from scripts.fuzzy_match.resolvers import resolve_vendor, resolve_property_code, resolve_cash_account

def extract_gl_info(gl_val):
    if pd.isna(gl_val) or str(gl_val).strip() == "":
        return None, None
    parts = str(gl_val).strip().split(maxsplit=1)
    return parts[0], parts[1] if len(parts) > 1 else ""

def main():
    print("Starting Universal Processing (Amex/Citi)")

    # 1. FILE DETECTION
    jobs = []
    if os.path.exists("data/clean/normalized_amex.csv"):
        jobs.append(("data/clean/normalized_amex.csv", "amex"))
    
    if os.path.exists("data/clean/normalized_citi.csv"):
        jobs.append(("data/clean/normalized_citi.csv", "mastercard"))

    if not jobs:
        print("No normalized files found in data/clean/")
        return

    # 2. LOAD DIRECTORIES AND RULES (Se cargan una sola vez)
    rules_df = pd.read_excel("data/master/mapping_rules.xlsx", sheet_name="Rules")
    gl_directory = pd.read_csv("data/clean/normalized_gl_accounts.csv")
    vendor_directory = pd.read_csv("data/clean/normalized_vendor_directory.csv")
    
    property_directory = gl_directory.copy() 
    property_directory = property_directory.rename(columns={'code_raw': 'normalized_property', 'account_name': 'raw_property'})

    all_bills = []

    # 3. PROCESSING LOOP (Todo el procesamiento debe estar DENTRO del for)
    for path, card_key in jobs:
        print(f"Processing {card_key.upper()} from: {path}")
        df = pd.read_csv(path)

        # Configuración de cuenta de efectivo
        selected_cash_account = resolve_cash_account(card_key, rules_df)
        if not selected_cash_account:
            print(f"Skipping {card_key}: No Cash Account configured.")
            continue

        # Filtrado específico (Solo para Citi)
        df.columns = df.columns.str.lower()
        if card_key == "mastercard":
            df = df[df["company"].astype(str).str.upper() == "RAS"].copy()

        if df.empty:
            print(f"ℹNo transactions to process for {card_key}.")
            continue
        
        df = pd.read_csv(path)
        df.columns = df.columns.str.lower()

        # 1. Aplicar filtro (IMPORTANTE: No volver a leer el CSV después de esto)
        if card_key == "mastercard":
            df = df[df["company"].astype(str).str.upper() == "RAS"].copy()

        if df.empty:
            print(f"ℹ No transactions to process for {card_key}.")
            continue

        # 2. Asegurar que existe la columna gl_account
        if "gl_account" not in df.columns:
            df["gl_account"] = ""
            
        # Resolución de datos
        df[["prop_hint", "gl_hint"]] = df["gl_account"].apply(
            lambda x: pd.Series(extract_gl_info(x))
        )

        print(f"Applying resolvers for {card_key}...")
        df["resolved_vendor"] = df.apply(lambda r: resolve_vendor(r, vendor_directory, rules_df)[0], axis=1)
        df["resolved_property"] = df.apply(lambda r: resolve_property_code(r, property_directory, rules_df)[0], axis=1)
        
        def get_resolved_gl(row):
            vendor_res = row["resolved_vendor"]
            hint = rules_df[(rules_df["Category"] == "Vendor") & 
                            (rules_df["Mapped_Value"] == vendor_res)]["GL_Account_Hint"]
            return str(hint.iloc[0]).strip() if not hint.dropna().empty else "6435: General Repairs"

        df["resolved_gl"] = df.apply(get_resolved_gl, axis=1)

        # Crear DataFrame temporal para esta tarjeta
        temp_bill = pd.DataFrame({
            "Bill Property Code*": df["resolved_property"],
            "Vendor Payee Name*": df["resolved_vendor"],
            "Amount*": df["amount"],
            "Bill Account*": df["resolved_gl"],
            "Bill Date*": df["date"],
            "Due Date*": df["date"],
            "Posting Date*": df["date"],
            "Description": (f"AMEX Payment | " if card_key == "amex" else "CITI Payment | ") + df["merchant"].astype(str),
            "Cash Account": selected_cash_account
        })
        all_bills.append(temp_bill)

    # 4. CONSOLIDATION AND EXPORT
    if all_bills:
        final_bulk_bill = pd.concat(all_bills, ignore_index=True)
        
        # Columnas requeridas por AppFolio
        for col in ["Bill Reference", "Bill Remarks", "Memo For Check", "Purchase Order Number"]:
            final_bulk_bill[col] = ""

        output_path = "data/clean/appfolio_ras_bulk_bill.csv"
        final_bulk_bill.to_csv(output_path, index=False, encoding="utf-8-sig")

        print(f"Success: Exported {len(final_bulk_bill)} total lines.")
        print(f"Total Consolidated Amount: ${final_bulk_bill['Amount*'].sum():.2f}")
    else:
        print("No data was processed.")

if __name__ == "__main__":
    main()