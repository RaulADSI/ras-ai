import pandas as pd
import re
import os

# ============================================================
# 1. CONFIGURACIÓN
# ============================================================
VENDOR_LEDGER = "data/raw/appfolio/vendor_ledger.csv"
RULES_FILE = "data/master/mapping_rules.xlsx"

JOBS = [
    {
        "input": "data/clean/normalized_amex.csv",
        "output": "data/clean/amex_ras_net_of_appfolio.csv",
        "label": "AMEX"
    },
    {
        "input": "data/clean/normalized_citi.csv",
        "output": "data/clean/normalized_citi.csv", 
        "label": "CITI MASTERCARD"
    }
]

AMOUNT_TOLERANCE = 0.01

# ============================================================
# 2. CARGA DE REGLAS Y UTILS
# ============================================================
if os.path.exists(RULES_FILE):
    rules = pd.read_excel(RULES_FILE, sheet_name="Rules")
    rules = rules.rename(columns={
        "Raw_Text (Key)": "match_pattern",
        "Mapped_Value": "normalized_merchant",
        "Category": "vendor_class",
        "GL_Account_Hint": "gl_hint",
    }).dropna(subset=["match_pattern"])
    if "priority" not in rules.columns:
        rules["priority"] = 10
    rules = rules.sort_values("priority", ascending=False)
    rules["match_pattern"] = rules["match_pattern"].astype(str).str.lower()
else:
    rules = None

def apply_mapping_rules(merchant_raw, rules_df):
    if rules_df is None or pd.isna(merchant_raw):
        return pd.Series({"vendor": str(merchant_raw).upper(), "class": "UNCLASSIFIED"})

    m = str(merchant_raw).lower()
    for _, r in rules_df.iterrows():
        pattern = str(r["match_pattern"])
        if pattern == "nan": continue
        try:
            if re.search(pattern, m): 
                return pd.Series({"vendor": r["normalized_merchant"], "class": r["vendor_class"]})
        except re.error:
            continue
            
    return pd.Series({"vendor": str(merchant_raw).upper(), "class": "UNCLASSIFIED"})

def safe_clean_currency(df, col):
    if col not in df.columns:
        return pd.Series(0.0, index=df.index)
    return pd.to_numeric(df[col].astype(str).replace(r"[^\d\.-]", "", regex=True), errors="coerce").fillna(0.0).round(2)

# ============================================================
# 3. FUNCIÓN CORE: CRUCE POR TOKENS Y SEGUIMIENTO DEL LEDGER
# ============================================================
def clean_tokens(text):
    if pd.isna(text):
        return set()
    clean = re.sub(r"[^\w\s]", " ", str(text).upper())
    tokens = set(clean.split())
    noise = {"THE", "INC", "CORP", "LLC", "CO", "AND", "DE", "LA", "LAS", "LOS", "SERVICES", "GROUP"}
    tokens -= noise
    return tokens

def deduplicate_card_against_ledger(card_df, ledger_df, vendor_key):
    to_remove = set()
    card_tokens = clean_tokens(vendor_key)
    if not card_tokens:
        return to_remove

    def is_vendor_match(row):
        ledger_v_tokens = clean_tokens(row["vendor"])
        ledger_d_tokens = clean_tokens(row["desc_clean"])
        combined_ledger_tokens = ledger_v_tokens | ledger_d_tokens
        return bool(card_tokens.intersection(combined_ledger_tokens))

    # Identificar las filas del Ledger que pertenecen a este proveedor
    vendor_mask = ledger_df.apply(is_vendor_match, axis=1)
    bills = ledger_df[vendor_mask].copy()
    if bills.empty:
        return to_remove

    # Filtrar la tarjeta
    card_vendor = card_df[card_df["vendor"].str.upper().str.contains(list(card_tokens)[0], na=False, regex=False)].copy()
    if card_vendor.empty:
        return to_remove

    # Asegurar e identificar referencias
    bills["reference_clean"] = bills["reference"].astype(str).str.strip()
    valid_ref_mask = (
        (bills["reference_clean"].str.len() > 1) & 
        (~bills["reference_clean"].str.lower().isin(["nan", "0", "00", "none", "null"]))
    )
    
    referenced_bills = bills[valid_ref_mask].copy()
    unreferenced_bills = bills[~valid_ref_mask].copy()
    
    logical_invoices = []
    
    # Universo A: Agrupados por Referencia
    if not referenced_bills.empty:
        grouped = referenced_bills.groupby("reference_clean")
        for ref, group in grouped:
            total_unpaid = group["unpaid_clean"].sum()
            logical_invoices.append({
                "id": ref,
                "type": "ID_REFERENCE",
                "amount": abs(total_unpaid),
                "original_indices": group.index.tolist()
            })
            
    # Universo B: Filas individuales sin referencia
    for idx, row in unreferenced_bills.iterrows():
        logical_invoices.append({
            "id": f"ROW_{idx}",
            "type": "INDIVIDUAL_ROW",
            "amount": abs(row["unpaid_clean"]),
            "original_indices": [idx]
        })
        
    logical_invoices = sorted(logical_invoices, key=lambda x: (x["type"] != "ID_REFERENCE", -x["amount"]))
    
    # Cruce matemático
    for inv in logical_invoices:
        inv_amount = inv["amount"]
        potential_matches = card_vendor[
            (~card_vendor.index.isin(to_remove)) & 
            (abs(card_vendor["amount"].abs() - inv_amount) <= AMOUNT_TOLERANCE)
        ].sort_values("date")
        
        if not potential_matches.empty:
            match_idx = potential_matches.index[0]
            to_remove.add(match_idx)
            
            # 🔥 MARCAR EN EL LEDGER ORIGINAL QUE ESTAS FILAS YA HICIERON MATCH
            ledger_df.loc[inv["original_indices"], "matched_to_card"] = True
                
    return to_remove

# ============================================================
# 4. PIPELINE PRINCIPAL
# ============================================================
def main():
    project_root = os.getcwd()
    
    if not os.path.exists(VENDOR_LEDGER):
        print(f"Error: No se encontró el Ledger en {VENDOR_LEDGER}")
        return

    # Cargar Ledger
    ledger = pd.read_csv(VENDOR_LEDGER)
    ledger.columns = ledger.columns.str.strip().str.lower()
    
    ledger = ledger.rename(columns={
        "gl accour": "gl_account",
        "descriptic": "description",
        "payee name": "vendor",    
        "payee": "vendor",         
        "vendor name": "vendor"    
    })
    
    if "vendor" not in ledger.columns:
        for col in ledger.columns:
            if "payee" in col or "vendor" in col or "name" in col:
                ledger = ledger.rename(columns={col: "vendor"})
                break

    if "vendor" not in ledger.columns:
        ledger["vendor"] = ledger.get("description", "")

    # Limpieza de totales e inválidos
    ledger["vendor"] = ledger["vendor"].astype(str).str.strip()
    invalid_vendors = ["NAN", "NONE", "NULL", ""]
    ledger.loc[ledger["vendor"].str.upper().isin(invalid_vendors), "vendor"] = None
    ledger = ledger.dropna(subset=["vendor"]).copy()
    
    ledger["desc_clean"] = ledger.get("description", "").astype(str).str.upper()
    total_mask = (
        ledger["vendor"].str.upper().str.contains("TOTAL", na=False) |
        ledger["desc_clean"].str.contains("TOTAL", na=False)
    )
    ledger = ledger[~total_mask].copy()

    ledger["unpaid_clean"] = safe_clean_currency(ledger, "unpaid")
    ledger = ledger[ledger["unpaid_clean"] > 0].copy()

    # Inicializamos la columna de control para marcar qué deudas se pagaron con tarjeta
    ledger["matched_to_card"] = False

    print(f"--- DIAGNÓSTICO DEL LEDGER APPFOLIO (CONSOLIDADO) ---")
    print(f"Total de deuda viva real en Ledger: ${ledger['unpaid_clean'].sum():,.2f}")
    print(r"--------------------------------------------------")

    for job in JOBS:
        input_file_path = os.path.join(project_root, job["input"])
        if not os.path.exists(input_file_path):
            print(f"\nSaltando {job['label']}: archivo no encontrado.")
            continue
            
        print(f"\n🚀 Deduplicando {job['label']} contra AppFolio...")
        card = pd.read_csv(input_file_path, parse_dates=["date"])
        
        if "vendor" not in card.columns:
            mapped_cols = card["merchant"].apply(lambda x: apply_mapping_rules(x, rules))
            card = pd.concat([card, mapped_cols], axis=1)

        unique_vendors = card["vendor"].dropna().astype(str).str.upper().unique()
        all_to_remove = set()
        
        for v_key in unique_vendors:
            detected_indices = deduplicate_card_against_ledger(card, ledger, v_key)
            all_to_remove.update(detected_indices)

        card["appfolio_duplicate"] = card.index.isin(all_to_remove)
        
        # Archivo neto listo para el main.py
        final_df = card[~card["appfolio_duplicate"]].sort_values("date").reset_index(drop=True)
        output_work_path = os.path.join(project_root, job["output"])
        final_df.to_csv(output_work_path, index=False, encoding="utf-8-sig")
        
        initial_amt = card["amount"].sum()
        clean_amt = final_df["amount"].sum()
        duplicated_amt = card.loc[list(all_to_remove), "amount"].sum()
        
        print(f"   Monto inicial {job['label']}: ${initial_amt:,.2f}")
        print(f"   ❌ Duplicados removidos de tarjeta:   ${duplicated_amt:,.2f}")
        print(f"   ✅ Neto tarjeta a contabilizar:      ${clean_amt:,.2f}")

    # ============================================================
    # 🔥 GENERACIÓN DEL REPORTE DE LAS FACTURAS HUÉRFANAS ($136.58)
    # ============================================================
    print("\n📊 Generando reporte de deudas AppFolio sin cobrar...")
    unmatched_ledger = ledger[~ledger["matched_to_card"]].copy()
    
    # Removemos columnas temporales internas de control de Python
    unmatched_ledger = unmatched_ledger.drop(columns=["desc_clean", "unpaid_clean", "matched_to_card"], errors="ignore")
    
    output_report_path = os.path.join(project_root, "data", "clean", "reporte_facturas_AppFolio_NO_encontradas.csv")
    unmatched_ledger.to_csv(output_report_path, index=False, encoding="utf-8-sig")
    
    print(f"   ❌ Monto total huérfano en AppFolio: ${safe_clean_currency(unmatched_ledger, 'unpaid').sum():,.2f}")
    print(f"   📂 Archivo de auditoría listo en: data/clean/reporte_facturas_AppFolio_NO_encontradas.csv")

if __name__ == "__main__":
    main()