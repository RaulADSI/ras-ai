import sys
import os
import pandas as pd
from rapidfuzz import process, fuzz

# --- Configuración de Rutas ---
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from scripts.fuzzy_match.resolvers import resolve_vendor

def extract_gl_info(gl_val):
    """Separa '930 General repairs' -> ('930', 'General repairs')"""
    if pd.isna(gl_val) or str(gl_val).strip() == "":
        return None, None
    parts = str(gl_val).split(maxsplit=1)
    prop_hint = parts[0]
    gl_hint = parts[1] if len(parts) > 1 else ""
    return prop_hint, gl_hint

def main():
    print("🚀 Iniciando procesamiento para AppFolio (Solo RAS)...")

    # --- 1. Cargar Datos y Recursos ---
    try:
        df = pd.read_csv("data/clean/normalized_statement_citi.csv")
        gl_directory = pd.read_csv("data/clean/normalized_gl_accounts.csv")
        vendor_directory = pd.read_csv("data/clean/normalized_vendor_directory.csv")
        
        # Cargar Master Excel para mapeo de propiedades (930 -> Nombre Largo)
        rules_df = pd.read_excel("data/master/mapping_rules.xlsx", sheet_name='Rules')
        prop_master = dict(rules_df[rules_df['Category'] == 'Property'][['Raw_Text (Key)', 'Mapped_Value']].values)
        
        gl_choices = gl_directory["code_raw"].tolist()
    except Exception as e:
        print(f"❌ Error al cargar archivos: {e}")
        return

    # --- 2. Filtrado y Preparación ---
    # Asegurar que los nombres de columnas estén en minúsculas para evitar KeyErrors
    df.columns = df.columns.str.lower()
    
    # Filtrar solo RAS
    df = df[df['company'].astype(str).str.upper() == 'RAS'].copy()
    
    if df.empty:
        print("⚠️ No se encontraron transacciones para RAS.")
        return

    # --- 3. Pipeline de Resolución ---

    # A. Separar Property y GL Account
    # Usamos la columna 'gl_account' que viene del paso de normalización
    df[['prop_hint', 'gl_hint']] = df['gl_account'].apply(lambda x: pd.Series(extract_gl_info(x)))

    # B. Resolver Cuenta GL (Fuzzy Match)
    def resolve_gl_account(hint):
        if not hint: return "6435: General Repairs"
        match = process.extractOne(hint, gl_choices, scorer=fuzz.WRatio)
        return match[0] if match and match[1] > 70 else "6435: General Repairs"

    print("Resolviendo cuentas contables...")
    df['resolved_gl'] = df['gl_hint'].apply(resolve_gl_account)

    # C. Resolver Vendor
    print("Resolviendo vendors...")
    df[["resolved_vendor", "v_score"]] = df.apply(
        lambda r: pd.Series(resolve_vendor(r, vendor_directory)), axis=1
    )

    # --- 4. Construir Estructura Final (Según imágenes) ---
    print("Construyendo Bulk Bill...")
    bulk_bill = pd.DataFrame()
    
    # Mapeo de Propiedad (930 -> Nombre Largo de AppFolio)
    bulk_bill["Bill Property Code*"] = df["prop_hint"].astype(str).str.upper().map(prop_master).fillna("REVISAR: " + df["prop_hint"].astype(str))
    
    bulk_bill["Vendor Payee Name*"] = df["resolved_vendor"]
    bulk_bill["Amount*"] = df["amount"]
    bulk_bill["Bill Account*"] = df["resolved_gl"]
    bulk_bill["Bill Date*"] = df["date"]
    bulk_bill["Due Date*"] = df["date"]
    bulk_bill["Posting Date*"] = df["date"]
    bulk_bill["Description"] = "Citi 1180 | " + df["merchant"].astype(str)
    
    # Columnas adicionales de tu formato maestro
    bulk_bill["Bill Reference"] = ""
    bulk_bill["Bill Remarks"] = ""
    bulk_bill["Memo For Check"] = ""
    bulk_bill["Purchase Order Number"] = ""
    bulk_bill["Cash Account"] = "1180: AA Mastercard"

    # --- 5. Guardar ---
    output_path = "data/clean/appfolio_ras_bulk_bill.csv"
    bulk_bill.to_csv(output_path, index=False, encoding="utf-8-sig")
    
    print(f"✅ ¡Éxito! Se procesaron {len(bulk_bill)} líneas de RAS.")
    print(f"📂 Archivo generado para AppFolio: {output_path}")

if __name__ == "__main__":
    main()