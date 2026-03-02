import sys
import os
import pandas as pd
from datetime import datetime

# --- Configuración de Rutas ---
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from scripts.fuzzy_match.resolvers import (
    resolve_vendor,
    resolve_property_code,
    resolve_cash_account
)

# --- CONFIGURACIÓN DEL LOGGING ---
log_dir = "logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Nombre del archivo de log basado en la fecha actual
log_filename = os.path.join(log_dir, f"audit_log_{datetime.now().strftime('%Y-%m')}.csv")

def write_error_log(df_errors):
    """Guarda las excepciones y alertas en un archivo persistente para auditoría."""
    if df_errors.empty:
        return
    
    # Añadimos timestamp del procesamiento
    df_errors = df_errors.copy()
    df_errors['processed_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Si el archivo no existe, lo creamos con cabecera. Si existe, añadimos (append).
    file_exists = os.path.isfile(log_filename)
    df_errors.to_csv(log_filename, mode='a', index=False, header=not file_exists, encoding="utf-8-sig")

def apply_validation_rules(row, alerts_df):
    """Evalúa dinámicamente las reglas de la pestaña 'Alerts' del Excel."""
    acc = str(row.get('account_holder', '')).upper()
    comp = str(row.get('company', '')).upper()
    gl_hint = str(row.get('gl_account', '')).upper()

    if alerts_df is None or alerts_df.empty:
        return "OK", ""

    for _, rule in alerts_df.iterrows():
        # Extraemos los valores de la regla en el Excel (y manejamos celdas vacías)
        r_type = str(rule.get('Rule_Type', '')).strip().upper()
        if r_type == 'NAN' or not r_type:
            continue

        r_acc = str(rule.get('Account_Contains', '')).strip().upper()
        r_comp = str(rule.get('Company_Contains', '')).strip().upper()
        r_miss_gl = str(rule.get('Missing_Keyword_GL', '')).strip().upper()
        r_miss_comp = str(rule.get('Missing_Keyword_Company', '')).strip().upper()
        msg = str(rule.get('Message', '')).strip()

        # Asumimos que la fila cumple la regla, hasta demostrar lo contrario
        match = True
        
        # Verificamos cada condición: Si el Excel pide algo y la transacción no lo tiene, fallamos el match.
        if r_acc != 'NAN' and r_acc and r_acc not in acc:
            match = False
        if r_comp != 'NAN' and r_comp and r_comp not in comp:
            match = False
            
        # Para las palabras faltantes (Ej. debe FALTAR 'RAS'). Si la palabra SÍ ESTÁ, fallamos el match.
        if r_miss_gl != 'NAN' and r_miss_gl and r_miss_gl in gl_hint:
            match = False
        if r_miss_comp != 'NAN' and r_miss_comp and r_miss_comp in comp:
            match = False

        # Si después de pasar los filtros el match sigue siendo True, aplicamos la alerta
        if match:
            return r_type, msg

    return "OK", ""

def main():
    print(f"Iniciando procesamiento | Log: {log_filename}")

    jobs = []
    if os.path.exists("data/clean/normalized_amex.csv"):
        jobs.append(("data/clean/normalized_amex.csv", "amex"))
    if os.path.exists("data/clean/normalized_citi.csv"):
        jobs.append(("data/clean/normalized_citi.csv", "mastercard"))

    if not jobs:
        print("No se encontraron archivos de entrada.")
        return

    # Carga de catálogos
    
    rules_df = pd.read_excel("data/master/mapping_rules.xlsx", sheet_name="Rules")
    
    # NUEVO: Cargamos la pestaña de alertas (con un try-except por si a alguien se le borra la pestaña)
    try:
        alerts_df = pd.read_excel("data/master/mapping_rules.xlsx", sheet_name="Alerts")
    except Exception as e:
        print(" No se encontró la pestaña 'Alerts' en el Excel. Saltando validaciones especiales.")
        alerts_df = None

    gl_directory = pd.read_csv("data/clean/normalized_gl_accounts.csv")
    vendor_directory = pd.read_csv("data/clean/normalized_vendor_directory.csv")
    prop_dir = gl_directory.rename(columns={"code_raw": "normalized_property", "account_name": "raw_property"})

    for path, card_key in jobs:
        print(f"\nProcesando {card_key.upper()}...")

        df = pd.read_csv(path)
        df.columns = df.columns.str.lower()

        # 1. Aplicar Reglas y Generar Log de Errores (AQUÍ CAMBIAMOS EL NOMBRE DE LA FUNCIÓN)
        df[['validation_status', 'validation_note']] = df.apply(
            lambda r: pd.Series(apply_validation_rules(r, alerts_df)), axis=1
        )

        # Filtrar errores (ALERT y EXCEPTION) para guardarlos en el histórico de logs
        errors_to_log = df[df['validation_status'] != "OK"].copy()
        errors_to_log['card_type'] = card_key
        write_error_log(errors_to_log)

        if not errors_to_log.empty:
            print(f"  {len(errors_to_log)} inconsistencias registradas en el log histórico.")

        # 2. Filtrado: Solo pasan transacciones OK y ALERT (Las EXCEPTION se bloquean)
        df = df[df["validation_status"].isin(["OK", "ALERT"])].copy()

        if df.empty:
            continue

        # 3. Resolución y Neteado (Lógica anterior)
        df["prop_hint"] = df.iloc[:, 6].astype(str)
        df["resolved_vendor"] = df.apply(lambda r: resolve_vendor(r, vendor_directory, rules_df)[0], axis=1)
        df["resolved_property"] = df.apply(lambda r: resolve_property_code(r, prop_dir, rules_df)[0], axis=1)
        df["abs_amount"] = df["amount"].abs().round(2)
        
        group_cols = ["date", "merchant", "resolved_vendor", "resolved_property", "abs_amount", "validation_status"]

        df_netted = df.groupby(group_cols, as_index=False).agg({
            "amount": "sum", 
            "validation_note" : "first"
        })
        df_netted = df_netted[df_netted["amount"].round(2) != 0].copy()

        # 4. Construcción Final
        selected_cash_account = resolve_cash_account(card_key, rules_df)
        
        def get_resolved_gl(row):
            vendor_res = row["resolved_vendor"]
            hint = rules_df[(rules_df["Category"] == "Vendor") & (rules_df["Mapped_Value"] == vendor_res)]["GL_Account_Hint"]
            return str(hint.iloc[0]).strip() if not hint.dropna().empty else "6435: General Repairs"

        final_df = pd.DataFrame({
            "Bill Property Code*": df_netted["resolved_property"],
            "Vendor Payee Name*": df_netted["resolved_vendor"],
            "Amount*": df_netted["amount"],
            "Bill Account*": df_netted.apply(get_resolved_gl, axis=1),
            "Bill Date*": df_netted["date"],
            "Description": f"{card_key.upper()} | {df_netted['merchant']} | {df_netted['validation_status']} {df_netted['validation_note']}".strip(),
            "Cash Account": selected_cash_account
        })
        
        final_df["Description"] = (
            card_key.upper()
            + " | "
            + df_netted["merchant"].astype(str)
            + " | "
            + df_netted["validation_status"].astype(str)
            + " "
            + df_netted["validation_note"].fillna("").astype(str)
        )
        assert len(final_df) == len(df_netted), "Mismatch entre df_netted y final_df"
        assert not final_df["Description"].str.contains("dtype:", na=False).any()
        
        output_path = f"data/clean/appfolio_ras_bulk_bill_{card_key}.csv"
        final_df.to_csv(output_path, index=False, encoding="utf-8-sig")
        
        print(f"Total Neteado: ${final_df['Amount*'].sum():,.2f} | Archivo listo.")

if __name__ == "__main__":
    main()