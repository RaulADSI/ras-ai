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

def apply_richard_rules(row):
    acc = str(row.get('account_holder', '')).upper()
    comp = str(row.get('company', '')).upper()
    gl_hint = str(row.get('gl_account', '')).upper()

    # REGLA: EXCEPTION
    if "RICHARD LIBUTTI" in acc and "HAPPY TRAILERS" in comp:
        return "EXCEPTION", "Richard Libutti no opera Happy Trailers"

    # REGLA: ALERT
    if "RR REITER REALTY" in comp:
        if "RAS" not in gl_hint and "RAS" not in comp:
            return "ALERT", "RR Reiter pagado sin marca RAS"

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
    gl_directory = pd.read_csv("data/clean/normalized_gl_accounts.csv")
    vendor_directory = pd.read_csv("data/clean/normalized_vendor_directory.csv")
    prop_dir = gl_directory.rename(columns={"code_raw": "normalized_property", "account_name": "raw_property"})

    for path, card_key in jobs:
        print(f"\nProcesando {card_key.upper()}...")

        df = pd.read_csv(path)
        df.columns = df.columns.str.lower()

        # 1. Aplicar Reglas y Generar Log de Errores
        df[['validation_status', 'validation_note']] = df.apply(
            lambda r: pd.Series(apply_richard_rules(r)), axis=1
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