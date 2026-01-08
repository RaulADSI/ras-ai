import sys
import os
import pandas as pd

# --- Configuración Dinámica de Rutas ---
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from scripts.fuzzy_match.resolvers import (
    resolve_vendor,
    resolve_property_code,
    resolve_cash_account
)

def apply_richard_rules(row):
    acc = str(row.get('account_holder', '')).upper()
    comp = str(row.get('company', '')).upper()
    gl_hint = str(row.get('gl_account', '')).upper()

    # Defaults
    status = "OK"
    note = ""

    # EXCEPTION – prioridad máxima
    if "RICHARD LIBUTTI" in acc and "HAPPY TRAILERS" in comp:
        return "EXCEPTION", "Richard Libutti no opera Happy Trailers"

    # ALERT – no degrada EXCEPTION
    if "RR REITER REALTY" in comp:
        if "RAS" not in gl_hint and "RAS" not in comp:
            return "ALERT", "RR Reiter pagado sin marca RAS"

    return status, note


def main():
    print("Iniciando procesamiento con neteo y Reglas Libutti/Armas")

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
        print(f"\nProcesando {card_key.upper()} → {path}")

        df = pd.read_csv(path)
        df.columns = df.columns.str.lower()

        # --- APLICAR REGLAS DE RICHARD LIBUTTI ---
        df[['validation_status', 'validation_note']] = df.apply(
        lambda r: pd.Series(apply_richard_rules(r)),
        axis=1
        )

        
        # Reportar alertas en consola antes de filtrar
        alerts = df[df['validation_status'] != "OK"]
        if not alerts.empty:
            print(f"Se encontraron {len(alerts)} inconsistencias de negocio (Excepciones/Alertas).")

        # IMPORTANTE:
        # No filtrar por company == 'RAS'.
        # La validez de la transacción se determina por validation_status,
        # no por el texto del statement.

        df = df[
        df["validation_status"].isin(["OK", "ALERT"])
        ].copy()


        if df.empty:
            print(f"Sin transacciones RAS para {card_key}.")
            continue

        # Resolución de Vendor y Propiedad (Fuzzy Match)
        df["resolved_vendor"] = df.apply(lambda r: resolve_vendor(r, vendor_directory, rules_df)[0], axis=1)
        df["resolved_property"] = df.apply(lambda r: resolve_property_code(r, prop_dir, rules_df)[0], axis=1)

        # --- LÓGICA DE NETEO ---
        df["abs_amount"] = df["amount"].abs().round(2)
        group_cols = ["date", "merchant", "resolved_vendor", "resolved_property", "abs_amount", "validation_status"]

        df_netted = (
            df.groupby(group_cols, as_index=False)
            .agg({"amount": "sum", "validation_note" : "first"})
        )
        df_netted = df_netted[df_netted["amount"].round(2) != 0].copy()

        # Resolución de Cash Account y GL
        selected_cash_account = resolve_cash_account(card_key, rules_df)
        
        def get_resolved_gl(row):
            vendor_res = row["resolved_vendor"]
            hint = rules_df[(rules_df["Category"] == "Vendor") & 
                            (rules_df["Mapped_Value"] == vendor_res)]["GL_Account_Hint"]
            return str(hint.iloc[0]).strip() if not hint.dropna().empty else "6435: General Repairs"

        # 4. Construcción del DataFrame para AppFolio
        final_df = pd.DataFrame({
            "Bill Property Code*": df_netted["resolved_property"],
            "Vendor Payee Name*": df_netted["resolved_vendor"],
            "Amount*": df_netted["amount"],
            "Bill Account*": df_netted.apply(get_resolved_gl, axis=1),
            "Bill Date*": df_netted["date"],
            "Due Date*": df_netted["date"],
            "Posting Date*": df_netted["date"],
            "Description": (
                f"{card_key.upper()} | "
                + df_netted["merchant"].astype(str)
                + " | "
                + df_netted["validation_status"]
                + " - "
                + df_netted["validation_note"]
        ),
            "Cash Account": selected_cash_account
        })

        for col in ["Bill Reference", "Bill Remarks", "Memo For Check", "Purchase Order Number"]:
            final_df[col] = ""

        # 5. Exportación
        output_path = f"data/clean/appfolio_ras_bulk_bill_{card_key}.csv"
        final_df.to_csv(output_path, index=False, encoding="utf-8-sig")

        print(f"Total Bruto:   ${df['amount'].sum():,.2f}")
        print(f"Total Neteado: ${final_df['Amount*'].sum():,.2f}")
        print(f"Archivo: {output_path}")

if __name__ == "__main__":
    main()