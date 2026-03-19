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
    
    df_errors = df_errors.copy()
    df_errors['processed_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
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
        r_type = str(rule.get('Rule_Type', '')).strip().upper()
        if r_type == 'NAN' or not r_type:
            continue

        r_acc = str(rule.get('Account_Contains', '')).strip().upper()
        r_comp = str(rule.get('Company_Contains', '')).strip().upper()
        r_miss_gl = str(rule.get('Missing_Keyword_GL', '')).strip().upper()
        r_miss_comp = str(rule.get('Missing_Keyword_Company', '')).strip().upper()
        msg = str(rule.get('Message', '')).strip()

        match = True
        
        if r_acc != 'NAN' and r_acc and r_acc not in acc:
            match = False
        if r_comp != 'NAN' and r_comp and r_comp not in comp:
            match = False
            
        if r_miss_gl != 'NAN' and r_miss_gl and r_miss_gl in gl_hint:
            match = False
        if r_miss_comp != 'NAN' and r_miss_comp and r_miss_comp in comp:
            match = False

        if match:
            return r_type, msg

    return "OK", ""

def load_allocations(filepath):
    """Carga los grupos de prorrateo y sus porcentajes desde el Excel."""
    try:
        alloc_df = pd.read_excel(filepath, sheet_name="Allocations")
        alloc_df.columns = alloc_df.columns.str.strip()
        alloc_df = alloc_df.dropna(subset=["Group_Name", "Property_Code"])
        
        # Si la columna Weight no existe en el Excel, asume partes iguales (1.0 para todos)
        if "Weight" not in alloc_df.columns:
            alloc_df["Weight"] = 1.0  
            
        groups = {}
        for group_name, group_data in alloc_df.groupby("Group_Name"):
            props = []
            for _, row in group_data.iterrows():
                # Guardamos el nombre del edificio y su peso matematico
                props.append((str(row["Property_Code"]).strip(), float(row["Weight"])))
            groups[str(group_name).strip()] = props
            
        print(f"Reglas de Prorrateo cargadas: {len(groups)} grupos con sus respectivos pesos.")
        return groups
    except Exception as e:
        print(f"Error cargando 'Allocations'. Saltando prorrateos. Detalle: {e}")
        return {}

def split_allocations(df_netted, property_groups):
    """
    Divide los gastos usando la proporción (Weight) de cada propiedad.
    Detecta si la propiedad asignada es un Grupo o un edificio individual de un grupo.
    """
    if not property_groups:
        return df_netted

    # 1. Crear un diccionario inverso para saber a qué grupo pertenece cada propiedad
    property_to_group = {}
    for group_name, props in property_groups.items():
        for prop_name, weight in props:
            property_to_group[prop_name] = group_name

    expanded_rows = []
    
    for _, row in df_netted.iterrows():
        prop = str(row["resolved_property"]).strip()
        
        # 2. Determinar si debemos disparar el prorrateo
        target_group = None
        
        if prop in property_groups:
            # Caso A: El Excel asignó el nombre del grupo directamente
            target_group = prop
        elif prop in property_to_group:
            # Caso B: El Excel asignó una propiedad que pertenece a un grupo
            target_group = property_to_group[prop]
            
        if target_group:
            # ¡Prorrateo Activado!
            group_props = property_groups[target_group]
            
            # Calcular la suma total de los pesos (por si no suman 100 o 1.0)
            total_weight = sum(w for _, w in group_props)
            if total_weight == 0:
                expanded_rows.append(row)
                continue
                
            total_amount = row["amount"]
            allocated_total = 0.0
            
            # 3. Generar las filas divididas
            for i, (g_prop, weight) in enumerate(group_props):
                new_row = row.copy()
                new_row["resolved_property"] = g_prop
                
                # Al último edificio le damos el remanente exacto para cuadrar centavos
                if i == len(group_props) - 1:
                    final_amount = round(total_amount - allocated_total, 2)
                    new_row["amount"] = final_amount
                else:
                    split_amount = round(total_amount * (weight / total_weight), 2)
                    new_row["amount"] = split_amount
                    allocated_total += split_amount
                    
                expanded_rows.append(new_row)
        else:
            # Si no es grupo ni pertenece a uno, pasa intacto
            expanded_rows.append(row)
            
    return pd.DataFrame(expanded_rows)

def split_allocations(df_netted, property_groups):
    """
    Divide los gastos usando la proporción (Weight) de cada propiedad.
    Detecta si la propiedad asignada es un Grupo ("CAST_CAPITAL") o 
    un edificio individual que pertenece a un grupo ("301 Sharar Ave").
    """
    if not property_groups:
        return df_netted

    # 1. Crear un diccionario inverso para saber a qué grupo pertenece cada propiedad
    # Ej: {"301 Sharar Ave...": "CAST_CAPITAL", "11505 NW 22nd...": "WESTVIEW"}
    property_to_group = {}
    for group_name, props in property_groups.items():
        for prop_name, weight in props:
            property_to_group[prop_name] = group_name

    expanded_rows = []
    
    for _, row in df_netted.iterrows():
        prop = str(row["resolved_property"]).strip()
        
        # 2. Determinar si debemos disparar el prorrateo
        target_group = None
        
        if prop in property_groups:
            # Caso A: El Excel asignó el nombre del grupo directamente (Ej. "CAST_CAPITAL")
            target_group = prop
        elif prop in property_to_group:
            # Caso B: El Excel asignó una propiedad, pero esa propiedad es parte de un grupo
            target_group = property_to_group[prop]
            
        if target_group:
            # ¡Prorrateo Activado! Obtenemos la lista de propiedades y sus pesos
            group_props = property_groups[target_group]
            
            # Calcular la suma total de los pesos (por si no suman 100)
            total_weight = sum(w for _, w in group_props)
            if total_weight == 0:
                expanded_rows.append(row)
                continue
                
            total_amount = row["amount"]
            allocated_total = 0.0
            
            # 3. Generar las filas divididas matemáticamente
            for i, (g_prop, weight) in enumerate(group_props):
                new_row = row.copy()
                new_row["resolved_property"] = g_prop
                
                # Si es la última propiedad de la lista, le asignamos el resto exacto 
                # para asegurarnos de que no se pierda ni un solo centavo por redondeos.
                if i == len(group_props) - 1:
                    final_amount = round(total_amount - allocated_total, 2)
                    new_row["amount"] = final_amount
                else:
                    # Multiplicamos por la proporción del peso
                    split_amount = round(total_amount * (weight / total_weight), 2)
                    new_row["amount"] = split_amount
                    allocated_total += split_amount
                    
                expanded_rows.append(new_row)
        else:
            # Si no es grupo ni pertenece a uno, pasa intacto
            expanded_rows.append(row)
            
    return pd.DataFrame(expanded_rows)


def main():
    print(f"Iniciando procesamiento | Log: {log_filename}")

    jobs = []
    if os.path.exists("data/clean/amex_ras_net_of_appfolio.csv"):
        jobs.append(("data/clean/amex_ras_net_of_appfolio.csv", "amex"))
        if os.path.exists("data/clean/normalized_citi.csv"):
            jobs.append(("data/clean/normalized_citi.csv", "mastercard"))

    if not jobs:
        print("No se encontraron archivos de entrada.")
        return

    # Carga de catálogos y Alertas
    rules_filepath = "data/master/mapping_rules.xlsx"
    rules_df = pd.read_excel(rules_filepath, sheet_name="Rules")
    
    try:
        alerts_df = pd.read_excel(rules_filepath, sheet_name="Alerts")
    except Exception as e:
        print("No se encontró la pestaña 'Alerts' en el Excel. Saltando validaciones especiales.")
        alerts_df = None

    # Cargar los prorrateos (NUEVO)
    property_groups = load_allocations(rules_filepath)

    gl_directory = pd.read_csv("data/clean/normalized_gl_accounts.csv")
    vendor_directory = pd.read_csv("data/clean/normalized_vendor_directory.csv")
    prop_dir = gl_directory.rename(columns={"code_raw": "normalized_property", "account_name": "raw_property"})

    for path, card_key in jobs:
        print(f"\nProcesando {card_key.upper()}...")

        df = pd.read_csv(path)
        df.columns = df.columns.str.lower()

        # 1. Aplicar Reglas y Generar Log de Errores
        df[['validation_status', 'validation_note']] = df.apply(
            lambda r: pd.Series(apply_validation_rules(r, alerts_df)), axis=1
        )

        errors_to_log = df[df['validation_status'] != "OK"].copy()
        errors_to_log['card_type'] = card_key
        write_error_log(errors_to_log)

        if not errors_to_log.empty:
            print(f"  {len(errors_to_log)} inconsistencias registradas en el log histórico.")

        # 2. Filtrado
        df = df[df["validation_status"].isin(["OK", "ALERT"])].copy()

        if df.empty:
            continue

        # 3. Resolución y Neteado
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

        # ---------------------------------------------------------------------
        # APLICAR PRORRATEOS (SPLITS) DINÁMICAMENTE DESDE EL EXCEL
        # ---------------------------------------------------------------------
        df_netted = split_allocations(df_netted, property_groups)

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
            "Due Date*": df_netted["date"],
            "Posting Date": df_netted["date"],
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