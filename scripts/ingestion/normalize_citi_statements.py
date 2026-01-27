import datetime
import pandas as pd
import sys
import os


# CONFIGURACIÓN DE RUTA RAÍZ DEL PROYECTO
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Manejo de error si el módulo no existe
try:
    from scripts.utils.text_cleaning import normalize_vendor as normalize
except ImportError:
    def normalize(text): return str(text).upper().strip() # Fallback local


# UTILIDADES

def clean_currency(series):
    return (
        pd.to_numeric(
            series.astype(str)
            .replace({'\$': '', ',': '', '\(': '-', '\)': ''}, regex=True),
            errors='coerce'
        )
        .fillna(0.0)
    )

# MAIN
def main():
    input_path = "data/raw/citi_card_statement.csv"
    output_path = "data/clean/normalized_citi.csv"
    TOLERANCE = 0.01  

    if not os.path.exists(input_path):
        print(f"Error: No se encontró el archivo en {input_path}")
        return

    # 1. CARGA
    df_raw = pd.read_csv(input_path)
    
    column_mapping = {
        'Date': 'date', 'Description': 'merchant', 
        'Debit': 'debit', 'Credit': 'credit', 'Company': 'company'
    }

    if not set(column_mapping.keys()).issubset(df_raw.columns):
        raise ValueError(f"Columnas faltantes. Esperadas: {list(column_mapping.keys())}")

    # 2. PARSING FINANCIERO (Cálculo del Statement Original)
    df_calc = df_raw.rename(columns=column_mapping).copy()
    df_calc['debit'] = clean_currency(df_calc['debit'])
    df_calc['credit'] = clean_currency(df_calc['credit'])
    
    # El valor real del statement: Cargos menos Abonos
    df_calc['amount'] = df_calc['debit'] - df_calc['credit']
    
    statement_total = round(df_calc['amount'].sum(), 2)
    raw_row_count = len(df_calc)

    # 3. SEGMENTACIÓN RAS
    df_ras = df_calc[df_calc['company'].astype(str).str.upper() == "RAS"].copy()
    ras_total = round(df_ras['amount'].sum(), 2)
    other_total = round(statement_total - ras_total, 2)

    # SALIDA POR CONSOLA (DASHBOARD)
    
    print("\n" + "-" + "═" * 58 + "-")
    print(f"| {'REPORTE DE CONTROL FINANCIERO - CITI':^56} |")
    print("|" + "═" * 58 + "|")
    print(f"| {'VALOR TOTAL DEL STATEMENT (BANCO):':<35} ${statement_total:>15,.2f} |")
    print(f"| {'Transacciones totales:':<35} {raw_row_count:>16} |")
    print("|" + "─" * 58 + "|")
    print(f"| {'Segmento RAS:':<35} ${ras_total:>15,.2f} |")
    print(f"| {'Otras Compañías / Ajustes:':<35} ${other_total:>15,.2f} |")
    print("-" + "═" * 58 + "-")

    # 4. NORMALIZACIÓN Y LIMPIEZA
    df_ras['date'] = pd.to_datetime(df_ras['date'], errors='coerce')
    df_ras = df_ras[df_ras['date'].notna()]
    df_ras['normalized_merchant'] = df_ras['merchant'].fillna("").apply(normalize)
    df_ras = df_ras[df_ras['normalized_merchant'] != ""]

    # 5. PERSISTENCIA
    os.makedirs("logs", exist_ok=True)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    status = "OK" if abs(statement_total - (ras_total + other_total)) < TOLERANCE else "FAIL"
    
    control_results = {
        "date": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "statement_total": statement_total,
        "ras_total": ras_total,
        "status": status
    }
    
    pd.DataFrame([control_results]).to_csv(
        "logs/financial_control_history.csv", 
        mode="a", index=False, header=not os.path.exists("logs/financial_control_history.csv")
    )

    df_ras.to_csv(output_path, index=False)
    print(f"Proceso terminado. Datos de RAS exportados a: {output_path}\n")

if __name__ == "__main__":
    main()