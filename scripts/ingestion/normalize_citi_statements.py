import pandas as pd
import sys
import os
import re

# 1. Configurar la ruta raíz del proyecto dinámicamente
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# 2. Ahora la importación funcionará sin errores
from scripts.utils.text_cleaning import normalize_vendor as normalize

def main():
    input_path = "data/raw/citi_card_statement.csv" # Asegúrate de que este nombre sea correcto
    
    if not os.path.exists(input_path):
        print(f"Archivo no encontrado: {input_path}")
        return

    df = pd.read_csv(input_path)

    # 3. Mapeo específico para Citi (ajusta según los nombres reales de tus columnas)
    column_mapping = {
        'Date': 'date',
        'Description': 'merchant',
        'Debit': 'amount',
        'Company': 'company'
    }
    df = df.rename(columns=column_mapping)

    # 4. Regla de negocio Citi: Filtrar solo transacciones RAS
    if 'company' in df.columns:
        df = df[df['company'].astype(str).str.upper() == "RAS"].copy()

    # 5. Normalización de montos
    if 'amount' in df.columns:
        df['amount'] = (
            df['amount']
            .astype(str)
            .replace({'\$': '', ',': '', '\(': '-', '\)': ''}, regex=True)
            .astype(float)
            .abs()
        )

    # 6. Limpieza y Normalización de Merchant
    # (Aquí podrías aplicar la misma función clean_merchant de Amex)
    df['normalized_merchant'] = df['merchant'].fillna("").apply(normalize)

    # 7. Guardar en la carpeta clean
    output_path = "data/clean/normalized_citi.csv"
    df.to_csv(output_path, index=False)
    print(f"Citi normalizado con éxito: {len(df)} filas guardadas en {output_path}")

if __name__ == "__main__":
    main()