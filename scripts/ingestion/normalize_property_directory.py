"""
scripts/ingestion/normalize_property_directory.py
Estandariza el catálogo de propiedades desde rentify_entity_dictionary.xlsx,
extrayendo códigos numéricos/cortos para habilitar índices O(1) en RulesManager.
"""

import sys
from pathlib import Path

# --- BOOTSTRAP DE RUTA RAÍZ ---
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import re
import pandas as pd
from scripts.config import ENTITY_DICTIONARY, PROPERTY_DIRECTORY


def normalize(text: object) -> str:
    if text is None or pd.isna(text):
        return ""
    cleaned = re.sub(r"\s+", " ", str(text).strip())
    return cleaned.upper()


def extract_property_code(raw_property: str) -> str:
    """Extrae un código numérico inicial de una propiedad."""
    if not raw_property:
        return ""

    match = re.match(r"^(\d{2,6})(?:\b|$)", raw_property.strip())

    return match.group(1) if match else ""


def run() -> int:
    if not ENTITY_DICTIONARY.exists():
        print(f"⚠️ Archivo no encontrado: {ENTITY_DICTIONARY}")
        return 0

    excel = pd.ExcelFile(ENTITY_DICTIONARY)
    if "property_directory" not in excel.sheet_names:
        print("⚠️ Hoja 'property_directory' no encontrada en el diccionario.")
        return 0

    df = pd.read_excel(excel, sheet_name="property_directory")
    col_prop = "property" if "property" in df.columns else df.columns[0]

    df["raw_property"] = df[col_prop].fillna("").astype(str).str.strip()
    df["normalized_property"] = df["raw_property"].apply(normalize)

    # Detección de columna de código explícita o extracción automática
    if "code" in df.columns:
        df["property_code"] = df["code"].fillna("").astype(str).str.strip().str.upper()
    elif "property_code" in df.columns:
        df["property_code"] = df["property_code"].fillna("").astype(str).str.strip().str.upper()
    else:
        df["property_code"] = df["raw_property"].apply(extract_property_code)

    df_final = df[["raw_property", "normalized_property", "property_code"]].drop_duplicates("normalized_property")
    df_final.to_csv(PROPERTY_DIRECTORY, index=False, encoding="utf-8-sig")
    print(f"✅ Catálogo de propiedades generado: {len(df_final)} registros -> {PROPERTY_DIRECTORY.name}")
    return len(df_final)


if __name__ == "__main__":
    run()