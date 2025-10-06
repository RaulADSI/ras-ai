# scripts/utils/text_cleaning.py

from unidecode import unidecode
import re
import pandas as pd

# --- Normalización de GL Accounts ---
def normalize_gl_account(text):
    """
    Limpia y normaliza textos de cuentas contables (GL Accounts).
    - Elimina acentos, mayúsculas, puntuación y espacios extra.
    """
    if pd.isna(text):
        return ""
    text = unidecode(str(text))  # elimina acentos
    text = text.lower().strip()
    text = re.sub(r"[^\w\s]", "", text)  # quita puntuación
    text = re.sub(r"\s+", " ", text)     # normaliza espacios
    return text


# --- Normalización de Vendors ---
KEEP_THE = {"the home depot", "The Right Fix"}

def normalize_vendor(text):
    if not isinstance(text, str) or text.strip() == "":
        return ""

    text = unidecode(text)  # elimina acentos
    text = text.lower().strip()

    # Reemplazar guiones y * por espacio
    text = text.replace("-", " ").replace("*", " ")

    # Eliminar números largos (referencias de tarjeta, etc.)
    text = re.sub(r"\d{3,}", " ", text)

    # Quitar caracteres especiales restantes
    text = re.sub(r"[^\w\s]", " ", text)

    # Normalizar abreviaturas comunes
    text = text.replace("hdwe", "hardware")

    # Colapsar espacios
    text = re.sub(r"\s+", " ", text).strip()

    # Manejo de "the"
    if text.startswith("the ") and text not in KEEP_THE:
        text = text[4:]

    # Casos especiales de vendors conocidos
    if "amazon" in text or "amzn" in text:
        return "amazon"
    if "sherwin williams" in text:
        return "sherwin williams"
    if "home depot" in text:
        return "the home depot"

    # Eliminar sufijos de ubicación (ciudad + estado)
    text = re.sub(r"\b(miami|cleveland|fort lauderd[a-z]*|davie|hialeah|opa locka|north miami)\b", "", text)
    text = re.sub(r"\b(fl|oh|ca|wa|tx|ny|pa|il|ga|nc|az|ma|mi)\b$", "", text).strip()

    # Colapsar otra vez espacios
    text = re.sub(r"\s+", " ", text).strip()

    return text

