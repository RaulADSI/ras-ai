from unidecode import unidecode
import pandas as pd
import re

# Function to clean and normalize GL Account names
def normalize(text):
    if pd.isna(text):
        return ""
    text = unidecode(str(text))  # Delete accents
    text = text.lower().strip()
    text = re.sub(r"[^\w\s]", "", text)  # Delete punctuation
    text = re.sub(r"\s+", " ", text)     # Normalize whitespace
    return text