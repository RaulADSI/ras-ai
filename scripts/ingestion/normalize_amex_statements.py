import pandas as pd
import sys
import os
import re
import glob
import datetime

# ============================================================
# 1. CONFIGURACIÓN DE ENTORNO
# ============================================================
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

try:
    from scripts.utils.text_cleaning import normalize_vendor as normalize
except ImportError:
    def normalize(text): return str(text).upper().strip()

CANONICAL_COLUMNS = ['date', 'merchant', 'account_holder', 'column', 'amount', 'company', 'gl_account']

# ============================================================
# 2. UTILIDADES DE LIMPIEZA
# ============================================================
def clean_currency(series):
    return (
        pd.to_numeric(
            series.astype(str)
            .replace({'\$': '', ',': '', '\(': '-', '\)': ''}, regex=True),
            errors='coerce'
        ).fillna(0.0).round(2)
    )

def clean_merchant(text: str) -> str:
    if not text: return ""
    t = str(text).upper()
    t = re.sub(r"\b\d{4,}\b", "", t)
    t = re.sub(r"[^\w\s]", " ", t)
    return re.sub(r"\s+", " ", t).strip()

# ============================================================
# 3. REGLAS DE NEGOCIO (RAS - ESTRICTO SIN RR)
# ============================================================
def apply_business_rules(df: pd.DataFrame) -> pd.DataFrame:
    def validate_row(row):
        acc = str(row.get('account_holder', '')).upper().strip()
        merc = str(row.get('merchant', '')).upper()
        comp = str(row.get('company', '')).upper()
        gl = str(row.get('gl_account', '')).upper()

        status, notes = "KEEP", []

        # 1. Identificación de Titulares (Respaldo en Merchant si Account está vacío)
        is_armando = ("ARMANDO ARMAS" in acc) or (acc in ["", "NAN", "NONE"] and "ARMANDO ARMAS" in merc)
        is_richard = ("RICHARD LIBUTTI" in acc) or (acc in ["", "NAN", "NONE"] and "RICHARD LIBUTTI" in merc)

        # 2. Marcas Explícitas (SOLO RAS y REITER, excluyendo RR solo)
        is_ras_marked = any(x in comp or x in gl for x in ["RAS", "REITER"])

        # EXCEPCIÓN
        if is_richard and "HAPPY TRAILERS" in comp:
            return pd.Series(["EXCEPTION", "Richard Libutti no opera Happy Trailers"])

        # LÓGICA DE FILTRADO
        if is_ras_marked or is_armando or is_richard:
            status = "KEEP"
            if is_ras_marked: notes.append("Marca RAS/REITER detectada")
            if acc in ["", "NAN", "NONE"] and (is_armando or is_richard): notes.append("Titular en Merchant")
        else:
            status = "SKIP"

        return pd.Series([status, "; ".join(notes)])

    df = df.copy()
    df[['validation_status', 'business_notes']] = df.apply(validate_row, axis=1)
    return df

# ============================================================
# 4. CARGA DE ARCHIVOS (CORREGIDO)
# ============================================================
def load_amex_file(filepath: str) -> pd.DataFrame:
    ext = filepath.lower().split('.')[-1]
    
    # Lectura inicial para inspección
    raw = pd.read_excel(filepath, header=None) if ext == "xlsx" else pd.read_csv(filepath, header=None)

    header_row = None

    # Buscar encabezados reales (ej. "DATE", "AMOUNT")
    for i in range(min(15, len(raw))):
        row_values = [str(val).upper() for val in raw.iloc[i].values]
        row_text = " ".join(row_values)

        if "DATE" in row_text and "AMOUNT" in row_text:
            header_row = i
            break

    # -------------------------------
    # CASO 1: ARCHIVO CON ENCABEZADOS
    # -------------------------------
    if header_row is not None:
        df = pd.read_excel(filepath, header=header_row) if ext == "xlsx" else pd.read_csv(filepath, header=header_row)
        df.columns = [str(c).strip().upper() for c in df.columns]

    # ---------------------------------------------------------
    # CASO 2: ARCHIVO SIN ENCABEZADOS (Tu caso actual)
    # ---------------------------------------------------------
    else:
        # IMPORTANTE: Re-leer con header=None para NO perder la primera fila de datos
        df = pd.read_excel(filepath, header=None) if ext == "xlsx" else pd.read_csv(filepath, header=None)
        
        # Asignar nombres temporales para que el resto del script no falle
        n_cols = df.shape[1]
        temp_columns = CANONICAL_COLUMNS[:n_cols]
        if n_cols > len(CANONICAL_COLUMNS):
            temp_columns += [f"extra_{i}" for i in range(n_cols - len(CANONICAL_COLUMNS))]
        
        df.columns = temp_columns

    # --- NORMALIZACIÓN FINAL DE COLUMNAS ---
    if df.shape[1] < len(CANONICAL_COLUMNS):
        raise ValueError(f"Columnas insuficientes en {os.path.basename(filepath)}")

    # Aseguramos el orden canónico exacto
    df.columns = CANONICAL_COLUMNS + [
        f"extra_{i}" for i in range(df.shape[1] - len(CANONICAL_COLUMNS))
    ]

    return df


# ============================================================
# 5. PIPELINE PRINCIPAL
# ============================================================
def main():
    INPUT_FOLDER = "data/raw/unify_all_amex/"
    OUTPUT_PATH = "data/clean/normalized_amex.csv"
    
    files = glob.glob(os.path.join(INPUT_FOLDER, "*.csv")) + glob.glob(os.path.join(INPUT_FOLDER, "*.xlsx"))

    if not files:
        print("❌ No se encontraron archivos.")
        return

    dfs = []
    for f in files:
        try:
            df = load_amex_file(f)
            df["source_file"] = os.path.basename(f)
            dfs.append(df)
        except Exception as e:
            print(f"❌ Error en {f}: {e}")

    if not dfs:
        return

    df_raw = pd.concat(dfs, ignore_index=True)
    df_raw['amount'] = clean_currency(df_raw['amount'])

    # Deduplicación
    group_cols = ['date', 'merchant', 'account_holder', 'amount', 'company']
    df_raw['occurrence'] = df_raw.groupby(group_cols).cumcount()
    df_raw['dedup_key'] = (df_raw['date'].astype(str) + "|" + df_raw['amount'].astype(str) + "|" + 
    df_raw['merchant'].str.upper().str.strip() + "|" + df_raw['occurrence'].astype(str))
    
    df_dedup = df_raw.drop_duplicates(subset='dedup_key', keep='first').copy()
    statement_total = round(df_dedup['amount'].sum(), 2)

    df_final = apply_business_rules(df_dedup)
    
    processed_df = df_final[df_final['validation_status'] != "SKIP"].copy()
    charges_only = round(processed_df[processed_df['amount'] > 0]['amount'].sum(), 2)
    credits_only = round(processed_df[processed_df['amount'] < 0]['amount'].sum(), 2)
    skipped_total = round(df_final[df_final['validation_status'] == "SKIP"]['amount'].sum(), 2)
    diff = round(statement_total - (charges_only + credits_only + skipped_total), 2)

    

    print("\n" + "╔" + "═" * 62 + "╗")
    print(f"║ {'REPORTE FINANCIERO RAS - AMEX (VERSIÓN FINAL)':^60} ║")
    print("╠" + "═" * 62 + "╣")
    print(f"║ {'VALOR REAL GASTOS RAS (Cargos):':<35} ${charges_only:>18,.2f} ║")
    print(f"║ {'Abonos/Devoluciones Procesados:':<35} ${credits_only:>18,.2f} ║")
    print(f"║ {'Pagos a Tarjeta (Omitidos):':<35} ${skipped_total:>18,.2f} ║")
    print("╟" + "─" * 62 + "╢")
    print(f"║ {'NETO BANCARIO (Statement Total):':<35} ${statement_total:>18,.2f} ║")
    print("╠" + "═" * 62 + "╣")
    print(f"║ {'✅ CONCILIACIÓN RAS EXITOSA':^60} ║" if abs(diff) <= 0.01 else f"║ {'🚨 DIFERENCIA: $' + str(diff):^60} ║")
    print("╚" + "═" * 62 + "╝")

    processed_df['normalized_merchant'] = processed_df['merchant'].apply(clean_merchant).apply(normalize)
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    processed_df.to_csv(OUTPUT_PATH, index=False)

if __name__ == "__main__":
    main()