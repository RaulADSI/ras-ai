"""
scripts/config.py
Fuente única de verdad para rutas, contratos de esquema y artefactos del pipeline.
Módulo puro: sin lógica de negocio ni efectos secundarios en el sistema de archivos al importarse.
"""

import uuid
from pathlib import Path
from datetime import datetime
from typing import List, Tuple

# ==============================================================================
# 1. ÁRBOL DE DIRECTORIOS INMUTABLE
# ==============================================================================
PROJECT_ROOT = Path(__file__).resolve().parents[1]

DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
CLEAN_DIR = DATA_DIR / "clean"
MASTER_DIR = DATA_DIR / "master"

LOGS_DIR = PROJECT_ROOT / "logs"
AUDIT_DIR = PROJECT_ROOT / "audit"

def ensure_directories() -> None:
    """Crea explícitamente los directorios requeridos por el pipeline."""
    for directory in (LOGS_DIR, AUDIT_DIR, CLEAN_DIR, RAW_DIR, MASTER_DIR):
        directory.mkdir(parents=True, exist_ok=True)

# ==============================================================================
# 2. ENTRADAS CRUDAS Y ARCHIVOS MAESTROS
# ==============================================================================
MAPPING_RULES = MASTER_DIR / "mapping_rules.xlsx"
ENTITY_DICTIONARY = RAW_DIR / "rentify_entity_dictionary.xlsx"
VENDOR_LEDGER = RAW_DIR / "appfolio" / "vendor_ledger.csv"

AMEX_RAW_DIR = RAW_DIR / "unify_all_amex"
CITI_RAW_INPUT = RAW_DIR / "citi_card_statement.csv"

# ==============================================================================
# 3. CATÁLOGOS Y ARCHIVOS INTERMEDIOS NORMALIZADOS
# ==============================================================================
GL_DIRECTORY = CLEAN_DIR / "normalized_gl_accounts.csv"
PROPERTY_DIRECTORY = CLEAN_DIR / "normalized_property_directory.csv"
VENDOR_DIRECTORY = CLEAN_DIR / "normalized_vendor_directory.csv"

AMEX_NORMALIZED = CLEAN_DIR / "normalized_amex.csv"
CITI_NORMALIZED = CLEAN_DIR / "normalized_citi.csv"

# ==============================================================================
# 4. ARCHIVOS NETEADOS Y CONCILIADOS (IDEMPOTENTES)
# ==============================================================================
AMEX_NETTED = CLEAN_DIR / "amex_ras_net_of_appfolio.csv"
CITI_NETTED = CLEAN_DIR / "citi_ras_net_of_appfolio.csv"

# ==============================================================================
# 5. SALIDAS FINALES (BULK BILLS APPFOLIO Y AUDITORÍA DE FACTURAS)
# ==============================================================================
AMEX_BULK_BILL = CLEAN_DIR / "appfolio_ras_bulk_bill_amex.csv"
CITI_BULK_BILL = CLEAN_DIR / "appfolio_ras_bulk_bill_mastercard.csv"
UNMATCHED_LEDGER_REPORT = CLEAN_DIR / "reporte_facturas_AppFolio_NO_encontradas.csv"

# ==============================================================================
# 6. CONTRATOS DE ESQUEMA (SCHEMAS EXPLÍCITOS)
# ==============================================================================
# Contrato de salida garantizado por normalize_amex.py y normalize_citi.py
NORMALIZED_STATEMENT_SCHEMA: List[str] = [
    "date",
    "merchant",
    "account_holder",
    "amount",
    "company",
    "gl_account",
    "property_hint"  
]

# Contrato estricto exigido por AppFolio para Bulk Bills
FINAL_APPFOLIO_COLUMNS: List[str] = [
    "Bill Property Code*",
    "Vendor Payee Name*",
    "Amount*",
    "Bill Account*",
    "Bill Date*",
    "Due Date*",
    "Posting Date",
    "Description",
    "Cash Account"
]

# ==============================================================================
# 7. GENERADOR DE IDENTIFICADORES Y RUTAS DE AUDITORÍA
# ==============================================================================
def generate_run_id() -> str:
    """Genera un identificador único con timestamp y entropía hexadecimal."""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    short_hash = uuid.uuid4().hex[:6]
    return f"{ts}_{short_hash}"

def get_run_audit_path(run_id: str) -> Path:
    """Ruta del artefacto de auditoría estructurado para una corrida específica."""
    return AUDIT_DIR / f"pipeline_run_{run_id}.csv"

def get_run_log_path(date_str: str = None) -> Path:
    """Ruta del archivo de log general de aplicación."""
    if date_str is None:
        date_str = datetime.now().strftime("%Y-%m-%d")
    return LOGS_DIR / f"pipeline_{date_str}.log"