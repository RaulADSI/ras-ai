"""
scripts/rules_manager.py
Motor centralizado de reglas contables, resolución de entidades y validaciones.
Único punto de acceso autorizado para leer e interpretar mapping_rules.xlsx.
"""

import re
import pandas as pd
from pathlib import Path
from typing import Dict, List, Tuple, Optional

from scripts.config import (
    MAPPING_RULES,
    PROPERTY_DIRECTORY,
    VENDOR_DIRECTORY
)


# ==============================================================================
# PIPELINE ÚNICO DE NORMALIZACIÓN DE TEXTO
# ==============================================================================
def normalize_text(text: object) -> str:
    """
    Función canónica de normalización para todo el motor de reglas.
    Convierte nulos en cadena vacía, colapsa espacios múltiples y pasa a mayúsculas.
    """
    if text is None or pd.isna(text):
        return ""
    cleaned = re.sub(r"\s+", " ", str(text).strip())
    return cleaned.upper()


# Fallback seguro de Fuzzy Matching
try:
    from rapidfuzz import process, fuzz

    def get_best_match(query_norm: str, choices_norm: List[str], score_cutoff: int = 50) -> Tuple[Optional[str], float]:
        if not query_norm or not choices_norm:
            return None, 0.0
        result = process.extractOne(query_norm, choices_norm, scorer=fuzz.token_set_ratio, score_cutoff=score_cutoff)
        if result is None:
            return None, 0.0
        return result[0], float(result[1])
except ImportError:
    def get_best_match(query_norm: str, choices_norm: List[str], score_cutoff: int = 50) -> Tuple[Optional[str], float]:
        return None, 0.0


class RulesManager:
    # 1. Diccionario de Overrides Manuales (Base)
    _RAW_MANUAL_VENDOR_RULES: Dict[str, str] = {
        "THE HOME DEPOT": "The Home Depot",
        "HOME DEPOT": "The Home Depot",
        "THE SHERWIN WILLIAMS": "Sherwin Williams",
        "THE SHERWINWILLIAMSCLEVELAND": "Sherwin Williams",
        "SHERWIN WILLIAMS": "Sherwin Williams",
        "ACE HDWE OF OPA LOCKOPA LOCKA": "Ace Hardware",
        "ACE HARDWARE": "Ace Hardware",
        "ACE HDWE OF OPA LOCKA": "Ace Hardware",
        "ACE HDWE": "Ace Hardware",
        "SYKES ACE HARDWARE": "Ace Hardware",
        "BRANDSMART USA": "Brandsmart USA",
        "7-ELEVEN": "7-Eleven",
        "7ELEVEN": "7-Eleven",
        "USPS": "USPS",
        "AMAZON": "Amazon",
        "AMAZON.COM": "Amazon",
        "IN *SWIFTPIX REAL ES": "Swiftpix Real Estate",
        "SHINEPAY LAUNDRY APP": "Shinepay Laundry",
        "WINDOWS & DOORS": "Windows & Doors",
        "WINDOWS DOORS": "Windows & Doors"
    }

    def __init__(self, rules_path: Path = MAPPING_RULES):
        self.rules_path = rules_path
        
        # Índices en memoria O(1)
        self.vendor_to_gl: Dict[str, str] = {}
        self.cash_accounts: Dict[str, str] = {}
        self.property_groups: Dict[str, List[Tuple[str, float]]] = {}
        self.vendor_directory_map: Dict[str, str] = {}
        self.property_directory_map: Dict[str, str] = {}
        self.property_code_map: Dict[str, str] = {}  # <-- Nuevo Índice O(1) por Código Corto

        # Matchers y Catálogos
        self.manual_vendor_matchers: List[Tuple[re.Pattern, str]] = []
        self.vendor_regex_rules: List[Tuple[re.Pattern, str]] = []
        self.property_regex_rules: List[Tuple[re.Pattern, str]] = []
        self.alerts_rules: List[dict] = []
        self.vendor_directory_choices: List[str] = []
        self.property_directory_choices: List[str] = []

        self._load_and_compile()

    def _load_and_compile(self) -> None:
        """Carga los DataFrames maestros y precalcula los índices y matchers compilados."""
        if not self.rules_path.exists():
            raise FileNotFoundError(f"Archivo maestro no encontrado: {self.rules_path}")

        # 1. Compilar Overrides Manuales sobre texto normalizado
        self.manual_vendor_matchers = [
            (re.compile(re.escape(normalize_text(k))), target)
            for k, target in self._RAW_MANUAL_VENDOR_RULES.items()
        ]

        excel = pd.ExcelFile(self.rules_path)

        # 2. Compilar Hoja 'Rules' (Manejo nativo de nulos)
        if "Rules" in excel.sheet_names:
            rules_df = pd.read_excel(excel, sheet_name="Rules")
            if "priority" not in rules_df.columns:
                rules_df["priority"] = 10
            rules_df = rules_df.sort_values("priority", ascending=False)

            for _, row in rules_df.iterrows():
                raw_key = row.get("Raw_Text (Key)")
                mapped_val = row.get("Mapped_Value")
                category = row.get("Category")
                gl_hint = row.get("GL_Account_Hint")

                if pd.isna(raw_key) or pd.isna(mapped_val):
                    continue

                norm_key = normalize_text(raw_key)
                norm_val = str(mapped_val).strip()
                cat = normalize_text(category)

                if not norm_key or not norm_val:
                    continue

                if cat == "VENDOR":
                    pattern = re.compile(re.escape(norm_key))
                    self.vendor_regex_rules.append((pattern, norm_val))

                    if pd.notna(gl_hint) and str(gl_hint).strip():
                        self.vendor_to_gl[normalize_text(norm_val)] = str(gl_hint).strip()

                elif cat == "PROPERTY":
                    pattern = re.compile(re.escape(norm_key))
                    self.property_regex_rules.append((pattern, norm_val))

                elif cat == "CASH":
                    self.cash_accounts[norm_key.lower()] = norm_val

        # 3. Compilar Hoja 'Allocations' (Prorrateos)
        if "Allocations" in excel.sheet_names:
            alloc_df = pd.read_excel(excel, sheet_name="Allocations")
            alloc_df.columns = alloc_df.columns.str.strip()
            alloc_df = alloc_df.dropna(subset=["Group_Name", "Property_Code"])
            if "Weight" not in alloc_df.columns:
                alloc_df["Weight"] = 1.0

            for group_name, group_data in alloc_df.groupby("Group_Name"):
                props = []
                for _, r in group_data.iterrows():
                    prop_code = str(r["Property_Code"]).strip()
                    weight = float(r["Weight"])
                    props.append((prop_code, weight))
                self.property_groups[normalize_text(group_name)] = props

        # 4. Compilar Hoja 'Alerts' (Validaciones)
        if "Alerts" in excel.sheet_names:
            alerts_df = pd.read_excel(excel, sheet_name="Alerts")
            for _, r in alerts_df.iterrows():
                r_type = r.get("Rule_Type")
                if pd.isna(r_type) or not str(r_type).strip():
                    continue

                self.alerts_rules.append({
                    "type": normalize_text(r_type),
                    "acc": normalize_text(r.get("Account_Contains")),
                    "comp": normalize_text(r.get("Company_Contains")),
                    "miss_gl": normalize_text(r.get("Missing_Keyword_GL")),
                    "miss_comp": normalize_text(r.get("Missing_Keyword_Company")),
                    "msg": str(r.get("Message", "")).strip() if pd.notna(r.get("Message")) else ""
                })

        # 5. Cargar Catálogos Canónicos para Fuzzy Matching
        if VENDOR_DIRECTORY.exists():
            v_df = pd.read_csv(VENDOR_DIRECTORY)
            if "normalized_company" in v_df.columns and "company_name" in v_df.columns:
                valid_v = v_df.dropna(subset=["normalized_company", "company_name"])
                for _, r in valid_v.iterrows():
                    norm_c = normalize_text(r["normalized_company"])
                    self.vendor_directory_map[norm_c] = str(r["company_name"]).strip()
                self.vendor_directory_choices = list(self.vendor_directory_map.keys())

        if PROPERTY_DIRECTORY.exists():
            p_df = pd.read_csv(PROPERTY_DIRECTORY)
            if "normalized_property" in p_df.columns and "raw_property" in p_df.columns:
                valid_p = p_df.dropna(subset=["normalized_property", "raw_property"])
                for _, r in valid_p.iterrows():
                    norm_p = normalize_text(r["normalized_property"])
                    real_p = str(r["raw_property"]).strip()
                    self.property_directory_map[norm_p] = real_p

                    # Poblar mapa de códigos si existe
                    if "property_code" in r and pd.notna(r["property_code"]):
                        code_norm = normalize_text(r["property_code"])
                        if code_norm:
                            self.property_code_map[code_norm] = real_p

    # ==============================================================================
    # RESOLVERS Y EVALUADORES
    # ==============================================================================
    def resolve_vendor(self, merchant_raw: object, score_cutoff: int = 67) -> Tuple[str, float, str]:
        """
        Jerarquía Determinística (Evaluada 100% sobre texto normalizado):
        1. Manual Override
        2. Exact / Pattern Rule en Excel Maestro
        3. Fuzzy Match sobre vendor_directory
        4. Fallback Raw
        """
        norm_merchant = normalize_text(merchant_raw)
        if not norm_merchant:
            return "UNKNOWN VENDOR", 0.0, "unresolved"

        raw_display = str(merchant_raw).strip()

        # 1. Manual Overrides
        for pattern, target in self.manual_vendor_matchers:
            if pattern.search(norm_merchant):
                return target, 100.0, "manual_override"

        # 2. Excel Rules
        for pattern, target in self.vendor_regex_rules:
            if pattern.search(norm_merchant):
                return target, 100.0, "excel_rule"

        # 3. Fuzzy Match
        if self.vendor_directory_choices:
            match_norm, score = get_best_match(norm_merchant, self.vendor_directory_choices, score_cutoff=score_cutoff)
            if match_norm and match_norm in self.vendor_directory_map:
                return self.vendor_directory_map[match_norm], score, "fuzzy_match"

        return raw_display, 0.0, "fallback_raw"

    def resolve_property(self, prop_hint_raw: object, score_cutoff: int = 75) -> Tuple[str, float, str]:
        """
        Jerarquía Determinística de Resolución:
        1. Reglas / Aliases Explícitos en Excel Maestro (mapping_rules.xlsx)
        2. Búsqueda Exacta por Nombre en Catálogo O(1) (normalized_property_directory.csv)
        3. Búsqueda por Código Corto en Catálogo O(1) (property_code_map)
        4. Coincidencia Difusa O(M) sobre el Catálogo
        5. Fallback a Reporte de Revisión
        """
        norm_prop = normalize_text(prop_hint_raw)
        if not norm_prop:
            return "REVISAR PROP: VACIO", 0.0, "unresolved"

        raw_display = str(prop_hint_raw).strip()

        # 1. Reglas Explícitas en Excel
        for pattern, target in self.property_regex_rules:
            if pattern.search(norm_prop):
                return target, 100.0, "excel_rule"

        # 2. Match Exacto por Nombre O(1)
        if norm_prop in self.property_directory_map:
            return self.property_directory_map[norm_prop], 100.0, "directory_exact"

        # 3. Match Exacto por Código Corto O(1)
        if norm_prop in self.property_code_map:
            return self.property_code_map[norm_prop], 100.0, "directory_code"

        # 4. Fuzzy Matching O(M)
        if self.property_directory_choices:
            match_norm, score = get_best_match(norm_prop, self.property_directory_choices, score_cutoff=score_cutoff)
            if match_norm and match_norm in self.property_directory_map:
                return self.property_directory_map[match_norm], score, "fuzzy_match"

        return f"REVISAR PROP: {raw_display}", 0.0, "unresolved"

    def resolve_gl(self, resolved_vendor: str, default_gl: str = "6435: General Repairs") -> str:
        """Lookup O(1) de cuenta contable por proveedor resuelto."""
        if not resolved_vendor:
            return default_gl
        return self.vendor_to_gl.get(normalize_text(resolved_vendor), default_gl)

    def resolve_cash_account(self, card_key: str, default_cash: str = "1150: Operating") -> str:
        """Lookup O(1) de cuenta de contrapartida bancaria."""
        if not card_key:
            return default_cash
        return self.cash_accounts.get(str(card_key).lower().strip(), default_cash)

    def evaluate_row_alerts(self, row: pd.Series) -> Tuple[str, str]:
        """Evaluación determinística de alertas sin comparaciones artificiales con 'NAN'."""
        if not self.alerts_rules:
            return "OK", ""

        acc = normalize_text(row.get("account_holder"))
        comp = normalize_text(row.get("company"))
        gl_hint = normalize_text(row.get("gl_account"))

        for rule in self.alerts_rules:
            r_acc = rule["acc"]
            r_comp = rule["comp"]
            r_miss_gl = rule["miss_gl"]
            r_miss_comp = rule["miss_comp"]

            match = True
            if r_acc and r_acc not in acc:
                match = False
            if r_comp and r_comp not in comp:
                match = False
            if r_miss_gl and r_miss_gl in gl_hint:
                match = False
            if r_miss_comp and r_miss_comp in comp:
                match = False

            if match:
                return rule["type"], rule["msg"]

        return "OK", ""