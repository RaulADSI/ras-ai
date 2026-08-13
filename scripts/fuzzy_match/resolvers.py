"""
scripts/fuzzy_match/resolvers.py
Fachada de compatibilidad hacia atrás.
Delega toda la resolución a RulesManager, eliminando la duplicación de lógica.
"""

import pandas as pd
from typing import Tuple, Optional
from scripts.rules_manager import RulesManager

# Instancia compartida por defecto si no se inyecta una
_DEFAULT_MANAGER: Optional[RulesManager] = None

def _get_manager(rules_manager: Optional[RulesManager] = None) -> RulesManager:
    global _DEFAULT_MANAGER
    if rules_manager is not None:
        return rules_manager
    if _DEFAULT_MANAGER is None:
        _DEFAULT_MANAGER = RulesManager()
    return _DEFAULT_MANAGER

def resolve_vendor(row: pd.Series, vendor_directory=None, rules_df=None, score_cutoff: int = 67) -> Tuple[str, float]:
    """Adaptador de compatibilidad para resolve_vendor."""
    manager = _get_manager()
    merchant = row.get("merchant", "")
    target, score, _ = manager.resolve_vendor(merchant, score_cutoff=score_cutoff)
    return target, score

def resolve_property_code(row: pd.Series, property_directory=None, rules_df=None, score_cutoff: int = 75) -> Tuple[str, float, str]:
    """Adaptador de compatibilidad para resolve_property_code."""
    manager = _get_manager()
    prop_hint = row.get("prop_hint") or row.get("gl_account") or row.get("company", "")
    return manager.resolve_property(prop_hint, score_cutoff=score_cutoff)

def resolve_cash_account(card_key: str, rules_df=None) -> str:
    """Adaptador de compatibilidad para resolve_cash_account."""
    manager = _get_manager()
    return manager.resolve_cash_account(card_key)