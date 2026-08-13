"""
main.py
Punto de entrada directo para generar Bulk Bills de forma independiente.
"""

import sys
from pathlib import Path

# Asegurar path raíz
PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.rules_manager import RulesManager
from scripts.output.bulk_bill_generator import run_generation

if __name__ == "__main__":
    rules = RulesManager()
    metrics = run_generation(rules, "manual_run")
    print(f"✅ Generación completada: {metrics}")