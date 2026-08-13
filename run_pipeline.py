"""
run_pipeline.py
Orquestador principal determinístico del pipeline de conciliación y facturación.
Garantiza ejecución atómica por etapas, validación estricta de esquemas y auditoría trazable por corrida.
"""

import sys
import time
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional
import pandas as pd

from scripts.config import (
    ensure_directories,
    generate_run_id,
    get_run_audit_path,
    get_run_log_path,
    NORMALIZED_STATEMENT_SCHEMA,
    FINAL_APPFOLIO_COLUMNS,
    AMEX_NORMALIZED,
    CITI_NORMALIZED,
    AMEX_NETTED,
    CITI_NETTED,
    AMEX_BULK_BILL,
    CITI_BULK_BILL,
    UNMATCHED_LEDGER_REPORT
)
from scripts.rules_manager import RulesManager


# ==============================================================================
# CONFIGURACIÓN DE LOGGING
# ==============================================================================
def setup_logging(run_id: str) -> logging.Logger:
    ensure_directories()
    log_file = get_run_log_path()

    logger = logging.getLogger(f"rentify_{run_id}")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter(
        f"[%(asctime)s] [{run_id}] [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    return logger


# ==============================================================================
# GATEWAYS DE VALIDACIÓN DE ESQUEMAS
# ==============================================================================
def validate_schema(
    df: pd.DataFrame, 
    expected_columns: List[str], 
    stage_name: str, 
    strict: bool = False
) -> None:
    """
    Valida el contrato de datos:
    - strict=False: Verifica que todas las columnas requeridas estén presentes.
    - strict=True: Exige coincidencia exacta de columnas y orden (para AppFolio Bulk Bills).
    """
    actual_cols = list(df.columns)
    
    if strict:
        if actual_cols != expected_columns:
            missing = [c for c in expected_columns if c not in actual_cols]
            extra = [c for c in actual_cols if c not in expected_columns]
            raise ValueError(
                f"Fallo de Contrato Estricto en '{stage_name}'.\n"
                f"  - Columnas Faltantes: {missing}\n"
                f"  - Columnas No Permitidas: {extra}\n"
                f"  - Orden Actual: {actual_cols}"
            )
    else:
        missing = [c for c in expected_columns if c not in actual_cols]
        if missing:
            raise ValueError(
                f"Fallo de Contrato en '{stage_name}'. Columnas obligatorias ausentes: {missing}"
            )


# ==============================================================================
# ORQUESTADOR PRINCIPAL
# ==============================================================================
class PipelineOrchestrator:
    def __init__(self):
        ensure_directories()
        self.run_id = generate_run_id()
        self.logger = setup_logging(self.run_id)
        self.audit_records: List[Dict[str, Any]] = []

    def record_stage(
        self,
        stage: str,
        artifact: str,
        in_rows: int,
        out_rows: int,
        warnings: int,
        errors: int,
        status: str,
        duration: float,
        error_msg: str = ""
    ) -> None:
        """Registra la métrica atómica de una etapa en el dataset de auditoría."""
        self.audit_records.append({
            "run_id": self.run_id,
            "stage": stage,
            "artifact": artifact,
            "input_rows": in_rows,
            "output_rows": out_rows,
            "warnings": warnings,
            "errors": errors,
            "status": status,
            "duration_sec": round(duration, 3),
            "error_message": error_msg
        })

    def run(self) -> bool:
        self.logger.info(f"=== INICIANDO PIPELINE DE CONCILIACIÓN | RUN_ID: {self.run_id} ===")
        total_start = time.time()
        pipeline_success = True

        try:
            # ------------------------------------------------------------------
            # ETAPA 1: Carga y Compilación del Motor de Reglas
            # ------------------------------------------------------------------
            t0 = time.time()
            self.logger.info("Etapa 1: Cargando RulesManager y compilando índices O(1)...")
            rules = RulesManager()
            
            # Métrica real de reglas activas en memoria
            compiled_rules_count = (
                len(rules.manual_vendor_matchers)
                + len(rules.vendor_regex_rules)
                + len(rules.property_regex_rules)
                + len(rules.alerts_rules)
            )

            self.record_stage(
                stage="load_rules",
                artifact="mapping_rules.xlsx",
                in_rows=0,
                out_rows=compiled_rules_count,
                warnings=0,
                errors=0,
                status="SUCCESS",
                duration=time.time() - t0
            )
            # ------------------------------------------------------------------
            # ETAPA 2A: Normalización Amex
            # ------------------------------------------------------------------
            t0 = time.time()
            self.logger.info("Etapa 2A: Normalizando extractos AMEX...")
            from scripts.ingestion import normalize_amex
            amex_raw_count = normalize_amex.run(rules)
            
            if not AMEX_NORMALIZED.exists():
                raise FileNotFoundError(f"Artefacto esperado no generado: {AMEX_NORMALIZED}")
            
            df_amex_norm = pd.read_csv(AMEX_NORMALIZED)
            validate_schema(df_amex_norm, NORMALIZED_STATEMENT_SCHEMA, "normalize_amex", strict=False)
            
            self.record_stage(
                stage="normalize_amex",
                artifact=AMEX_NORMALIZED.name,
                in_rows=amex_raw_count,
                out_rows=len(df_amex_norm),
                warnings=0,
                errors=0,
                status="SUCCESS",
                duration=time.time() - t0
            )

            # ------------------------------------------------------------------
            # ETAPA 2B: Normalización Citi (Simétrica, recibe RulesManager)
            # ------------------------------------------------------------------
            t0 = time.time()
            self.logger.info("Etapa 2B: Normalizando extracto CITI Mastercard...")
            from scripts.ingestion import normalize_citi
            citi_raw_count = normalize_citi.run(rules)
            
            if not CITI_NORMALIZED.exists():
                raise FileNotFoundError(f"Artefacto esperado no generado: {CITI_NORMALIZED}")
            
            df_citi_norm = pd.read_csv(CITI_NORMALIZED)
            validate_schema(df_citi_norm, NORMALIZED_STATEMENT_SCHEMA, "normalize_citi", strict=False)
            
            self.record_stage(
                stage="normalize_citi",
                artifact=CITI_NORMALIZED.name,
                in_rows=citi_raw_count,
                out_rows=len(df_citi_norm),
                warnings=0,
                errors=0,
                status="SUCCESS",
                duration=time.time() - t0
            )

            # ------------------------------------------------------------------
            # ETAPA 3: Deduplicación vs Ledger de AppFolio
            # ------------------------------------------------------------------
            t0 = time.time()
            self.logger.info("Etapa 3: Conciliando y deduplicando contra AppFolio Ledger...")
            from scripts.reconciliation import dedup_appfolio
            dedup_results = dedup_appfolio.run(rules)
            
            # Verificación de artefactos
            if not AMEX_NETTED.exists() or not CITI_NETTED.exists():
                raise FileNotFoundError("Artefactos neteados no encontrados tras la deduplicación.")
            
            df_amex_net = pd.read_csv(AMEX_NETTED)
            df_citi_net = pd.read_csv(CITI_NETTED)

            self.record_stage(
                stage="dedup_amex",
                artifact=AMEX_NETTED.name,
                in_rows=len(df_amex_norm),
                out_rows=len(df_amex_net),
                warnings=dedup_results.get("amex_duplicates", 0),
                errors=0,
                status="SUCCESS",
                duration=time.time() - t0
            )
            self.record_stage(
                stage="dedup_citi",
                artifact=CITI_NETTED.name,
                in_rows=len(df_citi_norm),
                out_rows=len(df_citi_net),
                warnings=dedup_results.get("citi_duplicates", 0),
                errors=0,
                status="SUCCESS",
                duration=time.time() - t0
            )

            # ------------------------------------------------------------------
            # ETAPA 4: Resolución de Entidades, Prorrateo y Bulk Bills
            # ------------------------------------------------------------------
            t0 = time.time()
            self.logger.info("Etapa 4: Resolviendo entidades, prorrateos y generando Bulk Bills...")
            from scripts.output import bulk_bill_generator
            gen_metrics = bulk_bill_generator.run_generation(rules, self.run_id)

            if not AMEX_BULK_BILL.exists() or not CITI_BULK_BILL.exists():
                raise FileNotFoundError("Archivos finales Bulk Bill no generados.")

            df_amex_final = pd.read_csv(AMEX_BULK_BILL)
            df_citi_final = pd.read_csv(CITI_BULK_BILL)

            # Gateway Estricto: Validación de columnas y orden exacto de AppFolio
            validate_schema(df_amex_final, FINAL_APPFOLIO_COLUMNS, "bulk_bill_amex", strict=True)
            validate_schema(df_citi_final, FINAL_APPFOLIO_COLUMNS, "bulk_bill_citi", strict=True)

            # Business Validation Gate: Conteo seguro con na=False
            unresolved_props = int(
                df_amex_final["Bill Property Code*"].str.startswith("REVISAR PROP:", na=False).sum() +
                df_citi_final["Bill Property Code*"].str.startswith("REVISAR PROP:", na=False).sum()
            )
            if unresolved_props > 0:
                self.logger.warning(f"⚠️ Se detectaron {unresolved_props} propiedades no resueltas marcadas para revisión.")

            self.record_stage(
                stage="bulk_bills_generation",
                artifact=f"{AMEX_BULK_BILL.name} | {CITI_BULK_BILL.name}",
                in_rows=len(df_amex_net) + len(df_citi_net),
                out_rows=len(df_amex_final) + len(df_citi_final),
                warnings=gen_metrics.get("warnings", 0) + unresolved_props,
                errors=0,
                status="SUCCESS",
                duration=time.time() - t0
            )

            self.logger.info(f"=== PIPELINE FINALIZADO CON ÉXITO EN {time.time() - total_start:.2f}s ===")

        except Exception as e:
            pipeline_success = False
            self.logger.error(f"❌ ERROR CRÍTICO EN PIPELINE: {str(e)}", exc_info=True)
            self.record_stage(
                stage="pipeline_failure",
                artifact="N/A",
                in_rows=0,
                out_rows=0,
                warnings=0,
                errors=1,
                status="FAILED",
                duration=time.time() - total_start,
                error_msg=str(e)
            )

        finally:
            # ------------------------------------------------------------------
            # PERSISTENCIA DEL REPORTE DE AUDITORÍA ATÓMICO
            # ------------------------------------------------------------------
            audit_df = pd.DataFrame(self.audit_records)
            audit_path = get_run_audit_path(self.run_id)
            audit_df.to_csv(audit_path, index=False, encoding="utf-8-sig")
            self.logger.info(f"Artefacto de auditoría guardado en: {audit_path}")

        return pipeline_success


if __name__ == "__main__":
    orchestrator = PipelineOrchestrator()
    sys.exit(0 if orchestrator.run() else 1)