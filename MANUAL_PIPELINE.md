# Ejecución manual AMEX

Ejecutar `python run_pipeline.py` o `python main.py`. Ambos usan el flujo AMEX aprobado. Este flujo no procesa Citi.

El CSV vigente se indica en `audit/manual_pipeline_report.json` y se publica en `data/clean/appfolio_batch_<id>.csv`. El informe de propiedades y los proveedores pendientes de creación se publican en `audit/`.

Conserve `data/master/confirmed_amex_decisions.json`: contiene la procedencia exacta de las autorizaciones del usuario. No amplía esas decisiones a otros cargos.

El estado reside en `%LOCALAPPDATA%/Rentify/financial_pipeline`, fuera de OneDrive; configurable con `RENTIFY_STATE_DIR`. No borre el estado ni cambie su ruta para repetir una carga.

Con las mismas entradas se reutiliza el lote. Entradas modificadas o una ejecución interrumpida requieren revisión/recuperación antes de generar otro lote. La incorporación automática de nuevos períodos a lotes previos no está implementada. Exportado no significa importado en AppFolio.

Validación: 98 líneas, USD 3733.82; excluido USD 2854.28; pendiente USD 5018.40. Igualdad de campos comprobada normalizando únicamente la presentación de fechas e importes.
