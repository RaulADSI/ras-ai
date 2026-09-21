# Persistencia: primera etapa

El ejecutor es independiente del pipeline. No se crean bases de producción ni se
modifican extractos al importar estos módulos. Ledger implementa reserva, publicación
y confirmación por fracción. La ingestión y las decisiones manuales todavía no están
conectadas a este flujo; run_pipeline.py conserva su ejecución anterior.

Ejecutar pruebas desde la raíz:

```powershell
python -m unittest discover -s tests -v
```

Para ejecutar también invariantes e integración (requiere pytest):

```powershell
python -m pytest -q
```

## Exportación reservada

La migración 0003 añade clasificación e instantáneas persistidas. Las fracciones
elegibles deben tener propiedad, proveedor, GL y cuenta de efectivo antes de reservar.
Se comprueba cobertura con signo de sus eventos completos. Un lote no mezcla monedas.

`batch_id, snapshot = ledger.reserve_batch()` reserva sin cambiar elegibilidad.
`publish_reserved_batch(ledger, batch_id, output_dir)` en bulk_bill_generator genera
las nueve columnas del contrato AppFolio desde la instantánea, valida bytes y publica
un archivo por lote. Usa un enlace físico atómico sin sobrescritura en el mismo volumen;
si el sistema de archivos no lo soporta, falla conservando la reserva.

Al reiniciar, el mismo batch_id recupera el contenido mediante get_snapshot; no se
vuelven a evaluar reglas. Un archivo existente incompatible se rechaza. Los lotes
anteriores sin snapshot_json requieren revisión, no reconstrucción automática.
La verificación compara el archivo completo, no solo totales o hashes suministrados.
Los importados y fallos retenidos no se reservan nuevamente. No hay liberación manual
implementada ni confirmación automática desde AppFolio. Las pruebas no certifican
aceptación por AppFolio ni protegen contra edición externa posterior del CSV.

## Registro de ingestión (migración 0004)

`Ledger.register_transactions(records)` recibe diccionarios con `bank_id`,
`account_ref`, `bank_reference` (cadena vacía si falta), `amount_str`, `currency`,
`original_date`, `file_hash`, `sheet_name` y `row_number`. Para CSV usar `CSV` como
hoja y un número de fila de origen estable. El llamador debe calcular SHA-256 de los
bytes originales antes de normalizarlos; el Ledger calcula el hash de coordenadas.
Si se suministra `provenance_hash`, se verifica contra esas coordenadas.

Los importes se reciben como texto decimal sin símbolos ni separadores de miles;
no se admiten floats, valores no finitos, fracciones reales de centavo ni importes
fuera del entero SQLite de 64 bits. Ceros decimales adicionales, como `1.2300`, son
exactos y se aceptan. Cada nuevo evento crea una fracción 1:1 en REVIEW_REQUIRED.

La respuesta distingue `registered` (procedencias nuevas), `transactions_created`,
`provenance_linked` (procedencias nuevas de eventos existentes) y
`skipped_reingestion`. Una repetición debe tener datos idénticos; cualquier colisión
revierte el lote completo. No se fusionan eventos sin referencia por similitud.

La columna obligatoria antigua `transactions.provenance_hash` se conserva por
compatibilidad con bases y consumidores anteriores y se llena con la primera
procedencia; no decide la identidad bancaria. El índice parcial nuevo impone esa
identidad independientemente del archivo. `transaction_provenance` contiene las
coordenadas y rechaza actualizaciones y borrados. No se inventa archivo/hoja/fila
para eventos legados: requieren recuperar sus fuentes para completar el historial.
Las referencias duplicadas legadas bloquean la migración para revisión manual.

El migrador descubre 0004 automáticamente. No se alteraron migraciones anteriores
ni se ejecutó contra datos productivos. Los normalizadores y el orquestador aún no
llaman register_transactions; este paso implementa y prueba su interfaz de registro.

## Conciliación aislada (migración 0005)

`ReconciliationEngine(ledger).run(invoices, classifications)` recibe facturas
normalizadas y un mapa de classification por allocation_id. No descarga AppFolio
ni modifica dedup_appfolio.py, fuzzy_match o run_pipeline.py. El llamador debe
proporcionar un libro completo para el período; una lista vacía significa ausencia
confirmada de facturas, no un fallo de lectura. Los campos de factura se describen
en scripts/reconciliation/engine.py. Las clasificaciones deben venir de reglas
verificadas; la API no valida pertenencia de códigos a catálogos externos.

Coincide por importe entero con signo, moneda, proveedor y ventana inclusiva de
cinco días. Una referencia bancaria suministrada debe concordar; no utilizar como
tal el número interno de factura. La similitud SequenceMatcher no es una probabilidad
estadística. Requiere allow_fuzzy=True y umbral >=90 para conciliar; similitudes
entre 60 y ese umbral quedan en revisión. Proveedores con similitud menor a 60 se
consideran diferentes. La coincidencia exacta normaliza mayúsculas y espacios.

Múltiples candidatos, múltiples cargos que reclaman una factura y facturas consumidas
generan excepciones. No se decide arbitrariamente por orden. Los casos sin candidato
pueden ser READY_TO_IMPORT únicamente con clasificación completa y confianza >=90
(si no se proporciona confidence, se asume clasificación explícita verificada).

match_invoice, classify_allocation y create_exception usan transacciones individuales
y rechazan fracciones no pendientes o bloqueadas por exportación. La unicidad SQLite
protege el consumo concurrente. Los cambios y sus eventos de auditoría se confirman
juntos. Las excepciones idénticas no se duplican; sus cambios incrementan versión y
conservan el historial en audit_events. La clasificación o conciliación resuelve sus
excepciones pendientes. El proceso confirma por fracción, no por corrida completa;
si se interrumpe, una nueva ejecución solo selecciona las pendientes.

## Inbox de decisiones (migración 0006)

`process_inbox(ledger, inbox_dir, processed_dir, failed_dir)` en
scripts/reconciliation/inbox_processor.py procesa CSV UTF-8 con columnas:
decision_id, exception_id, allocation_id, action, corrected_fields_json, reason,
reviewed_by y opcionalmente exception_version (1 por defecto). Especificar la
versión vigente para excepciones modificadas. Acciones: CLASSIFY o LINK_INVOICE.
CLASSIFY admite property_code, vendor_name, gl_account, cash_account y description;
LINK_INVOICE admite únicamente appfolio_invoice_id. No se permiten cambios de
importe, moneda ni identidad. LINK_INVOICE representa una aprobación humana; no
consulta AppFolio para validar la evidencia del analista.

Cada decisión y su auditoría se confirman en una única transacción. El hash incluye
la solicitud completa y la versión. Un reintento idéntico devuelve ALREADY_APPLIED;
un ID con otra solicitud falla. Las decisiones persistidas son inmutables.

El archivo se parsea completo antes de aplicar filas. La atomicidad es por decisión,
no por CSV: si falla una fila, las anteriores permanecen aplicadas. El CSV original
se archiva en failed con nombre único; el resumen informa los efectos parciales.
Se pueden corregir y reenviar filas rechazadas conservando IDs de las ya aplicadas.
Un fallo de archivado deja el archivo en inbox para reintento idempotente; no revierte
decisiones confirmadas. Los errores están en summary['errors']; el llamador debe
inspeccionar files_failed y archive_errors. Otros archivos continúan procesándose.

Usar un consumidor por inbox y entregar archivos mediante rename tras terminar de
escribirlos. Las carpetas deben ser distintas, no anidadas y preferentemente del
mismo volumen. No se sobrescriben archivos históricos. El orquestador productivo
no invoca aún este procesador; todas las pruebas utilizan carpetas y bases temporales.

Crear previamente una carpeta local fuera de OneDrive y ejecutar explícitamente:

```powershell
python -m scripts.persistence.migrate --database C:/ruta/local/rentify/ledger.db
```

`--timeout` configura la espera por el escritor. Una base ocupada produce un error
controlado y salida 1; se puede reintentar. La conexión debe cerrarse explícitamente.

Las migraciones UTF-8 deben ser consecutivas desde `0001`, con nombres como
`0002_add_transactions.sql`. Nunca editar ni retirar una migración aplicada. Cada
migración se confirma por separado junto a su hash de bytes y fecha UTC. Un fallo
conserva las migraciones anteriores; revierte la actual. La tabla de seguimiento
se inicializa separadamente y puede existir vacía tras un primer fallo.

Las instrucciones requieren punto y coma final. SQLite analiza triggers y cadenas;
el autorizador prohíbe controles transaccionales, PRAGMA, ATTACH/DETACH y acceso a la
tabla de seguimiento desde migraciones. Solo ejecutar SQL local revisado: este
mecanismo no es un sandbox para código hostil ni una firma contra manipulación
simultánea de archivos y base de datos.

Para generar la referencia del esquema, añadir `--schema-output ruta/schema.sql`.
Ese archivo no se usa como entrada. Respaldar la base de forma consistente antes
de migraciones destructivas; no hay migraciones descendentes automáticas.
