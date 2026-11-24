# Cruce del vendor ledger de AppFolio

read_appfolio_ledger conserva el CSV original y devuelve invoices (cuenta Amex),
other_accounts (solo revisión), invalid_rows y audit. Se normalizan mayúsculas y
espacios de Bank Account. Se reconstruyen facturas por cuenta, proveedor, Reference,
fecha y moneda. Se usa Paid + Unpaid como importe original, con signo; comprobar esta
interpretación cuando cambie el tipo de reporte. USD es una suposición explícita del
llamador cuando no hay columna moneda. Las filas sin fecha ni proveedor se consideran
encabezados/subtotales. Las filas inválidas deben bloquear al llamador antes del cruce.

Las referencias vacías permanecen individuales. Sus IDs son identificadores internos
de procedencia (hash del archivo y línea), no IDs de API AppFolio. Una nueva exportación
del libro cambia estos IDs: no reutilizar automáticamente conciliaciones de huérfanas
entre distintos archivos sin una revisión adicional de identidad. Las facturas con
referencia usan un hash de su clave agrupada. Cada resultado conserva las líneas y
propiedades originales como evidencia.

Pasar invoices + other_accounts al motor. Una coincidencia candidata en otra cuenta
genera POSSIBLE_OTHER_ACCOUNT_MATCH; nunca se consume automáticamente. Las huérfanas
exigen los mismos controles de signo, importe, moneda, proveedor y ventana de fechas.
Los candidatos múltiples quedan pendientes. El score de similitud no es probabilidad.

Conciliar los importes originales antes del prorrateo de exportación. No cruzar una
factura completa contra cada fracción por separado. El nuevo adaptador no modifica
el orquestador productivo. Los informes de simulación no confirman cargas en AppFolio.
