Motor de Conciliación y Validación de Pagos (AMEX/Citi)
Este script procesa las transacciones normalizadas de tarjetas de crédito para generar archivos de carga masiva (Bulk Bills) compatibles con AppFolio. Su función principal es actuar como un filtro de auditoría inteligente que aplica reglas de negocio contables antes de autorizar cualquier pago.

🚀 Funcionalidades Principales
1. Sistema de Reglas de Negocio (Auditoría de Richard Libutti)
El script no solo mueve datos, sino que valida la integridad de cada transacción mediante tres niveles de control:

Validación de Titularidad: Identifica automáticamente a los miembros del equipo core (Armando Armas, Richard Libutti, Cory Reiter, etc.).

Filtro de Excepciones (Happy Trailers HRS): Bloquea automáticamente transacciones donde Richard Libutti aparezca vinculado a la compañía "Happy Trailers HRS", ya que contablemente se ha definido que él no opera dicha entidad.

Alertas de Conciliación (RR Reiter Realty): Marca como ALERT cualquier transacción de la empresa "RR Reiter Realty" que no tenga el identificador de pago RAS en las columnas de compañía o cuenta GL.

2. Recuperación de Datos de Armando Armas
A diferencia de procesos anteriores que dependían exclusivamente de la etiqueta "RAS", este motor prioriza la identidad del titular. Si una transacción pertenece a Armando Armas, el sistema la procesa independientemente de las etiquetas del statement, asegurando que no se pierdan cargos legítimos (como la validación de montos específicos de 69.97).

3. Inteligencia de Neteado (Netting)
El script realiza una suma matemática de cargos y créditos (devoluciones) bajo las siguientes condiciones:

Agrupa por fecha, comercio, vendedor resuelto y propiedad.

Diferenciación de estatus: No mezcla transacciones marcadas como OK con aquellas marcadas como ALERT, permitiendo una revisión clara en el archivo de salida.

Elimina balances de $0.00 provenientes de cancelaciones inmediatas.

4. Resolución de Entidades (Fuzzy Match)
Utiliza algoritmos de lógica difusa para:

Vendedores: Mapear nombres sucios del banco (ej. "THE HOME DEPOT #123") a nombres limpios del directorio oficial.

Propiedades: Asignar cada gasto al código de propiedad correcto en AppFolio basado en la cuenta GL y reglas de mapeo.

Cuentas Cash: Determina automáticamente la cuenta de salida (AMEX o Mastercard) según el archivo de origen.

📊 Formato de Salida (AppFolio Ready)
El archivo generado en data/clean/appfolio_ras_bulk_bill_*.csv incluye una columna de Description enriquecida:

Ejemplo: AMEX | THE HOME DEPOT | ALERT - RR Reiter pagado sin marca RAS

Esto permite que el equipo contable visualice el resultado de la auditoría directamente en el software financiero antes de aprobar el pago.
