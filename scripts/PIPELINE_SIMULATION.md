# Simulación aislada

Ejecutar desde la raíz: `python -m scripts.pipeline_simulator`. Crea una carpeta
temporal exclusiva, conserva el CSV y el informe JSON, e imprime las rutas. No llama
a run_pipeline.py ni lee extractos, reglas o credenciales de producción.

Para escenarios propios, llamar primero a `initialize_simulation(carpeta_vacía)` y
pasar a `run_pipeline_simulation` la base directamente dentro de esa carpeta y las
subcarpetas inbox, processed, failed y exports. Reutilizar esas rutas para comprobar
idempotencia. No mover bases productivas al entorno: el marcador es una protección
contra errores de ruta, no un mecanismo de autenticación. Un consumidor por entorno.

La entrada son registros ya adaptados al contrato de register_transactions, facturas
normalizadas y clasificaciones por ID de fracción. No prueba todavía la lectura de
extractos AMEX/Citi originales ni la aceptación de los CSV por AppFolio.

Cada ejecución guarda un informe JSON con métricas, estados, cobertura y errores.
Un error crítico se registra y vuelve a lanzarse. La cobertura se verifica antes de
publicar, incluyendo transacciones sin fracciones, ceros y signos. La suma se hace
con enteros Python para evitar overflow en SUM de SQLite.

Se recuperan primero los lotes RESERVED con sus instantáneas originales; después se
reservan nuevas fracciones. publish_reserved_batch ya verifica y marca EXPORTED, por
lo que no se vuelve a invocar mark_exported. La publicación es recuperable, no una
transacción distribuida real entre SQLite y archivos.

El inbox confirma por decisión. Ante un error de negocio en una fila, las anteriores
permanecen confirmadas y las posteriores del mismo archivo no se ejecutan. Los demás
archivos continúan. Un error de formato/JSON impide procesar todo el archivo, pues se
parsea completo antes de aplicar decisiones. Los informes distinguen estos fallos
de una ejecución sin pendientes. Revisión pendiente no bloquea las filas válidas.

Pruebas: `python -m pytest -q tests/test_pipeline_simulator.py`.
