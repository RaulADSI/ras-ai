# Persistencia: primera etapa

El ejecutor es independiente del pipeline. No se crean bases de producción ni se
modifican extractos al importar estos módulos. No implementa todavía las operaciones
financieras de reserva, decisiones o confirmación de importación.

Ejecutar pruebas desde la raíz:

```powershell
python -m unittest discover -s tests -v
```

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
