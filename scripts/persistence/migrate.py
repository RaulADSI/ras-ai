"""Migraciones inmutables y atómicas, sin executescript ni dependencias externas."""

import argparse
import hashlib
import re
import sqlite3
from pathlib import Path

from scripts.persistence.ledger import connect

DEFAULT_MIGRATIONS = Path(__file__).with_name("migrations")


class MigrationError(RuntimeError):
    pass


class MigrationBusyError(MigrationError):
    pass


def statements(sql):
    buffer = ""
    for char in sql:
        buffer += char
        if char == ";" and sqlite3.complete_statement(buffer):
            yield buffer
            buffer = ""
    # Only trailing whitespace/comments may remain. Never strip comments from SQL.
    tail = re.sub(r"--[^\n]*(?:\n|$)|/\*[\s\S]*?\*/", "", buffer).strip()
    if tail:
        raise MigrationError("Instrucción incompleta: se exige punto y coma final")


def load_migrations(directory):
    migrations = []
    for path in sorted(Path(directory).glob("*.sql")):
        match = re.fullmatch(r"(\d{4})_[a-z0-9_]+\.sql", path.name)
        if not match:
            raise MigrationError(f"Nombre inválido: {path.name}")
        raw = path.read_bytes()
        migrations.append((int(match[1]), path.name, hashlib.sha256(raw).hexdigest(),
                           list(statements(raw.decode("utf-8-sig")))))
    if not migrations or [m[0] for m in migrations] != list(range(1, len(migrations) + 1)):
        raise MigrationError("Versiones vacías, duplicadas o discontinuas")
    return migrations


def _authorize(action, arg1, arg2, database, source):
    # SQLite parses SQL, including triggers and comments, before this callback.
    forbidden = {sqlite3.SQLITE_TRANSACTION, sqlite3.SQLITE_SAVEPOINT,
                 sqlite3.SQLITE_ATTACH, sqlite3.SQLITE_DETACH, sqlite3.SQLITE_PRAGMA}
    if action in forbidden or (arg1 or "").lower() == "schema_migrations":
        return sqlite3.SQLITE_DENY
    return sqlite3.SQLITE_OK


def migrate(database, directory=DEFAULT_MIGRATIONS, timeout=5.0):
    migrations = load_migrations(directory)
    conn = connect(database, timeout)
    applied = []
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("""CREATE TABLE IF NOT EXISTS schema_migrations (
            version INTEGER PRIMARY KEY, name TEXT NOT NULL,
            hash_sha256 TEXT NOT NULL,
            applied_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
        )""")
        history = conn.execute("SELECT version,name,hash_sha256 FROM schema_migrations ORDER BY version").fetchall()
        expected = [m[:3] for m in migrations]
        if history != expected[:len(history)] or len(history) > len(expected):
            raise MigrationError("Historial alterado o migración aplicada ausente")
        conn.commit()
        for version, name, digest, sql_statements in migrations:
            conn.execute("BEGIN IMMEDIATE")
            # Re-read under the writer lock: another process may have migrated meanwhile.
            history = conn.execute("SELECT version,name,hash_sha256 FROM schema_migrations ORDER BY version").fetchall()
            if history != expected[:len(history)] or len(history) > len(expected):
                raise MigrationError("Historial incompatible")
            if version <= len(history):
                conn.commit()
                continue
            try:
                conn.set_authorizer(_authorize)
                for statement in sql_statements:
                    conn.execute(statement)
            finally:
                conn.set_authorizer(None)
            if conn.execute("PRAGMA foreign_key_check").fetchone():
                raise MigrationError("Integridad referencial inválida")
            conn.execute("INSERT INTO schema_migrations(version,name,hash_sha256) VALUES (?,?,?)",
                         (version, name, digest))
            conn.commit()
            applied.append(version)
        return applied
    except sqlite3.Error as exc:
        conn.rollback()
        if getattr(exc, "sqlite_errorcode", 0) & 255 in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED):
            raise MigrationBusyError("Base ocupada; vuelva a intentar la migración") from exc
        raise MigrationError(f"Migración revertida: {exc}") from exc
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()


def export_schema(database, destination):
    """Referencia generada; nunca se utiliza para aplicar migraciones."""
    conn = sqlite3.connect(Path(database).resolve().as_uri() + "?mode=ro", uri=True)
    try:
        rows = conn.execute("SELECT sql FROM sqlite_schema WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%' ORDER BY type,name")
        text = "-- Generado automáticamente; editar migrations/*.sql.\n"
        text += "\n\n".join(row[0] + ";" for row in rows) + "\n"
        Path(destination).write_text(text, encoding="utf-8")
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--migrations", type=Path, default=DEFAULT_MIGRATIONS)
    parser.add_argument("--timeout", type=float, default=5.0)
    parser.add_argument("--schema-output", type=Path)
    args = parser.parse_args()
    try:
        versions = migrate(args.database, args.migrations, args.timeout)
        if args.schema_output:
            export_schema(args.database, args.schema_output)
    except (MigrationError, OSError, sqlite3.Error, ValueError) as exc:
        parser.exit(1, f"Error: {exc}\n")
    print(f"Migraciones aplicadas: {versions}")


if __name__ == "__main__":
    main()
