import multiprocessing
import sqlite3
import tempfile
import unittest
from pathlib import Path

from scripts.persistence.ledger import connect
from scripts.persistence.migrate import (
    DEFAULT_MIGRATIONS, MigrationBusyError, MigrationError,
    export_schema, migrate,
)


def hold_lock(database, ready, release):
    conn = connect(database)
    conn.execute("BEGIN IMMEDIATE")
    conn.execute("CREATE TABLE interrupted(value TEXT)")
    ready.set()
    release.wait(15)
    conn.rollback()
    conn.close()


def worker(database, directory, gate, results):
    gate.wait(10)
    try:
        results.put(("ok", migrate(database, directory, timeout=10)))
    except Exception as exc:
        results.put(("error", str(exc)))


class MigrationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.db = self.root / "test.db"
        self.directory = self.root / "migrations"
        self.directory.mkdir()
        self.write(1, "CREATE TABLE parent(id INTEGER PRIMARY KEY);")

    def write(self, version, sql):
        path = self.directory / f"{version:04d}_test.sql"
        path.write_text(sql, encoding="utf-8")
        return path

    def query(self, sql):
        conn = connect(self.db)
        try:
            return conn.execute(sql).fetchall()
        finally:
            conn.close()

    def test_initial_and_repeat(self):
        self.assertEqual(migrate(self.db, self.directory), [1])
        self.assertEqual(migrate(self.db, self.directory), [])
        row = self.query("SELECT version,length(hash_sha256),applied_at FROM schema_migrations")[0]
        self.assertEqual(row[:2], (1, 64))
        self.assertTrue(row[2].endswith("Z"))

    def test_partial_failure_preserves_previous(self):
        migrate(self.db, self.directory)
        self.write(2, "CREATE TABLE partial(id INTEGER); INSERT INTO parent VALUES(1); INVALID SQL;")
        with self.assertRaises(MigrationError):
            migrate(self.db, self.directory)
        self.assertEqual(self.query("SELECT * FROM parent"), [])
        self.assertEqual(self.query("SELECT name FROM sqlite_schema WHERE name='partial'"), [])
        self.assertEqual(self.query("SELECT version FROM schema_migrations"), [(1,)])

    def test_changed_hash_and_missing_migration(self):
        migrate(self.db, self.directory)
        path = self.write(1, "CREATE TABLE parent(id INTEGER PRIMARY KEY); -- changed")
        with self.assertRaises(MigrationError):
            migrate(self.db, self.directory)
        path.unlink()
        with self.assertRaises(MigrationError):
            migrate(self.db, self.directory)

    def test_invalid_versions(self):
        self.write(3, "SELECT 1;")
        with self.assertRaises(MigrationError):
            migrate(self.db, self.directory)
        (self.directory / "0003_test.sql").unlink()
        (self.directory / "0001_duplicate.sql").write_text("SELECT 1;")
        with self.assertRaises(MigrationError):
            migrate(self.db, self.directory)

    def test_sql_strings_comments_triggers(self):
        self.write(2, """-- comment ;
CREATE TABLE notes(value TEXT); INSERT INTO notes VALUES('a;b');
/* comment ; */ CREATE TRIGGER inserted AFTER INSERT ON parent BEGIN
 INSERT INTO notes VALUES('trigger;one');
 INSERT INTO notes VALUES('trigger;two'); END;
INSERT INTO parent VALUES(1); -- final comment
""")
        migrate(self.db, self.directory)
        self.assertEqual(self.query("SELECT value FROM notes"), [('a;b',), ('trigger;one',), ('trigger;two',)])

    def test_prohibited_statements_rollback(self):
        migrate(self.db, self.directory)
        for sql in ["COMMIT;", "ROLLBACK;", "BEGIN;", "SAVEPOINT x;", "PRAGMA foreign_keys=OFF;",
                    "DELETE FROM schema_migrations;", "ATTACH DATABASE ':memory:' AS other;"]:
            with self.subTest(sql=sql):
                self.write(2, "CREATE TABLE partial(id INTEGER); " + sql)
                with self.assertRaises(MigrationError):
                    migrate(self.db, self.directory)
                self.assertEqual(self.query("SELECT name FROM sqlite_schema WHERE name='partial'"), [])
                self.assertEqual(self.query("SELECT version FROM schema_migrations"), [(1,)])

    def test_foreign_keys_each_connection(self):
        self.write(2, "CREATE TABLE child(parent_id INTEGER REFERENCES parent(id));")
        migrate(self.db, self.directory)
        for _ in range(2):
            conn = connect(self.db)
            try:
                with self.assertRaises(sqlite3.IntegrityError):
                    conn.execute("INSERT INTO child VALUES(999)")
            finally:
                conn.close()

    def test_lock_timeout_and_process_crash(self):
        migrate(self.db, self.directory)
        ctx = multiprocessing.get_context("spawn")
        ready, release = ctx.Event(), ctx.Event()
        process = ctx.Process(target=hold_lock, args=(self.db, ready, release))
        process.start()
        try:
            self.assertTrue(ready.wait(10))
            with self.assertRaises(MigrationBusyError):
                migrate(self.db, self.directory, timeout=0.05)
        finally:
            process.terminate()
            process.join(10)
            process.close()
        self.assertEqual(self.query("SELECT name FROM sqlite_schema WHERE name='interrupted'"), [])
        self.assertEqual(migrate(self.db, self.directory), [])

    def test_concurrent_migrators(self):
        ctx = multiprocessing.get_context("spawn")
        gate, results = ctx.Event(), ctx.Queue()
        processes = [ctx.Process(target=worker, args=(self.db, self.directory, gate, results)) for _ in range(2)]
        for process in processes:
            process.start()
        try:
            gate.set()
            outcomes = [results.get(timeout=20) for _ in processes]
            self.assertEqual(sorted(outcomes), [("ok", []), ("ok", [1])])
            self.assertEqual(self.query("SELECT count(*) FROM schema_migrations"), [(1,)])
        finally:
            for process in processes:
                process.join(10)
                if process.is_alive():
                    process.terminate()
                    process.join()
                process.close()
            results.close()

    def test_initial_audit_and_schema_export(self):
        migrate(self.db, DEFAULT_MIGRATIONS)
        conn = connect(self.db)
        try:
            conn.execute("INSERT INTO audit_events(event_type,payload_json) VALUES('test','{}')")
            for sql in ["UPDATE audit_events SET event_type='changed'", "DELETE FROM audit_events"]:
                with self.assertRaises(sqlite3.IntegrityError):
                    conn.execute(sql)
        finally:
            conn.close()
        destination = self.root / "schema.sql"
        export_schema(self.db, destination)
        self.assertIn("CREATE TABLE audit_events", destination.read_text(encoding="utf-8"))

    def test_incomplete_statement(self):
        self.write(2, "CREATE TABLE incomplete(id INTEGER)")
        with self.assertRaises(MigrationError):
            migrate(self.db, self.directory)


if __name__ == "__main__":
    unittest.main()
