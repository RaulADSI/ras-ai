import pytest

from scripts.persistence.ledger import connect
from scripts.persistence.migrate import migrate


@pytest.fixture
def temp_db_conn(tmp_path):
    """Base real aislada, creada mediante las migraciones del proyecto."""
    database = tmp_path / "ledger.db"
    migrate(database)
    conn = connect(database)
    try:
        yield conn
    finally:
        conn.close()
