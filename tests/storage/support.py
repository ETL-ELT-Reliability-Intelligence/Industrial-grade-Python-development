"""Shared setup for tests that need a real PostgreSQL database.

The tests are skipped unless TEST_DATABASE_URL is set and the packages of
storage/requirements.txt are installed. The database is migrated on start and
emptied before every test, so it must be a dedicated test database.
"""

import os
from pathlib import Path
import unittest

try:
    from alembic import command
    from alembic.config import Config
    import psycopg
    from storage.postgres import connect
except ImportError:  # storage dependencies are optional for the rest of the suite
    psycopg = None

TEST_DATABASE_URL = os.environ.get("TEST_DATABASE_URL")
ALEMBIC_INI = Path(__file__).resolve().parents[2] / "storage" / "postgres" / "alembic.ini"

requires_database = unittest.skipUnless(
    psycopg is not None and TEST_DATABASE_URL,
    "Set TEST_DATABASE_URL and install storage/requirements.txt",
)


def migrate(url: str) -> None:
    """Apply all migrations to the database at url."""
    config = Config(str(ALEMBIC_INI))
    config.set_main_option("sqlalchemy.url", url)
    command.upgrade(config, "head")


@requires_database
class DatabaseTestCase(unittest.TestCase):
    """Run every test on migrated, empty tables."""

    @classmethod
    def setUpClass(cls):
        """Migrate the test database and open a connection."""
        migrate(TEST_DATABASE_URL)
        cls.connection = connect(TEST_DATABASE_URL)

    @classmethod
    def tearDownClass(cls):
        """Close the connection."""
        cls.connection.close()

    def setUp(self):
        """Remove all rows; dependent tables are truncated through foreign keys."""
        self.connection.execute(
            "TRUNCATE datasets, pipeline_runs, incidents, lineage_nodes, "
            "ml_model_metadata, user_actions CASCADE"
        )
