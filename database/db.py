import os
from pathlib import Path
from typing import Any, Dict, List, NamedTuple, Optional
import aiosqlite
from loguru import logger


DB_PATH = Path("data/places_cache.db")


class Migration(NamedTuple):
    version: int
    description: str
    script: str


async def init_db_connection():
    """Ensures the DB directory exists."""
    if not DB_PATH.parent.exists():
        logger.debug(f"Creating DB directory: {DB_PATH.parent}")
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)


class MigrationManager:
    def __init__(self, service_name: str, migrations_dir: str):
        self.service_name = service_name
        self.db_path = DB_PATH
        self.migrations_dir = Path(migrations_dir)

    async def _create_history_table(self, db):
        """Creates the schema_version table to track applied SQL files."""

        await db.execute("""
            CREATE TABLE IF NOT EXISTS schema_version (
                installed_rank INTEGER PRIMARY KEY AUTOINCREMENT,
                service_name TEXT NOT NULL,
                version INTEGER NOT NULL,
                description TEXT,
                script_name TEXT NOT NULL,
                installed_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                success BOOLEAN NOT NULL,
                UNIQUE(service_name, version)
            )
        """)

    def _load_migrations_from_files(self) -> List[Migration]:
        """Scans the directory for V1__name.sql files."""

        migrations = []
        if not self.migrations_dir.exists():
            logger.warning(f"Migration directory missing: {self.migrations_dir}")
            return []

        for file in sorted(os.listdir(self.migrations_dir)):
            if not file.endswith(".sql") or not file.startswith("V"):
                continue
            try:
                parts = file.split("__", 1)
                version_str = parts[0][1:]
                if not version_str.isdigit():
                    continue

                version = int(version_str)
                description = parts[1].replace(".sql", "").replace("_", " ")

                with open(self.migrations_dir / file, "r", encoding="utf-8") as f:
                    script_content = f.read()

                migrations.append(Migration(version, description, script_content))
                logger.trace(f"Found migration: {file}")
            except Exception as e:
                logger.error(f"Error reading migration file {file}: {e}")

        return migrations

    async def migrate(self):
        """Applies new migrations in a transaction."""
        await init_db_connection()
        file_migrations = self._load_migrations_from_files()

        if not file_migrations:
            logger.info(f"No SQL migrations found for {self.service_name}")
            return

        async with aiosqlite.connect(self.db_path) as db:
            await self._create_history_table(db)

            cursor = await db.execute(
                "SELECT max(version) FROM schema_version WHERE service_name = ? AND success = 1",
                (self.service_name,),
            )

            row = await cursor.fetchone()
            current_version = row[0] if row and row[0] else 0

            for mig in sorted(file_migrations, key=lambda x: x.version):
                if mig.version <= current_version:
                    continue

                logger.info(f"Applying Migration V{mig.version}: {mig.description}")
                try:
                    await db.executescript(mig.script)

                    await db.execute(
                        "INSERT INTO schema_version (service_name, version, description, script_name, success) VALUES (?, ?, ?, ?, ?)",
                        (
                            self.service_name,
                            mig.version,
                            mig.description,
                            f"V{mig.version}",
                            True,
                        ),
                    )
                    await db.commit()
                    logger.success(f"Migration V{mig.version} applied successfully.")

                except Exception as e:
                    logger.critical(f"Migration V{mig.version} failed: {e}")
                    await db.execute(
                        "INSERT INTO schema_version (service_name, version, description, script_name, success) VALUES (?, ?, ?, ?, ?)",
                        (
                            self.service_name,
                            mig.version,
                            mig.description,
                            f"V{mig.version}",
                            False,
                        ),
                    )

                    await db.commit()
                    raise e


async def get_place_from_cache(
    service_name: str, place_name: str
) -> Optional[Dict[str, Any]]:
    table_name = f"{service_name.lower()}_places"
    if not DB_PATH.exists():
        return None

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        try:
            cursor = await db.execute(
                f"SELECT * FROM {table_name} WHERE place_name = ?", (place_name,)
            )
            row = await cursor.fetchone()
            return dict(row) if row else None
        except Exception:
            return None


async def save_place_to_cache(service_name: str, data: Dict[str, Any]):
    table_name = f"{service_name.lower()}_places"
    if not data:
        return

    cols = ", ".join(data.keys())
    placeholders = ", ".join(["?"] * len(data))

    async with aiosqlite.connect(DB_PATH) as db:
        try:
            await db.execute(
                f"INSERT OR REPLACE INTO {table_name} ({cols}) VALUES ({placeholders})",
                tuple(data.values()),
            )
            await db.commit()
            logger.trace(f"Cached: {data.get('place_name')}")
        except Exception as e:
            logger.error(f"Cache Write Failed: {e}")
