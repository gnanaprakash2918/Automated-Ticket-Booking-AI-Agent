import os
from pathlib import Path
import re
import sys
from loguru import logger


def create_migration_file(description: str, service: str = "tnstc"):
    base_dir = Path(__file__).parent.parent
    migration_dir = base_dir / "services" / service / "db" / "migrations"

    logger.debug(f"Ensuring migration directory exists at: {migration_dir}")
    migration_dir.mkdir(parents=True, exist_ok=True)

    current_version = 0
    for filename in os.listdir(migration_dir):
        match = re.match(r"V(\d+)__", filename)
        if match:
            version = int(match.group(1))
            if version > current_version:
                current_version = version

    next_version = current_version + 1

    logger.debug(f"Next migration version determined: V{next_version}")
    clean_desc = description.lower().replace(" ", "_")
    filename = f"V{next_version}__{clean_desc}.sql"
    filepath = migration_dir / filename

    with open(filepath, "w") as f:
        f.write(f"-- Migration V{next_version}: {description}\n")
        f.write("-- Created automatically. Add your SQL below:\n\n")

    logger.info(f"✅ Created migration file: {filepath}")
    logger.info("   Now edit this file to add your SQL commands.")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        logger.error("Usage: python create_migration.py <description> [service_name]")
        sys.exit(1)

    desc = sys.argv[1]
    svc = sys.argv[2] if len(sys.argv) > 2 else "tnstc"
    create_migration_file(desc, svc)
