from database.db import Migration


# Define migrations for the TNSTC service
# Version 1: Initial Table Setup
V1_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS tnstc_places (
    place_name TEXT PRIMARY KEY,
    place_code TEXT NOT NULL,
    place_id TEXT NOT NULL
);
"""

# Version 2: Example of adding a new column (Future proofing)
# V2_ADD_TIMESTAMP = "ALTER TABLE tnstc_places ADD COLUMN last_updated TIMESTAMP;"

TNSTC_MIGRATIONS = [
    Migration(
        version=1,
        description="Create initial tnstc_places table",
        script=V1_CREATE_TABLE,
    ),
    # Migration(version=2, description="Add timestamp", script=V2_ADD_TIMESTAMP),
]
