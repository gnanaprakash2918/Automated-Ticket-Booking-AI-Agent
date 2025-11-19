import aiosqlite
import logging
from typing import Optional, NamedTuple
from pathlib import Path

log = logging.getLogger(__name__)

class PlaceInfo(NamedTuple):
    """Represents cached place information."""
    place_id: str
    place_code: str
    place_name: str

# Use database path relative to this file's directory
DB_PATH = Path(__file__).parent.parent / "data" / "places_cache.db"

async def init_db():
    """Initializes the SQLite database and creates the places table if it doesn't exist."""
    # Ensure the data directory exists
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS places (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                service_name TEXT NOT NULL,
                place_name TEXT NOT NULL,
                place_code TEXT NOT NULL,
                place_id TEXT NOT NULL,
                raw_data TEXT,
                UNIQUE(service_name, place_name)
            )
        """)
        
        # Create index for faster lookups
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_service_place 
            ON places(service_name, place_name)
        """)
        
        await db.commit()
    log.info(f"Database initialized at {DB_PATH}")

async def get_place_from_cache(service_name: str, place_name: str) -> Optional[PlaceInfo]:
    """
    Retrieves a place from the cache.
    Returns: PlaceInfo namedtuple or None
    """
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT place_id, place_code, place_name FROM places WHERE service_name = ? AND place_name = ? COLLATE NOCASE",
            (service_name, place_name)
        ) as cursor:
            row = await cursor.fetchone()
            if row:
                log.debug(f"Cache HIT for {place_name} ({service_name})")
                return PlaceInfo(*row)
            else:
                log.debug(f"Cache MISS for {place_name} ({service_name})")
                return None

async def save_place_to_cache(service_name: str, place_name: str, place_code: str, place_id: str, raw_data: str = ""):
    """Saves a place to the cache, updating if it already exists."""
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                """
                INSERT INTO places (service_name, place_name, place_code, place_id, raw_data)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(service_name, place_name) 
                DO UPDATE SET 
                    place_code = excluded.place_code,
                    place_id = excluded.place_id,
                    raw_data = excluded.raw_data
                """,
                (service_name, place_name, place_code, place_id, raw_data)
            )
            await db.commit()
        log.debug(f"Cached {place_name} ({service_name})")
    except Exception as e:
        log.error(f"Failed to cache place {place_name}: {e}")