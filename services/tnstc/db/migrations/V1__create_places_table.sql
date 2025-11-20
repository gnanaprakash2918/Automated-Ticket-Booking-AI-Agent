-- Create table for storing TNSTC places
CREATE TABLE IF NOT EXISTS tnstc_places (
    place_name TEXT PRIMARY KEY,
    place_code TEXT NOT NULL,
    place_id TEXT NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for faster lookups by name (though PK already indexes it, this is for partial matches if needed later)
CREATE INDEX IF NOT EXISTS idx_tnstc_places_name ON tnstc_places(place_name);
