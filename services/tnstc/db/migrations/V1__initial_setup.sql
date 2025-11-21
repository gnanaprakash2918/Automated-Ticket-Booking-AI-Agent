-- TNSTC Places Table
CREATE TABLE IF NOT EXISTS tnstc_places (
    place_name TEXT PRIMARY KEY,
    place_code TEXT NOT NULL,
    place_id TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_tnstc_place_code ON tnstc_places(place_code);