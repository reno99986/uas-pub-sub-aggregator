-- Database initialization script for Log Aggregator
-- This creates the schema for idempotent event processing with deduplication

-- Table: processed_events (Dedup Store)
-- This is the core deduplication mechanism using unique constraint
CREATE TABLE IF NOT EXISTS processed_events (
    id BIGSERIAL PRIMARY KEY,
    topic VARCHAR(255) NOT NULL,
    event_id VARCHAR(255) NOT NULL,
    processed_at TIMESTAMP NOT NULL DEFAULT NOW(),
    
    -- Unique constraint for deduplication (prevents duplicate processing)
    CONSTRAINT unique_event UNIQUE (topic, event_id)
);

-- Indexes for query performance
CREATE INDEX idx_processed_events_topic ON processed_events(topic);
CREATE INDEX idx_processed_events_processed_at ON processed_events(processed_at);

-- Table: events (Event Data Storage)
-- Stores the actual event data after deduplication
CREATE TABLE IF NOT EXISTS events (
    id BIGSERIAL PRIMARY KEY,
    topic VARCHAR(255) NOT NULL,
    event_id VARCHAR(255) NOT NULL,
    timestamp TEXT NOT NULL,  -- Changed to TEXT to accept ISO 8601 strings
    source VARCHAR(255) NOT NULL,
    payload JSONB NOT NULL,
    received_at TIMESTAMP NOT NULL DEFAULT NOW(),
    
    -- Foreign key to processed_events for data integrity
    CONSTRAINT fk_processed FOREIGN KEY (topic, event_id) 
        REFERENCES processed_events(topic, event_id) ON DELETE CASCADE
);

-- Indexes for efficient querying
CREATE INDEX idx_events_topic ON events(topic);
CREATE INDEX idx_events_timestamp ON events(timestamp);
CREATE INDEX idx_events_received_at ON events(received_at);

-- Table: stats (System Statistics)
-- Single-row table to track aggregated statistics
CREATE TABLE IF NOT EXISTS stats (
    id INTEGER PRIMARY KEY DEFAULT 1,
    received_count BIGINT NOT NULL DEFAULT 0,
    unique_processed_count BIGINT NOT NULL DEFAULT 0,
    duplicate_dropped_count BIGINT NOT NULL DEFAULT 0,
    started_at TIMESTAMP NOT NULL DEFAULT NOW(),
    last_updated TIMESTAMP NOT NULL DEFAULT NOW(),
    
    -- Ensure only one row exists
    CONSTRAINT single_row CHECK (id = 1)
);

-- Initialize stats table with a single row
INSERT INTO stats (id) VALUES (1) ON CONFLICT (id) DO NOTHING;

-- Grant permissions (if needed)
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO agguser;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO agguser;
