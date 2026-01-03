-- Kronos Reference Counting Schema
-- This schema tracks reference counts for content-addressed blocks
-- to enable safe garbage collection of unreferenced data.

-- Enable foreign key constraints
PRAGMA foreign_keys = ON;

-- Enable WAL mode for better concurrency
PRAGMA journal_mode = WAL;

-- Main table for tracking block references
CREATE TABLE IF NOT EXISTS block_references (
    block_hash TEXT PRIMARY KEY,              -- SHA-256 hash of the block
    ref_count INTEGER NOT NULL DEFAULT 0,     -- Current reference count
    block_size INTEGER NOT NULL,              -- Size of the block in bytes (on-disk, compressed)
    uncompressed_size INTEGER,                -- Original size before compression (NULL if not compressed)
    created_at INTEGER NOT NULL,              -- Unix timestamp when first referenced
    last_accessed INTEGER NOT NULL,           -- Unix timestamp of last access
    metadata TEXT,                            -- Optional JSON metadata

    -- Constraints
    CHECK (ref_count >= 0),
    CHECK (block_size > 0),
    CHECK (uncompressed_size IS NULL OR uncompressed_size >= block_size)
);

-- Index for finding unreferenced blocks (GC)
CREATE INDEX IF NOT EXISTS idx_ref_count ON block_references(ref_count);

-- Index for LRU eviction policies
CREATE INDEX IF NOT EXISTS idx_last_accessed ON block_references(last_accessed);

-- Index for size-based queries
CREATE INDEX IF NOT EXISTS idx_block_size ON block_references(block_size);

-- Transaction log for crash recovery and atomicity
CREATE TABLE IF NOT EXISTS ref_transactions (
    transaction_id INTEGER PRIMARY KEY AUTOINCREMENT,
    start_timestamp INTEGER NOT NULL,         -- When transaction started
    commit_timestamp INTEGER,                 -- When transaction committed (NULL if pending)
    state TEXT NOT NULL DEFAULT 'PENDING',    -- PENDING, COMMITTED, ROLLED_BACK
    description TEXT,                         -- Optional transaction description
    
    CHECK (state IN ('PENDING', 'COMMITTED', 'ROLLED_BACK'))
);

-- Individual operations within a transaction
CREATE TABLE IF NOT EXISTS ref_transaction_ops (
    op_id INTEGER PRIMARY KEY AUTOINCREMENT,
    transaction_id INTEGER NOT NULL,
    block_hash TEXT NOT NULL,
    operation TEXT NOT NULL,                  -- INCREMENT, DECREMENT
    delta INTEGER NOT NULL DEFAULT 1,         -- Change amount (usually 1)
    block_size INTEGER,                       -- Size for new blocks
    timestamp INTEGER NOT NULL,               -- Operation timestamp
    
    FOREIGN KEY (transaction_id) REFERENCES ref_transactions(transaction_id),
    CHECK (operation IN ('INCREMENT', 'DECREMENT')),
    CHECK (delta > 0)
);

-- Index for transaction operations lookup
CREATE INDEX IF NOT EXISTS idx_transaction_ops ON ref_transaction_ops(transaction_id);

-- Statistics table for monitoring
CREATE TABLE IF NOT EXISTS ref_statistics (
    stat_name TEXT PRIMARY KEY,
    stat_value INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

-- Initialize statistics
INSERT OR IGNORE INTO ref_statistics (stat_name, stat_value, updated_at) VALUES
    ('total_blocks', 0, strftime('%s', 'now')),
    ('total_references', 0, strftime('%s', 'now')),
    ('total_bytes', 0, strftime('%s', 'now')),
    ('total_uncompressed_bytes', 0, strftime('%s', 'now')),
    ('transactions_completed', 0, strftime('%s', 'now')),
    ('transactions_rolled_back', 0, strftime('%s', 'now')),
    ('gc_runs', 0, strftime('%s', 'now')),
    ('gc_blocks_collected', 0, strftime('%s', 'now')),
    ('gc_bytes_reclaimed', 0, strftime('%s', 'now'));

-- Audit log for important operations
CREATE TABLE IF NOT EXISTS ref_audit_log (
    log_id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp INTEGER NOT NULL,
    operation TEXT NOT NULL,
    block_hash TEXT,
    old_value INTEGER,
    new_value INTEGER,
    transaction_id INTEGER,
    details TEXT
);

-- Index for audit log queries
CREATE INDEX IF NOT EXISTS idx_audit_timestamp ON ref_audit_log(timestamp);
CREATE INDEX IF NOT EXISTS idx_audit_block ON ref_audit_log(block_hash);

-- Views for common queries

-- View for unreferenced blocks (garbage collection candidates)
CREATE VIEW IF NOT EXISTS unreferenced_blocks AS
SELECT
    block_hash,
    block_size,
    uncompressed_size,
    created_at,
    last_accessed,
    (strftime('%s', 'now') - last_accessed) AS age_seconds
FROM block_references
WHERE ref_count = 0
ORDER BY last_accessed ASC;

-- View for block statistics
CREATE VIEW IF NOT EXISTS block_statistics AS
SELECT
    COUNT(*) AS total_blocks,
    SUM(ref_count) AS total_references,
    SUM(block_size) AS total_bytes,
    SUM(COALESCE(uncompressed_size, block_size)) AS total_uncompressed_bytes,
    SUM(COALESCE(uncompressed_size, block_size)) - SUM(block_size) AS compression_saved_bytes,
    COUNT(CASE WHEN ref_count = 0 THEN 1 END) AS unreferenced_blocks,
    SUM(CASE WHEN ref_count = 0 THEN block_size ELSE 0 END) AS unreferenced_bytes,
    AVG(ref_count) AS avg_ref_count,
    MAX(ref_count) AS max_ref_count
FROM block_references;

-- View for transaction history
CREATE VIEW IF NOT EXISTS transaction_history AS
SELECT 
    t.transaction_id,
    t.start_timestamp,
    t.commit_timestamp,
    t.state,
    t.description,
    COUNT(o.op_id) AS operation_count,
    SUM(CASE WHEN o.operation = 'INCREMENT' THEN o.delta ELSE 0 END) AS increments,
    SUM(CASE WHEN o.operation = 'DECREMENT' THEN o.delta ELSE 0 END) AS decrements
FROM ref_transactions t
LEFT JOIN ref_transaction_ops o ON t.transaction_id = o.transaction_id
GROUP BY t.transaction_id
ORDER BY t.start_timestamp DESC;

-- Triggers for maintaining statistics

-- Update statistics on block reference changes
CREATE TRIGGER IF NOT EXISTS update_stats_on_insert
AFTER INSERT ON block_references
BEGIN
    UPDATE ref_statistics
    SET stat_value = stat_value + 1,
        updated_at = strftime('%s', 'now')
    WHERE stat_name = 'total_blocks';

    UPDATE ref_statistics
    SET stat_value = stat_value + NEW.ref_count,
        updated_at = strftime('%s', 'now')
    WHERE stat_name = 'total_references';

    UPDATE ref_statistics
    SET stat_value = stat_value + NEW.block_size,
        updated_at = strftime('%s', 'now')
    WHERE stat_name = 'total_bytes';

    -- Track uncompressed bytes (use uncompressed_size if set, otherwise block_size)
    UPDATE ref_statistics
    SET stat_value = stat_value + COALESCE(NEW.uncompressed_size, NEW.block_size),
        updated_at = strftime('%s', 'now')
    WHERE stat_name = 'total_uncompressed_bytes';
END;

CREATE TRIGGER IF NOT EXISTS update_stats_on_update
AFTER UPDATE OF ref_count ON block_references
BEGIN
    UPDATE ref_statistics 
    SET stat_value = stat_value + (NEW.ref_count - OLD.ref_count),
        updated_at = strftime('%s', 'now')
    WHERE stat_name = 'total_references';
    
    -- Log significant changes to audit log
    INSERT INTO ref_audit_log (timestamp, operation, block_hash, old_value, new_value)
    VALUES (strftime('%s', 'now'), 'UPDATE_REF_COUNT', NEW.block_hash, OLD.ref_count, NEW.ref_count);
END;

CREATE TRIGGER IF NOT EXISTS update_stats_on_delete
AFTER DELETE ON block_references
BEGIN
    UPDATE ref_statistics
    SET stat_value = stat_value - 1,
        updated_at = strftime('%s', 'now')
    WHERE stat_name = 'total_blocks';

    UPDATE ref_statistics
    SET stat_value = stat_value - OLD.ref_count,
        updated_at = strftime('%s', 'now')
    WHERE stat_name = 'total_references';

    UPDATE ref_statistics
    SET stat_value = stat_value - OLD.block_size,
        updated_at = strftime('%s', 'now')
    WHERE stat_name = 'total_bytes';

    -- Decrement uncompressed bytes (use uncompressed_size if set, otherwise block_size)
    UPDATE ref_statistics
    SET stat_value = stat_value - COALESCE(OLD.uncompressed_size, OLD.block_size),
        updated_at = strftime('%s', 'now')
    WHERE stat_name = 'total_uncompressed_bytes';

    -- Log deletion
    INSERT INTO ref_audit_log (timestamp, operation, block_hash, old_value, details)
    VALUES (strftime('%s', 'now'), 'DELETE_BLOCK', OLD.block_hash, OLD.ref_count,
            json_object('size', OLD.block_size, 'uncompressed_size', OLD.uncompressed_size));
END;

-- Update last_accessed timestamp on reference count changes
CREATE TRIGGER IF NOT EXISTS update_last_accessed
AFTER UPDATE OF ref_count ON block_references
WHEN NEW.ref_count > OLD.ref_count
BEGIN
    UPDATE block_references 
    SET last_accessed = strftime('%s', 'now')
    WHERE block_hash = NEW.block_hash;
END;