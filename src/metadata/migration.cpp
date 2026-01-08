#include <spdlog/spdlog.h>
#include <cstdint>
#include <iomanip>
#include <sstream>
#include <unordered_set>
#include <yams/metadata/migration.h>
#include <yams/metadata/path_utils.h>

namespace yams::metadata {

// MigrationManager implementation
MigrationManager::MigrationManager(Database& db) : db_(db) {}

Result<void> MigrationManager::initialize() {
    return createMigrationTables();
}

void MigrationManager::registerMigration(Migration migration) {
    migrations_[migration.version] = std::move(migration);
}

void MigrationManager::registerMigrations(std::vector<Migration> migrations) {
    for (auto& migration : migrations) {
        registerMigration(std::move(migration));
    }
}

Result<int> MigrationManager::getCurrentVersion() {
    auto stmtResult = db_.prepare("SELECT MAX(version) FROM migration_history WHERE success = 1");
    if (!stmtResult)
        return stmtResult.error();

    Statement stmt = std::move(stmtResult).value();
    auto stepResult = stmt.step();
    if (!stepResult)
        return stepResult.error();

    if (stepResult.value() && !stmt.isNull(0)) {
        return stmt.getInt(0);
    }

    return 0; // No migrations applied yet
}

int MigrationManager::getLatestVersion() const {
    if (migrations_.empty())
        return 0;
    return migrations_.rbegin()->first;
}

Result<bool> MigrationManager::needsMigration() {
    auto currentResult = getCurrentVersion();
    if (!currentResult)
        return currentResult.error();

    return currentResult.value() < getLatestVersion();
}

Result<void> MigrationManager::migrate() {
    return migrateTo(getLatestVersion());
}

Result<void> MigrationManager::migrateTo(int targetVersion) {
    auto currentResult = getCurrentVersion();
    if (!currentResult)
        return currentResult.error();

    int currentVersion = currentResult.value();

    if (currentVersion == targetVersion) {
        spdlog::debug("Already at version {}", targetVersion);
        return {};
    }

    if (currentVersion > targetVersion) {
        return rollbackTo(targetVersion);
    }

    // Apply migrations in order
    int totalMigrations = 0;
    for (const auto& [version, _] : migrations_) {
        if (version > currentVersion && version <= targetVersion) {
            totalMigrations++;
        }
    }

    int appliedMigrations = 0;

    for (const auto& [version, migration] : migrations_) {
        if (version > currentVersion && version <= targetVersion) {
            spdlog::debug("Applying migration {} '{}' ({}/{})", version, migration.name,
                          ++appliedMigrations, totalMigrations);

            if (progressCallback_) {
                progressCallback_(appliedMigrations, totalMigrations, migration.name);
            }

            auto start = std::chrono::steady_clock::now();

            auto result = applyMigration(migration);

            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - start);

            if (!result) {
                auto recordResult = recordMigration(version, migration.name, duration, false,
                                                    result.error().message);
                if (!recordResult) {
                    spdlog::error("Failed to record migration failure for version {}: {}", version,
                                  recordResult.error().message);
                }
                return result;
            }

            auto recordResult = recordMigration(version, migration.name, duration, true);
            if (!recordResult)
                return recordResult;

            currentVersion = version;
        }
    }

    spdlog::debug("Migration complete. Now at version {}", currentVersion);
    return {};
}

Result<void> MigrationManager::rollbackTo(int targetVersion) {
    auto currentResult = getCurrentVersion();
    if (!currentResult)
        return currentResult.error();

    int currentVersion = currentResult.value();

    if (currentVersion <= targetVersion) {
        return Error{ErrorCode::InvalidData, "Cannot rollback to a higher version"};
    }

    // Rollback migrations in reverse order
    auto it = migrations_.rbegin();
    while (it != migrations_.rend() && it->first > targetVersion) {
        if (it->first <= currentVersion) {
            spdlog::debug("Rolling back migration {} '{}'", it->first, it->second.name);

            auto start = std::chrono::steady_clock::now();

            auto result = rollbackMigration(it->second);

            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - start);

            if (!result) {
                auto recordResult = recordMigration(-it->first, "Rollback: " + it->second.name,
                                                    duration, false, result.error().message);
                if (!recordResult) {
                    spdlog::error("Failed to record rollback failure for version {}: {}", it->first,
                                  recordResult.error().message);
                }
                return result;
            }

            auto recordResult =
                recordMigration(-it->first, "Rollback: " + it->second.name, duration, true);
            if (!recordResult)
                return recordResult;
        }
        ++it;
    }

    spdlog::debug("Rollback complete. Now at version {}", targetVersion);
    return {};
}

Result<std::vector<MigrationHistory>> MigrationManager::getHistory() {
    auto stmtResult = db_.prepare("SELECT version, name, applied_at, duration_ms, success, error "
                                  "FROM migration_history ORDER BY applied_at DESC");
    if (!stmtResult)
        return stmtResult.error();

    Statement stmt = std::move(stmtResult).value();
    std::vector<MigrationHistory> history;

    while (true) {
        auto stepResult = stmt.step();
        if (!stepResult)
            return stepResult.error();
        if (!stepResult.value())
            break;

        MigrationHistory entry;
        entry.version = stmt.getInt(0);
        entry.name = stmt.getString(1);
        entry.appliedAt =
            std::chrono::system_clock::time_point(std::chrono::seconds(stmt.getInt64(2)));
        entry.duration = std::chrono::milliseconds(stmt.getInt64(3));
        entry.success = stmt.getInt(4) != 0;
        entry.error = stmt.getString(5);

        history.push_back(entry);
    }

    return history;
}

Result<void> MigrationManager::verifyIntegrity() {
    // Check table integrity
    auto result = db_.execute("PRAGMA integrity_check");
    if (!result)
        return result;

    // Verify FTS5 tables if present
    auto ftsCheck = db_.tableExists("documents_fts");
    if (ftsCheck && ftsCheck.value()) {
        auto ftsResult =
            db_.execute("INSERT INTO documents_fts(documents_fts) VALUES('integrity-check')");
        if (!ftsResult) {
            return Error{ErrorCode::DatabaseError, "FTS5 integrity check failed"};
        }
    }

    return {};
}

void MigrationManager::setProgressCallback(ProgressCallback callback) {
    progressCallback_ = std::move(callback);
}

Result<void> MigrationManager::applyMigration(const Migration& migration) {
    auto run = [&]() -> Result<void> {
        if (!migration.upSQL.empty()) {
            auto result = db_.execute(migration.upSQL);
            if (!result) {
                return result;
            }
        }
        if (migration.upFunc) {
            return migration.upFunc(db_);
        }
        if (migration.upSQL.empty() && !migration.upFunc) {
            return Error{ErrorCode::InvalidData, "Migration has no up function or SQL"};
        }
        return {};
    };

    if (migration.wrapInTransaction) {
        return db_.transaction(run);
    }

    return run();
}

Result<void> MigrationManager::rollbackMigration(const Migration& migration) {
    auto run = [&]() -> Result<void> {
        if (migration.downFunc) {
            auto result = migration.downFunc(db_);
            if (!result) {
                return result;
            }
        }
        if (!migration.downSQL.empty()) {
            auto result = db_.execute(migration.downSQL);
            if (!result) {
                return result;
            }
        }
        if (migration.downSQL.empty() && !migration.downFunc) {
            return Error{ErrorCode::InvalidData, "Migration has no down function or SQL"};
        }
        return {};
    };

    if (migration.wrapInTransaction) {
        return db_.transaction(run);
    }

    return run();
}

Result<void> MigrationManager::recordMigration(int version, const std::string& name,
                                               std::chrono::milliseconds duration, bool success,
                                               const std::string& error) {
    auto stmtResult = db_.prepare(
        "INSERT INTO migration_history (version, name, applied_at, duration_ms, success, error) "
        "VALUES (?, ?, ?, ?, ?, ?) "
        "ON CONFLICT(version) DO UPDATE SET "
        " name=excluded.name, applied_at=excluded.applied_at, duration_ms=excluded.duration_ms,"
        " success=excluded.success, error=excluded.error");
    if (!stmtResult)
        return stmtResult.error();

    Statement stmt = std::move(stmtResult).value();
    auto now = std::chrono::system_clock::now().time_since_epoch();
    auto seconds = std::chrono::duration_cast<std::chrono::seconds>(now).count();

    auto bindResult = stmt.bindAll(version, name, static_cast<int64_t>(seconds),
                                   static_cast<int64_t>(duration.count()), success ? 1 : 0, error);
    if (!bindResult)
        return bindResult;

    return stmt.execute();
}

Result<void> MigrationManager::createMigrationTables() {
    return db_.execute(R"(
        CREATE TABLE IF NOT EXISTS migration_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            version INTEGER NOT NULL,
            name TEXT NOT NULL,
            applied_at INTEGER NOT NULL,
            duration_ms INTEGER NOT NULL,
            success INTEGER NOT NULL,
            error TEXT,
            UNIQUE(version)
        )
    )");
}

// YamsMetadataMigrations implementation
std::vector<Migration> YamsMetadataMigrations::getAllMigrations() {
    return {createInitialSchema(),
            createFTS5Tables(),
            createMetadataIndexes(),
            createRelationshipTables(),
            createSearchTables(),
            createCollectionIndexes(),
            createKnowledgeGraphSchema(),
            createBinarySignatureSchema(),
            createVectorSearchSchema(),
            upgradeFTS5Tokenization(),
            createTreeSnapshotsSchema(),
            createTreeDiffsSchema(),
            addPathIndexingSchema(),
            chunkedPathIndexingBackfill(),
            createPathTreeSchema(),
            createSymbolMetadataSchema(),
            addFTS5PorterStemmer(),
            removeFTS5ContentType(),
            renameDocEntitiesToKgDocEntities(),
            createSessionIndexes(),
            createRepairTrackingSchema(),
            createSymbolExtractionStateSchema(),
            createSymSpellSchema()};
}

Migration YamsMetadataMigrations::createInitialSchema() {
    Migration m;
    m.version = 1;
    m.name = "Create initial schema";
    m.created = std::chrono::system_clock::now();

    m.upSQL = R"(
        -- Main documents table with comprehensive metadata
        CREATE TABLE documents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_path TEXT NOT NULL,
            file_name TEXT NOT NULL,
            file_extension TEXT,
            file_size INTEGER NOT NULL,
            sha256_hash TEXT UNIQUE NOT NULL,
            mime_type TEXT,
            created_time INTEGER,
            modified_time INTEGER,
            indexed_time INTEGER,
            content_extracted BOOLEAN DEFAULT 0,
            extraction_status TEXT DEFAULT 'pending',
            extraction_error TEXT
        );
        
        -- Document content table for extracted text
        CREATE TABLE document_content (
            document_id INTEGER PRIMARY KEY,
            content_text TEXT,
            content_length INTEGER,
            extraction_method TEXT,
            language TEXT,
            FOREIGN KEY (document_id) REFERENCES documents(id) ON DELETE CASCADE
        );
        
        -- Generic metadata key-value store
        CREATE TABLE metadata (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            document_id INTEGER NOT NULL,
            key TEXT NOT NULL,
            value TEXT,
            value_type TEXT DEFAULT 'string',
            FOREIGN KEY (document_id) REFERENCES documents(id) ON DELETE CASCADE,
            UNIQUE(document_id, key)
        );
        
        -- Basic indexes for performance
        CREATE INDEX idx_documents_path ON documents(file_path);
        CREATE UNIQUE INDEX idx_documents_hash ON documents(sha256_hash);
        CREATE INDEX idx_documents_modified ON documents(modified_time);
        CREATE INDEX idx_documents_indexed ON documents(indexed_time);
        CREATE INDEX idx_documents_filename ON documents(file_name);
        CREATE INDEX idx_documents_extension ON documents(file_extension);
        CREATE INDEX idx_metadata_document ON metadata(document_id);
        CREATE INDEX idx_metadata_key ON metadata(key);
    )";

    m.downSQL = R"(
        DROP TABLE IF EXISTS metadata;
        DROP TABLE IF EXISTS document_content;
        DROP TABLE IF EXISTS documents;
    )";

    return m;
}

Migration YamsMetadataMigrations::createFTS5Tables() {
    Migration m;
    m.version = 2;
    m.name = "Create FTS5 tables";
    m.created = std::chrono::system_clock::now();

    m.upFunc = [](Database& db) -> Result<void> {
        // Check if FTS5 is available
        auto fts5Result = db.hasFTS5();
        if (!fts5Result)
            return fts5Result.error();

        if (!fts5Result.value()) {
            spdlog::warn("FTS5 not available, skipping FTS table creation");
            return {};
        }

        return db.execute(R"(
            -- Full-text search table for document content
            CREATE VIRTUAL TABLE documents_fts USING fts5(
                content,
                title,
                content_type,
                -- Preserve code identifiers and paths by treating '_' and '-' as token characters
                tokenize='unicode61 tokenchars ''_-'''
            );
            
            -- No triggers needed - we'll manually populate FTS when content is extracted
        )");
    };

    m.downFunc = [](Database& db) -> Result<void> {
        return db.execute(R"(
            DROP TABLE IF EXISTS documents_fts;
        )");
    };

    return m;
}

Migration YamsMetadataMigrations::upgradeFTS5Tokenization() {
    Migration m;
    m.version = 10;
    m.name = "Rebuild FTS5 with tokenchars _ and -";
    m.created = std::chrono::system_clock::now();

    m.upFunc = [](Database& db) -> Result<void> {
        auto fts5Result = db.hasFTS5();
        if (!fts5Result)
            return fts5Result.error();
        if (!fts5Result.value())
            return {};

        // Rebuild FTS using the outer migration transaction from MigrationManager.
        // Do not issue explicit BEGIN/COMMIT here to avoid nested transactions.
        Result<void> rc;

        (void)db.execute("DROP TABLE IF EXISTS documents_fts_new;");

        rc = db.execute("DROP TABLE IF EXISTS documents_fts;");
        if (!rc)
            return rc;

        rc = db.execute(R"(
            CREATE VIRTUAL TABLE IF NOT EXISTS documents_fts USING fts5(
                content,
                title,
                content_type,
                tokenize='unicode61 tokenchars ''_-'''
            );
        )");
        if (!rc)
            return rc;

        // Backfill from existing extracted content in chunks to avoid long-running transactions
        // Determine total rows to process
        auto countStmtRes = db.prepare(R"(
            SELECT COUNT(*) FROM documents WHERE content_extracted = 1
        )");
        if (!countStmtRes)
            return countStmtRes.error();
        {
            auto stmt = std::move(countStmtRes).value();
            auto step = stmt.step();
            if (!step)
                return step.error();
            std::int64_t total = 0;
            if (step.value()) {
                total = stmt.getInt64(0);
            }
            const std::int64_t kChunk = 10000; // configurable future
            for (std::int64_t offset = 0; offset < total; offset += kChunk) {
                auto ins = db.prepare(R"(
                    INSERT OR REPLACE INTO documents_fts (rowid, content, title, content_type)
                    SELECT d.id,
                           COALESCE(dc.content_text, ''),
                           d.file_name,
                           COALESCE(d.mime_type, '')
                    FROM documents d
                    LEFT JOIN document_content dc ON dc.document_id = d.id
                    WHERE d.content_extracted = 1
                    ORDER BY d.id
                    LIMIT ? OFFSET ?
                )");
                if (!ins)
                    return ins.error();
                auto q = std::move(ins).value();
                auto b1 = q.bind(1, static_cast<std::int64_t>(kChunk));
                if (!b1)
                    return b1;
                auto b2 = q.bind(2, offset);
                if (!b2)
                    return b2;
                auto ex = q.execute();
                if (!ex)
                    return ex;
                // Periodic progress log (every ~100k rows)
                if ((offset / kChunk) % 10 == 0) {
                    spdlog::info("[FTS5 v10] Backfill progress: {}/{} rows", offset, total);
                }
            }
            spdlog::info("[FTS5 v10] Backfill complete: {} rows", total);
        }

        return rc;
    };

    m.downFunc = [](Database& db) -> Result<void> {
        return db.execute(R"(
            DROP TABLE IF EXISTS documents_fts_new;
        )");
    };

    return m;
}

Migration YamsMetadataMigrations::createMetadataIndexes() {
    Migration m;
    m.version = 3;
    m.name = "Create additional metadata indexes";
    m.created = std::chrono::system_clock::now();

    m.upSQL = R"(
        -- Content extraction related indexes
        CREATE INDEX idx_documents_extraction_status ON documents(extraction_status);
        CREATE INDEX idx_documents_content_extracted ON documents(content_extracted);
        CREATE INDEX idx_document_content_method ON document_content(extraction_method);
        CREATE INDEX idx_document_content_language ON document_content(language);
        
        -- Common metadata queries
        CREATE INDEX idx_metadata_tags ON metadata(value) WHERE key = 'tag';
        CREATE INDEX idx_metadata_author ON metadata(value) WHERE key = 'author';
        CREATE INDEX idx_metadata_category ON metadata(value) WHERE key = 'category';
        CREATE INDEX idx_metadata_type ON metadata(value_type);
    )";

    m.downSQL = R"(
        DROP INDEX IF EXISTS idx_metadata_type;
        DROP INDEX IF EXISTS idx_metadata_category;
        DROP INDEX IF EXISTS idx_metadata_author;
        DROP INDEX IF EXISTS idx_metadata_tags;
        DROP INDEX IF EXISTS idx_document_content_language;
        DROP INDEX IF EXISTS idx_document_content_method;
        DROP INDEX IF EXISTS idx_documents_content_extracted;
        DROP INDEX IF EXISTS idx_documents_extraction_status;
    )";

    return m;
}

Migration YamsMetadataMigrations::createRelationshipTables() {
    Migration m;
    m.version = 4;
    m.name = "Create document relationship tables";
    m.created = std::chrono::system_clock::now();

    m.upSQL = R"(
        -- Document relationships (parent/child, versions, etc.)
        CREATE TABLE document_relationships (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            parent_id INTEGER,
            child_id INTEGER NOT NULL,
            relationship_type TEXT NOT NULL,
            created_time INTEGER,
            FOREIGN KEY (parent_id) REFERENCES documents(id) ON DELETE CASCADE,
            FOREIGN KEY (child_id) REFERENCES documents(id) ON DELETE CASCADE,
            UNIQUE(parent_id, child_id, relationship_type)
        );
        
        CREATE INDEX idx_relationships_parent ON document_relationships(parent_id);
        CREATE INDEX idx_relationships_child ON document_relationships(child_id);
        CREATE INDEX idx_relationships_type ON document_relationships(relationship_type);
    )";

    m.downSQL = R"(
        DROP TABLE IF EXISTS document_relationships;
    )";

    return m;
}

Migration YamsMetadataMigrations::createSearchTables() {
    Migration m;
    m.version = 5;
    m.name = "Create search history and saved queries";
    m.created = std::chrono::system_clock::now();

    m.upSQL = R"(
        -- Search history
        CREATE TABLE search_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            query TEXT NOT NULL,
            query_time INTEGER NOT NULL,
            results_count INTEGER,
            execution_time_ms INTEGER,
            user_context TEXT
        );
        
        CREATE INDEX idx_search_history_time ON search_history(query_time);
        CREATE INDEX idx_search_history_query ON search_history(query);
        
        -- Saved queries
        CREATE TABLE saved_queries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            query TEXT NOT NULL,
            description TEXT,
            created_time INTEGER NOT NULL,
            last_used INTEGER,
            use_count INTEGER DEFAULT 0
        );
    )";

    m.downSQL = R"(
        DROP TABLE IF EXISTS saved_queries;
        DROP TABLE IF EXISTS search_history;
    )";

    return m;
}

// MigrationBuilder implementation
MigrationBuilder::MigrationBuilder(int version, const std::string& name) {
    migration_.version = version;
    migration_.name = name;
    migration_.created = std::chrono::system_clock::now();
}

MigrationBuilder& MigrationBuilder::createTable(const std::string& table) {
    // This is a simplified version - in practice you'd build the full CREATE TABLE
    upStatements_.push_back("CREATE TABLE " + table + " (id INTEGER PRIMARY KEY)");
    downStatements_.push_back("DROP TABLE IF EXISTS " + table);
    return *this;
}

MigrationBuilder& MigrationBuilder::dropTable(const std::string& table) {
    upStatements_.push_back("DROP TABLE IF EXISTS " + table);
    // Can't reliably recreate in down migration
    return *this;
}

MigrationBuilder& MigrationBuilder::renameTable(const std::string& from, const std::string& to) {
    upStatements_.push_back("ALTER TABLE " + from + " RENAME TO " + to);
    downStatements_.push_back("ALTER TABLE " + to + " RENAME TO " + from);
    return *this;
}

MigrationBuilder& MigrationBuilder::addColumn(const std::string& table, const std::string& column,
                                              const std::string& type, bool nullable) {
    std::string sql = "ALTER TABLE " + table + " ADD COLUMN " + column + " " + type;
    if (!nullable)
        sql += " NOT NULL";
    upStatements_.push_back(sql);
    // Can't remove columns in SQLite
    return *this;
}

MigrationBuilder& MigrationBuilder::dropColumn([[maybe_unused]] const std::string& table,
                                               [[maybe_unused]] const std::string& column) {
    // SQLite doesn't support DROP COLUMN directly
    // Would need to recreate table
    return *this;
}

MigrationBuilder& MigrationBuilder::renameColumn(const std::string& table, const std::string& from,
                                                 const std::string& to) {
    upStatements_.push_back("ALTER TABLE " + table + " RENAME COLUMN " + from + " TO " + to);
    downStatements_.push_back("ALTER TABLE " + table + " RENAME COLUMN " + to + " TO " + from);
    return *this;
}

MigrationBuilder& MigrationBuilder::createIndex(const std::string& index, const std::string& table,
                                                const std::vector<std::string>& columns,
                                                bool unique) {
    std::stringstream sql;
    sql << "CREATE ";
    if (unique)
        sql << "UNIQUE ";
    sql << "INDEX " << index << " ON " << table << " (";
    for (size_t i = 0; i < columns.size(); ++i) {
        if (i > 0)
            sql << ", ";
        sql << columns[i];
    }
    sql << ")";

    upStatements_.push_back(sql.str());
    downStatements_.push_back("DROP INDEX IF EXISTS " + index);
    return *this;
}

MigrationBuilder& MigrationBuilder::dropIndex(const std::string& index) {
    upStatements_.push_back("DROP INDEX IF EXISTS " + index);
    return *this;
}

MigrationBuilder& MigrationBuilder::up(const std::string& sql) {
    upStatements_.push_back(sql);
    return *this;
}

MigrationBuilder& MigrationBuilder::down(const std::string& sql) {
    downStatements_.push_back(sql);
    return *this;
}

MigrationBuilder& MigrationBuilder::upFunction(std::function<Result<void>(Database&)> func) {
    migration_.upFunc = std::move(func);
    return *this;
}

MigrationBuilder& MigrationBuilder::downFunction(std::function<Result<void>(Database&)> func) {
    migration_.downFunc = std::move(func);
    return *this;
}

MigrationBuilder& MigrationBuilder::wrapInTransaction(bool enabled) {
    migration_.wrapInTransaction = enabled;
    return *this;
}

Migration MigrationBuilder::build() const {
    Migration m = migration_;

    // Combine SQL statements
    if (!upStatements_.empty()) {
        std::stringstream sql;
        for (const auto& stmt : upStatements_) {
            sql << stmt << ";\n";
        }
        m.upSQL = sql.str();
    }

    if (!downStatements_.empty()) {
        std::stringstream sql;
        for (const auto& stmt : downStatements_) {
            sql << stmt << ";\n";
        }
        m.downSQL = sql.str();
    }

    return m;
}

Migration YamsMetadataMigrations::createCollectionIndexes() {
    Migration m;
    m.version = 6;
    m.name = "Add collection and snapshot indexes";
    m.created = std::chrono::system_clock::now();

    m.upSQL = R"(
        -- Create indexes for collection and snapshot-related metadata
        -- These improve performance when filtering by collection, snapshot, or path
        CREATE INDEX IF NOT EXISTS idx_metadata_collection 
            ON metadata(key, value) WHERE key = 'collection';
            
        CREATE INDEX IF NOT EXISTS idx_metadata_collection_id 
            ON metadata(key, value) WHERE key = 'collection_id';
            
        CREATE INDEX IF NOT EXISTS idx_metadata_snapshot_id 
            ON metadata(key, value) WHERE key = 'snapshot_id';
            
        CREATE INDEX IF NOT EXISTS idx_metadata_snapshot_label 
            ON metadata(key, value) WHERE key = 'snapshot_label';
            
        CREATE INDEX IF NOT EXISTS idx_metadata_path 
            ON metadata(key, value) WHERE key = 'path';
            
        CREATE INDEX IF NOT EXISTS idx_metadata_source_uri 
            ON metadata(key, value) WHERE key = 'source_uri';
            
        -- Compound index for common filtering patterns
        CREATE INDEX IF NOT EXISTS idx_metadata_collection_snapshot 
            ON metadata(document_id, key, value) 
            WHERE key IN ('collection', 'snapshot_id', 'snapshot_label');
    )";

    m.downSQL = R"(
        DROP INDEX IF EXISTS idx_metadata_collection;
        DROP INDEX IF EXISTS idx_metadata_collection_id;
        DROP INDEX IF EXISTS idx_metadata_snapshot_id;
        DROP INDEX IF EXISTS idx_metadata_snapshot_label;
        DROP INDEX IF EXISTS idx_metadata_path;
        DROP INDEX IF EXISTS idx_metadata_source_uri;
        DROP INDEX IF EXISTS idx_metadata_collection_snapshot;
    )";

    return m;
}

Migration YamsMetadataMigrations::createKnowledgeGraphSchema() {
    Migration m;
    m.version = 7;
    m.name = "Create knowledge graph schema";
    m.created = std::chrono::system_clock::now();

    m.upFunc = [](Database& db) -> Result<void> {
        // Base KG schema (tables + btree indexes)
        auto base = db.execute(R"(
        -- Knowledge Graph core tables
        CREATE TABLE IF NOT EXISTS kg_nodes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            node_key TEXT NOT NULL UNIQUE,
            label TEXT,
            type TEXT,
            created_time INTEGER,
            updated_time INTEGER,
            properties TEXT
        );

        CREATE TABLE IF NOT EXISTS kg_aliases (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            node_id INTEGER NOT NULL,
            alias TEXT NOT NULL,
            source TEXT,
            confidence REAL DEFAULT 1.0,
            FOREIGN KEY (node_id) REFERENCES kg_nodes(id) ON DELETE CASCADE,
            UNIQUE(node_id, alias)
        );

        CREATE TABLE IF NOT EXISTS kg_edges (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            src_node_id INTEGER NOT NULL,
            dst_node_id INTEGER NOT NULL,
            relation TEXT NOT NULL,
            weight REAL DEFAULT 1.0,
            created_time INTEGER,
            properties TEXT,
            FOREIGN KEY (src_node_id) REFERENCES kg_nodes(id) ON DELETE CASCADE,
            FOREIGN KEY (dst_node_id) REFERENCES kg_nodes(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS kg_node_embeddings (
            node_id INTEGER PRIMARY KEY,
            dim INTEGER NOT NULL,
            vector BLOB NOT NULL,
            model TEXT,
            updated_time INTEGER,
            FOREIGN KEY (node_id) REFERENCES kg_nodes(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS kg_doc_entities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            document_id INTEGER NOT NULL,
            entity_text TEXT NOT NULL,
            node_id INTEGER,
            start_offset INTEGER,
            end_offset INTEGER,
            confidence REAL,
            extractor TEXT,
            FOREIGN KEY (document_id) REFERENCES documents(id) ON DELETE CASCADE,
            FOREIGN KEY (node_id) REFERENCES kg_nodes(id) ON DELETE SET NULL
        );

        CREATE TABLE IF NOT EXISTS kg_node_stats (
            node_id INTEGER PRIMARY KEY,
            degree INTEGER DEFAULT 0,
            pagerank REAL,
            neighbor_count INTEGER,
            last_computed INTEGER,
            FOREIGN KEY (node_id) REFERENCES kg_nodes(id) ON DELETE CASCADE
        );

        -- Indexes
        CREATE INDEX IF NOT EXISTS idx_kg_nodes_type ON kg_nodes(type);
        CREATE INDEX IF NOT EXISTS idx_kg_aliases_alias ON kg_aliases(alias);
        CREATE INDEX IF NOT EXISTS idx_kg_edges_src ON kg_edges(src_node_id);
        CREATE INDEX IF NOT EXISTS idx_kg_edges_dst ON kg_edges(dst_node_id);
        CREATE INDEX IF NOT EXISTS idx_kg_edges_relation ON kg_edges(relation);
        CREATE INDEX IF NOT EXISTS idx_kg_doc_entities_document ON kg_doc_entities(document_id);
        CREATE INDEX IF NOT EXISTS idx_kg_doc_entities_node ON kg_doc_entities(node_id);
        CREATE INDEX IF NOT EXISTS idx_kg_embeddings_model ON kg_node_embeddings(model);
        )");
        if (!base)
            return base;

        // Optional FTS5 virtual index for fast alias lookup (if available)
        auto fts5 = db.hasFTS5();
        if (!fts5)
            return fts5.error();
        if (fts5.value()) {
            auto r1 = db.execute(R"(
                CREATE VIRTUAL TABLE IF NOT EXISTS kg_aliases_fts USING fts5(
                    alias,
                    content='kg_aliases',
                    content_rowid='id',
                    tokenize='porter unicode61'
                )
            )");
            if (!r1)
                return r1;

            // Initial backfill
            auto r2 = db.execute(R"(
                INSERT INTO kg_aliases_fts(rowid, alias)
                SELECT id, alias FROM kg_aliases
            )");
            if (!r2)
                return r2;

            // Sync triggers
            auto r3 = db.execute(R"(
                CREATE TRIGGER IF NOT EXISTS trg_kg_aliases_ai
                AFTER INSERT ON kg_aliases BEGIN
                    INSERT INTO kg_aliases_fts(rowid, alias)
                    VALUES (new.id, new.alias);
                END
            )");
            if (!r3)
                return r3;

            auto r4 = db.execute(R"(
                CREATE TRIGGER IF NOT EXISTS trg_kg_aliases_ad
                AFTER DELETE ON kg_aliases BEGIN
                    INSERT INTO kg_aliases_fts(kg_aliases_fts, rowid, alias)
                    VALUES ('delete', old.id, old.alias);
                END
            )");
            if (!r4)
                return r4;

            auto r5 = db.execute(R"(
                CREATE TRIGGER IF NOT EXISTS trg_kg_aliases_au
                AFTER UPDATE OF alias ON kg_aliases BEGIN
                    INSERT INTO kg_aliases_fts(kg_aliases_fts, rowid, alias)
                    VALUES ('delete', old.id, old.alias);
                    INSERT INTO kg_aliases_fts(rowid, alias)
                    VALUES (new.id, new.alias);
                END
            )");
            if (!r5)
                return r5;
        }

        return Result<void>();
    };

    m.downSQL = R"(
        -- Drop FTS virtual table and its triggers if they were created
        DROP TRIGGER IF EXISTS trg_kg_aliases_ai;
        DROP TRIGGER IF EXISTS trg_kg_aliases_ad;
        DROP TRIGGER IF EXISTS trg_kg_aliases_au;
        DROP TABLE IF EXISTS kg_aliases_fts;

        DROP INDEX IF EXISTS idx_kg_embeddings_model;
        DROP INDEX IF EXISTS idx_doc_entities_node;
        DROP INDEX IF EXISTS idx_doc_entities_document;
        DROP INDEX IF EXISTS idx_kg_edges_relation;
        DROP INDEX IF EXISTS idx_kg_edges_dst;
        DROP INDEX IF EXISTS idx_kg_edges_src;
        DROP INDEX IF EXISTS idx_kg_aliases_alias;
        DROP INDEX IF EXISTS idx_kg_nodes_type;

        DROP TABLE IF EXISTS kg_node_stats;
        DROP TABLE IF EXISTS kg_doc_entities;
        DROP TABLE IF EXISTS kg_node_embeddings;
        DROP TABLE IF EXISTS kg_edges;
        DROP TABLE IF EXISTS kg_aliases;
        DROP TABLE IF EXISTS kg_nodes;
    )";

    return m;
}

Migration YamsMetadataMigrations::createBinarySignatureSchema() {
    Migration m;
    m.version = 8;
    m.name = "Create binary signature schema";
    m.created = std::chrono::system_clock::now();

    m.upSQL = R"(
        -- Table for storing binary file signatures/magic numbers
        CREATE TABLE IF NOT EXISTS document_signatures (
            document_id INTEGER PRIMARY KEY,
            signature BLOB NOT NULL,           -- First 512 bytes of file
            signature_length INTEGER NOT NULL, -- Actual length stored (may be less than 512)
            mime_type_detected TEXT,           -- MIME type detected from signature
            file_type TEXT,                    -- File type classification (e.g., 'executable', 'archive', 'image')
            is_binary BOOLEAN DEFAULT 1,       -- Whether file is binary or text
            magic_number TEXT,                 -- Hex representation of first few bytes (e.g., 'FFD8' for JPEG)
            updated_time INTEGER,
            FOREIGN KEY (document_id) REFERENCES documents(id) ON DELETE CASCADE
        );

        -- Index for fast pattern matching on signatures
        CREATE INDEX IF NOT EXISTS idx_signatures_magic ON document_signatures(magic_number);
        CREATE INDEX IF NOT EXISTS idx_signatures_type ON document_signatures(file_type);
        CREATE INDEX IF NOT EXISTS idx_signatures_mime ON document_signatures(mime_type_detected);
        
        -- Common file patterns table for fast lookup
        CREATE TABLE IF NOT EXISTS file_patterns (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pattern BLOB NOT NULL,             -- Binary pattern to match
            pattern_hex TEXT NOT NULL,         -- Hex representation for easier querying
            offset INTEGER DEFAULT 0,          -- Offset in file where pattern should appear
            file_type TEXT NOT NULL,           -- File type this pattern indicates
            mime_type TEXT,                    -- Associated MIME type
            description TEXT,                  -- Human-readable description
            confidence REAL DEFAULT 1.0,       -- Confidence level for this pattern match
            UNIQUE(pattern_hex, offset)
        );
        
        -- Version tracking for pattern database
        CREATE TABLE IF NOT EXISTS file_patterns_version (
            id INTEGER PRIMARY KEY,
            version TEXT NOT NULL,
            updated_time INTEGER NOT NULL,
            source TEXT                        -- Source of patterns (e.g., 'libmagic', 'json', 'custom')
        );
    )";

    m.downSQL = R"(
        DROP INDEX IF EXISTS idx_signatures_mime;
        DROP INDEX IF EXISTS idx_signatures_type;
        DROP INDEX IF EXISTS idx_signatures_magic;
        DROP TABLE IF EXISTS file_patterns_version;
        DROP TABLE IF EXISTS file_patterns;
        DROP TABLE IF EXISTS document_signatures;
    )";

    return m;
}

Migration YamsMetadataMigrations::createVectorSearchSchema() {
    Migration m;
    m.version = 9;
    m.name = "Create vector search schema";
    m.created = std::chrono::system_clock::now();

    // Use a function to handle sqlite-vec extension loading
    m.upFunc = [](Database& db) -> Result<void> {
        // First, record that vector search is enabled
        auto result = db.execute(R"(
            -- Create or update schema features table
            CREATE TABLE IF NOT EXISTS schema_features (
                feature_name TEXT PRIMARY KEY,
                enabled INTEGER DEFAULT 1,
                config TEXT,  -- JSON configuration
                created_at INTEGER DEFAULT (unixepoch()),
                updated_at INTEGER DEFAULT (unixepoch())
            );
            
            -- Record that vector search is enabled
            INSERT OR REPLACE INTO schema_features (feature_name, enabled, updated_at)
            VALUES ('vector_search', 1, unixepoch());
        )");
        if (!result)
            return result;

        // Create metadata table for vector models
        result = db.execute(R"(
            CREATE TABLE IF NOT EXISTS vector_models (
                model_id TEXT PRIMARY KEY,
                model_name TEXT NOT NULL,
                embedding_dim INTEGER NOT NULL,
                model_type TEXT,  -- 'sentence-transformer', 'openai', etc.
                model_path TEXT,   -- Path to ONNX model file
                config TEXT,       -- JSON configuration
                created_at INTEGER DEFAULT (unixepoch()),
                last_used INTEGER
            );
            
            -- Insert default model configuration
            INSERT OR IGNORE INTO vector_models (
                model_id, 
                model_name, 
                embedding_dim, 
                model_type,
                config
            ) VALUES (
                'all-MiniLM-L6-v2',
                'all-MiniLM-L6-v2',
                384,
                'sentence-transformer',
                '{"max_seq_length": 256, "normalize": true}'
            );
        )");
        if (!result)
            return result;

        // Create tracking table for which documents have embeddings
        result = db.execute(R"(
            CREATE TABLE IF NOT EXISTS document_embeddings_status (
                document_id INTEGER PRIMARY KEY,
                has_embedding BOOLEAN DEFAULT 0,
                model_id TEXT,
                chunk_count INTEGER DEFAULT 0,
                created_at INTEGER DEFAULT (unixepoch()),
                updated_at INTEGER DEFAULT (unixepoch()),
                FOREIGN KEY (document_id) REFERENCES documents(id) ON DELETE CASCADE,
                FOREIGN KEY (model_id) REFERENCES vector_models(model_id)
            );
            
            CREATE INDEX IF NOT EXISTS idx_doc_embed_status 
                ON document_embeddings_status(has_embedding);
            CREATE INDEX IF NOT EXISTS idx_doc_embed_model 
                ON document_embeddings_status(model_id);
        )");
        if (!result)
            return result;

        // Note: The actual vector table (doc_embeddings) is created by
        // SqliteVecBackend::createTables() when the vector database is initialized
        // This migration just sets up the supporting metadata tables

        spdlog::info("Vector search schema migration completed");
        return Result<void>();
    };

    m.downSQL = R"(
        -- Remove vector search metadata
        DELETE FROM schema_features WHERE feature_name = 'vector_search';
        
        -- Drop vector-related tables
        DROP INDEX IF EXISTS idx_doc_embed_model;
        DROP INDEX IF EXISTS idx_doc_embed_status;
        DROP TABLE IF EXISTS document_embeddings_status;
        DROP TABLE IF EXISTS vector_models;
        
        -- Note: The doc_embeddings virtual table would be dropped by the backend
    )";

    return m;
}

Migration YamsMetadataMigrations::createTreeSnapshotsSchema() {
    Migration m;
    m.version = 11;
    m.name = "Create tree snapshots schema";
    m.created = std::chrono::system_clock::now();

    m.upFunc = [](Database& db) -> Result<void> {
        // Create tree_snapshots table for storing directory snapshot metadata
        auto result = db.execute(R"(
            CREATE TABLE IF NOT EXISTS tree_snapshots (
                snapshot_id TEXT PRIMARY KEY,           -- ISO 8601 timestamp ID
                created_at INTEGER NOT NULL,            -- Unix timestamp
                directory_path TEXT NOT NULL,           -- Source directory path
                tree_root_hash TEXT,                    -- Merkle tree root hash (future use)
                snapshot_label TEXT,                    -- Optional human-readable label
                git_commit TEXT,                        -- Auto-detected git commit hash
                git_branch TEXT,                        -- Auto-detected git branch name
                git_remote TEXT,                        -- Auto-detected git remote URL
                files_count INTEGER DEFAULT 0,          -- Number of files in snapshot
                CHECK(files_count >= 0)
            );
        )");
        if (!result)
            return result;

        // Create indexes for common query patterns
        result = db.execute(R"(
            CREATE INDEX IF NOT EXISTS idx_snapshot_created 
                ON tree_snapshots(created_at);
            CREATE INDEX IF NOT EXISTS idx_snapshot_label 
                ON tree_snapshots(snapshot_label) WHERE snapshot_label IS NOT NULL;
            CREATE INDEX IF NOT EXISTS idx_snapshot_git_commit 
                ON tree_snapshots(git_commit) WHERE git_commit IS NOT NULL;
            CREATE INDEX IF NOT EXISTS idx_snapshot_directory 
                ON tree_snapshots(directory_path);
        )");
        if (!result)
            return result;

        spdlog::info("Tree snapshots schema migration completed");
        return Result<void>();
    };

    m.downSQL = R"(
        -- Drop indexes
        DROP INDEX IF EXISTS idx_snapshot_directory;
        DROP INDEX IF EXISTS idx_snapshot_git_commit;
        DROP INDEX IF EXISTS idx_snapshot_label;
        DROP INDEX IF EXISTS idx_snapshot_created;
        
        -- Drop table
        DROP TABLE IF EXISTS tree_snapshots;
    )";

    return m;
}

Migration YamsMetadataMigrations::createTreeDiffsSchema() {
    Migration m;
    m.version = 12;
    m.name = "Create tree diffs and changes schema";
    m.created = std::chrono::system_clock::now();

    m.upFunc = [](Database& db) -> Result<void> {
        // Create tree_diffs table for storing diff metadata
        auto result = db.execute(R"(
            CREATE TABLE IF NOT EXISTS tree_diffs (
                diff_id INTEGER PRIMARY KEY AUTOINCREMENT,
                base_snapshot_id TEXT NOT NULL,
                target_snapshot_id TEXT NOT NULL,
                computed_at INTEGER NOT NULL,                  -- Unix timestamp
                files_added INTEGER DEFAULT 0,
                files_deleted INTEGER DEFAULT 0,
                files_modified INTEGER DEFAULT 0,
                files_renamed INTEGER DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'complete',       -- complete|partial|failed
                delta_blob_hash TEXT,                          -- Optional: compressed diff in CAS
                FOREIGN KEY (base_snapshot_id) REFERENCES tree_snapshots(snapshot_id),
                FOREIGN KEY (target_snapshot_id) REFERENCES tree_snapshots(snapshot_id),
                UNIQUE(base_snapshot_id, target_snapshot_id)
            );
        )");
        if (!result)
            return result;

        // Create tree_changes table for individual file changes
        result = db.execute(R"(
            CREATE TABLE IF NOT EXISTS tree_changes (
                change_id INTEGER PRIMARY KEY AUTOINCREMENT,
                diff_id INTEGER NOT NULL,
                change_type TEXT NOT NULL,                     -- added|deleted|modified|renamed|moved
                old_path TEXT,                                 -- Path in base snapshot (NULL for added)
                new_path TEXT,                                 -- Path in target snapshot (NULL for deleted)
                old_hash TEXT,                                 -- Content hash in base (NULL for added)
                new_hash TEXT,                                 -- Content hash in target (NULL for deleted)
                old_mode INTEGER,                              -- File mode in base
                new_mode INTEGER,                              -- File mode in target
                is_directory INTEGER NOT NULL DEFAULT 0,
                file_size INTEGER DEFAULT 0,
                content_delta_hash TEXT,                       -- Optional: file-level delta in CAS
                FOREIGN KEY (diff_id) REFERENCES tree_diffs(diff_id) ON DELETE CASCADE
            );
        )");
        if (!result)
            return result;

        // Create indexes for efficient queries
        result = db.execute(R"(
            CREATE INDEX IF NOT EXISTS idx_tree_diffs_base 
                ON tree_diffs(base_snapshot_id);
            CREATE INDEX IF NOT EXISTS idx_tree_diffs_target 
                ON tree_diffs(target_snapshot_id);
            CREATE INDEX IF NOT EXISTS idx_tree_diffs_computed 
                ON tree_diffs(computed_at);
            CREATE INDEX IF NOT EXISTS idx_tree_changes_diff 
                ON tree_changes(diff_id);
            CREATE INDEX IF NOT EXISTS idx_tree_changes_old_path 
                ON tree_changes(old_path) WHERE old_path IS NOT NULL;
            CREATE INDEX IF NOT EXISTS idx_tree_changes_new_path 
                ON tree_changes(new_path) WHERE new_path IS NOT NULL;
            CREATE INDEX IF NOT EXISTS idx_tree_changes_type 
                ON tree_changes(change_type);
        )");
        if (!result)
            return result;

        spdlog::info("Tree diffs and changes schema migration completed");
        return Result<void>();
    };

    m.downSQL = R"(
        -- Drop indexes
        DROP INDEX IF EXISTS idx_tree_changes_type;
        DROP INDEX IF EXISTS idx_tree_changes_new_path;
        DROP INDEX IF EXISTS idx_tree_changes_old_path;
        DROP INDEX IF EXISTS idx_tree_changes_diff;
        DROP INDEX IF EXISTS idx_tree_diffs_computed;
        DROP INDEX IF EXISTS idx_tree_diffs_target;
        DROP INDEX IF EXISTS idx_tree_diffs_base;
        
        -- Drop tables (cascade will remove tree_changes)
        DROP TABLE IF EXISTS tree_changes;
        DROP TABLE IF EXISTS tree_diffs;
    )";

    return m;
}

Migration YamsMetadataMigrations::addPathIndexingSchema() {
    MigrationBuilder builder(13, "Add path indexing schema");

    // Columns and indexes will be created idempotently inside upFunction.

    // FTS vtable handling is performed in upFunction with hasFTS5() guard.

    // Triggers moved into upFunction after successful FTS creation.

    builder.down(R"(DROP TRIGGER IF EXISTS documents_au;)");
    builder.down(R"(DROP TRIGGER IF EXISTS documents_ad;)");
    builder.down(R"(DROP TRIGGER IF EXISTS documents_ai;)");
    builder.down(R"(DROP TABLE IF EXISTS documents_path_fts;)");
    builder.down(R"(DROP INDEX IF EXISTS idx_documents_parent_hash;)");
    builder.down(R"(DROP INDEX IF EXISTS idx_documents_path_hash;)");
    builder.down(R"(DROP INDEX IF EXISTS idx_documents_reverse_path;)");
    builder.down(R"(DROP INDEX IF EXISTS idx_documents_path_prefix;)");

    builder.upFunction([](Database& db) -> Result<void> {
        // Ensure required columns exist; add missing ones idempotently.
        std::unordered_set<std::string> cols;
        if (auto ti = db.prepare("PRAGMA table_info(documents)"); ti) {
            auto stmt = std::move(ti).value();
            while (true) {
                auto s = stmt.step();
                if (!s)
                    break;
                if (!s.value())
                    break;
                cols.insert(stmt.getString(1));
            }
        }
        auto add_col = [&](const std::string& name, const std::string& ddl) -> Result<void> {
            if (!cols.count(name)) {
                return db.execute("ALTER TABLE documents ADD COLUMN " + ddl);
            }
            return Result<void>();
        };
        if (auto r = add_col("path_prefix", "path_prefix TEXT"); !r)
            return r;
        if (auto r = add_col("reverse_path", "reverse_path TEXT"); !r)
            return r;
        if (auto r = add_col("path_hash", "path_hash TEXT"); !r)
            return r;
        if (auto r = add_col("parent_hash", "parent_hash TEXT"); !r)
            return r;
        if (auto r = add_col("path_depth", "path_depth INTEGER DEFAULT 0"); !r)
            return r;

        // Ensure indexes exist (no-op if already present)
        if (auto r = db.execute(
                "CREATE INDEX IF NOT EXISTS idx_documents_path_prefix ON documents (path_prefix)");
            !r)
            return r;
        if (auto r = db.execute("CREATE INDEX IF NOT EXISTS idx_documents_reverse_path ON "
                                "documents (reverse_path)");
            !r)
            return r;
        if (auto r = db.execute(
                "CREATE INDEX IF NOT EXISTS idx_documents_path_hash ON documents (path_hash)");
            !r)
            return r;
        if (auto r = db.execute(
                "CREATE INDEX IF NOT EXISTS idx_documents_parent_hash ON documents (parent_hash)");
            !r)
            return r;

        auto selectResult = db.prepare("SELECT id, file_path FROM documents");
        if (!selectResult)
            return selectResult.error();

        auto updateResult =
            db.prepare("UPDATE documents SET file_path = ?, path_prefix = ?, reverse_path = ?, "
                       "path_hash = ?, parent_hash = ?, path_depth = ? WHERE id = ?");
        if (!updateResult)
            return updateResult.error();

        Statement selectStmt = std::move(selectResult).value();
        Statement updateStmt = std::move(updateResult).value();

        while (true) {
            auto step = selectStmt.step();
            if (!step)
                return step.error();
            if (!step.value())
                break;

            int64_t id = selectStmt.getInt64(0);
            std::string originalPath = selectStmt.getString(1);
            auto derived = computePathDerivedValues(originalPath);

            updateStmt.reset();
            updateStmt.bind(1, derived.normalizedPath);
            updateStmt.bind(2, derived.pathPrefix);
            updateStmt.bind(3, derived.reversePath);
            updateStmt.bind(4, derived.pathHash);
            updateStmt.bind(5, derived.parentHash);
            updateStmt.bind(6, derived.pathDepth);
            updateStmt.bind(7, id);

            auto exec = updateStmt.execute();
            if (!exec)
                return exec.error();
        }

        // Create/rebuild the path FTS vtable defensively (only if FTS5 is available).
        // If any step fails, swallow it and continue so that column/index migration still
        // completes; path FTS usage will be opportunistic.
        auto fts5 = db.hasFTS5();
        if (!fts5 || !fts5.value()) {
            spdlog::info("v13: FTS5 not available; skipping path FTS vtable creation.");
            return Result<void>();
        }
        auto create = db.execute(R"(
            CREATE VIRTUAL TABLE IF NOT EXISTS documents_path_fts
            USING fts5(file_path, tokenize='unicode61', content='documents', content_rowid='id');
        )");
        if (!create) {
            spdlog::warn("v13: FTS creation failed ({}). Continuing without path FTS.",
                         create.error().message);
            return Result<void>();
        }

        auto rebuild =
            db.execute("INSERT INTO documents_path_fts(documents_path_fts) VALUES('rebuild')");
        if (!rebuild) {
            spdlog::warn("v13: FTS rebuild failed ({}). Attempting full backfill.",
                         rebuild.error().message);
            (void)db.execute("DROP TABLE IF EXISTS documents_path_fts");
            auto recreate = db.execute(R"(
                CREATE VIRTUAL TABLE IF NOT EXISTS documents_path_fts
                USING fts5(file_path, tokenize='unicode61', content='documents', content_rowid='id');
            )");
            if (recreate) {
                auto fill = db.execute(R"(
                    INSERT INTO documents_path_fts(rowid, file_path)
                    SELECT id, file_path FROM documents ORDER BY id
                )");
                if (!fill) {
                    spdlog::warn("v13: FTS backfill failed ({}). Continuing without path FTS.",
                                 fill.error().message);
                }
            } else {
                spdlog::warn("v13: FTS recreate failed ({}). Continuing without path FTS.",
                             recreate.error().message);
            }
        }

        // Best-effort trigger creation (no-fail). If FTS is unavailable, these may fail.
        (void)db.execute(R"(
            CREATE TRIGGER IF NOT EXISTS documents_ai
            AFTER INSERT ON documents BEGIN
                INSERT INTO documents_path_fts(rowid, file_path)
                VALUES (new.id, new.file_path);
            END;
        )");
        (void)db.execute(R"(
            CREATE TRIGGER IF NOT EXISTS documents_ad
            AFTER DELETE ON documents BEGIN
                INSERT INTO documents_path_fts(documents_path_fts, rowid, file_path)
                VALUES('delete', old.id, old.file_path);
            END;
        )");
        (void)db.execute(R"(
            CREATE TRIGGER IF NOT EXISTS documents_au
            AFTER UPDATE ON documents BEGIN
                INSERT INTO documents_path_fts(documents_path_fts, rowid, file_path)
                VALUES('delete', old.id, old.file_path);
                INSERT INTO documents_path_fts(rowid, file_path)
                VALUES(new.id, new.file_path);
            END;
        )");

        return Result<void>();
    });

    return builder.build();
}

Migration YamsMetadataMigrations::chunkedPathIndexingBackfill() {
    // Post-v13 hardening: re-backfill path columns in chunks to avoid long single transactions.
    MigrationBuilder builder(14, "Chunked path indexing backfill");
    // Keep outer migration transaction; chunk logic checks inTransaction().

    builder.upFunction([](Database& db) -> Result<void> {
        // Create a tiny progress KV table; idempotent.
        if (auto r = db.execute(R"(
                CREATE TABLE IF NOT EXISTS yams_migration_progress (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
            )");
            !r) {
            return r;
        }

        // Determine starting cursor (last processed id, default 0)
        int64_t lastId = 0;
        if (auto sel = db.prepare(
                "SELECT value FROM yams_migration_progress WHERE key='v14_path_backfill_last_id'");
            sel) {
            auto s = std::move(sel).value();
            if (auto step = s.step(); step && step.value() && !s.isNull(0)) {
                try {
                    lastId = std::stoll(s.getString(0));
                } catch (...) {
                    lastId = 0;
                }
            }
        }

        // Chunk size (default 1000); allow override via env.
        int chunk = 1000;
        if (const char* env = std::getenv("YAMS_PATH_BACKFILL_CHUNK"); env && *env) {
            try {
                chunk = std::max(1, std::stoi(env));
            } catch (...) {
            }
        }

        // Prepare SELECT and UPDATE statements reused within chunks.
        auto selStmtRes = db.prepare("SELECT id, file_path, path_hash, path_prefix, reverse_path "
                                     "FROM documents "
                                     "WHERE id > ? AND (path_hash IS NULL OR path_hash='' OR "
                                     "                 path_prefix IS NULL OR path_prefix='' OR "
                                     "                 reverse_path IS NULL OR reverse_path='') "
                                     "ORDER BY id LIMIT ?");
        if (!selStmtRes)
            return selStmtRes.error();
        auto updStmtRes =
            db.prepare("UPDATE documents SET file_path = ?, path_prefix = ?, reverse_path = ?, "
                       "path_hash = ?, parent_hash = ?, path_depth = ? WHERE id = ?");
        if (!updStmtRes)
            return updStmtRes.error();

        Statement selectStmt = std::move(selStmtRes).value();
        Statement updateStmt = std::move(updStmtRes).value();

        int processedTotal = 0;
        while (true) {
            // Bind cursor and chunk
            if (auto r = selectStmt.reset(); !r)
                return r.error();
            if (auto b1 = selectStmt.bind(1, lastId); !b1)
                return b1.error();
            if (auto b2 = selectStmt.bind(2, chunk); !b2)
                return b2.error();

            const bool outerTxn = db.inTransaction();
            if (!outerTxn) {
                if (auto r = db.beginTransaction(); !r)
                    return r;
            }

            int processedThisChunk = 0;
            int64_t maxIdThisChunk = lastId;
            while (true) {
                auto step = selectStmt.step();
                if (!step) {
                    if (!outerTxn)
                        db.rollback();
                    return step.error();
                }
                if (!step.value())
                    break;

                int64_t id = selectStmt.getInt64(0);
                std::string originalPath = selectStmt.getString(1);
                auto derived = computePathDerivedValues(originalPath);

                if (auto r = updateStmt.reset(); !r) {
                    if (!outerTxn)
                        db.rollback();
                    return r.error();
                }
                if (auto r = updateStmt.bindAll(derived.normalizedPath, derived.pathPrefix,
                                                derived.reversePath, derived.pathHash,
                                                derived.parentHash, derived.pathDepth, id);
                    !r) {
                    if (!outerTxn)
                        db.rollback();
                    return r.error();
                }
                if (auto r = updateStmt.execute(); !r) {
                    if (!outerTxn)
                        db.rollback();
                    return r.error();
                }

                maxIdThisChunk = id;
                ++processedThisChunk;
            }

            if (!outerTxn) {
                if (auto r = db.commit(); !r)
                    return r; // commit even if 0 processed
            }

            if (processedThisChunk == 0)
                break; // no more rows to process

            processedTotal += processedThisChunk;
            lastId = maxIdThisChunk;

            // Persist progress (REPLACE ensures idempotency)
            if (auto prog = db.prepare("REPLACE INTO yams_migration_progress(key, value) "
                                       "VALUES('v14_path_backfill_last_id', ?)");
                prog) {
                auto st = std::move(prog).value();
                (void)st.bind(1, std::to_string(lastId));
                (void)st.execute();
            }

            spdlog::info("v14: path backfill processed {} rows (last_id={})", processedThisChunk,
                         lastId);
        }

        spdlog::info("v14: chunked path backfill complete (total processed={})", processedTotal);
        return Result<void>();
    });

    // No-op down migration (data transform)
    return builder.build();
}

Migration YamsMetadataMigrations::createPathTreeSchema() {
    MigrationBuilder builder(15, "Create path tree schema");

    builder.up(R"(
        CREATE TABLE IF NOT EXISTS path_tree_nodes (
            node_id INTEGER PRIMARY KEY AUTOINCREMENT,
            parent_id INTEGER REFERENCES path_tree_nodes(node_id) ON DELETE CASCADE,
            path_segment TEXT NOT NULL,
            full_path TEXT NOT NULL UNIQUE,
            doc_count INTEGER NOT NULL DEFAULT 0,
            centroid BLOB,
            centroid_weight INTEGER NOT NULL DEFAULT 0,
            fts_score REAL,
            diff_stats BLOB,
            last_updated INTEGER NOT NULL DEFAULT (unixepoch())
        );
    )");

    builder.up(R"(
        CREATE INDEX IF NOT EXISTS idx_path_tree_nodes_parent
            ON path_tree_nodes(parent_id);
    )");

    builder.up(R"(
        CREATE INDEX IF NOT EXISTS idx_path_tree_nodes_segment
            ON path_tree_nodes(path_segment);
    )");

    builder.up(R"(
        CREATE TABLE IF NOT EXISTS path_tree_node_documents (
            node_id INTEGER NOT NULL REFERENCES path_tree_nodes(node_id) ON DELETE CASCADE,
            document_id INTEGER NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
            PRIMARY KEY (node_id, document_id)
        );
    )");

    builder.down(R"(DROP TABLE IF EXISTS path_tree_node_documents;)");
    builder.down(R"(DROP INDEX IF EXISTS idx_path_tree_nodes_segment;)");
    builder.down(R"(DROP INDEX IF EXISTS idx_path_tree_nodes_parent;)");
    builder.down(R"(DROP TABLE IF EXISTS path_tree_nodes;)");

    return builder.build();
}

Migration YamsMetadataMigrations::createSymbolMetadataSchema() {
    MigrationBuilder builder(16, "Create symbol metadata schema");

    builder.up(R"(
        CREATE TABLE IF NOT EXISTS symbol_metadata (
            symbol_id INTEGER PRIMARY KEY AUTOINCREMENT,
            document_hash TEXT NOT NULL,
            file_path TEXT NOT NULL,
            symbol_name TEXT NOT NULL,
            qualified_name TEXT NOT NULL,
            kind TEXT NOT NULL,
            start_line INTEGER,
            end_line INTEGER,
            start_offset INTEGER,
            end_offset INTEGER,
            return_type TEXT,
            parameters TEXT,
            documentation TEXT,
            FOREIGN KEY (document_hash) REFERENCES documents(sha256_hash) ON DELETE CASCADE,
            UNIQUE(document_hash, qualified_name)
        );
    )");

    builder.up(R"(
        CREATE INDEX IF NOT EXISTS idx_symbol_name ON symbol_metadata(symbol_name);
    )");

    builder.up(R"(
        CREATE INDEX IF NOT EXISTS idx_symbol_document ON symbol_metadata(document_hash);
    )");

    builder.up(R"(
        CREATE INDEX IF NOT EXISTS idx_symbol_file_path ON symbol_metadata(file_path);
    )");

    builder.up(R"(
        CREATE INDEX IF NOT EXISTS idx_symbol_kind ON symbol_metadata(kind);
    )");

    builder.up(R"(
        CREATE INDEX IF NOT EXISTS idx_symbol_qualified ON symbol_metadata(qualified_name);
    )");

    builder.down(R"(
        DROP INDEX IF EXISTS idx_symbol_qualified;
    )");
    builder.down(R"(
        DROP INDEX IF EXISTS idx_symbol_kind;
    )");
    builder.down(R"(
        DROP INDEX IF EXISTS idx_symbol_file_path;
    )");
    builder.down(R"(
        DROP INDEX IF EXISTS idx_symbol_document;
    )");
    builder.down(R"(
        DROP INDEX IF EXISTS idx_symbol_name;
    )");
    builder.down(R"(
        DROP TABLE IF EXISTS symbol_metadata;
    )");

    return builder.build();
}

Migration YamsMetadataMigrations::addFTS5PorterStemmer() {
    Migration m;
    m.version = 17;
    m.name = "Enable FTS5 Porter stemmer for better search";
    m.created = std::chrono::system_clock::now();

    m.upFunc = [](Database& db) -> Result<void> {
        auto fts5Result = db.hasFTS5();
        if (!fts5Result)
            return fts5Result.error();
        if (!fts5Result.value())
            return {};

        // Rebuild FTS using the outer migration transaction from MigrationManager.
        Result<void> rc;

        // Drop any leftover temp table from previous failed attempts
        (void)db.execute("DROP TABLE IF EXISTS documents_fts_new;");

        // Create a new FTS5 table with Porter stemmer
        rc = db.execute(R"(
            CREATE VIRTUAL TABLE IF NOT EXISTS documents_fts_new USING fts5(
                content,
                title,
                content_type,
                tokenize='porter unicode61 tokenchars ''_-'''
            );
        )");
        if (!rc)
            return rc;

        // Backfill from existing extracted content in chunks
        auto countStmtRes = db.prepare(R"(
            SELECT COUNT(*) FROM documents WHERE content_extracted = 1
        )");
        if (!countStmtRes)
            return countStmtRes.error();
        {
            auto stmt = std::move(countStmtRes).value();
            auto step = stmt.step();
            if (!step)
                return step.error();
            std::int64_t total = 0;
            if (step.value()) {
                total = stmt.getInt64(0);
            }
            const std::int64_t kChunk = 10000;
            for (std::int64_t offset = 0; offset < total; offset += kChunk) {
                auto ins = db.prepare(R"(
                    INSERT OR REPLACE INTO documents_fts_new (rowid, content, title, content_type)
                    SELECT d.id,
                           COALESCE(dc.content_text, ''),
                           d.file_name,
                           COALESCE(d.mime_type, '')
                    FROM documents d
                    LEFT JOIN document_content dc ON dc.document_id = d.id
                    WHERE d.content_extracted = 1
                    ORDER BY d.id
                    LIMIT ? OFFSET ?
                )");
                if (!ins)
                    return ins.error();
                auto q = std::move(ins).value();
                auto b1 = q.bind(1, static_cast<std::int64_t>(kChunk));
                if (!b1)
                    return b1;
                auto b2 = q.bind(2, offset);
                if (!b2)
                    return b2;
                auto ex = q.execute();
                if (!ex)
                    return ex;
                if ((offset / kChunk) % 10 == 0) {
                    spdlog::info("[FTS5 v17] Porter stemmer backfill progress: {}/{} rows", offset,
                                 total);
                }
            }
            spdlog::info("[FTS5 v17] Porter stemmer backfill complete: {} rows", total);
        }

        // Swap tables
        rc = db.execute(R"(
            DROP TABLE IF EXISTS documents_fts;
            ALTER TABLE documents_fts_new RENAME TO documents_fts;
        )");
        if (!rc)
            return rc;
        return rc;
    };

    m.downFunc = [](Database& db) -> Result<void> {
        return db.execute(R"(
            DROP TABLE IF EXISTS documents_fts_new;
        )");
    };

    return m;
}

Migration YamsMetadataMigrations::removeFTS5ContentType() {
    Migration m;
    m.version = 18;
    m.name = "Remove unused content_type column from FTS5 index";
    m.created = std::chrono::system_clock::now();

    m.upFunc = [](Database& db) -> Result<void> {
        auto fts5Result = db.hasFTS5();
        if (!fts5Result)
            return fts5Result.error();
        if (!fts5Result.value())
            return {};

        Result<void> rc;

        // Drop any leftover temp table from previous failed attempts
        (void)db.execute("DROP TABLE IF EXISTS documents_fts_new;");

        // Create a new FTS5 table WITHOUT content_type column
        // content_type is never queried via FTS MATCH - filtering by mime_type
        // is done via JOIN on documents table. Removing it saves index space.
        rc = db.execute(R"(
            CREATE VIRTUAL TABLE IF NOT EXISTS documents_fts_new USING fts5(
                content,
                title,
                tokenize='porter unicode61 tokenchars ''_-'''
            );
        )");
        if (!rc)
            return rc;

        // Backfill from existing extracted content in chunks
        auto countStmtRes = db.prepare(R"(
            SELECT COUNT(*) FROM documents WHERE content_extracted = 1
        )");
        if (!countStmtRes)
            return countStmtRes.error();
        {
            auto stmt = std::move(countStmtRes).value();
            auto step = stmt.step();
            if (!step)
                return step.error();
            std::int64_t total = 0;
            if (step.value()) {
                total = stmt.getInt64(0);
            }
            const std::int64_t kChunk = 10000;
            for (std::int64_t offset = 0; offset < total; offset += kChunk) {
                // Only insert content and title - no content_type
                auto ins = db.prepare(R"(
                    INSERT OR REPLACE INTO documents_fts_new (rowid, content, title)
                    SELECT d.id,
                           COALESCE(dc.content_text, ''),
                           d.file_name
                    FROM documents d
                    LEFT JOIN document_content dc ON dc.document_id = d.id
                    WHERE d.content_extracted = 1
                    ORDER BY d.id
                    LIMIT ? OFFSET ?
                )");
                if (!ins)
                    return ins.error();
                auto q = std::move(ins).value();
                auto b1 = q.bind(1, static_cast<std::int64_t>(kChunk));
                if (!b1)
                    return b1;
                auto b2 = q.bind(2, offset);
                if (!b2)
                    return b2;
                auto ex = q.execute();
                if (!ex)
                    return ex;
                if ((offset / kChunk) % 10 == 0) {
                    spdlog::info("[FTS5 v18] Removing content_type, progress: {}/{} rows", offset,
                                 total);
                }
            }
            spdlog::info("[FTS5 v18] FTS5 hygiene migration complete: {} rows", total);
        }

        // Swap tables
        rc = db.execute(R"(
            DROP TABLE IF EXISTS documents_fts;
            ALTER TABLE documents_fts_new RENAME TO documents_fts;
        )");
        if (!rc)
            return rc;
        return rc;
    };

    m.downFunc = [](Database& db) -> Result<void> {
        // Rollback: recreate with content_type for backward compatibility
        auto fts5Result = db.hasFTS5();
        if (!fts5Result)
            return fts5Result.error();
        if (!fts5Result.value())
            return {};

        Result<void> rc;
        (void)db.execute("DROP TABLE IF EXISTS documents_fts_new;");

        rc = db.execute(R"(
            CREATE VIRTUAL TABLE IF NOT EXISTS documents_fts_new USING fts5(
                content,
                title,
                content_type,
                tokenize='porter unicode61 tokenchars ''_-'''
            );
        )");
        if (!rc)
            return rc;

        auto countStmtRes = db.prepare(R"(
            SELECT COUNT(*) FROM documents WHERE content_extracted = 1
        )");
        if (!countStmtRes)
            return countStmtRes.error();
        {
            auto stmt = std::move(countStmtRes).value();
            auto step = stmt.step();
            if (!step)
                return step.error();
            std::int64_t total = 0;
            if (step.value()) {
                total = stmt.getInt64(0);
            }
            const std::int64_t kChunk = 10000;
            for (std::int64_t offset = 0; offset < total; offset += kChunk) {
                auto ins = db.prepare(R"(
                    INSERT OR REPLACE INTO documents_fts_new (rowid, content, title, content_type)
                    SELECT d.id,
                           COALESCE(dc.content_text, ''),
                           d.file_name,
                           COALESCE(d.mime_type, '')
                    FROM documents d
                    LEFT JOIN document_content dc ON dc.document_id = d.id
                    WHERE d.content_extracted = 1
                    ORDER BY d.id
                    LIMIT ? OFFSET ?
                )");
                if (!ins)
                    return ins.error();
                auto q = std::move(ins).value();
                auto b1 = q.bind(1, static_cast<std::int64_t>(kChunk));
                if (!b1)
                    return b1;
                auto b2 = q.bind(2, offset);
                if (!b2)
                    return b2;
                auto ex = q.execute();
                if (!ex)
                    return ex;
            }
        }

        rc = db.execute(R"(
            DROP TABLE IF EXISTS documents_fts;
            ALTER TABLE documents_fts_new RENAME TO documents_fts;
        )");
        return rc;
    };

    return m;
}

Migration YamsMetadataMigrations::renameDocEntitiesToKgDocEntities() {
    Migration m;
    m.version = 19;
    m.name = "Rename doc_entities to kg_doc_entities";
    m.created = std::chrono::system_clock::now();

    m.upFunc = [](Database& db) -> Result<void> {
        // Check if old table exists
        auto oldExists = db.tableExists("doc_entities");
        if (!oldExists)
            return oldExists.error();

        // Check if new table exists
        auto newExists = db.tableExists("kg_doc_entities");
        if (!newExists)
            return newExists.error();

        if (oldExists.value() && !newExists.value()) {
            // Rename old table to new name
            auto rc = db.execute("ALTER TABLE doc_entities RENAME TO kg_doc_entities;");
            if (!rc)
                return rc;

            // Rename indexes
            (void)db.execute("DROP INDEX IF EXISTS idx_doc_entities_document;");
            (void)db.execute("DROP INDEX IF EXISTS idx_doc_entities_node;");
            auto idx1 = db.execute("CREATE INDEX IF NOT EXISTS idx_kg_doc_entities_document ON "
                                   "kg_doc_entities(document_id);");
            if (!idx1)
                return idx1;
            auto idx2 = db.execute(
                "CREATE INDEX IF NOT EXISTS idx_kg_doc_entities_node ON kg_doc_entities(node_id);");
            if (!idx2)
                return idx2;

            spdlog::info("Renamed doc_entities to kg_doc_entities");
        } else if (!oldExists.value() && !newExists.value()) {
            // Neither exists - create the new table (shouldn't happen if v7 ran, but be safe)
            auto rc = db.execute(R"(
                CREATE TABLE IF NOT EXISTS kg_doc_entities (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    document_id INTEGER NOT NULL,
                    entity_text TEXT NOT NULL,
                    node_id INTEGER,
                    start_offset INTEGER,
                    end_offset INTEGER,
                    confidence REAL,
                    extractor TEXT,
                    FOREIGN KEY (document_id) REFERENCES documents(id) ON DELETE CASCADE,
                    FOREIGN KEY (node_id) REFERENCES kg_nodes(id) ON DELETE SET NULL
                );
                CREATE INDEX IF NOT EXISTS idx_kg_doc_entities_document ON kg_doc_entities(document_id);
                CREATE INDEX IF NOT EXISTS idx_kg_doc_entities_node ON kg_doc_entities(node_id);
            )");
            if (!rc)
                return rc;
            spdlog::info("Created kg_doc_entities table");
        }
        // If kg_doc_entities already exists, nothing to do

        return Result<void>();
    };

    m.downFunc = [](Database& db) -> Result<void> {
        // Reverse: rename kg_doc_entities back to doc_entities
        auto newExists = db.tableExists("kg_doc_entities");
        if (newExists && newExists.value()) {
            (void)db.execute("DROP INDEX IF EXISTS idx_kg_doc_entities_document;");
            (void)db.execute("DROP INDEX IF EXISTS idx_kg_doc_entities_node;");
            auto rc = db.execute("ALTER TABLE kg_doc_entities RENAME TO doc_entities;");
            if (!rc)
                return rc;
            (void)db.execute("CREATE INDEX IF NOT EXISTS idx_doc_entities_document ON "
                             "doc_entities(document_id);");
            (void)db.execute(
                "CREATE INDEX IF NOT EXISTS idx_doc_entities_node ON doc_entities(node_id);");
        }
        return Result<void>();
    };

    return m;
}

Migration YamsMetadataMigrations::createSessionIndexes() {
    Migration m;
    m.version = 20;
    m.name = "Add session_id index for session-isolated memory";
    m.created = std::chrono::system_clock::now();

    m.upSQL = R"(
        CREATE INDEX IF NOT EXISTS idx_metadata_session_id
            ON metadata(key, value) WHERE key = 'session_id';
    )";

    m.downSQL = R"(
        DROP INDEX IF EXISTS idx_metadata_session_id;
    )";

    return m;
}

Migration YamsMetadataMigrations::createRepairTrackingSchema() {
    Migration m;
    m.version = 21;
    m.name = "Add repair tracking fields to documents";
    m.created = std::chrono::system_clock::now();

    m.upFunc = [](Database& db) -> Result<void> {
        std::unordered_set<std::string> cols;
        if (auto ti = db.prepare("PRAGMA table_info(documents)"); ti) {
            auto stmt = std::move(ti).value();
            while (true) {
                auto s = stmt.step();
                if (!s)
                    break;
                if (!s.value())
                    break;
                cols.insert(stmt.getString(1));
            }
        }

        auto add_col = [&](const std::string& name, const std::string& ddl) -> Result<void> {
            if (!cols.count(name)) {
                return db.execute("ALTER TABLE documents ADD COLUMN " + ddl);
            }
            return Result<void>();
        };

        if (auto r = add_col("repair_status", "repair_status TEXT DEFAULT 'pending'"); !r)
            return r;
        if (auto r = add_col("repair_attempted_at", "repair_attempted_at INTEGER"); !r)
            return r;
        if (auto r = add_col("repair_attempts", "repair_attempts INTEGER DEFAULT 0"); !r)
            return r;

        if (auto r = db.execute("CREATE INDEX IF NOT EXISTS idx_documents_repair_status ON "
                                "documents(repair_status)");
            !r)
            return r;

        spdlog::info("v21: Added repair tracking fields to documents table");
        return Result<void>();
    };

    m.downSQL = R"(
        DROP INDEX IF EXISTS idx_documents_repair_status;
    )";

    return m;
}

Migration YamsMetadataMigrations::createSymbolExtractionStateSchema() {
    Migration m;
    m.version = 22;
    m.name = "Create symbol extraction state tracking schema";
    m.created = std::chrono::system_clock::now();

    m.upSQL = R"(
        -- Tracks symbol extraction state per document to enable versioned dedupe.
        -- This avoids re-extraction when:
        --   1. Extraction already completed (even with 0 symbols)
        --   2. Extractor version hasn't changed
        CREATE TABLE IF NOT EXISTS document_symbol_extraction_state (
            document_id INTEGER PRIMARY KEY,
            extractor_id TEXT NOT NULL,           -- e.g., "symbol_extractor_treesitter:v1"
            extractor_config_hash TEXT,           -- hash of grammar/config versions (optional)
            extracted_at INTEGER NOT NULL,        -- unix timestamp
            status TEXT NOT NULL DEFAULT 'complete',  -- complete|failed|pending
            entity_count INTEGER DEFAULT 0,       -- number of symbols/entities extracted
            error_message TEXT,
            FOREIGN KEY (document_id) REFERENCES documents(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_symbol_extraction_state_extractor
            ON document_symbol_extraction_state(extractor_id);
        CREATE INDEX IF NOT EXISTS idx_symbol_extraction_state_status
            ON document_symbol_extraction_state(status);
    )";

    m.downSQL = R"(
        DROP INDEX IF EXISTS idx_symbol_extraction_state_status;
        DROP INDEX IF EXISTS idx_symbol_extraction_state_extractor;
        DROP TABLE IF EXISTS document_symbol_extraction_state;
    )";

    return m;
}

Migration YamsMetadataMigrations::createSymSpellSchema() {
    Migration m;
    m.version = 23;
    m.name = "Create SymSpell fuzzy search tables";
    m.created = std::chrono::system_clock::now();

    m.upSQL = R"(
        -- SymSpell dictionary terms table
        -- Stores original terms with their frequencies for fuzzy matching
        CREATE TABLE IF NOT EXISTS symspell_terms (
            id INTEGER PRIMARY KEY,
            term TEXT UNIQUE NOT NULL,
            frequency INTEGER DEFAULT 1,
            created_at INTEGER DEFAULT (unixepoch())
        );

        -- SymSpell delete variants table
        -- Maps delete hashes to term IDs for O(1) lookup during fuzzy search
        -- WITHOUT ROWID saves ~30% storage for this lookup-only table
        CREATE TABLE IF NOT EXISTS symspell_deletes (
            delete_hash INTEGER NOT NULL,
            term_id INTEGER NOT NULL,
            FOREIGN KEY (term_id) REFERENCES symspell_terms(id) ON DELETE CASCADE,
            PRIMARY KEY (delete_hash, term_id)
        ) WITHOUT ROWID;

        -- Index for fast hash lookups (the core SymSpell query pattern)
        CREATE INDEX IF NOT EXISTS idx_symspell_deletes_hash 
            ON symspell_deletes(delete_hash);
        
        -- Index for term lookups by text (for frequency updates and exact matches)
        CREATE INDEX IF NOT EXISTS idx_symspell_terms_term 
            ON symspell_terms(term);
    )";

    m.downSQL = R"(
        DROP INDEX IF EXISTS idx_symspell_terms_term;
        DROP INDEX IF EXISTS idx_symspell_deletes_hash;
        DROP TABLE IF EXISTS symspell_deletes;
        DROP TABLE IF EXISTS symspell_terms;
    )";

    return m;
}

} // namespace yams::metadata
