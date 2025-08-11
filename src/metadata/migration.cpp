#include <yams/metadata/migration.h>
#include <spdlog/spdlog.h>
#include <sstream>
#include <iomanip>

namespace yams::metadata {

// MigrationManager implementation
MigrationManager::MigrationManager(Database& db) : db_(db) {
}

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
    auto stmtResult = db_.prepare(
        "SELECT MAX(version) FROM migration_history WHERE success = 1"
    );
    if (!stmtResult) return stmtResult.error();
    
    Statement stmt = std::move(stmtResult).value();
    auto stepResult = stmt.step();
    if (!stepResult) return stepResult.error();
    
    if (stepResult.value() && !stmt.isNull(0)) {
        return stmt.getInt(0);
    }
    
    return 0; // No migrations applied yet
}

int MigrationManager::getLatestVersion() const {
    if (migrations_.empty()) return 0;
    return migrations_.rbegin()->first;
}

Result<bool> MigrationManager::needsMigration() {
    auto currentResult = getCurrentVersion();
    if (!currentResult) return currentResult.error();
    
    return currentResult.value() < getLatestVersion();
}

Result<void> MigrationManager::migrate() {
    return migrateTo(getLatestVersion());
}

Result<void> MigrationManager::migrateTo(int targetVersion) {
    auto currentResult = getCurrentVersion();
    if (!currentResult) return currentResult.error();
    
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
            spdlog::debug("Applying migration {} '{}' ({}/{})", 
                        version, migration.name, 
                        ++appliedMigrations, totalMigrations);
            
            if (progressCallback_) {
                progressCallback_(appliedMigrations, totalMigrations, migration.name);
            }
            
            auto start = std::chrono::steady_clock::now();
            
            auto result = applyMigration(migration);
            
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - start
            );
            
            if (!result) {
                auto recordResult = recordMigration(
                    version, migration.name, duration, false, result.error().message
                );
                return result;
            }
            
            auto recordResult = recordMigration(
                version, migration.name, duration, true
            );
            if (!recordResult) return recordResult;
            
            currentVersion = version;
        }
    }
    
    spdlog::debug("Migration complete. Now at version {}", currentVersion);
    return {};
}

Result<void> MigrationManager::rollbackTo(int targetVersion) {
    auto currentResult = getCurrentVersion();
    if (!currentResult) return currentResult.error();
    
    int currentVersion = currentResult.value();
    
    if (currentVersion <= targetVersion) {
        return Error{ErrorCode::InvalidData, 
                    "Cannot rollback to a higher version"};
    }
    
    // Rollback migrations in reverse order
    auto it = migrations_.rbegin();
    while (it != migrations_.rend() && it->first > targetVersion) {
        if (it->first <= currentVersion) {
            spdlog::debug("Rolling back migration {} '{}'", 
                        it->first, it->second.name);
            
            auto start = std::chrono::steady_clock::now();
            
            auto result = rollbackMigration(it->second);
            
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - start
            );
            
            if (!result) {
                auto recordResult = recordMigration(
                    -it->first, "Rollback: " + it->second.name, 
                    duration, false, result.error().message
                );
                return result;
            }
            
            auto recordResult = recordMigration(
                -it->first, "Rollback: " + it->second.name, 
                duration, true
            );
            if (!recordResult) return recordResult;
        }
        ++it;
    }
    
    spdlog::debug("Rollback complete. Now at version {}", targetVersion);
    return {};
}

Result<std::vector<MigrationHistory>> MigrationManager::getHistory() {
    auto stmtResult = db_.prepare(
        "SELECT version, name, applied_at, duration_ms, success, error "
        "FROM migration_history ORDER BY applied_at DESC"
    );
    if (!stmtResult) return stmtResult.error();
    
    Statement stmt = std::move(stmtResult).value();
    std::vector<MigrationHistory> history;
    
    while (true) {
        auto stepResult = stmt.step();
        if (!stepResult) return stepResult.error();
        if (!stepResult.value()) break;
        
        MigrationHistory entry;
        entry.version = stmt.getInt(0);
        entry.name = stmt.getString(1);
        entry.appliedAt = std::chrono::system_clock::time_point(
            std::chrono::seconds(stmt.getInt64(2))
        );
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
    if (!result) return result;
    
    // Verify FTS5 tables if present
    auto ftsCheck = db_.tableExists("documents_fts");
    if (ftsCheck && ftsCheck.value()) {
        auto ftsResult = db_.execute(
            "INSERT INTO documents_fts(documents_fts) VALUES('integrity-check')"
        );
        if (!ftsResult) {
            return Error{ErrorCode::DatabaseError, 
                        "FTS5 integrity check failed"};
        }
    }
    
    return {};
}

void MigrationManager::setProgressCallback(ProgressCallback callback) {
    progressCallback_ = std::move(callback);
}

Result<void> MigrationManager::applyMigration(const Migration& migration) {
    return db_.transaction([&]() -> Result<void> {
        if (migration.upFunc) {
            return migration.upFunc(db_);
        } else if (!migration.upSQL.empty()) {
            return db_.execute(migration.upSQL);
        } else {
            return Error{ErrorCode::InvalidData, 
                        "Migration has no up function or SQL"};
        }
    });
}

Result<void> MigrationManager::rollbackMigration(const Migration& migration) {
    return db_.transaction([&]() -> Result<void> {
        if (migration.downFunc) {
            return migration.downFunc(db_);
        } else if (!migration.downSQL.empty()) {
            return db_.execute(migration.downSQL);
        } else {
            return Error{ErrorCode::InvalidData, 
                        "Migration has no down function or SQL"};
        }
    });
}

Result<void> MigrationManager::recordMigration(int version, const std::string& name,
                                              std::chrono::milliseconds duration,
                                              bool success, const std::string& error) {
    auto stmtResult = db_.prepare(
        "INSERT INTO migration_history "
        "(version, name, applied_at, duration_ms, success, error) "
        "VALUES (?, ?, ?, ?, ?, ?)"
    );
    if (!stmtResult) return stmtResult.error();
    
    Statement stmt = std::move(stmtResult).value();
    auto now = std::chrono::system_clock::now().time_since_epoch();
    auto seconds = std::chrono::duration_cast<std::chrono::seconds>(now).count();
    
    auto bindResult = stmt.bindAll(
        version, name, seconds, duration.count(), success ? 1 : 0, error
    );
    if (!bindResult) return bindResult;
    
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
    return {
        createInitialSchema(),
        createFTS5Tables(),
        createMetadataIndexes(),
        createRelationshipTables(),
        createSearchTables()
    };
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
        if (!fts5Result) return fts5Result.error();
        
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
                tokenize='porter unicode61'
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

MigrationBuilder& MigrationBuilder::renameTable(const std::string& from, 
                                               const std::string& to) {
    upStatements_.push_back("ALTER TABLE " + from + " RENAME TO " + to);
    downStatements_.push_back("ALTER TABLE " + to + " RENAME TO " + from);
    return *this;
}

MigrationBuilder& MigrationBuilder::addColumn(const std::string& table, 
                                            const std::string& column,
                                            const std::string& type, 
                                            bool nullable) {
    std::string sql = "ALTER TABLE " + table + " ADD COLUMN " + column + " " + type;
    if (!nullable) sql += " NOT NULL";
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

MigrationBuilder& MigrationBuilder::renameColumn(const std::string& table,
                                                const std::string& from,
                                                const std::string& to) {
    upStatements_.push_back("ALTER TABLE " + table + " RENAME COLUMN " + from + " TO " + to);
    downStatements_.push_back("ALTER TABLE " + table + " RENAME COLUMN " + to + " TO " + from);
    return *this;
}

MigrationBuilder& MigrationBuilder::createIndex(const std::string& index,
                                              const std::string& table,
                                              const std::vector<std::string>& columns,
                                              bool unique) {
    std::stringstream sql;
    sql << "CREATE ";
    if (unique) sql << "UNIQUE ";
    sql << "INDEX " << index << " ON " << table << " (";
    for (size_t i = 0; i < columns.size(); ++i) {
        if (i > 0) sql << ", ";
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

Migration MigrationBuilder::build() const {
    Migration m = migration_;
    
    // Combine SQL statements
    if (!upStatements_.empty() && !m.upFunc) {
        std::stringstream sql;
        for (const auto& stmt : upStatements_) {
            sql << stmt << ";\n";
        }
        m.upSQL = sql.str();
    }
    
    if (!downStatements_.empty() && !m.downFunc) {
        std::stringstream sql;
        for (const auto& stmt : downStatements_) {
            sql << stmt << ";\n";
        }
        m.downSQL = sql.str();
    }
    
    return m;
}

} // namespace yams::metadata