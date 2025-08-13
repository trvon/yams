#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/database.h>
#include <yams/metadata/migration.h>
#include <yams/core/types.h>

#include <spdlog/spdlog.h>

#include <memory>
#include <utility>
#include <string>
#include <optional>
#include <vector>
#include <chrono>

namespace yams::metadata {

namespace {

// Map KG store config to a sensible ConnectionPool configuration
inline ConnectionPoolConfig toPoolConfig(const KnowledgeGraphStoreConfig& cfg) {
    ConnectionPoolConfig pcfg;
    pcfg.enableWAL = cfg.enable_wal;
    pcfg.enableForeignKeys = true;
    pcfg.busyTimeout = std::chrono::milliseconds{5000};
    return pcfg;
}

} // namespace

class SqliteKnowledgeGraphStore final : public KnowledgeGraphStore {
public:
    // Construct with an external pool (non-owning)
    SqliteKnowledgeGraphStore(ConnectionPool& pool, KnowledgeGraphStoreConfig cfg)
        : cfg_(std::move(cfg)), pool_(&pool) {}

    // Construct with owned pool created from dbPath
    static Result<std::unique_ptr<SqliteKnowledgeGraphStore>>
    createWithPath(const std::string& dbPath, const KnowledgeGraphStoreConfig& cfg) {
        // 1) Ensure DB exists and schema is migrated (in case the caller bypasses the CLI init path)
        {
            Database db;
            auto rOpen = db.open(dbPath, ConnectionMode::Create);
            if (!rOpen) return rOpen.error();

            // Best-effort config
            if (cfg.enable_wal) {
                auto rWal = db.enableWAL();
                if (!rWal) spdlog::warn("enableWAL failed during KG store init: {}", rWal.error().message);
            }
            auto rFK = db.execute("PRAGMA foreign_keys = ON");
            if (!rFK) spdlog::warn("Enabling foreign_keys failed during KG store init: {}", rFK.error().message);

            // Run metadata migrations which include the KG schema (v7)
            MigrationManager mm(db);
            auto rInit = mm.initialize();
            if (!rInit) return rInit.error();
            auto migrations = YamsMetadataMigrations::getAllMigrations();
            for (const auto& m : migrations) {
                mm.registerMigration(m);
            }
            auto rMig = mm.migrate();
            if (!rMig) return rMig.error();
        }

        // 2) Create and initialize a connection pool for ongoing operations
        auto pool = std::make_unique<ConnectionPool>(dbPath, toPoolConfig(cfg));
        auto rInit = pool->initialize();
        if (!rInit) return rInit.error();

        // 3) Create store owning the pool
        auto store = std::unique_ptr<SqliteKnowledgeGraphStore>(
            new SqliteKnowledgeGraphStore(*pool, cfg));
        store->owned_pool_ = std::move(pool);
        return store;
    }

    // KnowledgeGraphStore API

    void setConfig(const KnowledgeGraphStoreConfig& cfg) override {
        cfg_ = cfg;
    }

    const KnowledgeGraphStoreConfig& getConfig() const override { return cfg_; }

    // Nodes (not needed for SimpleKGScorer MVP)
    Result<std::int64_t> upsertNode(const KGNode&) override {
        return Error{ErrorCode::NotImplemented, "upsertNode not implemented"};
    }

    Result<std::vector<std::int64_t>> upsertNodes(const std::vector<KGNode>&) override {
        return Error{ErrorCode::NotImplemented, "upsertNodes not implemented"};
    }

    Result<std::optional<KGNode>> getNodeById(std::int64_t) override {
        return Error{ErrorCode::NotImplemented, "getNodeById not implemented"};
    }

    Result<std::optional<KGNode>> getNodeByKey(std::string_view) override {
        return Error{ErrorCode::NotImplemented, "getNodeByKey not implemented"};
    }

    Result<std::vector<KGNode>> findNodesByType(std::string_view,
                                                std::size_t,
                                                std::size_t) override {
        return Error{ErrorCode::NotImplemented, "findNodesByType not implemented"};
    }

    Result<void> deleteNodeById(std::int64_t) override {
        return Error{ErrorCode::NotImplemented, "deleteNodeById not implemented"};
    }

    // Aliases
    Result<std::int64_t> addAlias(const KGAlias& alias) override {
        return pool_->withConnection([&](Database& db) -> Result<std::int64_t> {
            auto stmtR = db.prepare("INSERT INTO kg_aliases (node_id, alias, source, confidence) VALUES (?, ?, ?, ?)");
            if (!stmtR) return stmtR.error();
            auto stmt = std::move(stmtR).value();

            // Bind parameters
            auto br = stmt.bind(1, static_cast<int64_t>(alias.nodeId));
            if (!br) return br.error();
            br = stmt.bind(2, alias.alias);
            if (!br) return br.error();
            if (alias.source.has_value()) {
                br = stmt.bind(3, alias.source.value());
            } else {
                br = stmt.bind(3, nullptr);
            }
            if (!br) return br.error();
            br = stmt.bind(4, static_cast<double>(alias.confidence));
            if (!br) return br.error();

            auto er = stmt.execute();
            if (!er) return er.error();

            return db.lastInsertRowId();
        });
    }

    Result<void> addAliases(const std::vector<KGAlias>& aliases) override {
        if (aliases.empty()) return Result<void>();
        return pool_->withConnection([&](Database& db) -> Result<void> {
            return db.transaction([&]() -> Result<void> {
                auto stmtR = db.prepare("INSERT INTO kg_aliases (node_id, alias, source, confidence) VALUES (?, ?, ?, ?)");
                if (!stmtR) return stmtR.error();
                auto stmt = std::move(stmtR).value();

                for (const auto& a : aliases) {
                    auto br = stmt.clearBindings();
                    if (!br) return br.error();
                    br = stmt.bind(1, static_cast<int64_t>(a.nodeId));
                    if (!br) return br.error();
                    br = stmt.bind(2, a.alias);
                    if (!br) return br.error();
                    if (a.source.has_value()) {
                        br = stmt.bind(3, a.source.value());
                    } else {
                        br = stmt.bind(3, nullptr);
                    }
                    if (!br) return br.error();
                    br = stmt.bind(4, static_cast<double>(a.confidence));
                    if (!br) return br;
                    auto er = stmt.execute();
                    if (!er) return er.error();
                    auto rr = stmt.reset();
                    if (!rr) return rr;
                }
                return Result<void>();
            });
        });
    }

    Result<std::vector<AliasResolution>> resolveAliasExact(std::string_view alias,
                                                           std::size_t limit) override {
        return pool_->withConnection([&](Database& db) -> Result<std::vector<AliasResolution>> {
            auto stmtR = db.prepare("SELECT node_id FROM kg_aliases WHERE alias = ? LIMIT ?");
            if (!stmtR) return stmtR.error();
            auto stmt = std::move(stmtR).value();

            auto br = stmt.bind(1, alias);
            if (!br) return br.error();
            br = stmt.bind(2, static_cast<int64_t>(limit));
            if (!br) return br.error();

            std::vector<AliasResolution> out;
            while (true) {
                auto step = stmt.step();
                if (!step) return step.error();
                if (!step.value()) break;
                AliasResolution ar;
                ar.nodeId = stmt.getInt64(0);
                ar.score = 1.0f;
                out.push_back(ar);
            }
            return out;
        });
    }

    Result<std::vector<AliasResolution>> resolveAliasFuzzy(std::string_view aliasQuery,
                                                           std::size_t limit) override {
        if (!cfg_.enable_alias_fts) {
            return resolveAliasExact(aliasQuery, limit);
        }
        return pool_->withConnection([&](Database& db) -> Result<std::vector<AliasResolution>> {
            // Use FTS if available; fall back to exact if table is missing
            auto existsR = db.tableExists("kg_aliases_fts");
            if (!existsR) return existsR.error();
            if (!existsR.value()) {
                return resolveAliasExact(aliasQuery, limit);
            }

            auto stmtR = db.prepare(
                "SELECT a.node_id, 1.0 AS score "
                "FROM kg_aliases_fts f "
                "JOIN kg_aliases a ON a.id = f.rowid "
                "WHERE kg_aliases_fts MATCH ? "
                "LIMIT ?"
            );
            if (!stmtR) return stmtR.error();
            auto stmt = std::move(stmtR).value();

            auto br = stmt.bind(1, aliasQuery);
            if (!br) return br.error();
            br = stmt.bind(2, static_cast<int64_t>(limit));
            if (!br) return br.error();

            std::vector<AliasResolution> out;
            while (true) {
                auto step = stmt.step();
                if (!step) return step.error();
                if (!step.value()) break;
                AliasResolution ar;
                ar.nodeId = stmt.getInt64(0);
                ar.score = 1.0f;
                out.push_back(ar);
            }
            return out;
        });
    }

    Result<void> removeAliasById(std::int64_t aliasId) override {
        return pool_->withConnection([&](Database& db) -> Result<void> {
            auto stmtR = db.prepare("DELETE FROM kg_aliases WHERE id = ?");
            if (!stmtR) return stmtR.error();
            auto stmt = std::move(stmtR).value();
            auto br = stmt.bind(1, aliasId);
            if (!br) return br.error();
            return stmt.execute();
        });
    }

    Result<void> removeAliasesForNode(std::int64_t nodeId) override {
        return pool_->withConnection([&](Database& db) -> Result<void> {
            auto stmtR = db.prepare("DELETE FROM kg_aliases WHERE node_id = ?");
            if (!stmtR) return stmtR.error();
            auto stmt = std::move(stmtR).value();
            auto br = stmt.bind(1, nodeId);
            if (!br) return br.error();
            return stmt.execute();
        });
    }

    // Edges (minimal neighbor support)
    Result<std::int64_t> addEdge(const KGEdge& edge) override {
        return pool_->withConnection([&](Database& db) -> Result<std::int64_t> {
            auto stmtR = db.prepare(
                "INSERT INTO kg_edges (src_node_id, dst_node_id, relation, weight, created_time, properties) "
                "VALUES (?, ?, ?, ?, ?, ?)"
            );
            if (!stmtR) return stmtR.error();
            auto stmt = std::move(stmtR).value();

            auto br = stmt.bind(1, edge.srcNodeId);
            if (!br) return br.error();
            br = stmt.bind(2, edge.dstNodeId);
            if (!br) return br.error();
            br = stmt.bind(3, edge.relation);
            if (!br) return br.error();
            br = stmt.bind(4, static_cast<double>(edge.weight));
            if (!br) return br.error();
            if (edge.createdTime.has_value()) {
                br = stmt.bind(5, edge.createdTime.value());
            } else {
                br = stmt.bind(5, nullptr);
            }
            if (!br) return br.error();
            if (edge.properties.has_value()) {
                br = stmt.bind(6, edge.properties.value());
            } else {
                br = stmt.bind(6, nullptr);
            }
            if (!br) return br.error();

            auto ex = stmt.execute();
            if (!ex) return ex.error();
            return db.lastInsertRowId();
        });
    }

    Result<void> addEdges(const std::vector<KGEdge>& edges) override {
        if (edges.empty()) return Result<void>();
        return pool_->withConnection([&](Database& db) -> Result<void> {
            return db.transaction([&]() -> Result<void> {
                auto stmtR = db.prepare(
                    "INSERT INTO kg_edges (src_node_id, dst_node_id, relation, weight, created_time, properties) "
                    "VALUES (?, ?, ?, ?, ?, ?)"
                );
                if (!stmtR) return stmtR.error();
                auto stmt = std::move(stmtR).value();

                for (const auto& e : edges) {
                    auto br = stmt.clearBindings();
                    if (!br) return br.error();
                    br = stmt.bind(1, e.srcNodeId);
                    if (!br) return br.error();
                    br = stmt.bind(2, e.dstNodeId);
                    if (!br) return br.error();
                    br = stmt.bind(3, e.relation);
                    if (!br) return br.error();
                    br = stmt.bind(4, static_cast<double>(e.weight));
                    if (!br) return br.error();
                    if (e.createdTime.has_value()) {
                        br = stmt.bind(5, e.createdTime.value());
                    } else {
                        br = stmt.bind(5, nullptr);
                    }
                    if (!br) return br;
                    if (e.properties.has_value()) {
                        br = stmt.bind(6, e.properties.value());
                    } else {
                        br = stmt.bind(6, nullptr);
                    }
                    if (!br) return br;

                    auto ex = stmt.execute();
                    if (!ex) return ex.error();
                    auto rr = stmt.reset();
                    if (!rr) return rr;
                }
                return Result<void>();
            });
        });
    }

    Result<std::vector<KGEdge>> getEdgesFrom(std::int64_t srcNodeId,
                                             std::optional<std::string_view> relation,
                                             std::size_t limit,
                                             std::size_t offset) override {
        return pool_->withConnection([&](Database& db) -> Result<std::vector<KGEdge>> {
            std::string sql = "SELECT id, src_node_id, dst_node_id, relation, weight, created_time, properties "
                              "FROM kg_edges WHERE src_node_id = ?";
            if (relation.has_value()) {
                sql += " AND relation = ?";
            }
            sql += " LIMIT ? OFFSET ?";

            auto stmtR = db.prepare(sql);
            if (!stmtR) return stmtR.error();
            auto stmt = std::move(stmtR).value();

            int idx = 1;
            auto br = stmt.bind(idx++, srcNodeId);
            if (!br) return br.error();
            if (relation.has_value()) {
                br = stmt.bind(idx++, relation.value());
                if (!br) return br.error();
            }
            br = stmt.bind(idx++, static_cast<int64_t>(limit));
            if (!br) return br.error();
            br = stmt.bind(idx++, static_cast<int64_t>(offset));
            if (!br) return br.error();

            std::vector<KGEdge> out;
            while (true) {
                auto step = stmt.step();
                if (!step) return step.error();
                if (!step.value()) break;
                KGEdge e;
                e.id = stmt.getInt64(0);
                e.srcNodeId = stmt.getInt64(1);
                e.dstNodeId = stmt.getInt64(2);
                e.relation = stmt.getString(3);
                e.weight = static_cast<float>(stmt.getDouble(4));
                if (!stmt.isNull(5)) e.createdTime = stmt.getInt64(5);
                if (!stmt.isNull(6)) e.properties = stmt.getString(6);
                out.push_back(std::move(e));
            }
            return out;
        });
    }

    Result<std::vector<KGEdge>> getEdgesTo(std::int64_t dstNodeId,
                                           std::optional<std::string_view> relation,
                                           std::size_t limit,
                                           std::size_t offset) override {
        return pool_->withConnection([&](Database& db) -> Result<std::vector<KGEdge>> {
            std::string sql = "SELECT id, src_node_id, dst_node_id, relation, weight, created_time, properties "
                              "FROM kg_edges WHERE dst_node_id = ?";
            if (relation.has_value()) {
                sql += " AND relation = ?";
            }
            sql += " LIMIT ? OFFSET ?";

            auto stmtR = db.prepare(sql);
            if (!stmtR) return stmtR.error();
            auto stmt = std::move(stmtR).value();

            int idx = 1;
            auto br = stmt.bind(idx++, dstNodeId);
            if (!br) return br.error();
            if (relation.has_value()) {
                br = stmt.bind(idx++, relation.value());
                if (!br) return br.error();
            }
            br = stmt.bind(idx++, static_cast<int64_t>(limit));
            if (!br) return br.error();
            br = stmt.bind(idx++, static_cast<int64_t>(offset));
            if (!br) return br.error();

            std::vector<KGEdge> out;
            while (true) {
                auto step = stmt.step();
                if (!step) return step.error();
                if (!step.value()) break;
                KGEdge e;
                e.id = stmt.getInt64(0);
                e.srcNodeId = stmt.getInt64(1);
                e.dstNodeId = stmt.getInt64(2);
                e.relation = stmt.getString(3);
                e.weight = static_cast<float>(stmt.getDouble(4));
                if (!stmt.isNull(5)) e.createdTime = stmt.getInt64(5);
                if (!stmt.isNull(6)) e.properties = stmt.getString(6);
                out.push_back(std::move(e));
            }
            return out;
        });
    }

    Result<std::vector<std::int64_t>> neighbors(std::int64_t nodeId,
                                                std::size_t maxNeighbors) override {
        return pool_->withConnection([&](Database& db) -> Result<std::vector<std::int64_t>> {
            auto stmtR = db.prepare(
                "SELECT dst_node_id FROM kg_edges WHERE src_node_id = ? LIMIT ?"
            );
            if (!stmtR) return stmtR.error();
            auto stmt = std::move(stmtR).value();

            auto br = stmt.bind(1, nodeId);
            if (!br) return br.error();
            br = stmt.bind(2, static_cast<int64_t>(maxNeighbors));
            if (!br) return br.error();

            std::vector<std::int64_t> out;
            while (true) {
                auto step = stmt.step();
                if (!step) return step.error();
                if (!step.value()) break;
                out.push_back(stmt.getInt64(0));
            }
            return out;
        });
    }

    Result<void> removeEdgeById(std::int64_t edgeId) override {
        return pool_->withConnection([&](Database& db) -> Result<void> {
            auto stmtR = db.prepare("DELETE FROM kg_edges WHERE id = ?");
            if (!stmtR) return stmtR.error();
            auto stmt = std::move(stmtR).value();
            auto br = stmt.bind(1, edgeId);
            if (!br) return br.error();
            return stmt.execute();
        });
    }

    // Embeddings (not needed for SimpleKGScorer MVP)
    Result<void> setNodeEmbedding(const KGNodeEmbedding&) override {
        return Error{ErrorCode::NotImplemented, "setNodeEmbedding not implemented"};
    }

    Result<std::optional<KGNodeEmbedding>> getNodeEmbedding(std::int64_t) override {
        return Error{ErrorCode::NotImplemented, "getNodeEmbedding not implemented"};
    }

    Result<void> deleteNodeEmbedding(std::int64_t) override {
        return Error{ErrorCode::NotImplemented, "deleteNodeEmbedding not implemented"};
    }

    // Document Entities
    Result<void> addDocEntities(const std::vector<DocEntity>& entities) override {
        if (entities.empty()) return Result<void>();
        return pool_->withConnection([&](Database& db) -> Result<void> {
            return db.transaction([&]() -> Result<void> {
                auto stmtR = db.prepare(
                    "INSERT INTO doc_entities (document_id, entity_text, node_id, start_offset, end_offset, confidence, extractor) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?)"
                );
                if (!stmtR) return stmtR.error();
                auto stmt = std::move(stmtR).value();

                for (const auto& de : entities) {
                    auto br = stmt.clearBindings();
                    if (!br) return br;
                    br = stmt.bind(1, de.documentId);
                    if (!br) return br;
                    br = stmt.bind(2, de.entityText);
                    if (!br) return br;
                    if (de.nodeId.has_value()) {
                        br = stmt.bind(3, de.nodeId.value());
                    } else {
                        br = stmt.bind(3, nullptr);
                    }
                    if (!br) return br;
                    if (de.startOffset.has_value()) {
                        br = stmt.bind(4, de.startOffset.value());
                    } else {
                        br = stmt.bind(4, nullptr);
                    }
                    if (!br) return br;
                    if (de.endOffset.has_value()) {
                        br = stmt.bind(5, de.endOffset.value());
                    } else {
                        br = stmt.bind(5, nullptr);
                    }
                    if (!br) return br;
                    if (de.confidence.has_value()) {
                        br = stmt.bind(6, static_cast<double>(de.confidence.value()));
                    } else {
                        br = stmt.bind(6, nullptr);
                    }
                    if (!br) return br;
                    if (de.extractor.has_value()) {
                        br = stmt.bind(7, de.extractor.value());
                    } else {
                        br = stmt.bind(7, nullptr);
                    }
                    if (!br) return br;

                    auto ex = stmt.execute();
                    if (!ex) return ex;
                    auto rr = stmt.reset();
                    if (!rr) return rr;
                }
                return Result<void>();
            });
        });
    }

    Result<std::optional<std::int64_t>> getDocumentIdByHash(std::string_view sha256) override {
        return pool_->withConnection([&](Database& db) -> Result<std::optional<std::int64_t>> {
            auto stmtR = db.prepare("SELECT id FROM documents WHERE sha256_hash = ? LIMIT 1");
            if (!stmtR) return stmtR.error();
            auto stmt = std::move(stmtR).value();
            auto br = stmt.bind(1, sha256);
            if (!br) return br.error();
            auto step = stmt.step();
            if (!step) return step.error();
            if (!step.value()) return std::optional<std::int64_t>{};
            return std::optional<std::int64_t>{stmt.getInt64(0)};
        });
    }

    Result<std::optional<std::int64_t>> getDocumentIdByPath(std::string_view file_path) override {
        return pool_->withConnection([&](Database& db) -> Result<std::optional<std::int64_t>> {
            auto stmtR = db.prepare("SELECT id FROM documents WHERE file_path = ? LIMIT 1");
            if (!stmtR) return stmtR.error();
            auto stmt = std::move(stmtR).value();
            auto br = stmt.bind(1, file_path);
            if (!br) return br.error();
            auto step = stmt.step();
            if (!step) return step.error();
            if (!step.value()) return std::optional<std::int64_t>{};
            return std::optional<std::int64_t>{stmt.getInt64(0)};
        });
    }

    Result<std::optional<std::int64_t>> getDocumentIdByName(std::string_view file_name) override {
        return pool_->withConnection([&](Database& db) -> Result<std::optional<std::int64_t>> {
            auto stmtR = db.prepare("SELECT id FROM documents WHERE file_name = ? LIMIT 1");
            if (!stmtR) return stmtR.error();
            auto stmt = std::move(stmtR).value();
            auto br = stmt.bind(1, file_name);
            if (!br) return br.error();
            auto step = stmt.step();
            if (!step) return step.error();
            if (!step.value()) return std::optional<std::int64_t>{};
            return std::optional<std::int64_t>{stmt.getInt64(0)};
        });
    }

    Result<std::optional<std::string>> getDocumentHashById(std::int64_t documentId) override {
        return pool_->withConnection([&](Database& db) -> Result<std::optional<std::string>> {
            auto stmtR = db.prepare("SELECT sha256_hash FROM documents WHERE id = ? LIMIT 1");
            if (!stmtR) return stmtR.error();
            auto stmt = std::move(stmtR).value();
            auto br = stmt.bind(1, documentId);
            if (!br) return br.error();
            auto step = stmt.step();
            if (!step) return step.error();
            if (!step.value()) return std::optional<std::string>{};
            return std::optional<std::string>{stmt.getString(0)};
        });
    }

    Result<std::vector<DocEntity>> getDocEntitiesForDocument(std::int64_t documentId,
                                                             std::size_t limit,
                                                             std::size_t offset) override {
        return pool_->withConnection([&](Database& db) -> Result<std::vector<DocEntity>> {
            auto stmtR = db.prepare(
                "SELECT id, document_id, entity_text, node_id, start_offset, end_offset, confidence, extractor "
                "FROM doc_entities WHERE document_id = ? LIMIT ? OFFSET ?"
            );
            if (!stmtR) return stmtR.error();
            auto stmt = std::move(stmtR).value();

            auto br = stmt.bind(1, documentId);
            if (!br) return br.error();
            br = stmt.bind(2, static_cast<int64_t>(limit));
            if (!br) return br.error();
            br = stmt.bind(3, static_cast<int64_t>(offset));
            if (!br) return br.error();

            std::vector<DocEntity> out;
            while (true) {
                auto step = stmt.step();
                if (!step) return step.error();
                if (!step.value()) break;
                DocEntity de;
                de.id = stmt.getInt64(0);
                de.documentId = stmt.getInt64(1);
                de.entityText = stmt.getString(2);
                if (!stmt.isNull(3)) de.nodeId = stmt.getInt64(3);
                if (!stmt.isNull(4)) de.startOffset = stmt.getInt64(4);
                if (!stmt.isNull(5)) de.endOffset = stmt.getInt64(5);
                if (!stmt.isNull(6)) de.confidence = static_cast<float>(stmt.getDouble(6));
                if (!stmt.isNull(7)) de.extractor = stmt.getString(7);
                out.push_back(std::move(de));
            }
            return out;
        });
    }

    Result<void> deleteDocEntitiesForDocument(std::int64_t documentId) override {
        return pool_->withConnection([&](Database& db) -> Result<void> {
            auto stmtR = db.prepare("DELETE FROM doc_entities WHERE document_id = ?");
            if (!stmtR) return stmtR.error();
            auto stmt = std::move(stmtR).value();
            auto br = stmt.bind(1, documentId);
            if (!br) return br.error();
            return stmt.execute();
        });
    }

    // Statistics (not needed for MVP)
    Result<std::optional<KGNodeStats>> getNodeStats(std::int64_t) override {
        return Error{ErrorCode::NotImplemented, "getNodeStats not implemented"};
    }

    Result<void> setNodeStats(const KGNodeStats&) override {
        return Error{ErrorCode::NotImplemented, "setNodeStats not implemented"};
    }

    Result<KGNodeStats> recomputeNodeStats(std::int64_t) override {
        return Error{ErrorCode::NotImplemented, "recomputeNodeStats not implemented"};
    }

    // Maintenance
    Result<void> optimize() override {
        auto res = pool_->withConnection([](Database& db) -> Result<void> {
            return db.execute("PRAGMA optimize");
        });
        if (!res) {
            spdlog::warn("KG store optimize failed: {}", res.error().message);
            return res.error();
        }
        return {};
    }

    void clearCaches() override {
        // no-op for now
    }

    Result<void> healthCheck() override {
        auto res = pool_->withConnection([](Database& db) -> Result<void> {
            return db.execute("PRAGMA integrity_check");
        });
        if (!res) {
            spdlog::warn("KG store healthCheck failed: {}", res.error().message);
            return res.error();
        }
        return {};
    }

private:
    KnowledgeGraphStoreConfig cfg_{};
    ConnectionPool* pool_{nullptr};                         // Non-owning
    std::unique_ptr<ConnectionPool> owned_pool_{nullptr};   // Owns pool when created from path
};

// Factory: SQLite store using a file path (owns its pool)
Result<std::unique_ptr<KnowledgeGraphStore>>
makeSqliteKnowledgeGraphStore(const std::string& dbPath,
                              const KnowledgeGraphStoreConfig& cfg) {
    auto s = SqliteKnowledgeGraphStore::createWithPath(dbPath, cfg);
    if (!s) return s.error();
    auto raw = std::move(s).value().release();
    return std::unique_ptr<KnowledgeGraphStore>(raw);
}

// Factory: SQLite store using an external ConnectionPool (non-owning)
Result<std::unique_ptr<KnowledgeGraphStore>>
makeSqliteKnowledgeGraphStore(ConnectionPool& pool,
                              const KnowledgeGraphStoreConfig& cfg) {
    auto store = std::unique_ptr<KnowledgeGraphStore>(
        new SqliteKnowledgeGraphStore(pool, cfg));
    return store;
}

} // namespace yams::metadata