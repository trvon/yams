#include <yams/core/types.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/database.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/migration.h>

#include <spdlog/spdlog.h>

#include <chrono>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

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
        // 1) Ensure DB exists and schema is migrated (in case the caller bypasses the CLI init
        // path)
        {
            Database db;
            auto rOpen = db.open(dbPath, ConnectionMode::Create);
            if (!rOpen)
                return rOpen.error();

            // Best-effort config
            if (cfg.enable_wal) {
                auto rWal = db.enableWAL();
                if (!rWal)
                    spdlog::warn("enableWAL failed during KG store init: {}", rWal.error().message);
            }
            auto rFK = db.execute("PRAGMA foreign_keys = ON");
            if (!rFK)
                spdlog::warn("Enabling foreign_keys failed during KG store init: {}",
                             rFK.error().message);

            // Run metadata migrations which include the KG schema (v7)
            MigrationManager mm(db);
            auto rInit = mm.initialize();
            if (!rInit)
                return rInit.error();
            auto migrations = YamsMetadataMigrations::getAllMigrations();
            for (const auto& m : migrations) {
                mm.registerMigration(m);
            }
            auto rMig = mm.migrate();
            if (!rMig)
                return rMig.error();
        }

        // 2) Create and initialize a connection pool for ongoing operations
        auto pool = std::make_unique<ConnectionPool>(dbPath, toPoolConfig(cfg));
        auto rInit = pool->initialize();
        if (!rInit)
            return rInit.error();

        // 3) Create store owning the pool
        auto store =
            std::unique_ptr<SqliteKnowledgeGraphStore>(new SqliteKnowledgeGraphStore(*pool, cfg));
        store->owned_pool_ = std::move(pool);
        return store;
    }

    // KnowledgeGraphStore API

    void setConfig(const KnowledgeGraphStoreConfig& cfg) override { cfg_ = cfg; }

    const KnowledgeGraphStoreConfig& getConfig() const override { return cfg_; }

    // Nodes
    Result<std::int64_t> upsertNode(const KGNode& node) override {
        // Perform an INSERT ... ON CONFLICT(node_key) DO UPDATE to ensure presence
        return pool_->withConnection([&](Database& db) -> Result<std::int64_t> {
            // Use a transaction to make the upsert + select atomic
            auto trx = db.transaction([&]() -> Result<void> {
                auto stmtR = db.prepare(
                    "INSERT INTO kg_nodes (node_key, label, type, created_time, updated_time, "
                    "properties) "
                    "VALUES (?, ?, ?, COALESCE(?, unixepoch()), COALESCE(?, unixepoch()), ?) "
                    "ON CONFLICT(node_key) DO UPDATE SET "
                    "  label = COALESCE(excluded.label, kg_nodes.label), "
                    "  type = COALESCE(excluded.type, kg_nodes.type), "
                    "  updated_time = COALESCE(excluded.updated_time, unixepoch()), "
                    "  properties = COALESCE(excluded.properties, kg_nodes.properties)");
                if (!stmtR)
                    return stmtR.error();
                auto stmt = std::move(stmtR).value();

                auto br = stmt.bind(1, node.nodeKey);
                if (!br)
                    return br.error();
                if (node.label.has_value())
                    br = stmt.bind(2, node.label.value());
                else
                    br = stmt.bind(2, nullptr);
                if (!br)
                    return br.error();
                if (node.type.has_value())
                    br = stmt.bind(3, node.type.value());
                else
                    br = stmt.bind(3, nullptr);
                if (!br)
                    return br.error();
                if (node.createdTime.has_value())
                    br = stmt.bind(4, node.createdTime.value());
                else
                    br = stmt.bind(4, nullptr);
                if (!br)
                    return br.error();
                if (node.updatedTime.has_value())
                    br = stmt.bind(5, node.updatedTime.value());
                else
                    br = stmt.bind(5, nullptr);
                if (!br)
                    return br.error();
                if (node.properties.has_value())
                    br = stmt.bind(6, node.properties.value());
                else
                    br = stmt.bind(6, nullptr);
                if (!br)
                    return br.error();

                auto er = stmt.execute();
                if (!er)
                    return er.error();
                return Result<void>();
            });
            if (!trx)
                return trx.error();

            // Retrieve id
            auto idStmtR = db.prepare("SELECT id FROM kg_nodes WHERE node_key = ? LIMIT 1");
            if (!idStmtR)
                return idStmtR.error();
            auto idStmt = std::move(idStmtR).value();
            auto br = idStmt.bind(1, node.nodeKey);
            if (!br)
                return br.error();
            auto step = idStmt.step();
            if (!step)
                return step.error();
            if (!step.value())
                return Error{ErrorCode::NotFound, "node not found after upsert"};
            return idStmt.getInt64(0);
        });
    }

    Result<std::vector<std::int64_t>> upsertNodes(const std::vector<KGNode>& nodes) override {
        std::vector<std::int64_t> ids;
        ids.reserve(nodes.size());
        for (const auto& n : nodes) {
            auto r = upsertNode(n);
            if (!r)
                return r.error();
            ids.push_back(r.value());
        }
        return ids;
    }

    Result<std::optional<KGNode>> getNodeById(std::int64_t nodeId) override {
        return pool_->withConnection([&](Database& db) -> Result<std::optional<KGNode>> {
            auto stmtR = db.prepare(
                "SELECT id, node_key, label, type, created_time, updated_time, properties "
                "FROM kg_nodes WHERE id = ? LIMIT 1");
            if (!stmtR)
                return stmtR.error();
            auto stmt = std::move(stmtR).value();
            auto br = stmt.bind(1, nodeId);
            if (!br)
                return br.error();
            auto step = stmt.step();
            if (!step)
                return step.error();
            if (!step.value())
                return std::optional<KGNode>{};
            KGNode n;
            n.id = stmt.getInt64(0);
            n.nodeKey = stmt.getString(1);
            if (!stmt.isNull(2))
                n.label = stmt.getString(2);
            if (!stmt.isNull(3))
                n.type = stmt.getString(3);
            if (!stmt.isNull(4))
                n.createdTime = stmt.getInt64(4);
            if (!stmt.isNull(5))
                n.updatedTime = stmt.getInt64(5);
            if (!stmt.isNull(6))
                n.properties = stmt.getString(6);
            return std::optional<KGNode>{std::move(n)};
        });
    }

    Result<std::optional<KGNode>> getNodeByKey(std::string_view nodeKey) override {
        return pool_->withConnection([&](Database& db) -> Result<std::optional<KGNode>> {
            auto stmtR = db.prepare(
                "SELECT id, node_key, label, type, created_time, updated_time, properties "
                "FROM kg_nodes WHERE node_key = ? LIMIT 1");
            if (!stmtR)
                return stmtR.error();
            auto stmt = std::move(stmtR).value();
            auto br = stmt.bind(1, nodeKey);
            if (!br)
                return br.error();
            auto step = stmt.step();
            if (!step)
                return step.error();
            if (!step.value())
                return std::optional<KGNode>{};
            KGNode n;
            n.id = stmt.getInt64(0);
            n.nodeKey = stmt.getString(1);
            if (!stmt.isNull(2))
                n.label = stmt.getString(2);
            if (!stmt.isNull(3))
                n.type = stmt.getString(3);
            if (!stmt.isNull(4))
                n.createdTime = stmt.getInt64(4);
            if (!stmt.isNull(5))
                n.updatedTime = stmt.getInt64(5);
            if (!stmt.isNull(6))
                n.properties = stmt.getString(6);
            return std::optional<KGNode>{std::move(n)};
        });
    }

    Result<std::vector<KGNode>> getNodesByIds(const std::vector<std::int64_t>& nodeIds) override {
        if (nodeIds.empty()) {
            return std::vector<KGNode>{};
        }
        return pool_->withConnection([&](Database& db) -> Result<std::vector<KGNode>> {
            // Build IN clause with placeholders: WHERE id IN (?, ?, ...)
            std::string placeholders;
            for (std::size_t i = 0; i < nodeIds.size(); ++i) {
                if (i > 0)
                    placeholders += ", ";
                placeholders += "?";
            }
            std::string sql =
                "SELECT id, node_key, label, type, created_time, updated_time, properties "
                "FROM kg_nodes WHERE id IN (" +
                placeholders + ")";

            auto stmtR = db.prepare(sql);
            if (!stmtR)
                return stmtR.error();
            auto stmt = std::move(stmtR).value();

            // Bind all node IDs
            for (std::size_t i = 0; i < nodeIds.size(); ++i) {
                auto br = stmt.bind(static_cast<int>(i + 1), nodeIds[i]);
                if (!br)
                    return br.error();
            }

            std::vector<KGNode> out;
            out.reserve(nodeIds.size());
            while (true) {
                auto step = stmt.step();
                if (!step)
                    return step.error();
                if (!step.value())
                    break;
                KGNode n;
                n.id = stmt.getInt64(0);
                n.nodeKey = stmt.getString(1);
                if (!stmt.isNull(2))
                    n.label = stmt.getString(2);
                if (!stmt.isNull(3))
                    n.type = stmt.getString(3);
                if (!stmt.isNull(4))
                    n.createdTime = stmt.getInt64(4);
                if (!stmt.isNull(5))
                    n.updatedTime = stmt.getInt64(5);
                if (!stmt.isNull(6))
                    n.properties = stmt.getString(6);
                out.push_back(std::move(n));
            }
            return out;
        });
    }

    Result<std::vector<KGNode>> findNodesByType(std::string_view type, std::size_t limit,
                                                std::size_t offset) override {
        return pool_->withConnection([&](Database& db) -> Result<std::vector<KGNode>> {
            auto stmtR = db.prepare(
                "SELECT id, node_key, label, type, created_time, updated_time, properties "
                "FROM kg_nodes WHERE type = ? LIMIT ? OFFSET ?");
            if (!stmtR)
                return stmtR.error();
            auto stmt = std::move(stmtR).value();
            auto br = stmt.bind(1, type);
            if (!br)
                return br.error();
            br = stmt.bind(2, static_cast<int64_t>(limit));
            if (!br)
                return br.error();
            br = stmt.bind(3, static_cast<int64_t>(offset));
            if (!br)
                return br.error();
            std::vector<KGNode> out;
            while (true) {
                auto step = stmt.step();
                if (!step)
                    return step.error();
                if (!step.value())
                    break;
                KGNode n;
                n.id = stmt.getInt64(0);
                n.nodeKey = stmt.getString(1);
                if (!stmt.isNull(2))
                    n.label = stmt.getString(2);
                if (!stmt.isNull(3))
                    n.type = stmt.getString(3);
                if (!stmt.isNull(4))
                    n.createdTime = stmt.getInt64(4);
                if (!stmt.isNull(5))
                    n.updatedTime = stmt.getInt64(5);
                if (!stmt.isNull(6))
                    n.properties = stmt.getString(6);
                out.push_back(std::move(n));
            }
            return out;
        });
    }

    Result<std::size_t> countNodesByType(std::string_view type) override {
        return pool_->withConnection([&](Database& db) -> Result<std::size_t> {
            auto stmtR = db.prepare("SELECT COUNT(1) FROM kg_nodes WHERE type = ?");
            if (!stmtR)
                return stmtR.error();
            auto stmt = std::move(stmtR).value();
            auto br = stmt.bind(1, type);
            if (!br)
                return br.error();
            auto step = stmt.step();
            if (!step)
                return step.error();
            if (!step.value())
                return static_cast<std::size_t>(0);
            return static_cast<std::size_t>(stmt.getInt64(0));
        });
    }

    Result<void> deleteNodeById(std::int64_t nodeId) override {
        return pool_->withConnection([&](Database& db) -> Result<void> {
            auto stmtR = db.prepare("DELETE FROM kg_nodes WHERE id = ?");
            if (!stmtR)
                return stmtR.error();
            auto stmt = std::move(stmtR).value();
            auto br = stmt.bind(1, nodeId);
            if (!br)
                return br.error();
            return stmt.execute();
        });
    }

    // Aliases
    Result<std::int64_t> addAlias(const KGAlias& alias) override {
        return pool_->withConnection([&](Database& db) -> Result<std::int64_t> {
            auto stmtR = db.prepare(
                "INSERT INTO kg_aliases (node_id, alias, source, confidence) VALUES (?, ?, ?, ?)");
            if (!stmtR)
                return stmtR.error();
            auto stmt = std::move(stmtR).value();

            // Bind parameters
            auto br = stmt.bind(1, static_cast<int64_t>(alias.nodeId));
            if (!br)
                return br.error();
            br = stmt.bind(2, alias.alias);
            if (!br)
                return br.error();
            if (alias.source.has_value()) {
                br = stmt.bind(3, alias.source.value());
            } else {
                br = stmt.bind(3, nullptr);
            }
            if (!br)
                return br.error();
            br = stmt.bind(4, static_cast<double>(alias.confidence));
            if (!br)
                return br.error();

            auto er = stmt.execute();
            if (!er)
                return er.error();

            return db.lastInsertRowId();
        });
    }

    Result<void> addAliases(const std::vector<KGAlias>& aliases) override {
        if (aliases.empty())
            return Result<void>();
        return pool_->withConnection([&](Database& db) -> Result<void> {
            return db.transaction([&]() -> Result<void> {
                auto stmtR = db.prepare("INSERT INTO kg_aliases (node_id, alias, source, "
                                        "confidence) VALUES (?, ?, ?, ?)");
                if (!stmtR)
                    return stmtR.error();
                auto stmt = std::move(stmtR).value();

                for (const auto& a : aliases) {
                    auto br = stmt.clearBindings();
                    if (!br)
                        return br.error();
                    br = stmt.bind(1, static_cast<int64_t>(a.nodeId));
                    if (!br)
                        return br.error();
                    br = stmt.bind(2, a.alias);
                    if (!br)
                        return br.error();
                    if (a.source.has_value()) {
                        br = stmt.bind(3, a.source.value());
                    } else {
                        br = stmt.bind(3, nullptr);
                    }
                    if (!br)
                        return br.error();
                    br = stmt.bind(4, static_cast<double>(a.confidence));
                    if (!br)
                        return br;
                    auto er = stmt.execute();
                    if (!er)
                        return er.error();
                    auto rr = stmt.reset();
                    if (!rr)
                        return rr;
                }
                return Result<void>();
            });
        });
    }

    Result<std::vector<AliasResolution>> resolveAliasExact(std::string_view alias,
                                                           std::size_t limit) override {
        return pool_->withConnection([&](Database& db) -> Result<std::vector<AliasResolution>> {
            auto stmtR = db.prepare("SELECT node_id FROM kg_aliases WHERE alias = ? LIMIT ?");
            if (!stmtR)
                return stmtR.error();
            auto stmt = std::move(stmtR).value();

            auto br = stmt.bind(1, alias);
            if (!br)
                return br.error();
            br = stmt.bind(2, static_cast<int64_t>(limit));
            if (!br)
                return br.error();

            std::vector<AliasResolution> out;
            while (true) {
                auto step = stmt.step();
                if (!step)
                    return step.error();
                if (!step.value())
                    break;
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
            if (!existsR)
                return existsR.error();
            if (!existsR.value()) {
                return resolveAliasExact(aliasQuery, limit);
            }

            auto stmtR = db.prepare("SELECT a.node_id, 1.0 AS score "
                                    "FROM kg_aliases_fts f "
                                    "JOIN kg_aliases a ON a.id = f.rowid "
                                    "WHERE kg_aliases_fts MATCH ? "
                                    "LIMIT ?");
            if (!stmtR)
                return stmtR.error();
            auto stmt = std::move(stmtR).value();

            auto br = stmt.bind(1, aliasQuery);
            if (!br)
                return br.error();
            br = stmt.bind(2, static_cast<int64_t>(limit));
            if (!br)
                return br.error();

            std::vector<AliasResolution> out;
            while (true) {
                auto step = stmt.step();
                if (!step)
                    return step.error();
                if (!step.value())
                    break;
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
            if (!stmtR)
                return stmtR.error();
            auto stmt = std::move(stmtR).value();
            auto br = stmt.bind(1, aliasId);
            if (!br)
                return br.error();
            return stmt.execute();
        });
    }

    Result<void> removeAliasesForNode(std::int64_t nodeId) override {
        return pool_->withConnection([&](Database& db) -> Result<void> {
            auto stmtR = db.prepare("DELETE FROM kg_aliases WHERE node_id = ?");
            if (!stmtR)
                return stmtR.error();
            auto stmt = std::move(stmtR).value();
            auto br = stmt.bind(1, nodeId);
            if (!br)
                return br.error();
            return stmt.execute();
        });
    }

    // Edges (minimal neighbor support)
    Result<std::int64_t> addEdge(const KGEdge& edge) override {
        return pool_->withConnection([&](Database& db) -> Result<std::int64_t> {
            auto stmtR = db.prepare("INSERT INTO kg_edges (src_node_id, dst_node_id, relation, "
                                    "weight, created_time, properties) "
                                    "VALUES (?, ?, ?, ?, ?, ?)");
            if (!stmtR)
                return stmtR.error();
            auto stmt = std::move(stmtR).value();

            auto br = stmt.bind(1, edge.srcNodeId);
            if (!br)
                return br.error();
            br = stmt.bind(2, edge.dstNodeId);
            if (!br)
                return br.error();
            br = stmt.bind(3, edge.relation);
            if (!br)
                return br.error();
            br = stmt.bind(4, static_cast<double>(edge.weight));
            if (!br)
                return br.error();
            if (edge.createdTime.has_value()) {
                br = stmt.bind(5, edge.createdTime.value());
            } else {
                br = stmt.bind(5, nullptr);
            }
            if (!br)
                return br.error();
            if (edge.properties.has_value()) {
                br = stmt.bind(6, edge.properties.value());
            } else {
                br = stmt.bind(6, nullptr);
            }
            if (!br)
                return br.error();

            auto ex = stmt.execute();
            if (!ex)
                return ex.error();
            return db.lastInsertRowId();
        });
    }

    Result<void> addEdges(const std::vector<KGEdge>& edges) override {
        if (edges.empty())
            return Result<void>();
        return pool_->withConnection([&](Database& db) -> Result<void> {
            return db.transaction([&]() -> Result<void> {
                auto stmtR = db.prepare("INSERT INTO kg_edges (src_node_id, dst_node_id, relation, "
                                        "weight, created_time, properties) "
                                        "VALUES (?, ?, ?, ?, ?, ?)");
                if (!stmtR)
                    return stmtR.error();
                auto stmt = std::move(stmtR).value();

                for (const auto& e : edges) {
                    auto br = stmt.clearBindings();
                    if (!br)
                        return br.error();
                    br = stmt.bind(1, e.srcNodeId);
                    if (!br)
                        return br.error();
                    br = stmt.bind(2, e.dstNodeId);
                    if (!br)
                        return br.error();
                    br = stmt.bind(3, e.relation);
                    if (!br)
                        return br.error();
                    br = stmt.bind(4, static_cast<double>(e.weight));
                    if (!br)
                        return br.error();
                    if (e.createdTime.has_value()) {
                        br = stmt.bind(5, e.createdTime.value());
                    } else {
                        br = stmt.bind(5, nullptr);
                    }
                    if (!br)
                        return br;
                    if (e.properties.has_value()) {
                        br = stmt.bind(6, e.properties.value());
                    } else {
                        br = stmt.bind(6, nullptr);
                    }
                    if (!br)
                        return br;

                    auto ex = stmt.execute();
                    if (!ex)
                        return ex.error();
                    auto rr = stmt.reset();
                    if (!rr)
                        return rr;
                }
                return Result<void>();
            });
        });
    }

    Result<void> addEdgesUnique(const std::vector<KGEdge>& edges) override {
        if (edges.empty())
            return Result<void>();
        return pool_->withConnection([&](Database& db) -> Result<void> {
            return db.transaction([&]() -> Result<void> {
                // Use INSERT ... SELECT ... WHERE NOT EXISTS to avoid duplicates without requiring
                // a unique index
                auto stmtR = db.prepare("INSERT INTO kg_edges (src_node_id, dst_node_id, relation, "
                                        "weight, created_time, properties) "
                                        "SELECT ?, ?, ?, ?, ?, ? WHERE NOT EXISTS ("
                                        "  SELECT 1 FROM kg_edges WHERE src_node_id = ? AND "
                                        "dst_node_id = ? AND relation = ?"
                                        ")");
                if (!stmtR)
                    return stmtR.error();
                auto stmt = std::move(stmtR).value();

                for (const auto& e : edges) {
                    auto br = stmt.clearBindings();
                    if (!br)
                        return br.error();
                    // Insert value params
                    br = stmt.bind(1, e.srcNodeId);
                    if (!br)
                        return br.error();
                    br = stmt.bind(2, e.dstNodeId);
                    if (!br)
                        return br.error();
                    br = stmt.bind(3, e.relation);
                    if (!br)
                        return br.error();
                    br = stmt.bind(4, static_cast<double>(e.weight));
                    if (!br)
                        return br.error();
                    if (e.createdTime.has_value())
                        br = stmt.bind(5, e.createdTime.value());
                    else
                        br = stmt.bind(5, nullptr);
                    if (!br)
                        return br.error();
                    if (e.properties.has_value())
                        br = stmt.bind(6, e.properties.value());
                    else
                        br = stmt.bind(6, nullptr);
                    if (!br)
                        return br.error();
                    // WHERE NOT EXISTS params
                    br = stmt.bind(7, e.srcNodeId);
                    if (!br)
                        return br.error();
                    br = stmt.bind(8, e.dstNodeId);
                    if (!br)
                        return br.error();
                    br = stmt.bind(9, e.relation);
                    if (!br)
                        return br.error();
                    auto ex = stmt.execute();
                    if (!ex)
                        return ex.error();
                    auto rr = stmt.reset();
                    if (!rr)
                        return rr;
                }
                return Result<void>();
            });
        });
    }

    Result<std::vector<KGEdge>> getEdgesFrom(std::int64_t srcNodeId,
                                             std::optional<std::string_view> relation,
                                             std::size_t limit, std::size_t offset) override {
        return pool_->withConnection([&](Database& db) -> Result<std::vector<KGEdge>> {
            std::string sql =
                "SELECT id, src_node_id, dst_node_id, relation, weight, created_time, properties "
                "FROM kg_edges WHERE src_node_id = ?";
            if (relation.has_value()) {
                sql += " AND relation = ?";
            }
            sql += " LIMIT ? OFFSET ?";

            auto stmtR = db.prepare(sql);
            if (!stmtR)
                return stmtR.error();
            auto stmt = std::move(stmtR).value();

            int idx = 1;
            auto br = stmt.bind(idx++, srcNodeId);
            if (!br)
                return br.error();
            if (relation.has_value()) {
                br = stmt.bind(idx++, relation.value());
                if (!br)
                    return br.error();
            }
            br = stmt.bind(idx++, static_cast<int64_t>(limit));
            if (!br)
                return br.error();
            br = stmt.bind(idx++, static_cast<int64_t>(offset));
            if (!br)
                return br.error();

            std::vector<KGEdge> out;
            while (true) {
                auto step = stmt.step();
                if (!step)
                    return step.error();
                if (!step.value())
                    break;
                KGEdge e;
                e.id = stmt.getInt64(0);
                e.srcNodeId = stmt.getInt64(1);
                e.dstNodeId = stmt.getInt64(2);
                e.relation = stmt.getString(3);
                e.weight = static_cast<float>(stmt.getDouble(4));
                if (!stmt.isNull(5))
                    e.createdTime = stmt.getInt64(5);
                if (!stmt.isNull(6))
                    e.properties = stmt.getString(6);
                out.push_back(std::move(e));
            }
            return out;
        });
    }

    Result<std::vector<KGEdge>> getEdgesTo(std::int64_t dstNodeId,
                                           std::optional<std::string_view> relation,
                                           std::size_t limit, std::size_t offset) override {
        return pool_->withConnection([&](Database& db) -> Result<std::vector<KGEdge>> {
            std::string sql =
                "SELECT id, src_node_id, dst_node_id, relation, weight, created_time, properties "
                "FROM kg_edges WHERE dst_node_id = ?";
            if (relation.has_value()) {
                sql += " AND relation = ?";
            }
            sql += " LIMIT ? OFFSET ?";

            auto stmtR = db.prepare(sql);
            if (!stmtR)
                return stmtR.error();
            auto stmt = std::move(stmtR).value();

            int idx = 1;
            auto br = stmt.bind(idx++, dstNodeId);
            if (!br)
                return br.error();
            if (relation.has_value()) {
                br = stmt.bind(idx++, relation.value());
                if (!br)
                    return br.error();
            }
            br = stmt.bind(idx++, static_cast<int64_t>(limit));
            if (!br)
                return br.error();
            br = stmt.bind(idx++, static_cast<int64_t>(offset));
            if (!br)
                return br.error();

            std::vector<KGEdge> out;
            while (true) {
                auto step = stmt.step();
                if (!step)
                    return step.error();
                if (!step.value())
                    break;
                KGEdge e;
                e.id = stmt.getInt64(0);
                e.srcNodeId = stmt.getInt64(1);
                e.dstNodeId = stmt.getInt64(2);
                e.relation = stmt.getString(3);
                e.weight = static_cast<float>(stmt.getDouble(4));
                if (!stmt.isNull(5))
                    e.createdTime = stmt.getInt64(5);
                if (!stmt.isNull(6))
                    e.properties = stmt.getString(6);
                out.push_back(std::move(e));
            }
            return out;
        });
    }

    Result<std::unordered_map<std::int64_t, std::vector<KGEdge>>>
    getEdgesToBatch(const std::vector<std::int64_t>& dstNodeIds,
                    std::optional<std::string_view> relation, std::size_t limitPerNode) override {
        using ResultMap = std::unordered_map<std::int64_t, std::vector<KGEdge>>;

        if (dstNodeIds.empty()) {
            return ResultMap{};
        }

        return pool_->withConnection([&](Database& db) -> Result<ResultMap> {
            // Build SQL with IN clause for batch lookup
            // Using window function to limit results per destination node
            std::string placeholders;
            for (size_t i = 0; i < dstNodeIds.size(); ++i) {
                if (i > 0)
                    placeholders += ",";
                placeholders += "?";
            }

            std::string sql =
                "SELECT id, src_node_id, dst_node_id, relation, weight, created_time, properties "
                "FROM ("
                "  SELECT *, ROW_NUMBER() OVER (PARTITION BY dst_node_id ORDER BY id) as rn "
                "  FROM kg_edges "
                "  WHERE dst_node_id IN (" +
                placeholders + ")";
            if (relation.has_value()) {
                sql += " AND relation = ?";
            }
            sql += ") WHERE rn <= ?";

            auto stmtR = db.prepare(sql);
            if (!stmtR)
                return stmtR.error();
            auto stmt = std::move(stmtR).value();

            int idx = 1;
            // Bind all destination node IDs
            for (const auto& nodeId : dstNodeIds) {
                auto br = stmt.bind(idx++, nodeId);
                if (!br)
                    return br.error();
            }
            // Bind relation filter if provided
            if (relation.has_value()) {
                auto br = stmt.bind(idx++, relation.value());
                if (!br)
                    return br.error();
            }
            // Bind limit per node
            auto br = stmt.bind(idx++, static_cast<int64_t>(limitPerNode));
            if (!br)
                return br.error();

            ResultMap out;
            out.reserve(dstNodeIds.size());

            while (true) {
                auto step = stmt.step();
                if (!step)
                    return step.error();
                if (!step.value())
                    break;
                KGEdge e;
                e.id = stmt.getInt64(0);
                e.srcNodeId = stmt.getInt64(1);
                e.dstNodeId = stmt.getInt64(2);
                e.relation = stmt.getString(3);
                e.weight = static_cast<float>(stmt.getDouble(4));
                if (!stmt.isNull(5))
                    e.createdTime = stmt.getInt64(5);
                if (!stmt.isNull(6))
                    e.properties = stmt.getString(6);
                out[e.dstNodeId].push_back(std::move(e));
            }
            return out;
        });
    }

    Result<std::vector<KGEdge>> getEdgesBidirectional(std::int64_t nodeId,
                                                      std::optional<std::string_view> relation,
                                                      std::size_t limit) override {
        return pool_->withConnection([&](Database& db) -> Result<std::vector<KGEdge>> {
            // Single query for both incoming and outgoing edges using UNION
            std::string sql =
                "SELECT id, src_node_id, dst_node_id, relation, weight, created_time, properties "
                "FROM kg_edges WHERE src_node_id = ?";
            if (relation.has_value()) {
                sql += " AND relation = ?";
            }
            sql +=
                " UNION ALL "
                "SELECT id, src_node_id, dst_node_id, relation, weight, created_time, properties "
                "FROM kg_edges WHERE dst_node_id = ?";
            if (relation.has_value()) {
                sql += " AND relation = ?";
            }
            sql += " LIMIT ?";

            auto stmtR = db.prepare(sql);
            if (!stmtR)
                return stmtR.error();
            auto stmt = std::move(stmtR).value();

            int idx = 1;
            // Bind for first SELECT (src_node_id = ?)
            auto br = stmt.bind(idx++, nodeId);
            if (!br)
                return br.error();
            if (relation.has_value()) {
                br = stmt.bind(idx++, relation.value());
                if (!br)
                    return br.error();
            }
            // Bind for second SELECT (dst_node_id = ?)
            br = stmt.bind(idx++, nodeId);
            if (!br)
                return br.error();
            if (relation.has_value()) {
                br = stmt.bind(idx++, relation.value());
                if (!br)
                    return br.error();
            }
            br = stmt.bind(idx++, static_cast<int64_t>(limit));
            if (!br)
                return br.error();

            std::vector<KGEdge> out;
            while (true) {
                auto step = stmt.step();
                if (!step)
                    return step.error();
                if (!step.value())
                    break;
                KGEdge e;
                e.id = stmt.getInt64(0);
                e.srcNodeId = stmt.getInt64(1);
                e.dstNodeId = stmt.getInt64(2);
                e.relation = stmt.getString(3);
                e.weight = static_cast<float>(stmt.getDouble(4));
                if (!stmt.isNull(5))
                    e.createdTime = stmt.getInt64(5);
                if (!stmt.isNull(6))
                    e.properties = stmt.getString(6);
                out.push_back(std::move(e));
            }
            return out;
        });
    }

    Result<std::vector<std::int64_t>> neighbors(std::int64_t nodeId,
                                                std::size_t maxNeighbors) override {
        return pool_->withConnection([&](Database& db) -> Result<std::vector<std::int64_t>> {
            auto stmtR =
                db.prepare("SELECT dst_node_id FROM kg_edges WHERE src_node_id = ? LIMIT ?");
            if (!stmtR)
                return stmtR.error();
            auto stmt = std::move(stmtR).value();

            auto br = stmt.bind(1, nodeId);
            if (!br)
                return br.error();
            br = stmt.bind(2, static_cast<int64_t>(maxNeighbors));
            if (!br)
                return br.error();

            std::vector<std::int64_t> out;
            while (true) {
                auto step = stmt.step();
                if (!step)
                    return step.error();
                if (!step.value())
                    break;
                out.push_back(stmt.getInt64(0));
            }
            return out;
        });
    }

    Result<void> removeEdgeById(std::int64_t edgeId) override {
        return pool_->withConnection([&](Database& db) -> Result<void> {
            auto stmtR = db.prepare("DELETE FROM kg_edges WHERE id = ?");
            if (!stmtR)
                return stmtR.error();
            auto stmt = std::move(stmtR).value();
            auto br = stmt.bind(1, edgeId);
            if (!br)
                return br.error();
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
        if (entities.empty())
            return Result<void>();
        return pool_->withConnection([&](Database& db) -> Result<void> {
            return db.transaction([&]() -> Result<void> {
                auto stmtR = db.prepare("INSERT INTO kg_doc_entities (document_id, entity_text, "
                                        "node_id, start_offset, end_offset, confidence, extractor) "
                                        "VALUES (?, ?, ?, ?, ?, ?, ?)");
                if (!stmtR)
                    return stmtR.error();
                auto stmt = std::move(stmtR).value();

                for (const auto& de : entities) {
                    auto br = stmt.clearBindings();
                    if (!br)
                        return br;
                    br = stmt.bind(1, de.documentId);
                    if (!br)
                        return br;
                    br = stmt.bind(2, de.entityText);
                    if (!br)
                        return br;
                    if (de.nodeId.has_value()) {
                        br = stmt.bind(3, de.nodeId.value());
                    } else {
                        br = stmt.bind(3, nullptr);
                    }
                    if (!br)
                        return br;
                    if (de.startOffset.has_value()) {
                        br = stmt.bind(4, de.startOffset.value());
                    } else {
                        br = stmt.bind(4, nullptr);
                    }
                    if (!br)
                        return br;
                    if (de.endOffset.has_value()) {
                        br = stmt.bind(5, de.endOffset.value());
                    } else {
                        br = stmt.bind(5, nullptr);
                    }
                    if (!br)
                        return br;
                    if (de.confidence.has_value()) {
                        br = stmt.bind(6, static_cast<double>(de.confidence.value()));
                    } else {
                        br = stmt.bind(6, nullptr);
                    }
                    if (!br)
                        return br;
                    if (de.extractor.has_value()) {
                        br = stmt.bind(7, de.extractor.value());
                    } else {
                        br = stmt.bind(7, nullptr);
                    }
                    if (!br)
                        return br;

                    auto ex = stmt.execute();
                    if (!ex)
                        return ex;
                    auto rr = stmt.reset();
                    if (!rr)
                        return rr;
                }
                return Result<void>();
            });
        });
    }

    Result<std::optional<std::int64_t>> getDocumentIdByHash(std::string_view sha256) override {
        return pool_->withConnection([&](Database& db) -> Result<std::optional<std::int64_t>> {
            auto stmtR = db.prepare("SELECT id FROM documents WHERE sha256_hash = ? LIMIT 1");
            if (!stmtR)
                return stmtR.error();
            auto stmt = std::move(stmtR).value();
            auto br = stmt.bind(1, sha256);
            if (!br)
                return br.error();
            auto step = stmt.step();
            if (!step)
                return step.error();
            if (!step.value())
                return std::optional<std::int64_t>{};
            return std::optional<std::int64_t>{stmt.getInt64(0)};
        });
    }

    Result<std::optional<std::int64_t>> getDocumentIdByPath(std::string_view file_path) override {
        return pool_->withConnection([&](Database& db) -> Result<std::optional<std::int64_t>> {
            auto stmtR = db.prepare("SELECT id FROM documents WHERE file_path = ? LIMIT 1");
            if (!stmtR)
                return stmtR.error();
            auto stmt = std::move(stmtR).value();
            auto br = stmt.bind(1, file_path);
            if (!br)
                return br.error();
            auto step = stmt.step();
            if (!step)
                return step.error();
            if (!step.value())
                return std::optional<std::int64_t>{};
            return std::optional<std::int64_t>{stmt.getInt64(0)};
        });
    }

    Result<std::optional<std::int64_t>> getDocumentIdByName(std::string_view file_name) override {
        return pool_->withConnection([&](Database& db) -> Result<std::optional<std::int64_t>> {
            auto stmtR = db.prepare("SELECT id FROM documents WHERE file_name = ? LIMIT 1");
            if (!stmtR)
                return stmtR.error();
            auto stmt = std::move(stmtR).value();
            auto br = stmt.bind(1, file_name);
            if (!br)
                return br.error();
            auto step = stmt.step();
            if (!step)
                return step.error();
            if (!step.value())
                return std::optional<std::int64_t>{};
            return std::optional<std::int64_t>{stmt.getInt64(0)};
        });
    }

    Result<std::optional<std::string>> getDocumentHashById(std::int64_t documentId) override {
        return pool_->withConnection([&](Database& db) -> Result<std::optional<std::string>> {
            auto stmtR = db.prepare("SELECT sha256_hash FROM documents WHERE id = ? LIMIT 1");
            if (!stmtR)
                return stmtR.error();
            auto stmt = std::move(stmtR).value();
            auto br = stmt.bind(1, documentId);
            if (!br)
                return br.error();
            auto step = stmt.step();
            if (!step)
                return step.error();
            if (!step.value())
                return std::optional<std::string>{};
            return std::optional<std::string>{stmt.getString(0)};
        });
    }

    Result<std::vector<DocEntity>> getDocEntitiesForDocument(std::int64_t documentId,
                                                             std::size_t limit,
                                                             std::size_t offset) override {
        return pool_->withConnection([&](Database& db) -> Result<std::vector<DocEntity>> {
            auto stmtR = db.prepare("SELECT id, document_id, entity_text, node_id, start_offset, "
                                    "end_offset, confidence, extractor "
                                    "FROM kg_doc_entities WHERE document_id = ? LIMIT ? OFFSET ?");
            if (!stmtR)
                return stmtR.error();
            auto stmt = std::move(stmtR).value();

            auto br = stmt.bind(1, documentId);
            if (!br)
                return br.error();
            br = stmt.bind(2, static_cast<int64_t>(limit));
            if (!br)
                return br.error();
            br = stmt.bind(3, static_cast<int64_t>(offset));
            if (!br)
                return br.error();

            std::vector<DocEntity> out;
            while (true) {
                auto step = stmt.step();
                if (!step)
                    return step.error();
                if (!step.value())
                    break;
                DocEntity de;
                de.id = stmt.getInt64(0);
                de.documentId = stmt.getInt64(1);
                de.entityText = stmt.getString(2);
                if (!stmt.isNull(3))
                    de.nodeId = stmt.getInt64(3);
                if (!stmt.isNull(4))
                    de.startOffset = stmt.getInt64(4);
                if (!stmt.isNull(5))
                    de.endOffset = stmt.getInt64(5);
                if (!stmt.isNull(6))
                    de.confidence = static_cast<float>(stmt.getDouble(6));
                if (!stmt.isNull(7))
                    de.extractor = stmt.getString(7);
                out.push_back(std::move(de));
            }
            return out;
        });
    }

    Result<void> deleteDocEntitiesForDocument(std::int64_t documentId) override {
        return pool_->withConnection([&](Database& db) -> Result<void> {
            auto stmtR = db.prepare("DELETE FROM kg_doc_entities WHERE document_id = ?");
            if (!stmtR)
                return stmtR.error();
            auto stmt = std::move(stmtR).value();
            auto br = stmt.bind(1, documentId);
            if (!br)
                return br.error();
            return stmt.execute();
        });
    }

    // Symbol Extraction State
    Result<std::optional<SymbolExtractionState>>
    getSymbolExtractionState(std::string_view documentHash) override {
        return pool_->withConnection(
            [&](Database& db) -> Result<std::optional<SymbolExtractionState>> {
                auto stmtR = db.prepare(R"(
                SELECT ses.document_id, ses.extractor_id, ses.extractor_config_hash,
                       ses.extracted_at, ses.status, ses.entity_count, ses.error_message
                FROM document_symbol_extraction_state ses
                JOIN documents d ON d.id = ses.document_id
                WHERE d.sha256_hash = ?
            )");
                if (!stmtR)
                    return stmtR.error();
                auto stmt = std::move(stmtR).value();
                auto br = stmt.bind(1, documentHash);
                if (!br)
                    return br.error();
                auto stepR = stmt.step();
                if (!stepR)
                    return stepR.error();
                if (!stepR.value())
                    return std::optional<SymbolExtractionState>{};

                SymbolExtractionState state;
                state.documentId = stmt.getInt64(0);
                state.extractorId = stmt.getString(1);
                if (!stmt.isNull(2))
                    state.extractorConfigHash = stmt.getString(2);
                state.extractedAt = stmt.getInt64(3);
                state.status = stmt.getString(4);
                state.entityCount = stmt.getInt64(5);
                if (!stmt.isNull(6))
                    state.errorMessage = stmt.getString(6);
                return std::optional<SymbolExtractionState>{std::move(state)};
            });
    }

    Result<void> upsertSymbolExtractionState(std::string_view documentHash,
                                             const SymbolExtractionState& state) override {
        return pool_->withConnection([&](Database& db) -> Result<void> {
            // Resolve document_id from hash
            auto docIdR = db.prepare("SELECT id FROM documents WHERE sha256_hash = ?");
            if (!docIdR)
                return docIdR.error();
            auto docIdStmt = std::move(docIdR).value();
            auto br = docIdStmt.bind(1, documentHash);
            if (!br)
                return br.error();
            auto stepR = docIdStmt.step();
            if (!stepR)
                return stepR.error();
            if (!stepR.value()) {
                return Error{ErrorCode::NotFound,
                             "Document not found for hash: " + std::string(documentHash)};
            }
            std::int64_t documentId = docIdStmt.getInt64(0);

            // Upsert the extraction state
            auto stmtR = db.prepare(R"(
                INSERT INTO document_symbol_extraction_state
                    (document_id, extractor_id, extractor_config_hash, extracted_at, status, entity_count, error_message)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(document_id) DO UPDATE SET
                    extractor_id = excluded.extractor_id,
                    extractor_config_hash = excluded.extractor_config_hash,
                    extracted_at = excluded.extracted_at,
                    status = excluded.status,
                    entity_count = excluded.entity_count,
                    error_message = excluded.error_message
            )");
            if (!stmtR)
                return stmtR.error();
            auto stmt = std::move(stmtR).value();
            auto bindR = stmt.bindAll(
                documentId, state.extractorId,
                state.extractorConfigHash.value_or(std::string{}), // empty string if nullopt
                state.extractedAt, state.status, state.entityCount,
                state.errorMessage.value_or(std::string{}));
            if (!bindR)
                return bindR.error();
            return stmt.execute();
        });
    }

    // Document/File Cleanup
    Result<std::int64_t> deleteNodesForDocumentHash(std::string_view documentHash) override {
        return pool_->withConnection([&](Database& db) -> Result<std::int64_t> {
            std::int64_t totalDeleted = 0;

            // Delete the doc:<hash> node
            std::string docNodeKey = "doc:" + std::string(documentHash);
            auto docStmtR = db.prepare("DELETE FROM kg_nodes WHERE node_key = ?");
            if (!docStmtR)
                return docStmtR.error();
            auto docStmt = std::move(docStmtR).value();
            auto br = docStmt.bind(1, docNodeKey);
            if (!br)
                return br.error();
            auto execR = docStmt.execute();
            if (!execR)
                return execR.error();
            totalDeleted += db.changes();

            // Delete symbol nodes that have this document_hash in their properties
            auto symStmtR = db.prepare(
                "DELETE FROM kg_nodes WHERE json_extract(properties, '$.document_hash') = ?");
            if (!symStmtR)
                return symStmtR.error();
            auto symStmt = std::move(symStmtR).value();
            br = symStmt.bind(1, documentHash);
            if (!br)
                return br.error();
            execR = symStmt.execute();
            if (!execR)
                return execR.error();
            totalDeleted += db.changes();

            spdlog::debug("deleteNodesForDocumentHash: deleted {} nodes for hash {}", totalDeleted,
                          documentHash);
            return totalDeleted;
        });
    }

    Result<std::int64_t> deleteEdgesForSourceFile(std::string_view filePath) override {
        return pool_->withConnection([&](Database& db) -> Result<std::int64_t> {
            auto stmtR = db.prepare(
                "DELETE FROM kg_edges WHERE json_extract(properties, '$.source_file') = ?");
            if (!stmtR)
                return stmtR.error();
            auto stmt = std::move(stmtR).value();
            auto br = stmt.bind(1, filePath);
            if (!br)
                return br.error();
            auto execR = stmt.execute();
            if (!execR)
                return execR.error();
            auto deleted = db.changes();
            spdlog::debug("deleteEdgesForSourceFile: deleted {} edges for path {}", deleted,
                          filePath);
            return deleted;
        });
    }

    Result<std::vector<KGNode>> findIsolatedNodes(std::string_view nodeType,
                                                  std::string_view relation,
                                                  std::size_t limit) override {
        return pool_->withConnection([&](Database& db) -> Result<std::vector<KGNode>> {
            auto stmtR = db.prepare(R"(
                SELECT n.id, n.node_key, n.label, n.type, n.created_time, n.updated_time, n.properties
                FROM kg_nodes n
                WHERE n.type = ?
                  AND NOT EXISTS (
                      SELECT 1 FROM kg_edges e
                      WHERE e.dst_node_id = n.id AND e.relation = ?
                  )
                ORDER BY n.label
                LIMIT ?
            )");
            if (!stmtR)
                return stmtR.error();

            auto stmt = std::move(stmtR).value();
            auto br = stmt.bind(1, nodeType);
            if (!br)
                return br.error();
            br = stmt.bind(2, relation);
            if (!br)
                return br.error();
            br = stmt.bind(3, static_cast<std::int64_t>(limit));
            if (!br)
                return br.error();

            std::vector<KGNode> results;
            while (true) {
                auto step = stmt.step();
                if (!step)
                    return step.error();
                if (!step.value())
                    break;

                KGNode node;
                node.id = stmt.getInt64(0);
                node.nodeKey = stmt.getString(1);
                if (!stmt.isNull(2))
                    node.label = stmt.getString(2);
                if (!stmt.isNull(3))
                    node.type = stmt.getString(3);
                if (!stmt.isNull(4))
                    node.createdTime = stmt.getInt64(4);
                if (!stmt.isNull(5))
                    node.updatedTime = stmt.getInt64(5);
                if (!stmt.isNull(6))
                    node.properties = stmt.getString(6);

                results.push_back(std::move(node));
            }
            return results;
        });
    }

    Result<std::vector<std::pair<std::string, std::size_t>>> getNodeTypeCounts() override {
        return pool_->withConnection(
            [&](Database& db) -> Result<std::vector<std::pair<std::string, std::size_t>>> {
                auto stmtR = db.prepare(R"(
                SELECT type, COUNT(*) as cnt
                FROM kg_nodes
                WHERE type IS NOT NULL AND type != ''
                GROUP BY type
                ORDER BY cnt DESC
            )");
                if (!stmtR)
                    return stmtR.error();

                auto stmt = std::move(stmtR).value();
                std::vector<std::pair<std::string, std::size_t>> results;
                while (true) {
                    auto step = stmt.step();
                    if (!step)
                        return step.error();
                    if (!step.value())
                        break;

                    std::string typeName = stmt.getString(0);
                    std::size_t count = static_cast<std::size_t>(stmt.getInt64(1));
                    results.emplace_back(std::move(typeName), count);
                }
                return results;
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

    // Tree diff helpers (KG integration for PBI-043)
    Result<std::int64_t> ensureBlobNode(std::string_view sha256) override {
        return pool_->withConnection([&](Database& db) -> Result<std::int64_t> {
            // Check if blob node exists
            auto selectStmt =
                db.prepare("SELECT id FROM kg_nodes WHERE node_key = ? AND type = 'blob'");
            if (!selectStmt)
                return selectStmt.error();

            auto& stmt = selectStmt.value();
            std::string nodeKey = std::string("blob:") + std::string(sha256);
            auto bindRes = stmt.bindAll(nodeKey);
            if (!bindRes)
                return bindRes.error();

            auto stepRes = stmt.step();
            if (!stepRes)
                return stepRes.error();

            if (stepRes.value()) {
                // Node exists, return its ID
                return stmt.getInt64(0);
            }

            // Node doesn't exist, create it
            auto insertStmt =
                db.prepare("INSERT INTO kg_nodes (node_key, label, type, created_time) "
                           "VALUES (?, ?, 'blob', unixepoch())");
            if (!insertStmt)
                return insertStmt.error();

            auto& insert = insertStmt.value();
            std::string label = std::string(sha256).substr(0, 16) + "..."; // Short label
            auto insertBind = insert.bindAll(nodeKey, label);
            if (!insertBind)
                return insertBind.error();

            auto execRes = insert.execute();
            if (!execRes)
                return execRes.error();

            return db.lastInsertRowId();
        });
    }

    Result<std::int64_t> ensurePathNode(const PathNodeDescriptor& descriptor) override {
        return pool_->withConnection([&](Database& db) -> Result<std::int64_t> {
            // Node key: path:<snapshot_id>:<normalized_path>
            std::string nodeKey = "path:" + descriptor.snapshotId + ":" + descriptor.path;

            // Check if path node exists
            auto selectStmt =
                db.prepare("SELECT id FROM kg_nodes WHERE node_key = ? AND type = 'path'");
            if (!selectStmt)
                return selectStmt.error();

            auto& stmt = selectStmt.value();
            auto bindRes = stmt.bindAll(nodeKey);
            if (!bindRes)
                return bindRes.error();

            auto stepRes = stmt.step();
            if (!stepRes)
                return stepRes.error();

            std::int64_t snapshotNodeId = 0;
            if (stepRes.value()) {
                snapshotNodeId = stmt.getInt64(0);
            } else {
                // Node doesn't exist, create it with properties JSON
                auto insertStmt = db.prepare(
                    "INSERT INTO kg_nodes (node_key, label, type, created_time, properties) "
                    "VALUES (?, ?, 'path', unixepoch(), ?)");
                if (!insertStmt)
                    return insertStmt.error();

                auto& insert = insertStmt.value();

                // Build properties JSON
                std::string properties = "{";
                properties += "\"snapshot_id\":\"" + descriptor.snapshotId + "\",";
                properties += "\"path\":\"" + descriptor.path + "\",";
                properties +=
                    "\"is_directory\":" + std::string(descriptor.isDirectory ? "true" : "false");
                if (!descriptor.rootTreeHash.empty()) {
                    properties += ",\"root_tree_hash\":\"" + descriptor.rootTreeHash + "\"";
                }
                properties += "}";

                auto insertBind = insert.bindAll(nodeKey, descriptor.path, properties);
                if (!insertBind)
                    return insertBind.error();

                auto execRes = insert.execute();
                if (!execRes)
                    return execRes.error();

                snapshotNodeId = db.lastInsertRowId();
            }

            // Logical path node to group snapshot path nodes.
            std::string logicalKey = "path:logical:" + descriptor.path;
            auto logicalSelect =
                db.prepare("SELECT id FROM kg_nodes WHERE node_key = ? AND type = 'path'");
            if (!logicalSelect)
                return logicalSelect.error();

            auto& logicalStmt = logicalSelect.value();
            auto logicalBind = logicalStmt.bindAll(logicalKey);
            if (!logicalBind)
                return logicalBind.error();

            auto logicalStep = logicalStmt.step();
            if (!logicalStep)
                return logicalStep.error();

            std::int64_t logicalNodeId = 0;
            if (logicalStep.value()) {
                logicalNodeId = logicalStmt.getInt64(0);
            } else {
                auto logicalInsert = db.prepare(
                    "INSERT INTO kg_nodes (node_key, label, type, created_time, properties) "
                    "VALUES (?, ?, 'path', unixepoch(), ?)");
                if (!logicalInsert)
                    return logicalInsert.error();

                auto& insert = logicalInsert.value();
                std::string properties = "{";
                properties += "\"path\":\"" + descriptor.path + "\",";
                properties +=
                    "\"is_directory\":" + std::string(descriptor.isDirectory ? "true" : "false");
                properties += ",\"logical\":true";
                properties += "}";
                auto logicalInsertBind = insert.bindAll(logicalKey, descriptor.path, properties);
                if (!logicalInsertBind)
                    return logicalInsertBind.error();
                auto logicalExec = insert.execute();
                if (!logicalExec)
                    return logicalExec.error();
                logicalNodeId = db.lastInsertRowId();
            }

            if (logicalNodeId > 0 && snapshotNodeId > 0) {
                auto edgeStmt = db.prepare("INSERT INTO kg_edges (src_node_id, dst_node_id, "
                                           "relation, created_time, properties) "
                                           "SELECT ?, ?, 'path_version', unixepoch(), ? "
                                           "WHERE NOT EXISTS ("
                                           "  SELECT 1 FROM kg_edges WHERE src_node_id = ? AND "
                                           "dst_node_id = ? AND relation = 'path_version'"
                                           ")");
                if (!edgeStmt)
                    return edgeStmt.error();
                std::string edgeProps = "{\"snapshot_id\":\"" + descriptor.snapshotId + "\"}";
                auto& edge = edgeStmt.value();
                auto edgeBind = edge.bindAll(logicalNodeId, snapshotNodeId, edgeProps,
                                             logicalNodeId, snapshotNodeId);
                if (!edgeBind)
                    return edgeBind.error();
                auto edgeExec = edge.execute();
                if (!edgeExec)
                    return edgeExec.error();
            }

            return snapshotNodeId;
        });
    }

    Result<void> linkPathVersion(std::int64_t pathNodeId, std::int64_t blobNodeId,
                                 std::int64_t diffId) override {
        return pool_->withConnection([&](Database& db) -> Result<void> {
            // Create edge: path --[has_version]--> blob
            auto insertStmt = db.prepare("INSERT INTO kg_edges (src_node_id, dst_node_id, "
                                         "relation, created_time, properties) "
                                         "VALUES (?, ?, 'has_version', unixepoch(), ?)");
            if (!insertStmt)
                return insertStmt.error();

            auto& stmt = insertStmt.value();
            std::string properties = "{\"diff_id\":" + std::to_string(diffId) + "}";

            auto bindRes = stmt.bindAll(pathNodeId, blobNodeId, properties);
            if (!bindRes)
                return bindRes.error();

            auto execRes = stmt.execute();
            if (!execRes)
                return execRes.error();

            return Result<void>();
        });
    }

    Result<void> recordRenameEdge(std::int64_t fromPathNodeId, std::int64_t toPathNodeId,
                                  std::int64_t diffId) override {
        return pool_->withConnection([&](Database& db) -> Result<void> {
            // Create edge: old_path --[renamed_to]--> new_path
            auto insertStmt = db.prepare("INSERT INTO kg_edges (src_node_id, dst_node_id, "
                                         "relation, created_time, properties) "
                                         "VALUES (?, ?, 'renamed_to', unixepoch(), ?)");
            if (!insertStmt)
                return insertStmt.error();

            auto& stmt = insertStmt.value();
            std::string properties = "{\"diff_id\":" + std::to_string(diffId) + "}";

            auto bindRes = stmt.bindAll(fromPathNodeId, toPathNodeId, properties);
            if (!bindRes)
                return bindRes.error();

            auto execRes = stmt.execute();
            if (!execRes)
                return execRes.error();

            return Result<void>();
        });
    }

    Result<std::vector<PathHistoryRecord>> fetchPathHistory(std::string_view logicalPath,
                                                            std::size_t limit) override {
        return pool_->withConnection([&](Database& db) -> Result<std::vector<PathHistoryRecord>> {
            std::vector<PathHistoryRecord> history;

            // Query path nodes that match the logical path across all snapshots
            // Then follow rename edges to find the complete history
            auto queryStmt = db.prepare(R"(
                WITH RECURSIVE rename_chain AS (
                    -- Base case: find all path nodes matching the logical path
                    SELECT 
                        n.id,
                        n.node_key,
                        json_extract(n.properties, '$.snapshot_id') AS snapshot_id,
                        json_extract(n.properties, '$.path') AS path,
                        0 AS depth
                    FROM kg_nodes n
                    WHERE n.type = 'path'
                      AND json_extract(n.properties, '$.path') = ?
                    
                    UNION ALL
                    
                    -- Recursive case: follow renamed_to edges
                    SELECT 
                        dst_node.id,
                        dst_node.node_key,
                        json_extract(dst_node.properties, '$.snapshot_id') AS snapshot_id,
                        json_extract(dst_node.properties, '$.path') AS path,
                        rc.depth + 1
                    FROM rename_chain rc
                    JOIN kg_edges e ON rc.id = e.src_node_id AND e.relation = 'renamed_to'
                    JOIN kg_nodes dst_node ON e.dst_node_id = dst_node.id
                    WHERE rc.depth < 100  -- Safety limit
                )
                SELECT DISTINCT
                    rc.snapshot_id,
                    rc.path,
                    json_extract(blob_node.node_key, '$') AS blob_hash,
                    CAST(json_extract(ver_edge.properties, '$.diff_id') AS INTEGER) AS diff_id
                FROM rename_chain rc
                LEFT JOIN kg_edges ver_edge ON rc.id = ver_edge.src_node_id 
                    AND ver_edge.relation = 'has_version'
                LEFT JOIN kg_nodes blob_node ON ver_edge.dst_node_id = blob_node.id 
                    AND blob_node.type = 'blob'
                ORDER BY rc.snapshot_id DESC
                LIMIT ?
            )");

            if (!queryStmt)
                return queryStmt.error();

            auto& stmt = queryStmt.value();
            auto bindRes = stmt.bindAll(logicalPath, static_cast<std::int64_t>(limit));
            if (!bindRes)
                return bindRes.error();

            while (true) {
                auto stepRes = stmt.step();
                if (!stepRes)
                    return stepRes.error();

                if (!stepRes.value())
                    break;

                PathHistoryRecord record;
                record.snapshotId = stmt.getString(0);
                record.path = stmt.getString(1);

                // blob_hash column (handle NULL)
                if (!stmt.isNull(2)) {
                    std::string blobNodeKey = stmt.getString(2);
                    // Extract hash from node_key format "blob:hash"
                    if (blobNodeKey.starts_with("blob:")) {
                        record.blobHash = blobNodeKey.substr(5);
                    }
                }

                // diff_id column (handle NULL)
                if (!stmt.isNull(3)) {
                    record.diffId = stmt.getInt64(3);
                }

                record.changeType = std::nullopt; // Could be enhanced later

                history.push_back(record);
            }

            return history;
        });
    }

    // Maintenance
    Result<std::int64_t> pruneVersionNodes(const GraphVersionPruneConfig& cfg) override {
        if (cfg.keepLatestPerCanonical == 0) {
            return 0;
        }
        return pool_->withConnection([&](Database& db) -> Result<std::int64_t> {
            auto stmtR = db.prepare(R"(
                WITH ranked AS (
                    SELECT id,
                           ROW_NUMBER() OVER (
                               PARTITION BY json_extract(properties, '$.canonical_key')
                               ORDER BY COALESCE(updated_time, created_time, 0) DESC, id DESC
                           ) AS rn
                    FROM kg_nodes
                    WHERE json_extract(properties, '$.snapshot_id') IS NOT NULL
                      AND json_extract(properties, '$.canonical_key') IS NOT NULL
                )
                DELETE FROM kg_nodes
                WHERE id IN (SELECT id FROM ranked WHERE rn > ?)
            )");
            if (!stmtR)
                return stmtR.error();
            auto stmt = std::move(stmtR).value();
            auto br = stmt.bind(1, static_cast<std::int64_t>(cfg.keepLatestPerCanonical));
            if (!br)
                return br.error();
            auto execR = stmt.execute();
            if (!execR)
                return execR.error();
            return db.changes();
        });
    }

    Result<void> optimize() override {
        auto res = pool_->withConnection(
            [](Database& db) -> Result<void> { return db.execute("PRAGMA optimize"); });
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
        auto res = pool_->withConnection(
            [](Database& db) -> Result<void> { return db.execute("PRAGMA integrity_check"); });
        if (!res) {
            spdlog::warn("KG store healthCheck failed: {}", res.error().message);
            return res.error();
        }
        return {};
    }

    // Symbol Metadata
    Result<void> upsertSymbolMetadata(const std::vector<SymbolMetadata>& symbols) override {
        if (symbols.empty()) {
            return Result<void>();
        }

        return pool_->withConnection([&](Database& db) -> Result<void> {
            auto stmtR = db.prepare(R"(
                INSERT INTO symbol_metadata (
                    document_hash, file_path, symbol_name, qualified_name, kind,
                    start_line, end_line, start_offset, end_offset,
                    return_type, parameters, documentation
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(document_hash, qualified_name) DO UPDATE SET
                    file_path = excluded.file_path,
                    symbol_name = excluded.symbol_name,
                    kind = excluded.kind,
                    start_line = excluded.start_line,
                    end_line = excluded.end_line,
                    start_offset = excluded.start_offset,
                    end_offset = excluded.end_offset,
                    return_type = excluded.return_type,
                    parameters = excluded.parameters,
                    documentation = excluded.documentation
            )");
            if (!stmtR)
                return stmtR.error();

            auto stmt = std::move(stmtR).value();

            for (const auto& sym : symbols) {
                auto bindR = stmt.bindAll(
                    sym.documentHash, sym.filePath, sym.symbolName, sym.qualifiedName, sym.kind,
                    sym.startLine.value_or(0), sym.endLine.value_or(0), sym.startOffset.value_or(0),
                    sym.endOffset.value_or(0), sym.returnType.value_or(std::string{}),
                    sym.parameters.value_or(std::string{}),
                    sym.documentation.value_or(std::string{}));
                if (!bindR)
                    return bindR.error();

                auto execR = stmt.execute();
                if (!execR)
                    return execR.error();

                stmt.reset();
            }

            spdlog::debug("upsertSymbolMetadata: inserted/updated {} symbols", symbols.size());
            return Result<void>();
        });
    }

    Result<std::int64_t> deleteSymbolMetadataForDocument(std::string_view documentHash) override {
        return pool_->withConnection([&](Database& db) -> Result<std::int64_t> {
            auto stmtR = db.prepare("DELETE FROM symbol_metadata WHERE document_hash = ?");
            if (!stmtR)
                return stmtR.error();
            auto stmt = std::move(stmtR).value();
            auto br = stmt.bind(1, documentHash);
            if (!br)
                return br.error();
            auto execR = stmt.execute();
            if (!execR)
                return execR.error();
            auto deleted = db.changes();
            spdlog::debug("deleteSymbolMetadataForDocument: deleted {} symbols for hash {}",
                          deleted, documentHash);
            return deleted;
        });
    }

    Result<std::vector<SymbolMetadata>>
    querySymbolMetadata(std::optional<std::string_view> filePath,
                        std::optional<std::string_view> kind,
                        std::optional<std::string_view> namePattern, std::size_t limit,
                        std::size_t offset) override {
        return pool_->withConnection([&](Database& db) -> Result<std::vector<SymbolMetadata>> {
            std::ostringstream sql;
            sql << "SELECT symbol_id, document_hash, file_path, symbol_name, qualified_name, "
                << "kind, start_line, end_line, start_offset, end_offset, "
                << "return_type, parameters, documentation FROM symbol_metadata WHERE 1=1";

            std::vector<std::string> binds;
            if (filePath.has_value()) {
                sql << " AND file_path LIKE ?";
                binds.push_back("%" + std::string(filePath.value()) + "%");
            }
            if (kind.has_value()) {
                sql << " AND kind = ?";
                binds.push_back(std::string(kind.value()));
            }
            if (namePattern.has_value()) {
                sql << " AND (symbol_name LIKE ? OR qualified_name LIKE ?)";
                std::string pattern = "%" + std::string(namePattern.value()) + "%";
                binds.push_back(pattern);
                binds.push_back(pattern);
            }
            sql << " ORDER BY qualified_name LIMIT ? OFFSET ?";

            auto stmtR = db.prepare(sql.str());
            if (!stmtR)
                return stmtR.error();

            auto stmt = std::move(stmtR).value();
            int idx = 1;
            for (const auto& b : binds) {
                auto br = stmt.bind(idx++, b);
                if (!br)
                    return br.error();
            }
            auto br = stmt.bind(idx++, static_cast<std::int64_t>(limit));
            if (!br)
                return br.error();
            br = stmt.bind(idx++, static_cast<std::int64_t>(offset));
            if (!br)
                return br.error();

            std::vector<SymbolMetadata> results;
            while (true) {
                auto stepR = stmt.step();
                if (!stepR)
                    return stepR.error();
                if (!stepR.value())
                    break;

                SymbolMetadata sym;
                sym.symbolId = stmt.getInt64(0);
                sym.documentHash = stmt.getString(1);
                sym.filePath = stmt.getString(2);
                sym.symbolName = stmt.getString(3);
                sym.qualifiedName = stmt.getString(4);
                sym.kind = stmt.getString(5);
                if (!stmt.isNull(6))
                    sym.startLine = static_cast<std::int32_t>(stmt.getInt64(6));
                if (!stmt.isNull(7))
                    sym.endLine = static_cast<std::int32_t>(stmt.getInt64(7));
                if (!stmt.isNull(8))
                    sym.startOffset = static_cast<std::int32_t>(stmt.getInt64(8));
                if (!stmt.isNull(9))
                    sym.endOffset = static_cast<std::int32_t>(stmt.getInt64(9));
                if (!stmt.isNull(10)) {
                    auto s = stmt.getString(10);
                    if (!s.empty())
                        sym.returnType = s;
                }
                if (!stmt.isNull(11)) {
                    auto s = stmt.getString(11);
                    if (!s.empty())
                        sym.parameters = s;
                }
                if (!stmt.isNull(12)) {
                    auto s = stmt.getString(12);
                    if (!s.empty())
                        sym.documentation = s;
                }
                results.push_back(std::move(sym));
            }

            return results;
        });
    }

private:
    KnowledgeGraphStoreConfig cfg_{};
    ConnectionPool* pool_{nullptr};                       // Non-owning
    std::unique_ptr<ConnectionPool> owned_pool_{nullptr}; // Owns pool when created from path
};

// Factory: SQLite store using a file path (owns its pool)
Result<std::unique_ptr<KnowledgeGraphStore>>
makeSqliteKnowledgeGraphStore(const std::string& dbPath, const KnowledgeGraphStoreConfig& cfg) {
    auto s = SqliteKnowledgeGraphStore::createWithPath(dbPath, cfg);
    if (!s)
        return s.error();
    auto raw = std::move(s).value().release();
    return std::unique_ptr<KnowledgeGraphStore>(raw);
}

// Factory: SQLite store using an external ConnectionPool (non-owning)
Result<std::unique_ptr<KnowledgeGraphStore>>
makeSqliteKnowledgeGraphStore(ConnectionPool& pool, const KnowledgeGraphStoreConfig& cfg) {
    auto store = std::unique_ptr<KnowledgeGraphStore>(new SqliteKnowledgeGraphStore(pool, cfg));
    return store;
}

} // namespace yams::metadata
