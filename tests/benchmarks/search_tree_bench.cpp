#include <spdlog/spdlog.h>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <yams/metadata/document_metadata.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/migration.h>
#include <yams/metadata/path_utils.h>

#include "benchmark_base.h"

// Skeleton benchmark harness for PBI 051. The intent is to compare the existing search
// engine results with the prototype path-tree traversal once the prototype is available.
// Current implementation wires up repository setup and emits TODO markers for the actual
// benchmark loops.

namespace {

struct BenchmarkFixture {
    BenchmarkFixture() {
        using namespace yams::metadata;

        ConnectionPoolConfig poolCfg;
        poolCfg.minConnections = 1;
        poolCfg.maxConnections = 2;

        if (const char* existing = std::getenv("YAMS_BENCH_METADATA_DB")) {
            std::filesystem::path candidate(existing);
            if (!candidate.empty() && std::filesystem::exists(candidate)) {
                dbPath = candidate;
                ownsDb_ = false;
                spdlog::info("search_tree_bench: using existing metadata DB at {}",
                             dbPath.string());
            }
        }

        if (ownsDb_) {
            auto tmpDir = std::filesystem::temp_directory_path() / "yams_search_tree_bench";
            std::filesystem::create_directories(tmpDir);
            dbPath = tmpDir / "metadata.db";
            std::filesystem::remove(dbPath);

            Database db;
            auto open = db.open(dbPath.string(), ConnectionMode::Create);
            if (!open) {
                throw std::runtime_error("Failed to open metadata DB: " + open.error().message);
            }
            MigrationManager mgr(db);
            if (auto init = mgr.initialize(); !init) {
                throw std::runtime_error("Failed to initialize migrations: " +
                                         init.error().message);
            }
            mgr.registerMigrations(YamsMetadataMigrations::getAllMigrations());
            if (auto needs = mgr.needsMigration(); needs && needs.value()) {
                auto res = mgr.migrate();
                if (!res) {
                    throw std::runtime_error("Failed to run migrations: " + res.error().message);
                }
            }
        }

        pool = std::make_unique<ConnectionPool>(dbPath.string(), poolCfg);
        auto init = pool->initialize();
        if (!init) {
            throw std::runtime_error("Failed to initialize connection pool: " +
                                     init.error().message);
        }

        repo = std::make_shared<MetadataRepository>(*pool);
        if (ownsDb_) {
            populateSampleData();
        }
    }

    ~BenchmarkFixture() {
        pool.reset();
        if (ownsDb_) {
            std::error_code ec;
            std::filesystem::remove(dbPath, ec);
            std::filesystem::remove(dbPath.parent_path(), ec);
        }
    }

    std::filesystem::path dbPath;
    std::unique_ptr<yams::metadata::ConnectionPool> pool;
    std::shared_ptr<yams::metadata::MetadataRepository> repo;
    bool ownsDb_{true};

private:
    void populateSampleData() {
        using namespace yams::metadata;
        const std::vector<std::string> roots = {"/project/src", "/project/tests", "/project/docs"};

        for (int i = 0; i < 90; ++i) {
            auto dir = roots[static_cast<std::size_t>(i % roots.size())];
            std::filesystem::path path =
                std::filesystem::path(dir) / ("file_" + std::to_string(i) + ".txt");

            DocumentInfo info;
            info.filePath = path.generic_string();
            info.fileName = path.filename().string();
            info.fileExtension = path.extension().string();
            info.fileSize = 1024 + i;
            info.sha256Hash = "hash_" + std::to_string(i);
            info.mimeType = "text/plain";
            auto now = std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
            info.createdTime = now;
            info.modifiedTime = now;
            info.indexedTime = now;
            info.contentExtracted = true;
            info.extractionStatus = ExtractionStatus::Success;

            const auto derived = computePathDerivedValues(info.filePath);
            info.filePath = derived.normalizedPath;
            info.pathPrefix = derived.pathPrefix;
            info.reversePath = derived.reversePath;
            info.pathHash = derived.pathHash;
            info.parentHash = derived.parentHash;
            info.pathDepth = derived.pathDepth;

            auto insertRes = repo->insertDocument(info);
            if (!insertRes)
                throw std::runtime_error("Failed to insert sample document");
            info.id = insertRes.value();
            auto treeRes = repo->upsertPathTreeForDocument(info, info.id, true, {});
            if (!treeRes)
                throw std::runtime_error("Failed to upsert path tree for sample document");
        }
    }
};

void bench_existing_search(const BenchmarkFixture& fixture) {
    using namespace std::chrono;
    using namespace yams::metadata;

    std::vector<std::string> prefixes = {"/project/src", "/project/tests", "/project/docs"};
    for (const auto& prefix : prefixes) {
        DocumentQueryOptions opts;
        opts.pathPrefix = prefix;

        const auto start = steady_clock::now();
        for (int i = 0; i < 10; ++i) {
            auto res = fixture.repo->queryDocuments(opts);
            if (!res)
                spdlog::warn("Baseline query failed for {}: {}", prefix, res.error().message);
        }
        const auto end = steady_clock::now();
        spdlog::info("Baseline metadata search prefix='{}' took {} ms", prefix,
                     duration_cast<milliseconds>(end - start).count());
    }
}

void bench_path_tree_search(const BenchmarkFixture& fixture) {
    using namespace std::chrono;
    using namespace yams::metadata;

    const auto start = steady_clock::now();
    auto nodesResult = fixture.pool->withConnection(
        [&](Database& db) -> yams::Result<std::vector<std::pair<std::string, int64_t>>> {
            auto stmtRes =
                db.prepare("SELECT full_path, doc_count FROM path_tree_nodes ORDER BY full_path");
            if (!stmtRes)
                return stmtRes.error();
            auto stmt = std::move(stmtRes).value();

            std::vector<std::pair<std::string, int64_t>> nodes;
            while (true) {
                auto step = stmt.step();
                if (!step)
                    return step.error();
                if (!step.value())
                    break;
                nodes.emplace_back(stmt.getString(0), stmt.getInt64(1));
            }
            return nodes;
        });

    const auto end = steady_clock::now();
    if (!nodesResult) {
        spdlog::warn("Path-tree traversal failed: {}", nodesResult.error().message);
        return;
    }

    const auto& nodes = nodesResult.value();
    spdlog::info("Path-tree scan returned {} nodes in {} ms", nodes.size(),
                 duration_cast<milliseconds>(end - start).count());
    const std::size_t previewLimit = std::min<std::size_t>(nodes.size(), 25);
    for (std::size_t i = 0; i < previewLimit; ++i) {
        spdlog::info("  node='{}' doc_count={}", nodes[i].first, nodes[i].second);
    }
    if (nodes.size() > previewLimit) {
        spdlog::info("  ... {} additional nodes omitted", nodes.size() - previewLimit);
    }
}

} // namespace

int main() {
    try {
        BenchmarkFixture fixture;

        bench_existing_search(fixture);
        bench_path_tree_search(fixture);

        spdlog::info("Search tree benchmark skeleton executed successfully.");
        return 0;
    } catch (const std::exception& ex) {
        spdlog::error("Search tree benchmark failed: {}", ex.what());
        return 1;
    }
}
