#include <spdlog/spdlog.h>
#include <algorithm>
#include <array>
#include <chrono>
#include <cstdlib>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <numeric>
#include <optional>
#include <span>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <yams/metadata/document_metadata.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/migration.h>
#include <yams/metadata/path_utils.h>

#include <nlohmann/json.hpp>

#include "benchmark_base.h"

// Skeleton benchmark harness for PBI 051. The intent is to compare the existing search
// engine results with the prototype path-tree traversal once the prototype is available.
// Current implementation wires up repository setup and emits TODO markers for the actual
// benchmark loops.

namespace {

std::optional<std::filesystem::path> locateBundledDataset() {
    std::vector<std::filesystem::path> rootCandidates;
    auto cwd = std::filesystem::current_path();
    rootCandidates.push_back(cwd);
    if (auto parent = cwd.parent_path(); !parent.empty()) {
        rootCandidates.push_back(parent);
        if (auto grand = parent.parent_path(); !grand.empty()) {
            rootCandidates.push_back(grand);
        }
    }

    std::filesystem::path filePath(__FILE__);
    std::error_code absEc;
    auto absolutePath = std::filesystem::absolute(filePath, absEc);
    if (!absEc) {
        filePath = absolutePath;
    }
    for (int i = 0; i < 3; ++i) {
        filePath = filePath.parent_path();
    }
    if (!filePath.empty() &&
        std::find(rootCandidates.begin(), rootCandidates.end(), filePath) == rootCandidates.end()) {
        rootCandidates.push_back(filePath);
    }

    const std::array<std::filesystem::path, 3> relativeCandidates = {
        std::filesystem::path("bench/runtime/search_tree_fixture"),
        std::filesystem::path("bench/runtime/baseline/baseline_2"),
        std::filesystem::path("bench/runtime/baseline/baseline_1"),
    };

    std::error_code ec;
    for (const auto& root : rootCandidates) {
        for (const auto& rel : relativeCandidates) {
            auto candidate = root / rel;
            if (!std::filesystem::exists(candidate, ec))
                continue;
            if (std::filesystem::is_directory(candidate, ec)) {
                const auto metadataDb = candidate / "metadata.db";
                const auto yamsDb = candidate / "yams.db";
                if (std::filesystem::exists(metadataDb, ec) ||
                    std::filesystem::exists(yamsDb, ec)) {
                    return candidate;
                }
            } else if (candidate.extension() == ".db") {
                return candidate;
            }
        }
    }

    return std::nullopt;
}

struct RunStats {
    double avgMs{0.0};
    double minMs{0.0};
    double maxMs{0.0};
    std::size_t resultCount{0};
};

struct PathTreeRunStats : RunStats {
    std::vector<std::pair<std::string, int64_t>> topChildren;
    double aggregateScore{0.0};
    double bestChildScore{0.0};
    std::string bestChildPath;
};

struct SemanticRunStats {
    RunStats timing;
    double bestScore{0.0};
    std::string bestPath;
};

struct PathTreeScanResult {
    int64_t docCount{0};
    std::vector<std::pair<std::string, int64_t>> topChildren;
    std::vector<float> centroid;
};

constexpr std::size_t kEmbeddingDim = 3;

std::array<float, kEmbeddingDim> makeOrdinalEmbedding(int ordinal) {
    return {static_cast<float>((ordinal % 11) + 1), static_cast<float>(((ordinal * 3) % 17) - 8),
            static_cast<float>((ordinal % 5) - 2)};
}

std::vector<float> toVector(const std::array<float, kEmbeddingDim>& arr) {
    return {arr.begin(), arr.end()};
}

int extractOrdinalFromPath(const std::string& path) {
    auto underscore = path.find_last_of('_');
    auto dot = path.find_last_of('.');
    if (underscore == std::string::npos || dot == std::string::npos || dot <= underscore + 1)
        return -1;
    try {
        return std::stoi(path.substr(underscore + 1, dot - underscore - 1));
    } catch (...) {
        return -1;
    }
}

double dotProduct(std::span<const float> lhs, std::span<const float> rhs) {
    const auto len = std::min(lhs.size(), rhs.size());
    double sum = 0.0;
    for (std::size_t i = 0; i < len; ++i) {
        sum += static_cast<double>(lhs[i]) * static_cast<double>(rhs[i]);
    }
    return sum;
}

std::vector<float> makeQueryVectorForPrefix(const std::string& prefix) {
    int ordinal = 0;
    if (prefix.find("/project/src") == 0) {
        ordinal = 5;
    } else if (prefix.find("/project/tests") == 0) {
        ordinal = 25;
    } else if (prefix.find("/project/docs") == 0) {
        ordinal = 55;
    }
    return toVector(makeOrdinalEmbedding(ordinal));
}

struct BenchmarkFixture;

std::string normalizePrefix(std::string rawPrefix);

yams::Result<PathTreeScanResult> collectPathTreeMetrics(const BenchmarkFixture& fixture,
                                                        std::string_view prefix,
                                                        std::size_t childLimit);

struct BenchmarkFixture {
    BenchmarkFixture() {
        using namespace yams::metadata;

        ConnectionPoolConfig poolCfg;
        poolCfg.minConnections = 1;
        poolCfg.maxConnections = 2;

        if (!initializeFromDataset()) {
            if (!initializeFromExisting()) {
                setupFreshDatabase();
            }
        }

        pool = std::make_unique<ConnectionPool>(dbPath.string(), poolCfg);
        auto init = pool->initialize();
        if (!init) {
            throw std::runtime_error("Failed to initialize connection pool: " +
                                     init.error().message);
        }

        repo = std::make_shared<MetadataRepository>(*pool);
        if (usesFixture_) {
            ensurePathTreeBackfill();
        } else if (populateSampleData_) {
            populateSampleData();
        }
    }

    ~BenchmarkFixture() {
        pool.reset();
        if (ownsDb_ && !fixtureRoot_.empty()) {
            std::error_code ec;
            std::filesystem::remove_all(fixtureRoot_, ec);
        }
    }

    std::filesystem::path dbPath;
    std::unique_ptr<yams::metadata::ConnectionPool> pool;
    std::shared_ptr<yams::metadata::MetadataRepository> repo;
    bool ownsDb_{true};
    bool populateSampleData_{true};
    bool usesFixture_{false};
    std::unordered_map<std::string, int64_t> baselineDocCounts_;
    std::unordered_map<std::string, int64_t> diffDocCounts_;
    int nextDiffOrdinal_{200};

private:
    std::filesystem::path fixtureRoot_;

    bool tryMaterializeDataset(const std::filesystem::path& source) {
        using namespace yams::metadata;

        std::error_code ec;
        if (!std::filesystem::exists(source, ec)) {
            spdlog::warn("Dataset source '{}' does not exist; skipping", source.string());
            return false;
        }

        auto tempRoot = createUniqueTempDir("yams_search_tree_dataset");

        try {
            if (std::filesystem::is_directory(source, ec)) {
                std::filesystem::copy(source, tempRoot,
                                      std::filesystem::copy_options::recursive |
                                          std::filesystem::copy_options::overwrite_existing);
            } else {
                std::filesystem::copy_file(source, tempRoot / source.filename(),
                                           std::filesystem::copy_options::overwrite_existing);
            }
        } catch (const std::exception& ex) {
            spdlog::warn("Failed to copy dataset from '{}': {}", source.string(), ex.what());
            std::error_code removeEc;
            std::filesystem::remove_all(tempRoot, removeEc);
            return false;
        }

        std::filesystem::path candidateDb = tempRoot / "metadata.db";
        if (!std::filesystem::exists(candidateDb, ec))
            candidateDb = tempRoot / "yams.db";
        if (!std::filesystem::exists(candidateDb, ec)) {
            auto nested = tempRoot / source.filename();
            if (std::filesystem::is_directory(nested, ec)) {
                candidateDb = nested / "metadata.db";
                if (!std::filesystem::exists(candidateDb, ec))
                    candidateDb = nested / "yams.db";
            }
        }
        if (!std::filesystem::exists(candidateDb, ec)) {
            spdlog::warn("Dataset '{}' is missing metadata.db or yams.db; skipping",
                         source.string());
            std::error_code removeEc;
            std::filesystem::remove_all(tempRoot, removeEc);
            return false;
        }

        Database db;
        auto open = db.open(candidateDb.string(), ConnectionMode::ReadWrite);
        if (!open) {
            spdlog::warn("Failed to open dataset DB '{}': {}", candidateDb.string(),
                         open.error().message);
            std::error_code removeEc;
            std::filesystem::remove_all(tempRoot, removeEc);
            return false;
        }
        MigrationManager mgr(db);
        if (auto init = mgr.initialize(); !init) {
            spdlog::warn("Failed to initialize migrations for '{}': {}", candidateDb.string(),
                         init.error().message);
            std::error_code removeEc;
            std::filesystem::remove_all(tempRoot, removeEc);
            return false;
        }
        mgr.registerMigrations(YamsMetadataMigrations::getAllMigrations());
        if (auto needs = mgr.needsMigration(); needs && needs.value()) {
            auto res = mgr.migrate();
            if (!res) {
                spdlog::warn("Failed to migrate dataset DB '{}': {}", candidateDb.string(),
                             res.error().message);
                std::error_code removeEc;
                std::filesystem::remove_all(tempRoot, removeEc);
                return false;
            }
        }

        fixtureRoot_ = tempRoot;
        dbPath = candidateDb;
        ownsDb_ = true;
        populateSampleData_ = false;
        usesFixture_ = true;
        spdlog::info("search_tree_bench: using dataset fixture '{}'", source.string());
        return true;
    }

    std::filesystem::path createUniqueTempDir(const std::string& prefix) {
        auto base = std::filesystem::temp_directory_path();
        for (int attempt = 0; attempt < 64; ++attempt) {
            auto ticks = std::chrono::steady_clock::now().time_since_epoch().count();
            std::filesystem::path candidate =
                base / (prefix + "_" + std::to_string(ticks) + "_" + std::to_string(attempt));
            std::error_code ec;
            if (std::filesystem::create_directories(candidate, ec)) {
                return candidate;
            }
            if (!std::filesystem::exists(candidate)) {
                throw std::runtime_error("Failed to create temporary directory: " + ec.message());
            }
        }
        throw std::runtime_error("Unable to create unique temporary directory for benchmarks");
    }

    bool initializeFromDataset() {
        std::vector<std::filesystem::path> sources;
        const char* datasetEnv = std::getenv("YAMS_BENCH_DATASET_DIR");
        if (datasetEnv && std::string_view(datasetEnv).empty() == false) {
            sources.emplace_back(datasetEnv);
        }
        if (auto bundled = locateBundledDataset(); bundled) {
            if (std::find(sources.begin(), sources.end(), *bundled) == sources.end()) {
                sources.push_back(*bundled);
            }
        }

        for (const auto& source : sources) {
            if (tryMaterializeDataset(source)) {
                return true;
            }
        }

        return false;
    }

    bool initializeFromExisting() {
        using namespace yams::metadata;
        const char* existing = std::getenv("YAMS_BENCH_METADATA_DB");
        if (!existing)
            return false;
        std::filesystem::path candidate(existing);
        if (candidate.empty() || !std::filesystem::exists(candidate)) {
            spdlog::warn("YAMS_BENCH_METADATA_DB='{}' not found; ignoring", candidate.string());
            return false;
        }
        dbPath = candidate;
        ownsDb_ = false;
        populateSampleData_ = false;
        spdlog::info("search_tree_bench: using existing metadata DB at {}", dbPath.string());
        return true;
    }

    void setupFreshDatabase() {
        using namespace yams::metadata;
        fixtureRoot_ = createUniqueTempDir("yams_search_tree_bench");
        dbPath = fixtureRoot_ / "metadata.db";
        if (std::filesystem::exists(dbPath))
            std::filesystem::remove(dbPath);

        Database db;
        auto open = db.open(dbPath.string(), ConnectionMode::Create);
        if (!open) {
            throw std::runtime_error("Failed to open metadata DB: " + open.error().message);
        }
        MigrationManager mgr(db);
        if (auto init = mgr.initialize(); !init) {
            throw std::runtime_error("Failed to initialize migrations: " + init.error().message);
        }
        mgr.registerMigrations(YamsMetadataMigrations::getAllMigrations());
        if (auto needs = mgr.needsMigration(); needs && needs.value()) {
            auto res = mgr.migrate();
            if (!res) {
                throw std::runtime_error("Failed to run migrations: " + res.error().message);
            }
        }
        ownsDb_ = true;
        populateSampleData_ = true;
    }

    void ensurePathTreeBackfill() {
        if (!repo)
            return;
        auto existingChildren = repo->listPathTreeChildren("/", 1);
        if (!existingChildren) {
            spdlog::warn("Path-tree child listing failed: {}", existingChildren.error().message);
            return;
        }
        if (!existingChildren.value().empty())
            return;

        spdlog::info("Backfilling path-tree nodes for fixture dataset");
        yams::metadata::DocumentQueryOptions opts;
        opts.limit = 0;
        auto docsRes = repo->queryDocuments(opts);
        if (!docsRes) {
            throw std::runtime_error("Failed to enumerate documents for backfill: " +
                                     docsRes.error().message);
        }
        for (const auto& doc : docsRes.value()) {
            auto upsert = repo->upsertPathTreeForDocument(doc, doc.id, true, {});
            if (!upsert) {
                spdlog::warn("Path-tree backfill failed for {}: {}", doc.filePath,
                             upsert.error().message);
            }
        }
    }

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

            auto embedding = makeOrdinalEmbedding(i);
            auto centroidRes = repo->upsertPathTreeForDocument(
                info, info.id, false, std::span<const float>(embedding.data(), embedding.size()));
            if (!centroidRes)
                throw std::runtime_error("Failed to update centroid for sample document");
        }
    }

public:
    void captureBaselineCounts(const std::vector<std::string>& prefixes) {
        baselineDocCounts_.clear();
        diffDocCounts_.clear();
        for (const auto& rawPrefix : prefixes) {
            auto normalized = normalizePrefix(rawPrefix);
            auto metrics = collectPathTreeMetrics(*this, normalized, /*childLimit=*/0);
            if (!metrics)
                throw std::runtime_error("Failed to collect baseline path-tree metrics for " +
                                         normalized + ": " + metrics.error().message);
            baselineDocCounts_[normalized] = metrics.value().docCount;
        }
    }

    void applyDiffChanges(const std::vector<std::string>& prefixes) {
        using namespace yams::metadata;
        diffDocCounts_.clear();

        for (const auto& rawPrefix : prefixes) {
            auto normalized = normalizePrefix(rawPrefix);

            if (!ownsDb_) {
                diffDocCounts_[normalized] = baselineCountFor(normalized);
                continue;
            }

            for (int j = 0; j < 5; ++j) {
                const int ordinal = nextDiffOrdinal_++;
                std::filesystem::path path = std::filesystem::path(normalized) /
                                             ("diff_added_" + std::to_string(ordinal) + ".txt");

                DocumentInfo info;
                info.filePath = path.generic_string();
                info.fileName = path.filename().string();
                info.fileExtension = path.extension().string();
                info.fileSize = 512 + ordinal;
                info.sha256Hash = "diff_hash_" + std::to_string(ordinal);
                info.mimeType = "text/plain";
                auto now =
                    std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
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
                    throw std::runtime_error("Failed to insert diff document: " +
                                             insertRes.error().message);
                info.id = insertRes.value();

                auto treeRes = repo->upsertPathTreeForDocument(info, info.id, true, {});
                if (!treeRes)
                    throw std::runtime_error("Failed to upsert diff path tree node: " +
                                             treeRes.error().message);

                auto embedding = makeOrdinalEmbedding(ordinal);
                auto centroidRes = repo->upsertPathTreeForDocument(
                    info, info.id, false,
                    std::span<const float>(embedding.data(), embedding.size()));
                if (!centroidRes)
                    throw std::runtime_error("Failed to update diff centroid: " +
                                             centroidRes.error().message);
            }

            auto metrics = collectPathTreeMetrics(*this, normalized, /*childLimit=*/0);
            if (!metrics)
                throw std::runtime_error("Failed to collect diff metrics for " + normalized + ": " +
                                         metrics.error().message);
            diffDocCounts_[normalized] = metrics.value().docCount;
        }
    }

    int64_t baselineCountFor(const std::string& prefix) const {
        if (auto it = baselineDocCounts_.find(prefix); it != baselineDocCounts_.end())
            return it->second;
        return 0;
    }

    int64_t diffCountFor(const std::string& prefix) const {
        if (auto it = diffDocCounts_.find(prefix); it != diffDocCounts_.end())
            return it->second;
        return 0;
    }

    std::vector<std::string> topPrefixes(std::size_t limit) {
        std::vector<std::string> prefixes;
        if (!repo)
            return prefixes;
        auto childrenRes = repo->listPathTreeChildren("/", limit);
        if (!childrenRes)
            return prefixes;
        for (const auto& child : childrenRes.value()) {
            if (child.docCount > 0)
                prefixes.push_back(child.fullPath);
        }
        return prefixes;
    }
};

std::string normalizePrefix(std::string rawPrefix) {
    const bool originalAbsolute =
        !rawPrefix.empty() && (rawPrefix.front() == '/' || rawPrefix.front() == '\\');
    std::filesystem::path asPath(std::move(rawPrefix));
    auto normalized = asPath.lexically_normal();
    std::string result = normalized.generic_string();
    if (originalAbsolute && (result.empty() || result.front() != '/'))
        result.insert(result.begin(), '/');
    if (result.size() > 1 && result.back() == '/')
        result.pop_back();
    return result;
}

RunStats computeStats(const std::vector<double>& durations, std::size_t fallbackCount) {
    RunStats stats;
    if (durations.empty()) {
        stats.resultCount = fallbackCount;
        return stats;
    }

    auto [minIt, maxIt] = std::minmax_element(durations.begin(), durations.end());
    stats.minMs = *minIt;
    stats.maxMs = *maxIt;
    stats.avgMs = std::accumulate(durations.begin(), durations.end(), 0.0) /
                  static_cast<double>(durations.size());
    stats.resultCount = fallbackCount;
    return stats;
}

RunStats measureBaselineSearch(const BenchmarkFixture& fixture, std::string_view prefix,
                               int iterations) {
    using namespace std::chrono;
    using namespace yams::metadata;

    DocumentQueryOptions opts;
    opts.pathPrefix = std::string(prefix);
    opts.includeSubdirectories = true;

    std::vector<double> durations;
    durations.reserve(iterations);
    std::optional<std::size_t> lastCount;

    for (int i = 0; i < iterations; ++i) {
        const auto start = steady_clock::now();
        auto res = fixture.repo->queryDocuments(opts);
        const auto end = steady_clock::now();

        if (!res) {
            spdlog::warn("Baseline query failed for '{}': {}", prefix, res.error().message);
            continue;
        }

        durations.push_back(duration<double, std::milli>(end - start).count());
        lastCount = res.value().size();
    }

    return computeStats(durations, lastCount.value_or(0));
}

yams::Result<PathTreeScanResult> collectPathTreeMetrics(const BenchmarkFixture& fixture,
                                                        std::string_view prefix,
                                                        std::size_t childLimit) {
    using namespace yams::metadata;

    PathTreeScanResult scan;

    auto nodeRes = fixture.repo->findPathTreeNodeByFullPath(prefix);
    if (!nodeRes)
        return nodeRes.error();

    const auto& nodeOpt = nodeRes.value();
    if (!nodeOpt)
        return scan;

    scan.docCount = nodeOpt->docCount;
    scan.centroid = nodeOpt->centroid;

    if (childLimit == 0)
        return scan;

    auto childrenRes = fixture.pool->withConnection(
        [&](yams::metadata::Database& db)
            -> yams::Result<std::vector<std::pair<std::string, int64_t>>> {
            auto stmtRes = db.prepare("SELECT full_path, doc_count FROM path_tree_nodes "
                                      "WHERE parent_id = ? ORDER BY doc_count DESC LIMIT ?");
            if (!stmtRes)
                return stmtRes.error();

            auto stmt = std::move(stmtRes).value();
            if (auto bindParent = stmt.bind(1, nodeOpt->id); !bindParent)
                return bindParent.error();
            if (auto bindLimit = stmt.bind(2, static_cast<int64_t>(childLimit)); !bindLimit)
                return bindLimit.error();

            std::vector<std::pair<std::string, int64_t>> rows;
            while (true) {
                auto step = stmt.step();
                if (!step)
                    return step.error();
                if (!step.value())
                    break;
                rows.emplace_back(stmt.getString(0), stmt.getInt64(1));
            }
            return rows;
        });

    if (!childrenRes)
        return childrenRes.error();

    scan.topChildren = std::move(childrenRes.value());
    return scan;
}

PathTreeRunStats measurePathTree(const BenchmarkFixture& fixture, std::string_view prefix,
                                 int iterations, std::size_t childLimit,
                                 const std::vector<float>* semanticQuery = nullptr) {
    using namespace std::chrono;

    std::vector<double> durations;
    durations.reserve(iterations);
    std::optional<PathTreeScanResult> lastScan;

    for (int i = 0; i < iterations; ++i) {
        const auto start = steady_clock::now();
        auto scanRes = collectPathTreeMetrics(fixture, prefix, childLimit);
        const auto end = steady_clock::now();

        if (!scanRes) {
            spdlog::warn("Path-tree scan failed for '{}': {}", prefix, scanRes.error().message);
            continue;
        }

        durations.push_back(duration<double, std::milli>(end - start).count());
        lastScan = std::move(scanRes.value());
    }

    PathTreeRunStats stats;
    RunStats timing =
        computeStats(durations, lastScan ? static_cast<std::size_t>(lastScan->docCount) : 0);
    stats.avgMs = timing.avgMs;
    stats.minMs = timing.minMs;
    stats.maxMs = timing.maxMs;
    stats.resultCount = timing.resultCount;
    if (lastScan)
        stats.topChildren = std::move(lastScan->topChildren);
    if (semanticQuery && lastScan && !semanticQuery->empty()) {
        auto querySpan = std::span<const float>(semanticQuery->data(), semanticQuery->size());
        if (!lastScan->centroid.empty()) {
            stats.aggregateScore = dotProduct(
                std::span<const float>(lastScan->centroid.data(), lastScan->centroid.size()),
                querySpan);
        }

        double bestChildScore = -std::numeric_limits<double>::infinity();
        std::string bestChildPath;
        for (const auto& [childPath, _] : stats.topChildren) {
            const int ordinal = extractOrdinalFromPath(childPath);
            if (ordinal < 0)
                continue;
            auto childEmbedding = makeOrdinalEmbedding(ordinal);
            double score = dotProduct(
                std::span<const float>(childEmbedding.data(), childEmbedding.size()), querySpan);
            if (score > bestChildScore) {
                bestChildScore = score;
                bestChildPath = childPath;
            }
        }
        if (bestChildScore > -std::numeric_limits<double>::infinity()) {
            stats.bestChildScore = bestChildScore;
            stats.bestChildPath = std::move(bestChildPath);
        }
    }
    return stats;
}

void reportScenario(const std::string& prefix, const RunStats& baseline,
                    const PathTreeRunStats& prototype) {
    spdlog::info("=== Prefix '{}' ===", prefix);
    spdlog::info("Baseline   : avg {:.3f} ms (min {:.3f}, max {:.3f}) -> {} docs", baseline.avgMs,
                 baseline.minMs, baseline.maxMs, baseline.resultCount);
    spdlog::info("Path-tree  : avg {:.3f} ms (min {:.3f}, max {:.3f}) -> {} docs", prototype.avgMs,
                 prototype.minMs, prototype.maxMs, prototype.resultCount);

    if (baseline.resultCount != prototype.resultCount) {
        spdlog::warn("Result count mismatch for '{}': baseline={}, path-tree={}", prefix,
                     baseline.resultCount, prototype.resultCount);
    }

    const double deltaMs = prototype.avgMs - baseline.avgMs;
    const double pct = baseline.avgMs > 0.0 ? (deltaMs / baseline.avgMs) * 100.0 : 0.0;
    spdlog::info("Delta      : {:+.3f} ms ({:+.2f}%)", deltaMs, pct);

    if (!prototype.topChildren.empty()) {
        spdlog::info("Top children by doc_count:");
        for (const auto& [childPath, count] : prototype.topChildren) {
            spdlog::info("  {} -> {} docs", childPath, count);
        }
    }
}

SemanticRunStats measureSemanticBaseline(const BenchmarkFixture& fixture, std::string_view prefix,
                                         const std::vector<float>& queryVec, int iterations) {
    using namespace std::chrono;
    using namespace yams::metadata;

    DocumentQueryOptions opts;
    opts.pathPrefix = std::string(prefix);
    opts.includeSubdirectories = true;

    std::vector<double> durations;
    durations.reserve(iterations);
    std::optional<std::size_t> lastCount;
    double bestScore = -std::numeric_limits<double>::infinity();
    std::string bestPath;

    auto querySpan = std::span<const float>(queryVec.data(), queryVec.size());

    for (int i = 0; i < iterations; ++i) {
        const auto start = steady_clock::now();
        auto res = fixture.repo->queryDocuments(opts);
        const auto end = steady_clock::now();

        if (!res) {
            spdlog::warn("Semantic baseline query failed for '{}': {}", prefix,
                         res.error().message);
            continue;
        }

        durations.push_back(duration<double, std::milli>(end - start).count());
        lastCount = res.value().size();

        double iterationBest = -std::numeric_limits<double>::infinity();
        std::string iterationPath;
        for (const auto& doc : res.value()) {
            const int ordinal = extractOrdinalFromPath(doc.filePath);
            if (ordinal < 0)
                continue;
            auto embedding = makeOrdinalEmbedding(ordinal);
            double score =
                dotProduct(std::span<const float>(embedding.data(), embedding.size()), querySpan);
            if (score > iterationBest) {
                iterationBest = score;
                iterationPath = doc.filePath;
            }
        }
        if (iterationBest > -std::numeric_limits<double>::infinity()) {
            bestScore = iterationBest;
            bestPath = std::move(iterationPath);
        }
    }

    SemanticRunStats stats;
    stats.timing = computeStats(durations, lastCount.value_or(0));
    stats.bestScore = bestScore;
    stats.bestPath = bestPath;
    return stats;
}

void reportSemanticScenario(const std::string& prefix, const SemanticRunStats& baseline,
                            const PathTreeRunStats& prototype) {
    spdlog::info("=== Semantic prefix '{}' ===", prefix);
    spdlog::info("Baseline   : avg {:.3f} ms (min {:.3f}, max {:.3f}) -> {} docs",
                 baseline.timing.avgMs, baseline.timing.minMs, baseline.timing.maxMs,
                 baseline.timing.resultCount);
    spdlog::info("Baseline best: '{}' score {:.3f}", baseline.bestPath, baseline.bestScore);
    spdlog::info("Path-tree  : avg {:.3f} ms (min {:.3f}, max {:.3f}) -> {} docs", prototype.avgMs,
                 prototype.minMs, prototype.maxMs, prototype.resultCount);
    spdlog::info("Path-tree aggregate score {:.3f}, best child '{}' score {:.3f}",
                 prototype.aggregateScore, prototype.bestChildPath, prototype.bestChildScore);

    const double deltaMs = prototype.avgMs - baseline.timing.avgMs;
    const double pct =
        baseline.timing.avgMs > 0.0 ? (deltaMs / baseline.timing.avgMs) * 100.0 : 0.0;
    spdlog::info("Delta      : {:+.3f} ms ({:+.2f}%)", deltaMs, pct);
}

} // namespace

int main() {
    try {
        BenchmarkFixture fixture;

        std::vector<std::string> rawPrefixes = fixture.topPrefixes(3);
        if (rawPrefixes.empty()) {
            rawPrefixes = {"/project/src", "/project/tests", "/project/docs"};
        }
        {
            std::ostringstream oss;
            for (std::size_t i = 0; i < rawPrefixes.size(); ++i) {
                if (i)
                    oss << ", ";
                oss << rawPrefixes[i];
            }
            spdlog::info("search_tree_bench prefixes: [{}]", oss.str());
        }
        constexpr int kIterations = 25;
        constexpr std::size_t kChildLimit = 5;
        constexpr int kDiffIterations = 15;

        fixture.captureBaselineCounts(rawPrefixes);

        nlohmann::json prefixArray = nlohmann::json::array();
        nlohmann::json semanticArray = nlohmann::json::array();
        std::unordered_map<std::string, RunStats> baselineTimingByPrefix;

        auto toChildArray = [](const PathTreeRunStats& stats) {
            nlohmann::json arr = nlohmann::json::array();
            for (const auto& [path, count] : stats.topChildren) {
                arr.push_back({{"path", path}, {"doc_count", count}});
            }
            return arr;
        };

        for (const auto& rawPrefix : rawPrefixes) {
            const auto prefix = normalizePrefix(rawPrefix);

            auto baselinePrefix = measureBaselineSearch(fixture, prefix, kIterations);
            auto treePrefix = measurePathTree(fixture, prefix, kIterations, kChildLimit);
            reportScenario(prefix, baselinePrefix, treePrefix);

            baselineTimingByPrefix[prefix] = baselinePrefix;

            const double prefixDeltaMs = treePrefix.avgMs - baselinePrefix.avgMs;
            const double prefixDeltaPct =
                baselinePrefix.avgMs > 0.0 ? (prefixDeltaMs / baselinePrefix.avgMs) * 100.0 : 0.0;

            prefixArray.push_back({
                {"prefix", prefix},
                {"baseline",
                 {{"avg_ms", baselinePrefix.avgMs},
                  {"min_ms", baselinePrefix.minMs},
                  {"max_ms", baselinePrefix.maxMs},
                  {"docs", baselinePrefix.resultCount}}},
                {"path_tree",
                 {{"avg_ms", treePrefix.avgMs},
                  {"min_ms", treePrefix.minMs},
                  {"max_ms", treePrefix.maxMs},
                  {"docs", treePrefix.resultCount},
                  {"top_children", toChildArray(treePrefix)}}},
                {"delta_ms", prefixDeltaMs},
                {"delta_pct", prefixDeltaPct},
            });

            auto queryVec = makeQueryVectorForPrefix(prefix);
            auto baselineSemantic = measureSemanticBaseline(fixture, prefix, queryVec, kIterations);
            auto treeSemantic =
                measurePathTree(fixture, prefix, kIterations, kChildLimit, &queryVec);
            reportSemanticScenario(prefix, baselineSemantic, treeSemantic);

            const double semanticDeltaMs = treeSemantic.avgMs - baselineSemantic.timing.avgMs;
            const double semanticDeltaPct =
                baselineSemantic.timing.avgMs > 0.0
                    ? (semanticDeltaMs / baselineSemantic.timing.avgMs) * 100.0
                    : 0.0;

            semanticArray.push_back({
                {"prefix", prefix},
                {"baseline",
                 {{"avg_ms", baselineSemantic.timing.avgMs},
                  {"min_ms", baselineSemantic.timing.minMs},
                  {"max_ms", baselineSemantic.timing.maxMs},
                  {"docs", baselineSemantic.timing.resultCount},
                  {"best_path", baselineSemantic.bestPath},
                  {"best_score", baselineSemantic.bestScore}}},
                {"path_tree",
                 {{"avg_ms", treeSemantic.avgMs},
                  {"min_ms", treeSemantic.minMs},
                  {"max_ms", treeSemantic.maxMs},
                  {"docs", treeSemantic.resultCount},
                  {"aggregate_score", treeSemantic.aggregateScore},
                  {"best_child_path", treeSemantic.bestChildPath},
                  {"best_child_score", treeSemantic.bestChildScore},
                  {"top_children", toChildArray(treeSemantic)}}},
                {"delta_ms", semanticDeltaMs},
                {"delta_pct", semanticDeltaPct},
            });
        }

        fixture.applyDiffChanges(rawPrefixes);

        nlohmann::json diffArray = nlohmann::json::array();
        for (const auto& rawPrefix : rawPrefixes) {
            const auto prefix = normalizePrefix(rawPrefix);

            auto diffTreeStats = measurePathTree(fixture, prefix, kDiffIterations, kChildLimit);

            const int64_t baselineCount = fixture.baselineCountFor(prefix);
            const int64_t diffCount = fixture.diffCountFor(prefix);
            const double diffDeltaMs = diffTreeStats.avgMs - baselineTimingByPrefix[prefix].avgMs;
            const double diffDeltaPct =
                baselineTimingByPrefix[prefix].avgMs > 0.0
                    ? (diffDeltaMs / baselineTimingByPrefix[prefix].avgMs) * 100.0
                    : 0.0;

            diffArray.push_back({
                {"prefix", prefix},
                {"baseline",
                 {{"docs", baselineCount},
                  {"avg_ms", baselineTimingByPrefix[prefix].avgMs},
                  {"min_ms", baselineTimingByPrefix[prefix].minMs},
                  {"max_ms", baselineTimingByPrefix[prefix].maxMs}}},
                {"path_tree",
                 {{"docs", diffCount},
                  {"avg_ms", diffTreeStats.avgMs},
                  {"min_ms", diffTreeStats.minMs},
                  {"max_ms", diffTreeStats.maxMs},
                  {"top_children", toChildArray(diffTreeStats)}}},
                {"delta_docs", diffCount - baselineCount},
                {"delta_ms", diffDeltaMs},
                {"delta_pct", diffDeltaPct},
            });
        }

        nlohmann::json summary;
        summary["prefix_scenarios"] = std::move(prefixArray);
        summary["semantic_scenarios"] = std::move(semanticArray);
        summary["diff_scenarios"] = std::move(diffArray);

        const char* outputEnv = std::getenv("YAMS_BENCH_OUTPUT");
        std::filesystem::path defaultOutput = "data/benchmarks/search_tree/latest.json";
        std::filesystem::path outputPath =
            (outputEnv && std::string_view(outputEnv).empty() == false) ? outputEnv : defaultOutput;
        if (!outputPath.empty() && outputPath.has_parent_path()) {
            std::error_code ec;
            std::filesystem::create_directories(outputPath.parent_path(), ec);
        }
        if (std::ofstream out{outputPath}; out) {
            out << summary.dump(2) << std::endl;
        } else {
            spdlog::warn("Failed to write benchmark summary to '{}'", outputPath.string());
        }

        if (!outputEnv || std::string_view(outputEnv).empty()) {
            auto now = std::chrono::system_clock::now();
            std::time_t tt = std::chrono::system_clock::to_time_t(now);
            std::tm tm{};
#ifdef _WIN32
            localtime_s(&tm, &tt);
#else
            localtime_r(&tt, &tm);
#endif
            std::ostringstream oss;
            oss << "run-" << std::put_time(&tm, "%Y%m%d-%H%M%S") << ".json";
            std::filesystem::path historyPath = outputPath.parent_path() / oss.str();
            if (std::ofstream hist{historyPath}; hist) {
                hist << summary.dump(2) << std::endl;
            } else {
                spdlog::warn("Failed to write benchmark history file '{}'", historyPath.string());
            }
        }

        std::cout << summary.dump(2) << std::endl;

        spdlog::info("Search tree benchmark comparison complete.");
        return 0;
    } catch (const std::exception& ex) {
        spdlog::error("Search tree benchmark failed: {}", ex.what());
        return 1;
    }
}
