// Benchmark tree-based list queries with various filter combinations
#include <benchmark/benchmark.h>

#include <cstdlib>
#include <filesystem>
#include <memory>
#include <random>
#include <stdexcept>
#include <string>
#include <vector>

#include <yams/metadata/connection_pool.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/path_utils.h>

using namespace yams;
using namespace yams::metadata;

namespace {

struct RepoContext {
    std::filesystem::path dbPath;
    std::unique_ptr<ConnectionPool> pool;
    std::unique_ptr<MetadataRepository> repo;
};

DocumentInfo makeDocWithPathAndTags(const std::string& path, const std::string& hash,
                                    const std::vector<std::string>& /* tags */,
                                    const std::string& mimeType = "text/plain") {
    DocumentInfo info;
    info.filePath = path;
    info.fileName = std::filesystem::path(path).filename().string();
    info.fileExtension = std::filesystem::path(path).extension().string();
    info.fileSize = 4096;
    info.sha256Hash = hash;
    info.mimeType = mimeType;
    auto now_s = std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
    info.createdTime = now_s;
    info.modifiedTime = now_s;
    info.indexedTime = now_s;
    info.contentExtracted = true;
    info.extractionStatus = ExtractionStatus::Success;

    auto derived = computePathDerivedValues(path);
    info.filePath = derived.normalizedPath;
    info.pathPrefix = derived.pathPrefix;
    info.reversePath = derived.reversePath;
    info.pathHash = derived.pathHash;
    info.parentHash = derived.parentHash;
    info.pathDepth = derived.pathDepth;

    return info;
}

RepoContext makeRepositoryWithHierarchy(std::size_t filesPerDir, std::size_t dirCount) {
    RepoContext ctx;
    ctx.dbPath = std::filesystem::temp_directory_path() /
                 ("tree_list_bench_" + std::to_string(std::rand()) + ".db");
    std::filesystem::remove(ctx.dbPath);

    ConnectionPoolConfig config;
    config.minConnections = 1;
    config.maxConnections = 4;
    ctx.pool = std::make_unique<ConnectionPool>(ctx.dbPath.string(), config);
    auto init = ctx.pool->initialize();
    if (!init)
        throw std::runtime_error("Failed to initialize pool: " + init.error().message);

    ctx.repo = std::make_unique<MetadataRepository>(*ctx.pool);

    std::vector<std::string> dirs = {"/project/src/app",     "/project/src/lib",
                                     "/project/src/core",    "/project/docs/api",
                                     "/project/docs/guides", "/project/docs/tutorials",
                                     "/project/tests/unit",  "/project/tests/integration",
                                     "/project/benchmarks",  "/project/examples"};

    std::vector<std::string> extensions = {".cpp", ".hpp", ".h", ".md", ".txt", ".json"};
    std::vector<std::string> mimeTypes = {"text/x-c++",    "text/x-c++", "text/x-c",
                                          "text/markdown", "text/plain", "application/json"};

    std::vector<std::string> tagSets[] = {{"code", "source", "cpp"}, {"code", "header"},
                                          {"docs", "api"},           {"docs", "guide"},
                                          {"test", "unit"},          {"test", "integration"}};

    std::mt19937 rng(12345);
    std::uniform_int_distribution<size_t> extDist(0, extensions.size() - 1);
    std::uniform_int_distribution<size_t> tagDist(0, 5);

    int64_t docId = 0;
    for (std::size_t d = 0; d < std::min(dirCount, dirs.size()); ++d) {
        for (std::size_t f = 0; f < filesPerDir; ++f) {
            size_t extIdx = extDist(rng);
            auto path = dirs[d] + "/file_" + std::to_string(f) + extensions[extIdx];
            auto hash = "HASH" + std::to_string(docId) +
                        std::string(60 - std::to_string(docId).size(), 'A');

            auto doc = makeDocWithPathAndTags(path, hash.substr(0, 64), tagSets[tagDist(rng)],
                                              mimeTypes[extIdx]);

            auto inserted = ctx.repo->insertDocument(doc);
            if (!inserted)
                throw std::runtime_error("insertDocument failed: " + inserted.error().message);

            docId = inserted.value();

            // Add tags as metadata
            for (const auto& tag : tagSets[tagDist(rng)]) {
                ctx.repo->setMetadata(docId, tag, MetadataValue("true"));
            }

            // Upsert path tree node for this document
            auto treeRes = ctx.repo->upsertPathTreeForDocument(doc, docId, true, {});
            if (!treeRes)
                throw std::runtime_error("upsertPathTree failed");

            ++docId;
        }
    }

    return ctx;
}

class TreeListFilterFixture : public benchmark::Fixture {
public:
    void SetUp(const ::benchmark::State& /* state */) override {
        std::size_t filesPerDir = 10;
        std::size_t dirCount = 10;
        ctx_ = makeRepositoryWithHierarchy(filesPerDir, dirCount);
    }

    void TearDown(const ::benchmark::State&) override {
        ctx_.repo.reset();
        if (ctx_.pool)
            ctx_.pool->shutdown();
        ctx_.pool.reset();
        std::error_code ec;
        std::filesystem::remove(ctx_.dbPath, ec);
    }

protected:
    RepoContext ctx_;
};

// Benchmark 1: Tree query with path prefix only (baseline)
BENCHMARK_DEFINE_F(TreeListFilterFixture, TreeQueryPathPrefixOnly)(benchmark::State& state) {
    DocumentQueryOptions opts;
    opts.pathPrefix = "/project/src";
    opts.prefixIsDirectory = true;
    opts.limit = 100;

    for (auto _ : state) {
        auto result = ctx_.repo->queryDocuments(opts);
        if (!result)
            state.SkipWithError("Query failed");
        benchmark::DoNotOptimize(result);
    }

    state.SetItemsProcessed(state.iterations());
}

// Benchmark 2: Tree query with path prefix + single tag filter
BENCHMARK_DEFINE_F(TreeListFilterFixture, TreeQueryPathPrefixWithTag)(benchmark::State& state) {
    DocumentQueryOptions opts;
    opts.pathPrefix = "/project/src";
    opts.prefixIsDirectory = true;
    opts.tags = {"code"};
    opts.limit = 100;

    for (auto _ : state) {
        auto result = ctx_.repo->queryDocuments(opts);
        if (!result)
            state.SkipWithError("Query failed");
        benchmark::DoNotOptimize(result);
    }

    state.SetItemsProcessed(state.iterations());
}

// Benchmark 3: Tree query with path prefix + multiple tags
BENCHMARK_DEFINE_F(TreeListFilterFixture,
                   TreeQueryPathPrefixWithMultipleTags)(benchmark::State& state) {
    DocumentQueryOptions opts;
    opts.pathPrefix = "/project/src";
    opts.prefixIsDirectory = true;
    opts.tags = {"code", "source"};
    opts.limit = 100;

    for (auto _ : state) {
        auto result = ctx_.repo->queryDocuments(opts);
        if (!result)
            state.SkipWithError("Query failed");
        benchmark::DoNotOptimize(result);
    }

    state.SetItemsProcessed(state.iterations());
}

// Benchmark 4: Tree query with path prefix + MIME type filter
BENCHMARK_DEFINE_F(TreeListFilterFixture, TreeQueryPathPrefixWithMime)(benchmark::State& state) {
    DocumentQueryOptions opts;
    opts.pathPrefix = "/project/src";
    opts.prefixIsDirectory = true;
    opts.mimeType = "text/x-c++";
    opts.limit = 100;

    for (auto _ : state) {
        auto result = ctx_.repo->queryDocuments(opts);
        if (!result)
            state.SkipWithError("Query failed");
        benchmark::DoNotOptimize(result);
    }

    state.SetItemsProcessed(state.iterations());
}

// Benchmark 5: Tree query with path prefix + extension filter
BENCHMARK_DEFINE_F(TreeListFilterFixture,
                   TreeQueryPathPrefixWithExtension)(benchmark::State& state) {
    DocumentQueryOptions opts;
    opts.pathPrefix = "/project/src";
    opts.prefixIsDirectory = true;
    opts.extension = ".cpp";
    opts.limit = 100;

    for (auto _ : state) {
        auto result = ctx_.repo->queryDocuments(opts);
        if (!result)
            state.SkipWithError("Query failed");
        benchmark::DoNotOptimize(result);
    }

    state.SetItemsProcessed(state.iterations());
}

// Benchmark 6: Tree query with all filters combined
BENCHMARK_DEFINE_F(TreeListFilterFixture,
                   TreeQueryPathPrefixWithAllFilters)(benchmark::State& state) {
    DocumentQueryOptions opts;
    opts.pathPrefix = "/project/src";
    opts.prefixIsDirectory = true;
    opts.tags = {"code"};
    opts.mimeType = "text/x-c++";
    opts.extension = ".cpp";
    opts.limit = 100;

    for (auto _ : state) {
        auto result = ctx_.repo->queryDocuments(opts);
        if (!result)
            state.SkipWithError("Query failed");
        benchmark::DoNotOptimize(result);
    }

    state.SetItemsProcessed(state.iterations());
}

// Register benchmarks
BENCHMARK_REGISTER_F(TreeListFilterFixture, TreeQueryPathPrefixOnly)->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(TreeListFilterFixture, TreeQueryPathPrefixWithTag)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(TreeListFilterFixture, TreeQueryPathPrefixWithMultipleTags)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(TreeListFilterFixture, TreeQueryPathPrefixWithMime)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(TreeListFilterFixture, TreeQueryPathPrefixWithExtension)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(TreeListFilterFixture, TreeQueryPathPrefixWithAllFilters)
    ->Unit(benchmark::kMicrosecond);

} // namespace

BENCHMARK_MAIN();
