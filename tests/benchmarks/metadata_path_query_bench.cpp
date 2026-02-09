#include <benchmark/benchmark.h>

#include <cstdlib>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include <yams/metadata/connection_pool.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/path_utils.h>
#include <yams/metadata/query_helpers.h>

using namespace yams;
using namespace yams::metadata;

namespace {

struct RepoContext {
    std::filesystem::path dbPath;
    std::unique_ptr<ConnectionPool> pool;
    std::unique_ptr<MetadataRepository> repo;
};

DocumentInfo makeDocWithPath(const std::string& path, const std::string& hash) {
    DocumentInfo info;
    info.filePath = path;
    info.fileName = std::filesystem::path(path).filename().string();
    info.fileExtension = std::filesystem::path(path).extension().string();
    info.fileSize = 1764;
    info.sha256Hash = hash;
    info.mimeType = info.fileExtension == ".pdf" ? "application/pdf" : "text/plain";
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

RepoContext makeRepositoryWithDocs(std::size_t count) {
    RepoContext ctx;
    ctx.dbPath = std::filesystem::temp_directory_path() /
                 ("metadata_query_bench_" + std::to_string(std::rand()) + ".db");
    std::filesystem::remove(ctx.dbPath);

    ConnectionPoolConfig config;
    config.minConnections = 1;
    config.maxConnections = 2;
    ctx.pool = std::make_unique<ConnectionPool>(ctx.dbPath.string(), config);
    auto init = ctx.pool->initialize();
    if (!init)
        throw std::runtime_error("Failed to initialize pool: " + init.error().message);

    ctx.repo = std::make_unique<MetadataRepository>(*ctx.pool);

    for (std::size_t i = 0; i < count; ++i) {
        auto dir = "/workspace/project" + std::to_string(i % 16);
        auto basename = std::string("file_") + std::to_string(i) + ((i % 5 == 0) ? ".md" : ".txt");
        auto path = dir + "/" + basename;
        auto hash = "HASH" + std::to_string(i) + std::string(60, 'A' + (i % 16));
        auto doc = makeDocWithPath(path, hash.substr(0, 64));
        auto inserted = ctx.repo->insertDocument(doc);
        if (!inserted)
            throw std::runtime_error("insertDocument failed: " + inserted.error().message);
    }

    return ctx;
}

class MetadataQueryFixture : public benchmark::Fixture {
public:
    void SetUp(const ::benchmark::State& state) override {
        std::size_t count = static_cast<std::size_t>(state.range(0));
        ctx_ = makeRepositoryWithDocs(count);
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

BENCHMARK_DEFINE_F(MetadataQueryFixture, QueryExactPath)(benchmark::State& state) {
    DocumentQueryOptions opts;
    // i=5 is always present for Arg(128)/Arg(1024) and routes to project5 (i % 16).
    opts.exactPath = "/workspace/project5/file_5.md";
    for (auto _ : state) {
        auto res = ctx_.repo->queryDocuments(opts);
        benchmark::DoNotOptimize(res);
        if (!res)
            state.SkipWithError("queryDocuments exact path failed");
        else if (res.value().empty())
            state.SkipWithError("queryDocuments exact path returned no results");
    }
}

BENCHMARK_DEFINE_F(MetadataQueryFixture, QueryDirectoryPrefix)(benchmark::State& state) {
    DocumentQueryOptions opts;
    opts.pathPrefix = "/workspace/project7";
    opts.prefixIsDirectory = true;
    opts.includeSubdirectories = true;
    for (auto _ : state) {
        auto res = ctx_.repo->queryDocuments(opts);
        benchmark::DoNotOptimize(res);
        if (!res)
            state.SkipWithError("queryDocuments prefix failed");
    }
}

BENCHMARK_DEFINE_F(MetadataQueryFixture, QuerySuffixFts)(benchmark::State& state) {
    DocumentQueryOptions opts;
    opts.containsFragment = "file_11.md";
    opts.containsUsesFts = true;
    for (auto _ : state) {
        auto res = ctx_.repo->queryDocuments(opts);
        benchmark::DoNotOptimize(res);
        if (!res)
            state.SkipWithError("queryDocuments suffix failed");
    }
}

BENCHMARK_REGISTER_F(MetadataQueryFixture, QueryExactPath)->Arg(128)->Arg(1024);
BENCHMARK_REGISTER_F(MetadataQueryFixture, QueryDirectoryPrefix)->Arg(128)->Arg(1024);
BENCHMARK_REGISTER_F(MetadataQueryFixture, QuerySuffixFts)->Arg(128)->Arg(1024);

} // namespace

BENCHMARK_MAIN();
