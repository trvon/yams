#include <benchmark/benchmark.h>

#include <chrono>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include <yams/metadata/connection_pool.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/path_utils.h>

using namespace yams::metadata;

namespace {

struct BenchContext {
    std::filesystem::path dbPath;
    std::unique_ptr<ConnectionPool> pool;
    std::unique_ptr<MetadataRepository> repo;
    std::vector<DocumentInfo> docs;
};

DocumentInfo makeDoc(std::size_t i) {
    DocumentInfo info;
    const std::string path = "/bench/fts5/doc_" + std::to_string(i) + ".md";
    info.filePath = path;
    info.fileName = std::filesystem::path(path).filename().string();
    info.fileExtension = ".md";
    info.fileSize = 4096;
    info.sha256Hash = ("FTS5" + std::to_string(i) + std::string(64, 'a')).substr(0, 64);
    info.mimeType = "text/markdown";
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

std::string makeTitle(std::size_t i) {
    return "FTS5 benchmark document " + std::to_string(i);
}

std::string makeContent(std::size_t i) {
    return "alpha benchmark token " + std::to_string(i) +
           "\n\nThis document exercises the live FTS5 indexing path used by post-ingest metadata "
           "writes. "
           "The content includes repeated search terms: alpha beta gamma retrieval daemon plugin "
           "logging.";
}

BenchContext makeContext(std::size_t count) {
    BenchContext ctx;
    const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
    ctx.dbPath = std::filesystem::temp_directory_path() /
                 ("fts5_index_bench_" + std::to_string(stamp) + ".db");
    std::filesystem::remove(ctx.dbPath);

    ConnectionPoolConfig cfg;
    cfg.minConnections = 1;
    cfg.maxConnections = 2;
    ctx.pool = std::make_unique<ConnectionPool>(ctx.dbPath.string(), cfg);
    auto init = ctx.pool->initialize();
    if (!init) {
        throw std::runtime_error("pool init failed: " + init.error().message);
    }
    ctx.repo = std::make_unique<MetadataRepository>(*ctx.pool);

    for (std::size_t i = 0; i < count; ++i) {
        auto doc = makeDoc(i);
        auto inserted = ctx.repo->insertDocument(doc);
        if (!inserted) {
            throw std::runtime_error("insertDocument failed: " + inserted.error().message);
        }
        doc.id = inserted.value();
        ctx.docs.push_back(doc);
    }

    return ctx;
}

void destroyContext(BenchContext& ctx) {
    ctx.repo.reset();
    if (ctx.pool) {
        ctx.pool->shutdown();
    }
    ctx.pool.reset();
    std::error_code ec;
    std::filesystem::remove(ctx.dbPath, ec);
    ctx = BenchContext{};
}

class Fts5IndexFixture : public benchmark::Fixture {
public:
    void SetUp(const benchmark::State& state) override {
        ctx_ = makeContext(static_cast<std::size_t>(std::max<int64_t>(state.range(0), 1)));
    }

    void TearDown(const benchmark::State&) override { destroyContext(ctx_); }

protected:
    BenchContext ctx_;
};

BENCHMARK_DEFINE_F(Fts5IndexFixture, IndexDocumentContentTrusted)(benchmark::State& state) {
    auto& doc = ctx_.docs.front();
    const std::string title = makeTitle(0);
    const std::string content = makeContent(0);

    for (auto _ : state) {
        auto result = ctx_.repo->indexDocumentContentTrusted(doc.id, title, content, doc.mimeType);
        benchmark::DoNotOptimize(result);
        if (!result) {
            state.SkipWithError(result.error().message.c_str());
            break;
        }
    }
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * content.size()));
}

BENCHMARK_DEFINE_F(Fts5IndexFixture, BatchInsertContentAndIndex)(benchmark::State& state) {
    std::vector<BatchContentEntry> entries;
    entries.reserve(ctx_.docs.size());
    for (std::size_t i = 0; i < ctx_.docs.size(); ++i) {
        entries.push_back(BatchContentEntry{
            .documentId = ctx_.docs[i].id,
            .title = makeTitle(i),
            .abstract = "fts5 benchmark abstract",
            .contentText = makeContent(i),
            .mimeType = ctx_.docs[i].mimeType,
            .extractionMethod = "bench",
            .language = "en",
        });
    }

    for (auto _ : state) {
        auto result = ctx_.repo->batchInsertContentAndIndex(entries);
        benchmark::DoNotOptimize(result);
        if (!result) {
            state.SkipWithError(result.error().message.c_str());
            break;
        }
    }

    std::size_t totalBytes = 0;
    for (const auto& e : entries) {
        totalBytes += e.contentText.size();
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * entries.size()));
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * totalBytes));
}

BENCHMARK_DEFINE_F(Fts5IndexFixture, SearchKeyword)(benchmark::State& state) {
    std::vector<BatchContentEntry> entries;
    entries.reserve(ctx_.docs.size());
    for (std::size_t i = 0; i < ctx_.docs.size(); ++i) {
        entries.push_back(BatchContentEntry{
            .documentId = ctx_.docs[i].id,
            .title = makeTitle(i),
            .abstract = "fts5 benchmark abstract",
            .contentText = makeContent(i),
            .mimeType = ctx_.docs[i].mimeType,
            .extractionMethod = "bench",
            .language = "en",
        });
    }
    auto prep = ctx_.repo->batchInsertContentAndIndex(entries);
    if (!prep) {
        state.SkipWithError(prep.error().message.c_str());
        return;
    }

    for (auto _ : state) {
        auto result = ctx_.repo->search("alpha retrieval", 10, 0);
        benchmark::DoNotOptimize(result);
        if (!result) {
            state.SkipWithError(result.error().message.c_str());
            break;
        }
        benchmark::DoNotOptimize(result.value().results.size());
    }
}

BENCHMARK_REGISTER_F(Fts5IndexFixture, IndexDocumentContentTrusted)
    ->Arg(1)
    ->Unit(benchmark::kMicrosecond)
    ->Iterations(50);
BENCHMARK_REGISTER_F(Fts5IndexFixture, BatchInsertContentAndIndex)
    ->Arg(32)
    ->Arg(128)
    ->Unit(benchmark::kMillisecond)
    ->Iterations(20);
BENCHMARK_REGISTER_F(Fts5IndexFixture, SearchKeyword)
    ->Arg(128)
    ->Arg(512)
    ->Unit(benchmark::kMicrosecond)
    ->Iterations(50);

} // namespace

BENCHMARK_MAIN();
