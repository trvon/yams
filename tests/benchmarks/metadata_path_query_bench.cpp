#include <benchmark/benchmark.h>

#include <algorithm>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>
#include <tuple>
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
    std::vector<int64_t> docIds;
    std::vector<std::string> hashes;
    std::vector<DocumentInfo> docs;
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

std::string makeBenchHash(std::size_t seed) {
    std::string hash = "BENCH" + std::to_string(seed);
    if (hash.size() < 64) {
        hash.append(64 - hash.size(), static_cast<char>('A' + (seed % 16)));
    } else if (hash.size() > 64) {
        hash.resize(64);
    }
    return hash;
}

DocumentInfo makeNestedDocWithDepth(std::size_t depth, std::size_t seed,
                                    const std::string& extension = ".txt") {
    std::string path = "/bench";
    for (std::size_t i = 0; i < depth; ++i) {
        path += "/level_" + std::to_string(i) + "_" + std::to_string(seed % 17);
    }
    path += "/file_" + std::to_string(seed) + extension;
    return makeDocWithPath(path, makeBenchHash(seed));
}

DocumentInfo insertDocumentOrThrow(MetadataRepository& repo, DocumentInfo info) {
    auto inserted = repo.insertDocument(info);
    if (!inserted) {
        throw std::runtime_error("insertDocument failed: " + inserted.error().message);
    }
    info.id = inserted.value();
    return info;
}

std::vector<float> makeEmbeddingValues(std::size_t dimensions, float seed = 0.25F) {
    std::vector<float> values(dimensions);
    for (std::size_t i = 0; i < dimensions; ++i) {
        values[i] = seed + static_cast<float>(i) * 0.01F;
    }
    return values;
}

void destroyRepository(RepoContext& ctx) {
    ctx.repo.reset();
    if (ctx.pool) {
        ctx.pool->shutdown();
    }
    ctx.pool.reset();
    std::error_code ec;
    std::filesystem::remove(ctx.dbPath, ec);
    ctx = RepoContext{};
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

        const int64_t docId = inserted.value();
        doc.id = docId;
        ctx.docIds.push_back(docId);
        ctx.hashes.push_back(doc.sha256Hash);
        ctx.docs.push_back(doc);

        auto tag =
            ctx.repo->setMetadata(docId, "tag:bench", MetadataValue(i % 2 == 0 ? "even" : "odd"));
        if (!tag)
            throw std::runtime_error("setMetadata tag:bench failed: " + tag.error().message);
        auto category = ctx.repo->setMetadata(docId, "category",
                                              MetadataValue(i % 5 == 0 ? "markdown" : "plain"));
        if (!category)
            throw std::runtime_error("setMetadata category failed: " + category.error().message);
    }

    return ctx;
}

class MetadataQueryFixture : public benchmark::Fixture {
public:
    void SetUp(const ::benchmark::State& state) override {
        std::size_t count = static_cast<std::size_t>(state.range(0));
        ctx_ = makeRepositoryWithDocs(count);
    }

    void TearDown(const ::benchmark::State&) override { destroyRepository(ctx_); }

protected:
    RepoContext ctx_;
};

class MetadataPathTreeFixture : public benchmark::Fixture {
public:
    void SetUp(const ::benchmark::State& state) override {
        ctx_ = makeRepositoryWithDocs(0);
        embeddingValues_ =
            makeEmbeddingValues(static_cast<std::size_t>(std::max<int64_t>(state.range(1), 1)));
    }

    void TearDown(const ::benchmark::State&) override { destroyRepository(ctx_); }

protected:
    RepoContext ctx_;
    std::vector<float> embeddingValues_;
    std::size_t nextDocSeed_{0};
};

class MetadataPathDepthFixture : public benchmark::Fixture {
public:
    void SetUp(const ::benchmark::State& state) override {
        ctx_ = makeRepositoryWithDocs(0);

        const auto backgroundCount = static_cast<std::size_t>(std::max<int64_t>(state.range(0), 1));
        for (std::size_t i = 0; i < backgroundCount; ++i) {
            ctx_.docs.push_back(
                insertDocumentOrThrow(*ctx_.repo, makeNestedDocWithDepth(2 + (i % 8), i + 1)));
        }

        deepTemplate_ = makeNestedDocWithDepth(12, backgroundCount + 1000);
        shallowTemplate_ = makeDocWithPath("/bench/shallow/file.txt", deepTemplate_.sha256Hash);
        currentDeepDoc_ = insertDocumentOrThrow(*ctx_.repo, deepTemplate_);
        shallowCurrent_ = shallowTemplate_;
        shallowCurrent_.id = currentDeepDoc_.id;
    }

    void TearDown(const ::benchmark::State&) override { destroyRepository(ctx_); }

protected:
    void reinsertDeepDocument() {
        currentDeepDoc_ = insertDocumentOrThrow(*ctx_.repo, deepTemplate_);
        shallowCurrent_ = shallowTemplate_;
        shallowCurrent_.id = currentDeepDoc_.id;
    }

    RepoContext ctx_;
    DocumentInfo deepTemplate_;
    DocumentInfo shallowTemplate_;
    DocumentInfo currentDeepDoc_;
    DocumentInfo shallowCurrent_;
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

BENCHMARK_DEFINE_F(MetadataQueryFixture, BatchGetDocumentsByHash)(benchmark::State& state) {
    const auto batchSize =
        std::min<std::size_t>(static_cast<std::size_t>(state.range(1)), ctx_.hashes.size());
    std::vector<std::string> hashes(ctx_.hashes.begin(), ctx_.hashes.begin() + batchSize);
    for (auto _ : state) {
        auto res = ctx_.repo->batchGetDocumentsByHash(hashes);
        benchmark::DoNotOptimize(res);
        if (!res)
            state.SkipWithError("batchGetDocumentsByHash failed");
        else if (res.value().size() != batchSize)
            state.SkipWithError("batchGetDocumentsByHash returned wrong result count");
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(batchSize));
}

BENCHMARK_DEFINE_F(MetadataQueryFixture, MetadataValueCountsByTag)(benchmark::State& state) {
    DocumentQueryOptions opts;
    opts.pathPrefix = "/workspace";
    opts.prefixIsDirectory = true;
    opts.includeSubdirectories = true;
    const std::vector<std::string> keys = {"tag:bench", "category"};
    for (auto _ : state) {
        auto res = ctx_.repo->getMetadataValueCounts(keys, opts);
        benchmark::DoNotOptimize(res);
        if (!res)
            state.SkipWithError("getMetadataValueCounts failed");
        else if (res.value().at("tag:bench").empty())
            state.SkipWithError("getMetadataValueCounts returned no tag counts");
    }
}

BENCHMARK_DEFINE_F(MetadataQueryFixture, BatchSetMetadata)(benchmark::State& state) {
    const auto batchSize =
        std::min<std::size_t>(static_cast<std::size_t>(state.range(1)), ctx_.docIds.size());
    std::vector<std::tuple<int64_t, std::string, MetadataValue>> entries;
    entries.reserve(batchSize);
    for (std::size_t i = 0; i < batchSize; ++i) {
        entries.emplace_back(ctx_.docIds[i], "bench:touch", MetadataValue(static_cast<int64_t>(i)));
    }

    for (auto _ : state) {
        auto res = ctx_.repo->setMetadataBatch(entries);
        benchmark::DoNotOptimize(res);
        if (!res)
            state.SkipWithError("setMetadataBatch failed");
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(batchSize));
}

BENCHMARK_DEFINE_F(MetadataQueryFixture, BatchUpdateExtractionStatus)(benchmark::State& state) {
    const auto batchSize =
        std::min<std::size_t>(static_cast<std::size_t>(state.range(1)), ctx_.docIds.size());
    std::vector<ExtractionStatusUpdate> updates;
    updates.reserve(batchSize);
    for (std::size_t i = 0; i < batchSize; ++i) {
        updates.push_back(
            ExtractionStatusUpdate{ctx_.docIds[i], true, ExtractionStatus::Success, ""});
    }

    for (auto _ : state) {
        auto res = ctx_.repo->batchUpdateDocumentExtractionStatuses(updates);
        benchmark::DoNotOptimize(res);
        if (!res)
            state.SkipWithError("batchUpdateDocumentExtractionStatuses failed");
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(batchSize));
}

BENCHMARK_DEFINE_F(MetadataQueryFixture, BatchGetMetadataChunked)(benchmark::State& state) {
    const auto batchSize =
        std::min<std::size_t>(static_cast<std::size_t>(state.range(1)), ctx_.docIds.size());
    std::vector<int64_t> ids(ctx_.docIds.begin(), ctx_.docIds.begin() + batchSize);

    for (auto _ : state) {
        auto res = ctx_.repo->getMetadataForDocuments(ids);
        benchmark::DoNotOptimize(res);
        if (!res) {
            state.SkipWithError("getMetadataForDocuments failed");
            return;
        }
        if (res.value().size() != batchSize) {
            state.SkipWithError("getMetadataForDocuments returned wrong result count");
            return;
        }
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(batchSize));
}

BENCHMARK_DEFINE_F(MetadataPathTreeFixture, UpsertPathTreeForDocument)(benchmark::State& state) {
    const auto depth = static_cast<std::size_t>(std::max<int64_t>(state.range(0), 1));

    for (auto _ : state) {
        state.PauseTiming();
        auto doc = insertDocumentOrThrow(*ctx_.repo,
                                         makeNestedDocWithDepth(depth, nextDocSeed_++ + 10000));
        state.ResumeTiming();

        auto res = ctx_.repo->upsertPathTreeForDocument(doc, doc.id, true, embeddingValues_);
        benchmark::DoNotOptimize(res);
        if (!res) {
            state.SkipWithError("upsertPathTreeForDocument failed");
            return;
        }

        state.PauseTiming();
        auto cleanup = ctx_.repo->removePathTreeForDocument(doc, doc.id, embeddingValues_);
        if (!cleanup) {
            state.SkipWithError("removePathTreeForDocument cleanup failed");
            return;
        }
        auto deleted = ctx_.repo->deleteDocument(doc.id);
        if (!deleted) {
            state.SkipWithError("deleteDocument cleanup failed");
            return;
        }
        state.ResumeTiming();
    }

    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(depth));
}

BENCHMARK_DEFINE_F(MetadataPathTreeFixture, RemovePathTreeForDocument)(benchmark::State& state) {
    const auto depth = static_cast<std::size_t>(std::max<int64_t>(state.range(0), 1));

    for (auto _ : state) {
        state.PauseTiming();
        auto doc = insertDocumentOrThrow(*ctx_.repo,
                                         makeNestedDocWithDepth(depth, nextDocSeed_++ + 20000));
        auto seeded = ctx_.repo->upsertPathTreeForDocument(doc, doc.id, true, embeddingValues_);
        if (!seeded) {
            state.SkipWithError("upsertPathTreeForDocument setup failed");
            return;
        }
        state.ResumeTiming();

        auto res = ctx_.repo->removePathTreeForDocument(doc, doc.id, embeddingValues_);
        benchmark::DoNotOptimize(res);
        if (!res) {
            state.SkipWithError("removePathTreeForDocument failed");
            return;
        }

        state.PauseTiming();
        auto deleted = ctx_.repo->deleteDocument(doc.id);
        if (!deleted) {
            state.SkipWithError("deleteDocument cleanup failed");
            return;
        }
        state.ResumeTiming();
    }

    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(depth));
}

BENCHMARK_DEFINE_F(MetadataPathDepthFixture,
                   UpdateDocumentPathDepthShrink)(benchmark::State& state) {
    for (auto _ : state) {
        auto res = ctx_.repo->updateDocument(shallowCurrent_);
        benchmark::DoNotOptimize(res);
        if (!res) {
            state.SkipWithError("updateDocument shrink failed");
            return;
        }

        state.PauseTiming();
        auto restore = ctx_.repo->updateDocument(currentDeepDoc_);
        if (!restore) {
            state.SkipWithError("updateDocument restore failed");
            return;
        }
        state.ResumeTiming();
    }
}

BENCHMARK_DEFINE_F(MetadataPathDepthFixture,
                   DeleteDocumentPathDepthShrink)(benchmark::State& state) {
    for (auto _ : state) {
        auto res = ctx_.repo->deleteDocument(currentDeepDoc_.id);
        benchmark::DoNotOptimize(res);
        if (!res) {
            state.SkipWithError("deleteDocument failed");
            return;
        }

        state.PauseTiming();
        reinsertDeepDocument();
        state.ResumeTiming();
    }
}

BENCHMARK_REGISTER_F(MetadataQueryFixture, QueryExactPath)->Arg(128)->Arg(1024);
BENCHMARK_REGISTER_F(MetadataQueryFixture, QueryDirectoryPrefix)->Arg(128)->Arg(1024);
BENCHMARK_REGISTER_F(MetadataQueryFixture, QuerySuffixFts)->Arg(128)->Arg(1024);
BENCHMARK_REGISTER_F(MetadataQueryFixture, BatchGetDocumentsByHash)
    ->Args({128, 16})
    ->Args({1024, 64});
BENCHMARK_REGISTER_F(MetadataQueryFixture, MetadataValueCountsByTag)->Arg(128)->Arg(1024);
BENCHMARK_REGISTER_F(MetadataQueryFixture, BatchSetMetadata)->Args({128, 16})->Args({1024, 64});
BENCHMARK_REGISTER_F(MetadataQueryFixture, BatchUpdateExtractionStatus)
    ->Args({128, 16})
    ->Args({1024, 64});
BENCHMARK_REGISTER_F(MetadataQueryFixture, BatchGetMetadataChunked)
    ->Args({1024, 1024})
    ->Args({4096, 2048});
BENCHMARK_REGISTER_F(MetadataPathTreeFixture, UpsertPathTreeForDocument)
    ->Args({4, 16})
    ->Args({12, 16});
BENCHMARK_REGISTER_F(MetadataPathTreeFixture, RemovePathTreeForDocument)
    ->Args({4, 16})
    ->Args({12, 16});
BENCHMARK_REGISTER_F(MetadataPathDepthFixture, UpdateDocumentPathDepthShrink)->Arg(128)->Arg(1024);
BENCHMARK_REGISTER_F(MetadataPathDepthFixture, DeleteDocumentPathDepthShrink)->Arg(128)->Arg(1024);

} // namespace

BENCHMARK_MAIN();
