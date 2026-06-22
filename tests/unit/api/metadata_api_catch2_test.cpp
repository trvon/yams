// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

#include <catch2/catch_test_macros.hpp>

#include <yams/api/async_content_store.h>
#include <yams/api/metadata_api.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/path_utils.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <future>
#include <sstream>
#include <thread>

namespace yams::api::test {

// Catch2 decomposes comparison expressions inside CHECK/REQUIRE; this trips the
// chained-comparison lint even for ordinary assertions.
// NOLINTBEGIN(bugprone-chained-comparison)
namespace {

std::filesystem::path tempDbPath(const char* prefix) {
    const char* t = std::getenv("YAMS_TEST_TMPDIR");
    auto base = (t && *t) ? std::filesystem::path(t) : std::filesystem::temp_directory_path();
    std::error_code ec;
    std::filesystem::create_directories(base, ec);
    auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
    auto path = base / (std::string(prefix) + std::to_string(stamp) + ".db");
    std::filesystem::remove(path, ec);
    return path;
}

metadata::DocumentMetadata makeMetadata(std::string path, std::string hash,
                                        std::uint64_t size = 123, std::string mime = "text/plain") {
    metadata::DocumentMetadata metadata;
    metadata.info.filePath = std::move(path);
    metadata.info.fileName = std::filesystem::path(metadata.info.filePath).filename().string();
    metadata.info.fileExtension =
        std::filesystem::path(metadata.info.filePath).extension().string();
    metadata.info.sha256Hash = std::move(hash);
    metadata.info.fileSize = size;
    metadata.info.mimeType = std::move(mime);
    metadata.info.createdTime =
        std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
    metadata.info.modifiedTime = metadata.info.createdTime;
    metadata.info.indexedTime = metadata.info.createdTime;
    metadata::populatePathDerivedFields(metadata.info);
    return metadata;
}

struct MetadataApiFixture {
    MetadataApiFixture() {
        dbPath = tempDbPath("metadata_api_catch2_test_");
        metadata::ConnectionPoolConfig config;
        config.minConnections = 1;
        config.maxConnections = 2;
        pool = std::make_unique<metadata::ConnectionPool>(dbPath.string(), config);
        REQUIRE(pool->initialize().has_value());
        repository = std::make_shared<metadata::MetadataRepository>(*pool);
    }

    ~MetadataApiFixture() {
        repository.reset();
        if (pool) {
            pool->shutdown();
            pool.reset();
        }
        std::error_code ec;
        std::filesystem::remove(dbPath, ec);
    }

    std::filesystem::path dbPath;
    std::unique_ptr<metadata::ConnectionPool> pool;
    std::shared_ptr<metadata::MetadataRepository> repository;
};

class ScriptedContentStore final : public IContentStore {
public:
    std::atomic<int> existsCalls{0};
    std::atomic<int> removeCalls{0};
    std::atomic<int> metadataCalls{0};
    std::atomic<int> updateCalls{0};

    Result<StoreResult> store(const std::filesystem::path& path, const ContentMetadata&,
                              ProgressCallback progress) override {
        if (progress) {
            progress(Progress{.bytesProcessed = 1, .totalBytes = 1, .currentOperation = "store"});
        }
        StoreResult result;
        result.contentHash = path.filename().string();
        result.bytesStored = 1;
        return result;
    }

    Result<RetrieveResult> retrieve(const std::string&, const std::filesystem::path&,
                                    ProgressCallback) override {
        RetrieveResult result;
        result.found = true;
        result.size = 1;
        return result;
    }

    Result<StoreResult> storeStream(std::istream& stream, const ContentMetadata&,
                                    ProgressCallback) override {
        std::string ignored((std::istreambuf_iterator<char>(stream)),
                            std::istreambuf_iterator<char>());
        StoreResult result;
        result.contentHash = "stream";
        result.bytesStored = ignored.size();
        return result;
    }

    Result<RetrieveResult> retrieveStream(const std::string&, std::ostream& output,
                                          ProgressCallback) override {
        output << "data";
        RetrieveResult result;
        result.found = true;
        result.size = 4;
        return result;
    }

    Result<StoreResult> storeBytes(std::span<const std::byte> data,
                                   const ContentMetadata&) override {
        StoreResult result;
        result.contentHash = "bytes";
        result.bytesStored = data.size();
        return result;
    }

    Result<std::vector<std::byte>> retrieveBytes(const std::string&) override {
        return std::vector<std::byte>{std::byte{'x'}};
    }

    Result<std::vector<std::byte>> retrieveBytesPrefix(const std::string&,
                                                       std::size_t maxBytes) override {
        return std::vector<std::byte>(maxBytes, std::byte{'p'});
    }

    Result<RawContent> retrieveRaw(const std::string&) override { return RawContent{}; }

    std::future<Result<RawContent>> retrieveRawAsync(const std::string& hash) override {
        return std::async(std::launch::deferred, [this, hash] { return retrieveRaw(hash); });
    }

    Result<bool> exists(const std::string& hash) const override {
        const_cast<ScriptedContentStore*>(this)->existsCalls.fetch_add(1,
                                                                       std::memory_order_relaxed);
        return hash == "present";
    }

    Result<bool> remove(const std::string& hash) override {
        removeCalls.fetch_add(1, std::memory_order_relaxed);
        return hash == "present";
    }

    Result<ContentMetadata> getMetadata(const std::string&) const override {
        const_cast<ScriptedContentStore*>(this)->metadataCalls.fetch_add(1,
                                                                         std::memory_order_relaxed);
        ContentMetadata metadata;
        metadata.mimeType = "text/plain";
        return metadata;
    }

    Result<void> updateMetadata(const std::string&, const ContentMetadata&) override {
        updateCalls.fetch_add(1, std::memory_order_relaxed);
        return Result<void>();
    }

    std::vector<Result<StoreResult>> storeBatch(const std::vector<std::filesystem::path>& paths,
                                                const std::vector<ContentMetadata>&) override {
        std::vector<Result<StoreResult>> results;
        for (const auto& path : paths) {
            results.push_back(store(path, {}, nullptr));
        }
        return results;
    }

    std::vector<Result<bool>> removeBatch(const std::vector<std::string>& hashes) override {
        std::vector<Result<bool>> results;
        for (const auto& hash : hashes) {
            results.push_back(remove(hash));
        }
        return results;
    }

    ContentStoreStats getStats() const override { return {}; }
    HealthStatus checkHealth() const override { return {}; }
    Result<void> verify(ProgressCallback) override { return Result<void>(); }
    Result<void> compact(ProgressCallback) override { return Result<void>(); }
    Result<void> garbageCollect(ProgressCallback) override { return Result<void>(); }
};

} // namespace

TEST_CASE("Metadata request and response DTOs expose safe defaults", "[api][metadata-api][dto]") {
    CreateMetadataRequest create;
    CHECK(create.validateUniqueness);
    CHECK_FALSE(create.indexImmediately);
    CHECK(create.timestamp.time_since_epoch().count() > 0);

    QueryMetadataRequest query;
    CHECK(query.limit == 100);
    CHECK(query.includeCount);
    CHECK_FALSE(query.includeStats);

    ImportMetadataRequest import;
    CHECK(import.continueOnError);
    CHECK(import.conflictResolution == ImportMetadataRequest::ConflictResolution::Skip);

    MetadataResponse response;
    response.setError(ErrorCode::InvalidArgument, "bad request");
    CHECK_FALSE(response.success);
    CHECK(response.errorCode == ErrorCode::InvalidArgument);
    CHECK(response.message == "bad request");

    response.setSuccess("ok");
    CHECK(response.success);
    CHECK(response.errorCode == ErrorCode::Success);
    CHECK(response.message == "ok");

    BulkCreateResponse::CreateResult created;
    CHECK_FALSE(created.success);
    CHECK(created.documentId == 0);

    ValidateMetadataResponse::ValidationError validationError{"path", "invalid", "use absolute"};
    CHECK(validationError.field == "path");
}

TEST_CASE("MetadataApi validates and creates metadata through repository", "[api][metadata-api]") {
    MetadataApiFixture fixture;
    MetadataApi api(fixture.repository);

    ValidateMetadataRequest invalid;
    invalid.requestId = "validate-invalid";
    auto invalidResponse = api.validateMetadata(invalid);
    CHECK(invalidResponse.success);
    CHECK_FALSE(invalidResponse.isValid);
    CHECK_FALSE(invalidResponse.errors.empty());

    CreateMetadataRequest create;
    create.requestId = "create-one";
    create.metadata = makeMetadata("/tmp/yams-api/a.txt", "hash-a");
    auto created = api.createMetadata(create);
    REQUIRE(created.success);
    REQUIRE(created.documentId != 0);
    CHECK(created.createdMetadata.info.id == created.documentId);

    GetMetadataRequest get;
    get.requestId = "get-one";
    get.documentId = created.documentId;
    auto got = api.getMetadata(get);
    REQUIRE(got.success);
    CHECK(got.metadata.info.sha256Hash == "hash-a");

    ValidateMetadataRequest duplicate;
    duplicate.metadata = create.metadata;
    auto duplicateResponse = api.validateMetadata(duplicate);
    CHECK(duplicateResponse.success);
    CHECK_FALSE(duplicateResponse.uniquenessValid);

    CHECK(api.getStatistics().totalRequests >= 2);
    CHECK(api.getStatistics().successfulRequests >= 2);
    CHECK(api.isHealthy());
}

TEST_CASE("MetadataApi handles query import export and async paths", "[api][metadata-api]") {
    MetadataApiFixture fixture;
    auto api = MetadataApiFactory::create(fixture.repository, MetadataApiConfig{});

    BulkCreateRequest bulk;
    bulk.documents.push_back(makeMetadata("/tmp/yams-api/b.txt", "hash-b", 10, "text/plain"));
    bulk.documents.push_back(makeMetadata("/tmp/yams-api/c.md", "hash-c", 20, "text/markdown"));
    auto bulkResponse = api->bulkCreate(bulk);
    REQUIRE(bulkResponse.success);
    CHECK(bulkResponse.successCount == 2);

    QueryMetadataRequest query;
    query.filter.contentType = "text/plain";
    query.includeStats = true;
    query.sortBy = QueryMetadataRequest::SortField::Path;
    query.ascending = true;
    auto queried = api->queryMetadata(query);
    REQUIRE(queried.success);
    CHECK(queried.offset == 0);
    CHECK(queried.limit == 100);

    auto exportPath = fixture.dbPath.parent_path() / "metadata_api_export.json";
    ExportMetadataRequest exportRequest;
    exportRequest.outputPath = exportPath.string();
    auto exported = api->exportMetadata(exportRequest);
    REQUIRE(exported.success);
    CHECK(std::filesystem::exists(exportPath));

    auto importPath = fixture.dbPath.parent_path() / "metadata_api_import.json";
    {
        std::ofstream input(importPath);
        input
            << R"([{"title":"imported.txt","path":"/tmp/yams-api/imported.txt","contentType":"text/plain","fileSize":42,"contentHash":"hash-imported"}])";
    }

    ImportMetadataRequest importRequest;
    importRequest.inputPath = importPath.string();
    importRequest.continueOnError = true;
    auto imported = api->importMetadata(importRequest);
    CHECK(imported.success);
    CHECK(imported.documentsImported == 1);

    GetMetadataRequest asyncGet;
    asyncGet.contentHash = "hash-imported";
    auto asyncResponse = api->getMetadataAsync(asyncGet).get();
    CHECK(asyncResponse.success);

    GetStatisticsRequest statsRequest;
    auto stats = api->getStatistics(statsRequest);
    CHECK(stats.success);
    CHECK(stats.totalDocuments >= 1);

    std::error_code ec;
    std::filesystem::remove(exportPath, ec);
}

TEST_CASE("AsyncContentStore forwards future and callback operations",
          "[api][async-content-store]") {
    auto scripted = std::make_shared<ScriptedContentStore>();
    AsyncContentStore async(scripted);

    CHECK(async.getMaxConcurrentOperations() == 10);
    async.setMaxConcurrentOperations(2);
    CHECK(async.getMaxConcurrentOperations() == 2);
    CHECK_THROWS_AS(async.setMaxConcurrentOperations(0), std::invalid_argument);

    auto exists = async.existsAsync("present").get();
    REQUIRE(exists.has_value());
    CHECK(exists.value());

    auto removed = async.removeAsync("missing").get();
    REQUIRE(removed.has_value());
    CHECK_FALSE(removed.value());

    auto metadata = async.getMetadataAsync("present").get();
    REQUIRE(metadata.has_value());
    CHECK(metadata.value().mimeType == "text/plain");

    ContentMetadata replacement;
    replacement.mimeType = "application/test";
    CHECK(async.updateMetadataAsync("present", replacement).get().has_value());

    std::promise<Result<bool>> callbackPromise;
    auto callbackFuture = callbackPromise.get_future();
    async.existsAsync("present", [&callbackPromise](const Result<bool>& result) {
        callbackPromise.set_value(result);
    });
    auto callbackResult = callbackFuture.get();
    REQUIRE(callbackResult.has_value());
    CHECK(callbackResult.value());

    CHECK(scripted->existsCalls.load(std::memory_order_relaxed) >= 2);
    CHECK(scripted->removeCalls.load(std::memory_order_relaxed) == 1);
    CHECK(scripted->metadataCalls.load(std::memory_order_relaxed) == 1);
    CHECK(scripted->updateCalls.load(std::memory_order_relaxed) == 1);
    async.waitAll();
}

// NOLINTEND(bugprone-chained-comparison)
} // namespace yams::api::test
