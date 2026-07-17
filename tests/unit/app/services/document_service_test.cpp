#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <future>
#include <memory>
#include <span>
#include <thread>
#include <catch2/catch_test_macros.hpp>
#include <yams/api/content_store_builder.h>
#include <yams/app/services/services.hpp>
#include <yams/app/services/session_service.hpp>
#include <yams/compat/unistd.h>
#include <yams/core/cpp23_features.hpp>
#include <yams/daemon/components/WriteCoordinator.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/database.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/migration.h>
#include <yams/search/search_engine.h>
#include <yams/vector/vector_database.h>

#include "tests/common/test_helpers_catch2.h"

using namespace yams;
using namespace yams::app::services;
using namespace yams::metadata;
using namespace yams::api;

struct DocumentFixture {
    DocumentFixture() {
        setupTestEnvironment();
        setupDatabase();
        setupServices();
        setupTestDocuments();
    }

    ~DocumentFixture() {
        documentService_.reset();
        searchEngine_.reset();
        contentStore_.reset();

        // Clear all shared_ptr refs to metadataRepo BEFORE pool/database
        // destruction.  appContext_ holds its own copy of the shared_ptr,
        // and C++ destroys members in reverse declaration order — but
        // appContext_ is declared AFTER pool_, so pool_ would be destroyed
        // while appContext_ still owns a MetadataRepository that references it.
        metadataRepo_.reset();
        appContext_ = {}; // drops shared_ptr copies held by AppContext

        if (pool_) {
            pool_->shutdown();
            pool_.reset();
        }
        if (database_) {
            database_->close();
            database_.reset();
        }

        if (!testDir_.empty() && std::filesystem::exists(testDir_)) {
            std::error_code ec;
            std::filesystem::remove_all(testDir_, ec);
        }
    }

    void setupTestEnvironment() {
        auto pid = std::to_string(::getpid());
        auto timestamp =
            std::to_string(std::chrono::system_clock::now().time_since_epoch().count());
        testDir_ = std::filesystem::temp_directory_path() /
                   ("document_service_test_" + pid + "_" + timestamp);

        std::error_code ec;
        std::filesystem::create_directories(testDir_, ec);
        REQUIRE_FALSE(ec);
    }

    void setupDatabase() {
        dbPath_ = std::filesystem::absolute(testDir_ / "test.db");
        database_ = std::make_unique<Database>();
        auto openResult = database_->open(dbPath_.string(), ConnectionMode::Create);
        REQUIRE(openResult);

        ConnectionPoolConfig poolConfig;
        poolConfig.maxConnections = 4;
        pool_ = std::make_unique<ConnectionPool>(dbPath_.string(), poolConfig);
        metadataRepo_ = std::make_shared<MetadataRepository>(*pool_);

        MigrationManager mm(*database_);
        auto initResult = mm.initialize();
        REQUIRE(initResult);

        mm.registerMigrations(YamsMetadataMigrations::getAllMigrations());
        auto migrateResult = mm.migrate();
        REQUIRE(migrateResult);
    }

    void setupServices() {
        ContentStoreBuilder builder;
        auto storeResult = builder.withStoragePath(testDir_ / "storage")
                               .withChunkSize(65536)
                               .withCompression(true)
                               .withDeduplication(true)
                               .build();
        REQUIRE(storeResult);

        auto& uniqueStore = storeResult.value();
        contentStore_ = std::shared_ptr<IContentStore>(
            const_cast<std::unique_ptr<IContentStore>&>(uniqueStore).release());

        searchEngine_ = nullptr;

        appContext_.store = contentStore_;
        appContext_.metadataRepo = metadataRepo_;
        appContext_.searchEngine = searchEngine_;

        documentService_ = makeDocumentService(appContext_);
    }

    void setupTestDocuments() {
        auto testFile1 = testDir_ / "test1.txt";
        auto testFile2 = testDir_ / "test2.md";
        auto testFile3 = testDir_ / "libsample.so";

        std::ofstream{testFile1} << "Test document content for retrieval testing";
        std::ofstream{testFile2} << "Another test document with different content";
        {
            std::ofstream binary(testFile3, std::ios::binary);
            const unsigned char elfBytes[] = {0x7F, 0x45, 0x4C, 0x46, 0x02, 0x01, 0x01, 0x00};
            binary.write(reinterpret_cast<const char*>(elfBytes), sizeof(elfBytes));
        }

        StoreDocumentRequest storeReq1;
        storeReq1.path = testFile1.string();
        storeReq1.tags = {"test", "document"};

        StoreDocumentRequest storeReq2;
        storeReq2.path = testFile2.string();
        storeReq2.tags = {"test", "markdown"};

        StoreDocumentRequest storeReq3;
        storeReq3.path = testFile3.string();
        storeReq3.tags = {"test", "binary"};

        auto result1 = documentService_->store(storeReq1);
        auto result2 = documentService_->store(storeReq2);
        auto result3 = documentService_->store(storeReq3);

        REQUIRE(result1);
        REQUIRE(result2);
        REQUIRE(result3);

        testHash1_ = result1.value().hash;
        testHash2_ = result2.value().hash;
    }

    std::string createTestContent(std::string_view identifier) {
        return std::string("Test content for document: ") + std::string(identifier) +
               " created at " +
               std::to_string(std::chrono::system_clock::now().time_since_epoch().count());
    }

    std::filesystem::path testDir_;
    std::filesystem::path dbPath_;
    std::unique_ptr<Database> database_;
    std::unique_ptr<ConnectionPool> pool_;
    std::shared_ptr<MetadataRepository> metadataRepo_;
    std::shared_ptr<IContentStore> contentStore_;
    std::shared_ptr<search::SearchEngine> searchEngine_;
    AppContext appContext_;
    std::shared_ptr<IDocumentService> documentService_;
    std::string testHash1_;
    std::string testHash2_;
};

class RemoveFailingContentStore : public IContentStore {
public:
    RemoveFailingContentStore(std::shared_ptr<IContentStore> inner, std::string failHash,
                              Error error)
        : inner_(std::move(inner)), failHash_(std::move(failHash)), error_(std::move(error)) {}

    Result<StoreResult> store(const std::filesystem::path& path, const ContentMetadata& metadata,
                              yams::api::ProgressCallback progress) override {
        return inner_->store(path, metadata, progress);
    }

    Result<RetrieveResult> retrieve(const std::string& hash,
                                    const std::filesystem::path& outputPath,
                                    yams::api::ProgressCallback progress) override {
        return inner_->retrieve(hash, outputPath, progress);
    }

    Result<StoreResult> storeStream(std::istream& stream, const ContentMetadata& metadata,
                                    yams::api::ProgressCallback progress) override {
        return inner_->storeStream(stream, metadata, progress);
    }

    Result<RetrieveResult> retrieveStream(const std::string& hash, std::ostream& output,
                                          yams::api::ProgressCallback progress) override {
        return inner_->retrieveStream(hash, output, progress);
    }

    Result<StoreResult> storeBytes(std::span<const std::byte> data,
                                   const ContentMetadata& metadata) override {
        return inner_->storeBytes(data, metadata);
    }

    Result<std::vector<std::byte>> retrieveBytes(const std::string& hash) override {
        return inner_->retrieveBytes(hash);
    }

    Result<std::vector<std::byte>> retrieveBytesPrefix(const std::string& hash,
                                                       std::size_t maxBytes) override {
        return inner_->retrieveBytesPrefix(hash, maxBytes);
    }

    Result<RawContent> retrieveRaw(const std::string& hash) override {
        return inner_->retrieveRaw(hash);
    }

    std::future<Result<RawContent>> retrieveRawAsync(const std::string& hash) override {
        return inner_->retrieveRawAsync(hash);
    }

    Result<bool> exists(const std::string& hash) const override { return inner_->exists(hash); }

    Result<bool> remove(const std::string& hash) override {
        if (hash == failHash_) {
            return error_;
        }
        return inner_->remove(hash);
    }

    Result<ContentMetadata> getMetadata(const std::string& hash) const override {
        return inner_->getMetadata(hash);
    }

    Result<void> updateMetadata(const std::string& hash, const ContentMetadata& metadata) override {
        return inner_->updateMetadata(hash, metadata);
    }

    std::vector<Result<StoreResult>>
    storeBatch(const std::vector<std::filesystem::path>& paths,
               const std::vector<ContentMetadata>& metadata) override {
        return inner_->storeBatch(paths, metadata);
    }

    std::vector<Result<bool>> removeBatch(const std::vector<std::string>& hashes) override {
        return inner_->removeBatch(hashes);
    }

    ContentStoreStats getStats() const override { return inner_->getStats(); }

    HealthStatus checkHealth() const override { return inner_->checkHealth(); }

    Result<void> verify(yams::api::ProgressCallback progress) override {
        return inner_->verify(progress);
    }

    Result<void> compact(yams::api::ProgressCallback progress) override {
        return inner_->compact(progress);
    }

    Result<void> garbageCollect(yams::api::ProgressCallback progress) override {
        return inner_->garbageCollect(progress);
    }

private:
    std::shared_ptr<IContentStore> inner_;
    std::string failHash_;
    Error error_;
};

TEST_CASE("DocumentService batches content before metadata publication",
          "[document][service][batch][publication]") {
    DocumentFixture fixture;
    auto metadataWriter = std::make_shared<MetadataInsertWriter>(
        fixture.metadataRepo_, MetadataInsertWriter::Options{
                                   .maxBatchCount = 8, .maxDelay = std::chrono::milliseconds{5}});
    fixture.appContext_.metadataInsertWriter = metadataWriter;
    fixture.documentService_ = makeDocumentService(fixture.appContext_);
    metadataWriter->resetMetrics();
    resetDocumentStorePhaseTimings();

    std::vector<StoreDocumentRequest> requests;
    for (int i = 0; i < 2; ++i) {
        auto path = fixture.testDir_ / ("document-batch-" + std::to_string(i) + ".txt");
        std::ofstream(path) << "document batch " << i;
        StoreDocumentRequest request;
        request.path = path.string();
        request.snapshotId = "document-service-batch";
        request.skipInlineContentIndexing = true;
        requests.push_back(std::move(request));
    }

    auto results = fixture.documentService_->storeBatch(requests);
    REQUIRE(results.size() == requests.size());
    for (const auto& result : results) {
        REQUIRE(result.has_value());
        REQUIRE(result.value().documentId > 0);
        auto document = fixture.metadataRepo_->getDocumentByHash(result.value().hash);
        REQUIRE(document.has_value());
        REQUIRE(document.value().has_value());
    }

    const auto metrics = metadataWriter->metricsSnapshot();
    CHECK(metrics.submittedItems == 2);
    CHECK(metrics.completedItems == 2);
    CHECK(metrics.batches == 1);
    CHECK(metrics.maxBatchSize == 2);
    const auto timings = getDocumentStorePhaseTimingsSnapshot();
    CHECK(timings.at("content_store").calls == 1);
    CHECK(timings.at("store_total").calls == 1);
}

TEST_CASE("DocumentService batch reports metadata publication failures",
          "[document][service][batch][metadata][failure]") {
    DocumentFixture fixture;
    auto metadataWriter = std::make_shared<MetadataInsertWriter>(
        fixture.metadataRepo_, MetadataInsertWriter::Options{
                                   .maxBatchCount = 8, .maxDelay = std::chrono::milliseconds{5}});
    fixture.appContext_.metadataInsertWriter = metadataWriter;
    fixture.documentService_ = makeDocumentService(fixture.appContext_);

    auto conn = fixture.pool_->acquire();
    REQUIRE(conn.has_value());
    REQUIRE((*conn.value())
                ->execute(R"(
        CREATE TRIGGER abort_document_service_batch_insert
        BEFORE INSERT ON documents
        BEGIN
            SELECT RAISE(ABORT, 'injected metadata insert failure');
        END;
    )")
                .has_value());

    std::vector<StoreDocumentRequest> requests;
    for (int i = 0; i < 2; ++i) {
        auto path = fixture.testDir_ / ("metadata-failure-batch-" + std::to_string(i) + ".txt");
        std::ofstream(path) << "document whose metadata insert should fail " << i;
        StoreDocumentRequest request;
        request.path = path.string();
        request.snapshotId = "document-service-metadata-failure";
        request.skipInlineContentIndexing = true;
        requests.push_back(request);
    }

    auto results = fixture.documentService_->storeBatch(requests);
    REQUIRE(results.size() == requests.size());
    for (const auto& result : results) {
        REQUIRE_FALSE(result.has_value());
        CHECK(result.error().code == ErrorCode::DatabaseError);
        CHECK(
            (result.error().message.find("injected metadata insert failure") != std::string::npos));
    }
}

TEST_CASE("DocumentService batches fuzzy terms at the storage wave boundary",
          "[document][service][batch][symspell]") {
    DocumentFixture fixture;
    auto metadataWriter = std::make_shared<MetadataInsertWriter>(
        fixture.metadataRepo_, MetadataInsertWriter::Options{
                                   .maxBatchCount = 8, .maxDelay = std::chrono::milliseconds{5}});
    boost::asio::io_context io;
    daemon::WriteCoordinator::Config config;
    config.maxBatchDelayMs = std::chrono::milliseconds{1};
    daemon::WriteCoordinator coordinator(io, {}, fixture.metadataRepo_, config);

    fixture.appContext_.metadataInsertWriter = metadataWriter;
    fixture.appContext_.writeCoordinatorProvider = [&coordinator] { return &coordinator; };
    fixture.documentService_ = makeDocumentService(fixture.appContext_);

    std::vector<StoreDocumentRequest> requests;
    constexpr std::size_t kDocumentCount = 4;
    for (std::size_t i = 0; i < kDocumentCount; ++i) {
        auto path = fixture.testDir_ / ("fuzzy-batch-" + std::to_string(i) + ".txt");
        std::ofstream(path) << "fuzzy batch " << i;
        StoreDocumentRequest request;
        request.path = path.string();
        request.snapshotId = "document-service-fuzzy-batch";
        request.skipInlineContentIndexing = true;
        requests.push_back(std::move(request));
    }

    auto results = fixture.documentService_->storeBatch(requests);
    REQUIRE(results.size() == kDocumentCount);
    for (const auto& result : results) {
        REQUIRE(result.has_value());
    }

    coordinator.start();
    std::thread writerLoop([&io] { io.run(); });
    auto flushResult = coordinator.flush(std::chrono::seconds{10});
    coordinator.shutdown();
    io.stop();
    writerLoop.join();
    REQUIRE(flushResult.has_value());

    const auto stats = coordinator.getStats();
    const auto source = std::ranges::find_if(
        stats.hotSources, [](const auto& hotspot) { return hotspot.source == "doc_svc/symspell"; });
    REQUIRE(source != stats.hotSources.end());
    CHECK(source->batches == 1);
    CHECK(stats.symSpellTermsAdded == kDocumentCount * 2);

    fixture.appContext_.writeCoordinatorProvider = {};
    fixture.documentService_.reset();
}

TEST_CASE("DocumentService initializes fresh path series without follow-up writes",
          "[document][service][batch][versioning]") {
    DocumentFixture fixture;
    auto metadataWriter = std::make_shared<MetadataInsertWriter>(
        fixture.metadataRepo_, MetadataInsertWriter::Options{
                                   .maxBatchCount = 8, .maxDelay = std::chrono::milliseconds{5}});
    boost::asio::io_context io;
    daemon::WriteCoordinator::Config config;
    config.maxBatchDelayMs = std::chrono::milliseconds{1};
    daemon::WriteCoordinator coordinator(io, {}, fixture.metadataRepo_, config);

    fixture.appContext_.metadataInsertWriter = metadataWriter;
    fixture.appContext_.writeCoordinatorProvider = [&coordinator] { return &coordinator; };
    fixture.documentService_ = makeDocumentService(fixture.appContext_);

    std::vector<StoreDocumentRequest> requests;
    constexpr std::size_t kDocumentCount = 4;
    for (std::size_t i = 0; i < kDocumentCount; ++i) {
        auto path = fixture.testDir_ / ("versioned-batch-" + std::to_string(i) + ".txt");
        std::ofstream(path) << "versioned batch " << i;
        StoreDocumentRequest request;
        request.path = path.string();
        request.snapshotId = "document-service-versioned-batch";
        request.skipInlineContentIndexing = true;
        requests.push_back(std::move(request));
    }

    auto results = fixture.documentService_->storeBatch(requests);
    REQUIRE(results.size() == kDocumentCount);

    coordinator.start();
    std::thread writerLoop([&io] { io.run(); });
    auto flushResult = coordinator.flush(std::chrono::seconds{10});
    coordinator.shutdown();
    io.stop();
    writerLoop.join();
    REQUIRE(flushResult.has_value());

    for (std::size_t i = 0; i < kDocumentCount; ++i) {
        REQUIRE(results[i].has_value());
        const auto documentId = results[i].value().documentId;
        auto version = fixture.metadataRepo_->getMetadata(documentId, "version");
        auto latest = fixture.metadataRepo_->getMetadata(documentId, "is_latest");
        auto seriesKey = fixture.metadataRepo_->getMetadata(documentId, "series_key");
        auto document = fixture.metadataRepo_->getDocument(documentId);
        REQUIRE(version.has_value());
        REQUIRE(version.value().has_value());
        REQUIRE(latest.has_value());
        REQUIRE(latest.value().has_value());
        REQUIRE(seriesKey.has_value());
        REQUIRE(seriesKey.value().has_value());
        REQUIRE(document.has_value());
        REQUIRE(document.value().has_value());
        CHECK(version.value()->asInteger() == 1);
        CHECK(latest.value()->asBoolean());
        CHECK(seriesKey.value()->asString() == document.value()->filePath);
    }

    const auto stats = coordinator.getStats();
    CHECK(stats.metadataEntriesSet == 0);
    const auto versioning = std::ranges::find_if(stats.hotSources, [](const auto& hotspot) {
        return hotspot.source == "doc_svc/versioning";
    });
    CHECK(versioning == stats.hotSources.end());

    fixture.appContext_.writeCoordinatorProvider = {};
    fixture.documentService_.reset();
}

TEST_CASE("DocumentService - Listing", "[document][service][listing]") {
    DocumentFixture fixture;

    SECTION("List all documents") {
        ListDocumentsRequest request;
        request.limit = 100;

        auto result = fixture.documentService_->list(request);

        REQUIRE(result);
        CHECK(result.value().documents.size() >= 2);
    }

    SECTION("List with limit") {
        ListDocumentsRequest request;
        request.limit = 1;

        auto result = fixture.documentService_->list(request);

        REQUIRE(result);
        CHECK(result.value().documents.size() == 1);
    }

    SECTION("List with offset") {
        ListDocumentsRequest request;
        request.limit = 100;
        request.offset = 1;

        auto result = fixture.documentService_->list(request);

        REQUIRE(result);
        CHECK(result.value().documents.size() >= 1);
    }

    SECTION("List bare directory returns descendants") {
        namespace fs = std::filesystem;
        auto normalizedPathString = [](const fs::path& path) {
            std::error_code ec;
            auto resolved = fs::weakly_canonical(path, ec);
            if (ec) {
                resolved = path.lexically_normal();
            }
            auto normalized = resolved.generic_string();
#ifdef _WIN32
            std::transform(normalized.begin(), normalized.end(), normalized.begin(),
                           [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
#endif
            return normalized;
        };

        fs::create_directories(fixture.testDir_ / "nested" / "deep");
        std::ofstream(fixture.testDir_ / "nested" / "deep" / "descendant.txt")
            << "descendant content";

        StoreDocumentRequest nestedReq;
        nestedReq.path = (fixture.testDir_ / "nested" / "deep" / "descendant.txt").string();
        nestedReq.tags = {"nested"};
        auto nestedStore = fixture.documentService_->store(nestedReq);
        REQUIRE(nestedStore);

        const auto expectedPath =
            normalizedPathString(fixture.testDir_ / "nested" / "deep" / "descendant.txt");

        ListDocumentsRequest request;
        request.pattern = (fixture.testDir_ / "nested").string();
        request.limit = 100;

        auto result = fixture.documentService_->list(request);

        REQUIRE(result);
        bool foundDescendant = false;
        for (const auto& doc : result.value().documents) {
            if (normalizedPathString(doc.path) == expectedPath) {
                foundDescendant = true;
                break;
            }
        }
        CHECK(foundDescendant);
    }

    SECTION("List with tag filter") {
        ListDocumentsRequest request;
        request.limit = 100;
        request.tags = {"markdown"};

        auto result = fixture.documentService_->list(request);

        REQUIRE(result);

        bool foundMarkdown = false;
        for (const auto& doc : result.value().documents) {
            if (std::find(doc.tags.begin(), doc.tags.end(), "markdown") != doc.tags.end()) {
                foundMarkdown = true;
                break;
            }
        }
        CHECK(foundMarkdown);
    }

    SECTION("List with type filter") {
        ListDocumentsRequest request;
        request.limit = 100;
        request.type = "text";

        auto result = fixture.documentService_->list(request);

        REQUIRE(result);
    }

    SECTION("List with executable type filter") {
        ListDocumentsRequest request;
        request.limit = 100;
        request.type = "executable";

        auto result = fixture.documentService_->list(request);

        REQUIRE(result);
        REQUIRE_FALSE(result.value().documents.empty());

        bool foundSharedObject = false;
        for (const auto& doc : result.value().documents) {
            CHECK(doc.fileType == "executable");
            if (doc.fileName == "libsample.so") {
                foundSharedObject = true;
                CHECK(doc.mimeType == "application/x-sharedlib");
            }
        }
        CHECK(foundSharedObject);
    }

    SECTION("List recent documents") {
        ListDocumentsRequest request;
        request.limit = 5;
        request.recent = 5;

        auto result = fixture.documentService_->list(request);

        REQUIRE(result);
        CHECK(result.value().documents.size() >= 2);
    }
}

TEST_CASE("DocumentService - Retrieval", "[document][service][retrieval]") {
    DocumentFixture fixture;

    SECTION("Retrieve document by hash") {
        REQUIRE_FALSE(fixture.testHash1_.empty());

        RetrieveDocumentRequest request;
        request.hash = fixture.testHash1_;

        auto result = fixture.documentService_->retrieve(request);

        REQUIRE(result);
        REQUIRE(result.value().document.has_value());
        CHECK(result.value().document->hash == fixture.testHash1_);
    }

    SECTION("Retrieve with compressed content and metadata") {
        REQUIRE_FALSE(fixture.testHash1_.empty());

        RetrieveDocumentRequest request;
        request.hash = fixture.testHash1_;
        request.includeContent = true;

        auto result = fixture.documentService_->retrieve(request);

        REQUIRE(result);
        REQUIRE(result.value().document.has_value());
        CHECK(result.value().document->hash == fixture.testHash1_);
        CHECK(result.value().document->content.has_value());
    }

    SECTION("Retrieve document by name") {
        RetrieveDocumentRequest request;
        request.name = (fixture.testDir_ / "test1.txt").string();

        auto result = fixture.documentService_->retrieve(request);

        REQUIRE(result);
        REQUIRE(result.value().document.has_value());
        CHECK_FALSE(result.value().document->hash.empty());
    }

    SECTION("Retrieve non-existent document") {
        RetrieveDocumentRequest request;
        request.hash = "nonexistent_hash_12345";

        auto result = fixture.documentService_->retrieve(request);

        CHECK_FALSE(result);
    }
}

TEST_CASE("DocumentService - Name Resolution", "[document][service][resolve]") {
    DocumentFixture fixture;

    SECTION("Resolve name to hash via tree lookup") {
        auto path = (fixture.testDir_ / "test1.txt").string();

        RetrieveDocumentRequest request;
        request.name = path;

        auto result = fixture.documentService_->retrieve(request);

        REQUIRE(result);
        REQUIRE(result.value().document.has_value());
        CHECK_FALSE(result.value().document->hash.empty());
    }

    SECTION("Resolve by hash directly") {
        REQUIRE_FALSE(fixture.testHash1_.empty());

        RetrieveDocumentRequest request;
        request.hash = fixture.testHash1_;

        auto result = fixture.documentService_->retrieve(request);

        REQUIRE(result);
        REQUIRE(result.value().document.has_value());
        CHECK(result.value().document->hash == fixture.testHash1_);
    }

    SECTION("Resolve path suffix") {
        RetrieveDocumentRequest request;
        request.name = "test1.txt";

        auto result = fixture.documentService_->retrieve(request);

        REQUIRE(result);
        REQUIRE(result.value().document.has_value());
        CHECK_FALSE(result.value().document->hash.empty());
    }

    SECTION("Resolve not found") {
        RetrieveDocumentRequest request;
        request.name = "nonexistent_file.txt";

        auto result = fixture.documentService_->retrieve(request);

        CHECK_FALSE(result);
    }

    SECTION("Resolve with normalized path separators") {
        auto pathWithMixedSeparators = fixture.testDir_.string() + "/test1.txt";

        RetrieveDocumentRequest request;
        request.name = pathWithMixedSeparators;

        auto result = fixture.documentService_->retrieve(request);

        REQUIRE(result);
        REQUIRE(result.value().document.has_value());
    }

    SECTION("Resolve with empty name and hash") {
        RetrieveDocumentRequest request;

        auto result = fixture.documentService_->retrieve(request);

        CHECK_FALSE(result);
    }

    SECTION("Resolve relative path from cwd") {
        auto originalCwd = std::filesystem::current_path();
        std::filesystem::current_path(fixture.testDir_);

        RetrieveDocumentRequest request;
        request.name = "test1.txt";

        auto result = fixture.documentService_->retrieve(request);

        std::filesystem::current_path(originalCwd);

        REQUIRE(result);
        REQUIRE(result.value().document.has_value());
        CHECK_FALSE(result.value().document->hash.empty());
    }
}

TEST_CASE("DocumentService - Deletion", "[document][service][deletion]") {
    DocumentFixture fixture;

    SECTION("Delete document by name") {
        DeleteByNameRequest request;
        request.name = (fixture.testDir_ / "test2.md").string();

        auto result = fixture.documentService_->deleteByName(request);

        REQUIRE(result);
        CHECK_FALSE(result.value().deleted.empty());
    }

    SECTION("Delete document removes vector rows") {
        yams::test::ScopedEnvVar enableVectors("YAMS_DISABLE_VECTORS",
                                               std::optional<std::string>("0"));
        yams::test::ScopedEnvVar enableSqliteVecInit("YAMS_SQLITE_VEC_SKIP_INIT",
                                                     std::optional<std::string>("0"));

        yams::vector::VectorDatabaseConfig config;
        config.database_path = ":memory:";
        config.embedding_dim = 4;
        config.create_if_missing = true;
        config.use_in_memory = true;

        auto vectorDb = std::make_shared<yams::vector::VectorDatabase>(config);
        if (!vectorDb->initialize()) {
            SKIP(std::string("Vector database unavailable: ") + vectorDb->getLastError());
        }

        yams::vector::VectorRecord record;
        record.chunk_id = "delete_cleanup_chunk";
        record.document_hash = fixture.testHash2_;
        record.embedding = {1.0f, 0.0f, 0.0f, 0.0f};
        record.content = "vector cleanup content";
        record.start_offset = 0;
        record.end_offset = 22;
        REQUIRE(vectorDb->insertVector(record));
        REQUIRE(vectorDb->hasEmbedding(fixture.testHash2_));

        fixture.appContext_.vectorDatabase = vectorDb;
        fixture.documentService_ = makeDocumentService(fixture.appContext_);

        DeleteByNameRequest request;
        request.name = (fixture.testDir_ / "test2.md").string();

        auto result = fixture.documentService_->deleteByName(request);

        REQUIRE(result);
        CHECK_FALSE(result.value().deleted.empty());
        CHECK_FALSE(vectorDb->hasEmbedding(fixture.testHash2_));
    }

    SECTION("Delete document cleans orphan metadata when content is already missing") {
        auto removed = fixture.contentStore_->remove(fixture.testHash2_);
        REQUIRE(removed);

        auto before = fixture.metadataRepo_->getDocumentByHash(fixture.testHash2_);
        REQUIRE(before);
        REQUIRE(before.value().has_value());

        DeleteByNameRequest request;
        request.name = (fixture.testDir_ / "test2.md").string();

        auto result = fixture.documentService_->deleteByName(request);

        REQUIRE(result);
        CHECK(result.value().errors.empty());
        REQUIRE(result.value().deleted.size() == 1);
        CHECK(result.value().deleted.front().deleted);

        auto after = fixture.metadataRepo_->getDocumentByHash(fixture.testHash2_);
        REQUIRE(after);
        CHECK_FALSE(after.value().has_value());
    }

    SECTION("Delete document force-cleans metadata when store removal reports corruption") {
        auto failingStore = std::make_shared<RemoveFailingContentStore>(
            fixture.contentStore_, fixture.testHash2_,
            Error{ErrorCode::IOError, "Corrupted data while removing object"});
        fixture.appContext_.store = failingStore;
        fixture.documentService_ = makeDocumentService(fixture.appContext_);

        DeleteByNameRequest request;
        request.name = (fixture.testDir_ / "test2.md").string();

        auto failed = fixture.documentService_->deleteByName(request);
        REQUIRE(failed);
        CHECK(failed.value().deleted.empty());
        REQUIRE(failed.value().errors.size() == 1);

        auto stillPresent = fixture.metadataRepo_->getDocumentByHash(fixture.testHash2_);
        REQUIRE(stillPresent);
        REQUIRE(stillPresent.value().has_value());

        request.force = true;
        auto forced = fixture.documentService_->deleteByName(request);
        REQUIRE(forced);
        CHECK(forced.value().errors.empty());
        REQUIRE(forced.value().deleted.size() == 1);
        CHECK(forced.value().deleted.front().deleted);

        auto after = fixture.metadataRepo_->getDocumentByHash(fixture.testHash2_);
        REQUIRE(after);
        CHECK_FALSE(after.value().has_value());
    }

    SECTION("Content removal failure leaves document, metadata, and KG fully intact") {
        auto kgStoreRes = yams::metadata::makeSqliteKnowledgeGraphStore(*fixture.pool_);
        REQUIRE(kgStoreRes);
        std::shared_ptr<yams::metadata::KnowledgeGraphStore> kgStore =
            std::move(kgStoreRes.value());

        yams::metadata::KGNode node;
        node.nodeKey = "doc:" + fixture.testHash2_;
        node.type = "document";
        auto upserted = kgStore->upsertNode(node);
        REQUIRE(upserted);

        auto kgBefore = kgStore->getNodeByKey("doc:" + fixture.testHash2_);
        REQUIRE(kgBefore);
        REQUIRE(kgBefore.value().has_value());

        auto failingStore = std::make_shared<RemoveFailingContentStore>(
            fixture.contentStore_, fixture.testHash2_,
            Error{ErrorCode::IOError, "simulated content removal failure"});
        fixture.appContext_.store = failingStore;
        fixture.appContext_.kgStore = kgStore;
        fixture.documentService_ = makeDocumentService(fixture.appContext_);

        DeleteByNameRequest request;
        request.name = (fixture.testDir_ / "test2.md").string();

        auto result = fixture.documentService_->deleteByName(request);
        REQUIRE(result);
        CHECK(result.value().deleted.empty());
        REQUIRE(result.value().errors.size() == 1);

        // Document row + metadata must survive the content-removal failure.
        auto docAfter = fixture.metadataRepo_->getDocumentByHash(fixture.testHash2_);
        REQUIRE(docAfter);
        REQUIRE(docAfter.value().has_value());

        // KG node must also survive (no partial delete).
        auto kgAfter = kgStore->getNodeByKey("doc:" + fixture.testHash2_);
        REQUIRE(kgAfter);
        CHECK(kgAfter.value().has_value());
    }

    SECTION("Successful content removal cascades to metadata and KG") {
        auto kgStoreRes = yams::metadata::makeSqliteKnowledgeGraphStore(*fixture.pool_);
        REQUIRE(kgStoreRes);
        std::shared_ptr<yams::metadata::KnowledgeGraphStore> kgStore =
            std::move(kgStoreRes.value());

        yams::metadata::KGNode node;
        node.nodeKey = "doc:" + fixture.testHash2_;
        node.type = "document";
        auto upserted = kgStore->upsertNode(node);
        REQUIRE(upserted);

        fixture.appContext_.kgStore = kgStore;
        fixture.documentService_ = makeDocumentService(fixture.appContext_);

        DeleteByNameRequest request;
        request.name = (fixture.testDir_ / "test2.md").string();

        auto result = fixture.documentService_->deleteByName(request);
        REQUIRE(result);
        CHECK(result.value().errors.empty());
        REQUIRE(result.value().deleted.size() == 1);
        CHECK(result.value().deleted.front().deleted);

        auto docAfter = fixture.metadataRepo_->getDocumentByHash(fixture.testHash2_);
        REQUIRE(docAfter);
        CHECK_FALSE(docAfter.value().has_value());

        auto kgAfter = kgStore->getNodeByKey("doc:" + fixture.testHash2_);
        REQUIRE(kgAfter);
        CHECK_FALSE(kgAfter.value().has_value());
    }

    SECTION("Delete document by raw hash removes content even without metadata") {
        auto rawPath = fixture.testDir_ / "raw-content.txt";
        std::ofstream{rawPath} << "raw content without metadata";

        auto stored = fixture.contentStore_->store(rawPath, ContentMetadata{});
        REQUIRE(stored);
        const auto hash = stored.value().contentHash;
        auto existsBefore = fixture.contentStore_->exists(hash);
        REQUIRE(existsBefore);
        REQUIRE(existsBefore.value());

        DeleteByNameRequest request;
        request.hash = hash;

        auto result = fixture.documentService_->deleteByName(request);

        REQUIRE(result);
        CHECK(result.value().errors.empty());
        REQUIRE(result.value().deleted.size() == 1);
        CHECK(result.value().deleted.front().deleted);
        auto removedAgain = fixture.contentStore_->remove(hash);
        REQUIRE(removedAgain);
        CHECK_FALSE(removedAgain.value());
    }
}

TEST_CASE("DocumentService - Error Handling", "[document][service][errors]") {
    DocumentFixture fixture;

    SECTION("Emit helpful message on file not found with commas") {
        class NotFoundStore : public IContentStore {
        public:
            Result<StoreResult> store(const std::filesystem::path& path, const ContentMetadata&,
                                      api::ProgressCallback) override {
                return Error{ErrorCode::FileNotFound,
                             std::string("File not found: ") + path.string()};
            }
            Result<RetrieveResult> retrieve(const std::string&, const std::filesystem::path&,
                                            api::ProgressCallback) override {
                return ErrorCode::NotImplemented;
            }
            Result<StoreResult> storeStream(std::istream&, const ContentMetadata&,
                                            api::ProgressCallback) override {
                return ErrorCode::NotImplemented;
            }
            Result<RetrieveResult> retrieveStream(const std::string&, std::ostream&,
                                                  api::ProgressCallback) override {
                return ErrorCode::NotImplemented;
            }
            Result<StoreResult> storeBytes(std::span<const std::byte>,
                                           const ContentMetadata&) override {
                return ErrorCode::NotImplemented;
            }
            Result<std::vector<std::byte>> retrieveBytes(const std::string&) override {
                return ErrorCode::NotImplemented;
            }
            Result<std::vector<std::byte>> retrieveBytesPrefix(const std::string&,
                                                               std::size_t) override {
                return ErrorCode::NotImplemented;
            }
            Result<RawContent> retrieveRaw(const std::string&) override {
                return ErrorCode::NotImplemented;
            }
            std::future<Result<RawContent>> retrieveRawAsync(const std::string&) override {
                return std::async(std::launch::deferred,
                                  [] { return Result<RawContent>(ErrorCode::NotImplemented); });
            }
            Result<bool> exists(const std::string&) const override {
                return ErrorCode::NotImplemented;
            }
            Result<bool> remove(const std::string&) override { return ErrorCode::NotImplemented; }
            Result<ContentMetadata> getMetadata(const std::string&) const override {
                return ErrorCode::NotImplemented;
            }
            Result<void> updateMetadata(const std::string&, const ContentMetadata&) override {
                return ErrorCode::NotImplemented;
            }
            std::vector<Result<StoreResult>>
            storeBatch(const std::vector<std::filesystem::path>&,
                       const std::vector<ContentMetadata>&) override {
                return {};
            }
            std::vector<Result<bool>> removeBatch(const std::vector<std::string>&) override {
                return {};
            }
            ContentStoreStats getStats() const override { return {}; }
            HealthStatus checkHealth() const override { return {}; }
            Result<void> verify(api::ProgressCallback) override {
                return ErrorCode::NotImplemented;
            }
            Result<void> compact(api::ProgressCallback) override {
                return ErrorCode::NotImplemented;
            }
            Result<void> garbageCollect(api::ProgressCallback) override {
                return ErrorCode::NotImplemented;
            }
        };

        AppContext ctx;
        ctx.store = std::make_shared<NotFoundStore>();
        auto svc = makeDocumentService(ctx);

        StoreDocumentRequest req;
        req.path = "a,b,c";
        auto r = svc->store(req);

        REQUIRE_FALSE(r);
        CHECK((r.error().code == ErrorCode::FileNotFound));
        auto msg = r.error().message;
        CHECK((msg.find("a,b,c") != std::string::npos));
        CHECK((msg.find("commas") != std::string::npos));
    }
}

TEST_CASE("DocumentService - Performance", "[document][service][performance]") {
    DocumentFixture fixture;

    SECTION("List performance") {
        auto start = std::chrono::steady_clock::now();

        ListDocumentsRequest request;
        request.limit = 100;

        auto result = fixture.documentService_->list(request);

        auto end = std::chrono::steady_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

        REQUIRE(result);
        CHECK((duration.count() < 1000));
    }

    SECTION("Retrieval performance") {
        REQUIRE_FALSE(fixture.testHash1_.empty());

        auto start = std::chrono::steady_clock::now();

        RetrieveDocumentRequest request;
        request.hash = fixture.testHash1_;

        auto result = fixture.documentService_->retrieve(request);

        auto end = std::chrono::steady_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

        REQUIRE(result);
        CHECK((duration.count() < 1000));
    }
}

TEST_CASE("DocumentService - Integration", "[document][service][integration]") {
    DocumentFixture fixture;

    SECTION("Search integration") {
        ListDocumentsRequest request;
        request.limit = 100;

        auto result = fixture.documentService_->list(request);

        REQUIRE(result);
        CHECK((result.value().documents.size() >= 2));
    }

    SECTION("Service context integration") {
        REQUIRE(fixture.documentService_);
        CHECK(fixture.appContext_.store);
        CHECK(fixture.appContext_.metadataRepo);
    }

    SECTION("Inline extensionless binary content is not forced to text") {
        StoreDocumentRequest request;
        request.name = "88f051b8c01feda0b07cc00112233445";
        request.content = "?<<?"
                          "?Y?"
                          "?<[o+?g?<R`g=?#!={?;=?t?:}?";

        auto stored = fixture.documentService_->store(request);

        REQUIRE(stored);
        auto doc = fixture.metadataRepo_->getDocumentByHash(stored.value().hash);
        REQUIRE(doc);
        REQUIRE(doc.value().has_value());
        CHECK((doc.value()->mimeType == "application/octet-stream"));
        CHECK_FALSE(doc.value()->contentExtracted);
    }
}
