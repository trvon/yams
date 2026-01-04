#include <chrono>
#include <filesystem>
#include <fstream>
#include <future>
#include <memory>
#include <span>
#include <catch2/catch_test_macros.hpp>
#include <yams/api/content_store_builder.h>
#include <yams/app/services/services.hpp>
#include <yams/app/services/session_service.hpp>
#include <yams/core/cpp23_features.hpp>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/database.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/migration.h>
#include <yams/search/search_engine.h>

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

        if (database_) {
            database_->close();
            database_.reset();
        }
        if (pool_) {
            pool_->shutdown();
            pool_.reset();
        }
        metadataRepo_.reset();

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

        std::ofstream{testFile1} << "Test document content for retrieval testing";
        std::ofstream{testFile2} << "Another test document with different content";

        StoreDocumentRequest storeReq1;
        storeReq1.path = testFile1.string();
        storeReq1.tags = {"test", "document"};

        StoreDocumentRequest storeReq2;
        storeReq2.path = testFile2.string();
        storeReq2.tags = {"test", "markdown"};

        auto result1 = documentService_->store(storeReq1);
        auto result2 = documentService_->store(storeReq2);

        if (result1 && result2) {
            testHash1_ = result1.value().hash;
            testHash2_ = result2.value().hash;
        }
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
        CHECK(r.error().code == ErrorCode::FileNotFound);
        auto msg = r.error().message;
        CHECK(msg.find("a,b,c") != std::string::npos);
        CHECK(msg.find("commas") != std::string::npos);
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
        CHECK(duration.count() < 1000);
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
        CHECK(duration.count() < 1000);
    }
}

TEST_CASE("DocumentService - Integration", "[document][service][integration]") {
    DocumentFixture fixture;

    SECTION("Search integration") {
        ListDocumentsRequest request;
        request.limit = 100;

        auto result = fixture.documentService_->list(request);

        REQUIRE(result);
        CHECK(result.value().documents.size() >= 2);
    }

    SECTION("Service context integration") {
        REQUIRE(fixture.documentService_);
        CHECK(fixture.appContext_.store);
        CHECK(fixture.appContext_.metadataRepo);
    }
}
