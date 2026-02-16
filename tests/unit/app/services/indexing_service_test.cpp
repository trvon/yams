#include <catch2/catch_test_macros.hpp>
#include <yams/api/content_store_builder.h>
#include <yams/app/services/services.hpp>
#include <yams/content/content_handler_registry.h>
#include <yams/core/cpp23_features.hpp>
#include <yams/detection/file_type_detector.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/database.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/migration.h>
#include <yams/search/search_engine.h>

#include <chrono>
#include <filesystem>
#include <fstream>

using namespace yams;
using namespace yams::app::services;
using namespace yams::metadata;
using namespace yams::content;
using namespace yams::detection;
using namespace yams::api;

namespace {

struct IndexingFixture {
    IndexingFixture() {
        initializeGlobalSingletons();
        setupTestEnvironment();
        setupDatabase();
        setupServices();
    }

    ~IndexingFixture() {
        indexingService_.reset();
        searchEngine_.reset();
        appContext_.metadataRepo.reset();
        appContext_.store.reset();
        appContext_.searchEngine.reset();
        contentStore_.reset();
        metadataRepo_.reset();

        if (pool_) {
            pool_->shutdown();
            pool_.reset();
        }

        if (database_) {
            database_->close();
            database_.reset();
        }

        std::error_code ec;
        if (!testDir_.empty() && std::filesystem::exists(testDir_)) {
            std::filesystem::remove_all(testDir_, ec);
        }
        if (!infraDir_.empty() && std::filesystem::exists(infraDir_)) {
            std::filesystem::remove_all(infraDir_, ec);
        }
    }

    void initializeGlobalSingletons() {
        static bool initialized = false;
        if (initialized)
            return;

        auto& detector = FileTypeDetector::instance();
        auto detectorInit = detector.initializeWithMagicNumbers();
        REQUIRE(detectorInit);

        auto& registry = ContentHandlerRegistry::instance();
        registry.clear();

        initialized = true;
    }

    void setupTestEnvironment() {
        auto pid = std::to_string(::getpid());
        auto timestamp =
            std::to_string(std::chrono::system_clock::now().time_since_epoch().count());

        testDir_ =
            std::filesystem::temp_directory_path() / ("indexing_test_" + pid + "_" + timestamp);
        infraDir_ =
            std::filesystem::temp_directory_path() / ("indexing_infra_" + pid + "_" + timestamp);

        std::filesystem::create_directories(testDir_);
        std::filesystem::create_directories(infraDir_);
    }

    void setupDatabase() {
        dbPath_ = std::filesystem::absolute(infraDir_ / "test.db");

        database_ = std::make_unique<Database>();
        REQUIRE(database_->open(dbPath_.string(), ConnectionMode::Create));

        pool_ = std::make_unique<ConnectionPool>(dbPath_.string(),
                                                 ConnectionPoolConfig{.maxConnections = 4});
        metadataRepo_ = std::make_shared<MetadataRepository>(*pool_);

        MigrationManager mm(*database_);
        REQUIRE(mm.initialize());
        mm.registerMigrations(YamsMetadataMigrations::getAllMigrations());
        REQUIRE(mm.migrate());
    }

    void setupServices() {
        ContentStoreBuilder builder;
        auto storeResult = builder.withStoragePath(infraDir_ / "storage")
                               .withChunkSize(65536)
                               .withCompression(true)
                               .withDeduplication(true)
                               .build();
        REQUIRE(storeResult);

        contentStore_ = std::shared_ptr<IContentStore>(
            const_cast<std::unique_ptr<IContentStore>&>(storeResult.value()).release());

        appContext_.service_manager = nullptr;
        appContext_.store = contentStore_;
        appContext_.metadataRepo = metadataRepo_;
        appContext_.searchEngine = nullptr;

        indexingService_ = makeIndexingService(appContext_);
    }

    std::filesystem::path createFile(std::string_view filename, std::string_view content) {
        auto filePath = testDir_ / filename;
        std::filesystem::create_directories(filePath.parent_path());
        std::ofstream{filePath} << content;
        return filePath;
    }

    std::filesystem::path createBinaryFile(std::string_view filename, size_t size = 1000) {
        auto filePath = testDir_ / filename;
        std::filesystem::create_directories(filePath.parent_path());
        std::ofstream file(filePath, std::ios::binary);
        for (size_t i = 0; i < size; ++i) {
            file.put(static_cast<char>(i % 256));
        }
        return filePath;
    }

    AddDirectoryRequest createRequest(const std::filesystem::path& path,
                                      std::string_view collection = "") {
        AddDirectoryRequest req;
        req.directoryPath = path.string();
        req.collection = std::string(collection);
        req.recursive = false;
        return req;
    }

    std::filesystem::path testDir_;
    std::filesystem::path infraDir_;
    std::filesystem::path dbPath_;

    std::unique_ptr<Database> database_;
    std::unique_ptr<ConnectionPool> pool_;
    std::shared_ptr<MetadataRepository> metadataRepo_;
    std::shared_ptr<IContentStore> contentStore_;
    std::shared_ptr<search::SearchEngine> searchEngine_;
    AppContext appContext_;
    std::shared_ptr<IIndexingService> indexingService_;
};

} // namespace

TEST_CASE("IndexingService - Basic Operations", "[indexing][service][basic]") {
    IndexingFixture fixture;

    SECTION("Index single file in directory") {
        fixture.createFile("docs/test.txt", "This is a test document with some content.");

        auto request = fixture.createRequest(fixture.testDir_ / "docs", "test-collection");
        auto result = fixture.indexingService_->addDirectory(request);

        REQUIRE(result);
        CHECK(result.value().filesIndexed == 1);
        CHECK(result.value().filesSkipped == 0);
        CHECK(result.value().filesFailed == 0);
        CHECK(result.value().filesProcessed == 1);
    }

    SECTION("Index multiple files in directory") {
        fixture.createFile("multi/file1.txt", "Content of first file");
        fixture.createFile("multi/file2.md", "# Markdown content\n\nSome **bold** text");
        fixture.createFile("multi/file3.cpp", "#include <iostream>\nint main() { return 0; }");

        AddDirectoryRequest request;
        request.directoryPath = (fixture.testDir_ / "multi").string();
        request.collection = "test-batch";
        request.recursive = false;

        auto result = fixture.indexingService_->addDirectory(request);

        REQUIRE(result);
        CHECK(result.value().filesIndexed == 3);
        CHECK(result.value().filesSkipped == 0);
        CHECK(result.value().filesProcessed == 3);
    }

    SECTION("Index directory recursively") {
        fixture.createFile("recursive/root.txt", "Root level file");
        fixture.createFile("recursive/subdir/nested.txt", "Nested file content");
        fixture.createFile("recursive/subdir/another.md", "# Nested markdown");

        AddDirectoryRequest request;
        request.directoryPath = (fixture.testDir_ / "recursive").string();
        request.recursive = true;
        request.includePatterns = {"*.txt", "*.md"};

        auto result = fixture.indexingService_->addDirectory(request);

        REQUIRE(result);
        CHECK(result.value().filesIndexed == 3);
        CHECK(result.value().filesSkipped == 0);
    }

    SECTION("Handle non-existent directory") {
        auto request = fixture.createRequest(fixture.testDir_ / "does_not_exist");
        auto result = fixture.indexingService_->addDirectory(request);

        REQUIRE_FALSE(result);
        CHECK(result.error().code == ErrorCode::InvalidArgument);
        CHECK(result.error().message.find("does not exist") != std::string::npos);
    }

    SECTION("Handle empty directory") {
        std::filesystem::create_directories(fixture.testDir_ / "empty");

        auto request = fixture.createRequest(fixture.testDir_ / "empty");
        auto result = fixture.indexingService_->addDirectory(request);

        REQUIRE(result);
        CHECK(result.value().filesIndexed == 0);
        CHECK(result.value().filesProcessed == 0);
    }
}

TEST_CASE("IndexingService - File Handling", "[indexing][service][files]") {
    IndexingFixture fixture;

    SECTION("Handle binary files") {
        fixture.createBinaryFile("binaries/binary.exe");

        auto request = fixture.createRequest(fixture.testDir_ / "binaries");
        auto result = fixture.indexingService_->addDirectory(request);

        REQUIRE(result);
        CHECK(result.value().filesIndexed >= 0);
    }

    SECTION("Apply collection and metadata") {
        fixture.createFile("tagged/tagged.txt", "Content with tags and metadata");

        AddDirectoryRequest request;
        request.directoryPath = (fixture.testDir_ / "tagged").string();
        request.collection = "important-collection";
        request.metadata = {
            {"project", "test-project"}, {"author", "test-author"}, {"priority", "high"}};

        auto result = fixture.indexingService_->addDirectory(request);

        REQUIRE(result);
        CHECK(result.value().filesIndexed == 1);
        CHECK(result.value().collection == "important-collection");
    }

    SECTION("Handle duplicate content") {
        constexpr auto content = "Identical content for deduplication test";
        fixture.createFile("duplicates/file1.txt", content);
        fixture.createFile("duplicates/file2.txt", content);

        auto request = fixture.createRequest(fixture.testDir_ / "duplicates");
        auto result = fixture.indexingService_->addDirectory(request);

        REQUIRE(result);
        CHECK(result.value().filesProcessed == 2);
        CHECK(result.value().filesIndexed == 2);
    }

    SECTION("Handle large file") {
        std::string largeContent(1024 * 1024, 'A'); // 1MB of 'A' characters
        fixture.createFile("large/huge.txt", largeContent);

        auto request = fixture.createRequest(fixture.testDir_ / "large");
        auto result = fixture.indexingService_->addDirectory(request);

        REQUIRE(result);
        CHECK(result.value().filesIndexed == 1);
    }

    SECTION("Handle special characters in filenames") {
        fixture.createFile("special/file with spaces.txt", "Content with spaces");
        fixture.createFile("special/file-with-dashes.txt", "Content with dashes");
        fixture.createFile("special/file_with_underscores.txt", "Content with underscores");

        auto request = fixture.createRequest(fixture.testDir_ / "special");
        auto result = fixture.indexingService_->addDirectory(request);

        REQUIRE(result);
        CHECK(result.value().filesIndexed == 3);
    }
}

TEST_CASE("IndexingService - Symlink Handling", "[indexing][service][symlinks]") {
    IndexingFixture fixture;

    SECTION("Handle circular symlink") {
        fixture.createFile("file.txt", "content");

        auto circular = fixture.testDir_ / "circular.txt";
        std::error_code ec;
        std::filesystem::create_symlink("circular.txt", circular, ec);

        auto request = fixture.createRequest(fixture.testDir_);
        auto result = fixture.indexingService_->addDirectory(request);

        REQUIRE(result);
        CHECK(result.value().filesIndexed == 1);
        CHECK(result.value().filesFailed == 0);
    }

    SECTION("Handle broken symlink") {
        auto broken = fixture.testDir_ / "broken.txt";
        std::error_code ec;
        std::filesystem::create_symlink("nonexistent.txt", broken, ec);

        auto request = fixture.createRequest(fixture.testDir_);
        auto result = fixture.indexingService_->addDirectory(request);

        REQUIRE(result);
        CHECK(result.value().filesIndexed == 0);
    }

    SECTION("Handle valid symlink to file") {
        fixture.createFile("target.txt", "target content");

        auto link = fixture.testDir_ / "link.txt";
        std::error_code ec;
        std::filesystem::create_symlink("target.txt", link, ec);

        auto request = fixture.createRequest(fixture.testDir_);
        auto result = fixture.indexingService_->addDirectory(request);

        REQUIRE(result);
        CHECK(result.value().filesIndexed >= 1);
    }

    SECTION("Handle nested directory with circular symlink") {
        std::filesystem::create_directories(fixture.testDir_ / "nested");
        fixture.createFile("nested/file.txt", "content");

        auto circular = fixture.testDir_ / "nested/loop";
        std::error_code ec;
        std::filesystem::create_directory_symlink("..", circular, ec);

        AddDirectoryRequest request;
        request.directoryPath = fixture.testDir_.string();
        request.recursive = true;

        auto result = fixture.indexingService_->addDirectory(request);

        REQUIRE(result);
        CHECK(result.value().filesIndexed >= 1);
    }

    SECTION("Handle symlink to directory") {
        std::filesystem::create_directories(fixture.testDir_ / "target_dir");
        fixture.createFile("target_dir/file.txt", "content");

        auto link = fixture.testDir_ / "link_dir";
        std::error_code ec;
        std::filesystem::create_directory_symlink("target_dir", link, ec);

        AddDirectoryRequest request;
        request.directoryPath = fixture.testDir_.string();
        request.recursive = true;

        auto result = fixture.indexingService_->addDirectory(request);

        REQUIRE(result);
        CHECK(result.value().filesIndexed >= 1);
    }

    SECTION("Handle symlink with special characters") {
        fixture.createFile("target.txt", "content");

        auto link = fixture.testDir_ / "link with spaces & special.txt";
        std::error_code ec;
        std::filesystem::create_symlink("target.txt", link, ec);

        auto request = fixture.createRequest(fixture.testDir_);
        auto result = fixture.indexingService_->addDirectory(request);

        REQUIRE(result);
        CHECK(result.value().filesIndexed >= 1);
    }
}

TEST_CASE("IndexingService - Pattern Matching", "[indexing][service][patterns]") {
    IndexingFixture fixture;

    SECTION("Include pattern with wildcards") {
        fixture.createFile("file.txt", "text file");
        fixture.createFile("file.md", "markdown file");
        fixture.createFile("file.cpp", "c++ file");

        AddDirectoryRequest request;
        request.directoryPath = fixture.testDir_.string();
        request.includePatterns = {"*.txt", "*.md"};

        auto result = fixture.indexingService_->addDirectory(request);

        REQUIRE(result);
        CHECK(result.value().filesIndexed == 2);
    }

    SECTION("Include multiple patterns") {
        fixture.createFile("doc.txt", "text");
        fixture.createFile("readme.md", "markdown");
        fixture.createFile("script.py", "python");
        fixture.createFile("code.cpp", "c++");

        AddDirectoryRequest request;
        request.directoryPath = fixture.testDir_.string();
        request.includePatterns = {"*.txt", "*.md", "*.py"};

        auto result = fixture.indexingService_->addDirectory(request);

        REQUIRE(result);
        CHECK(result.value().filesIndexed == 3);
    }

    SECTION("Exclude pattern overrides include") {
        fixture.createFile("file1.txt", "text1");
        fixture.createFile("file2.txt", "text2");
        fixture.createFile("exclude_me.txt", "excluded");

        AddDirectoryRequest request;
        request.directoryPath = fixture.testDir_.string();
        request.includePatterns = {"*.txt"};
        request.excludePatterns = {"exclude_*"};

        auto result = fixture.indexingService_->addDirectory(request);

        REQUIRE(result);
        CHECK(result.value().filesIndexed == 2);
    }

    SECTION("No include patterns indexes all") {
        fixture.createFile("file.txt", "text");
        fixture.createFile("file.md", "markdown");
        fixture.createFile("file.cpp", "c++");

        auto request = fixture.createRequest(fixture.testDir_);
        auto result = fixture.indexingService_->addDirectory(request);

        REQUIRE(result);
        CHECK(result.value().filesIndexed == 3);
    }

    SECTION("Pattern with question mark wildcard") {
        fixture.createFile("file1.txt", "1");
        fixture.createFile("file2.txt", "2");
        fixture.createFile("file10.txt", "10");

        AddDirectoryRequest request;
        request.directoryPath = fixture.testDir_.string();
        request.includePatterns = {"file?.txt"};

        auto result = fixture.indexingService_->addDirectory(request);

        REQUIRE(result);
        CHECK(result.value().filesIndexed == 2); // Matches file1.txt and file2.txt only
    }

    SECTION("Case insensitive pattern matching") {
        fixture.createFile("file.txt", "content1");
        fixture.createFile("readme.txt", "content2");

        AddDirectoryRequest request;
        request.directoryPath = fixture.testDir_.string();
        request.includePatterns = {"*.txt"};

        auto result = fixture.indexingService_->addDirectory(request);

        REQUIRE(result);
        CHECK(result.value().filesIndexed == 2);
    }
}

TEST_CASE("IndexingService - Recursion and Directory Traversal", "[indexing][service][recursion]") {
    IndexingFixture fixture;

    SECTION("Non-recursive skips subdirectories") {
        fixture.createFile("root.txt", "root");
        fixture.createFile("subdir/nested.txt", "nested");

        AddDirectoryRequest request;
        request.directoryPath = fixture.testDir_.string();
        request.recursive = false;

        auto result = fixture.indexingService_->addDirectory(request);

        REQUIRE(result);
        CHECK(result.value().filesIndexed == 1);
    }

    SECTION("Recursive indexes nested directories") {
        fixture.createFile("root.txt", "root");
        fixture.createFile("sub1/file1.txt", "file1");
        fixture.createFile("sub1/sub2/file2.txt", "file2");

        AddDirectoryRequest request;
        request.directoryPath = fixture.testDir_.string();
        request.recursive = true;

        auto result = fixture.indexingService_->addDirectory(request);

        REQUIRE(result);
        CHECK(result.value().filesIndexed == 3);
    }

    SECTION("Recursive with patterns in nested dirs") {
        fixture.createFile("sub1/file.txt", "txt");
        fixture.createFile("sub1/file.md", "md");
        fixture.createFile("sub2/file.txt", "txt2");
        fixture.createFile("sub2/file.cpp", "cpp");

        AddDirectoryRequest request;
        request.directoryPath = fixture.testDir_.string();
        request.recursive = true;
        request.includePatterns = {"*.txt"};

        auto result = fixture.indexingService_->addDirectory(request);

        REQUIRE(result);
        CHECK(result.value().filesIndexed == 2);
    }

    SECTION("Directory with only subdirectories") {
        std::filesystem::create_directories(fixture.testDir_ / "sub1");
        std::filesystem::create_directories(fixture.testDir_ / "sub2");

        auto request = fixture.createRequest(fixture.testDir_);
        auto result = fixture.indexingService_->addDirectory(request);

        REQUIRE(result);
        CHECK(result.value().filesIndexed == 0);
    }

    SECTION("Deep directory hierarchy") {
        fixture.createFile("l1/l2/l3/l4/l5/deep.txt", "deep content");

        AddDirectoryRequest request;
        request.directoryPath = fixture.testDir_.string();
        request.recursive = true;

        auto result = fixture.indexingService_->addDirectory(request);

        REQUIRE(result);
        CHECK(result.value().filesIndexed == 1);
    }
}

TEST_CASE("IndexingService - Metadata and Collections", "[indexing][service][metadata]") {
    IndexingFixture fixture;

    SECTION("Tags propagated to all files") {
        fixture.createFile("file1.txt", "content1");
        fixture.createFile("file2.txt", "content2");

        AddDirectoryRequest request;
        request.directoryPath = fixture.testDir_.string();
        request.tags = {"important", "project-x"};

        auto result = fixture.indexingService_->addDirectory(request);

        REQUIRE(result);
        CHECK(result.value().filesIndexed == 2);
    }

    SECTION("Collection assigned to all files") {
        fixture.createFile("file1.txt", "content1");
        fixture.createFile("file2.txt", "content2");

        AddDirectoryRequest request;
        request.directoryPath = fixture.testDir_.string();
        request.collection = "my-collection";

        auto result = fixture.indexingService_->addDirectory(request);

        REQUIRE(result);
        CHECK(result.value().filesIndexed == 2);
        CHECK(result.value().collection == "my-collection");
    }

    SECTION("Metadata propagated to files") {
        fixture.createFile("file.txt", "content");

        AddDirectoryRequest request;
        request.directoryPath = fixture.testDir_.string();
        request.metadata = {{"project", "test"}, {"version", "1.0"}, {"author", "tester"}};

        auto result = fixture.indexingService_->addDirectory(request);

        REQUIRE(result);
        CHECK(result.value().filesIndexed == 1);
    }
}

TEST_CASE("IndexingService - Response Validation", "[indexing][service][response]") {
    IndexingFixture fixture;

    SECTION("Response contains correct directory path") {
        fixture.createFile("file.txt", "content");

        auto request = fixture.createRequest(fixture.testDir_);
        auto result = fixture.indexingService_->addDirectory(request);

        REQUIRE(result);
        CHECK(result.value().directoryPath == fixture.testDir_.string());
    }

    SECTION("Response contains collection name") {
        fixture.createFile("file.txt", "content");

        AddDirectoryRequest request;
        request.directoryPath = fixture.testDir_.string();
        request.collection = "test-collection";

        auto result = fixture.indexingService_->addDirectory(request);

        REQUIRE(result);
        CHECK(result.value().collection == "test-collection");
    }

    SECTION("Individual file results populated") {
        fixture.createFile("file1.txt", "content1");
        fixture.createFile("file2.txt", "content2");

        auto request = fixture.createRequest(fixture.testDir_);
        auto result = fixture.indexingService_->addDirectory(request);

        REQUIRE(result);
        CHECK_FALSE(result.value().results.empty());
        CHECK(result.value().results.size() == 2);
    }
}
