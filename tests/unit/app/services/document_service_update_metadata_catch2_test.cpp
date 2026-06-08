#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>

#include <catch2/catch_test_macros.hpp>

#include <yams/api/content_store_builder.h>
#include <yams/app/services/factory.hpp>
#include <yams/app/services/services.hpp>
#include <yams/compat/unistd.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/database.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/migration.h>

namespace {

using namespace yams;
using namespace yams::app::services;
using namespace yams::metadata;
using namespace yams::api;

struct UpdateMetadataFixture {
    UpdateMetadataFixture() {
        auto pid = std::to_string(::getpid());
        auto timestamp =
            std::to_string(std::chrono::system_clock::now().time_since_epoch().count());
        testDir = std::filesystem::temp_directory_path() /
                  ("document_update_metadata_test_" + pid + "_" + timestamp);
        std::filesystem::create_directories(testDir);

        dbPath = std::filesystem::absolute(testDir / "test.db");
        database = std::make_unique<Database>();
        REQUIRE(database->open(dbPath.string(), ConnectionMode::Create));

        ConnectionPoolConfig poolConfig;
        poolConfig.maxConnections = 4;
        pool = std::make_unique<ConnectionPool>(dbPath.string(), poolConfig);
        metadataRepo = std::make_shared<MetadataRepository>(*pool);

        MigrationManager mm(*database);
        REQUIRE(mm.initialize());
        mm.registerMigrations(YamsMetadataMigrations::getAllMigrations());
        REQUIRE(mm.migrate());

        ContentStoreBuilder builder;
        auto storeResult = builder.withStoragePath(testDir / "storage")
                               .withChunkSize(65536)
                               .withCompression(true)
                               .withDeduplication(true)
                               .build();
        REQUIRE(storeResult);
        auto& uniqueStore = storeResult.value();
        contentStore = std::shared_ptr<IContentStore>(
            const_cast<std::unique_ptr<IContentStore>&>(uniqueStore).release());

        appContext.store = contentStore;
        appContext.metadataRepo = metadataRepo;
        documentService = makeDocumentService(appContext);
        REQUIRE(documentService);

        auto docPath = testDir / "doc.txt";
        std::ofstream(docPath) << "hello update metadata";

        StoreDocumentRequest request;
        request.path = docPath.string();
        request.tags = {"test", "document"};
        auto stored = documentService->store(request);
        REQUIRE(stored);
        storedHash = stored.value().hash;
    }

    ~UpdateMetadataFixture() {
        documentService.reset();
        contentStore.reset();
        metadataRepo.reset();
        appContext = {};
        if (pool) {
            pool->shutdown();
            pool.reset();
        }
        if (database) {
            database->close();
            database.reset();
        }
        std::error_code ec;
        std::filesystem::remove_all(testDir, ec);
    }

    std::filesystem::path testDir;
    std::filesystem::path dbPath;
    std::unique_ptr<Database> database;
    std::unique_ptr<ConnectionPool> pool;
    std::shared_ptr<MetadataRepository> metadataRepo;
    std::shared_ptr<IContentStore> contentStore;
    AppContext appContext;
    std::shared_ptr<IDocumentService> documentService;
    std::string storedHash;
};

} // namespace

TEST_CASE("DocumentService atomic metadata updates preserve request-level counts",
          "[document][service][update]") {
    UpdateMetadataFixture fixture;

    UpdateMetadataRequest request;
    request.hash = fixture.storedHash;
    request.atomic = true;
    request.keyValues = {{"author", "alice"}};
    request.pairs = {"topic=systems", "topic=storage", "invalidpair"};
    request.addTags = {"fresh", "fresh"};

    auto result = fixture.documentService->updateMetadata(request);
    REQUIRE((result));
    REQUIRE((result.value().success));
    REQUIRE((result.value().documentId.has_value()));
    CHECK((result.value().updatesApplied == 3));
    CHECK((result.value().tagsAdded == 2));
    CHECK((result.value().tagsRemoved == 0));

    auto author = fixture.metadataRepo->getMetadata(*result.value().documentId, "author");
    REQUIRE((author));
    REQUIRE((author.value().has_value()));
    CHECK((author.value()->asString() == "alice"));

    auto topic = fixture.metadataRepo->getMetadata(*result.value().documentId, "topic");
    REQUIRE((topic));
    REQUIRE((topic.value().has_value()));
    CHECK((topic.value()->asString() == "storage"));

    auto tags = fixture.metadataRepo->getDocumentTags(*result.value().documentId);
    REQUIRE((tags));
    CHECK((std::find(tags.value().begin(), tags.value().end(), "fresh") != tags.value().end()));
}
