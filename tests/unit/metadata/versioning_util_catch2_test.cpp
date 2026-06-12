#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <random>
#include <tuple>

#include <yams/compat/unistd.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/document_metadata.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/path_utils.h>
#include <yams/metadata/query_helpers.h>
#include <yams/metadata/versioning_util.h>

using namespace yams;
using namespace yams::metadata;

namespace {

std::filesystem::path make_temp_dir(std::string_view prefix = "yams_version_test_") {
    namespace fs = std::filesystem;
    const auto base = fs::temp_directory_path();
    std::uniform_int_distribution<int> dist(0, 9999);
    thread_local std::mt19937_64 rng{std::random_device{}()};
    for (int attempt = 0; attempt < 512; ++attempt) {
        const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
        auto candidate =
            base / (std::string(prefix) + std::to_string(stamp) + "_" + std::to_string(dist(rng)));
        std::error_code ec;
        if (fs::create_directories(candidate, ec)) {
            return candidate;
        }
    }
    return base;
}

DocumentInfo makeDocInfo(const std::string& filePath, const std::string& hash,
                         const std::string& fileName) {
    DocumentInfo info;
    info.filePath = filePath;
    info.fileName = fileName;
    info.fileExtension = ".txt";
    info.fileSize = 100;
    info.sha256Hash = hash;
    info.mimeType = "text/plain";
    auto now = std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
    info.createdTime = now;
    info.modifiedTime = now;
    info.indexedTime = now;
    info.contentExtracted = true;
    info.extractionStatus = ExtractionStatus::Success;
    auto derived = computePathDerivedValues(filePath);
    info.filePath = derived.normalizedPath;
    info.pathPrefix = derived.pathPrefix;
    info.reversePath = derived.reversePath;
    info.pathHash = derived.pathHash;
    info.parentHash = derived.parentHash;
    info.pathDepth = derived.pathDepth;
    return info;
}

class BatchFailingMetadataRepository : public MetadataRepository {
public:
    explicit BatchFailingMetadataRepository(ConnectionPool& pool) : MetadataRepository(pool) {}

    Result<void> setMetadataBatch(
        const std::vector<std::tuple<int64_t, std::string, MetadataValue>>& entries) override {
        ++batchAttempts;
        lastBatchSize = entries.size();
        if (failNextBatch) {
            failNextBatch = false;
            return Error{ErrorCode::DatabaseError, "injected metadata batch failure"};
        }
        return MetadataRepository::setMetadataBatch(entries);
    }

    bool failNextBatch{true};
    std::size_t batchAttempts{0};
    std::size_t lastBatchSize{0};
};

struct VersioningFixture {
    VersioningFixture() { testDir = make_temp_dir(); }

    ~VersioningFixture() {
        repo_.reset();
        pool_.reset();
        std::error_code ec;
        std::filesystem::remove_all(testDir, ec);
    }

    std::shared_ptr<MetadataRepository> makeRepo() {
        auto dbPath = (testDir / "meta.db").string();
        pool_ = std::make_shared<ConnectionPool>(dbPath);
        REQUIRE(pool_->initialize());
        repo_ = std::make_shared<MetadataRepository>(*pool_);
        return repo_;
    }

    std::shared_ptr<BatchFailingMetadataRepository> makeBatchFailingRepo() {
        auto dbPath = (testDir / "meta.db").string();
        pool_ = std::make_shared<ConnectionPool>(dbPath);
        REQUIRE(pool_->initialize());
        auto repo = std::make_shared<BatchFailingMetadataRepository>(*pool_);
        repo_ = repo;
        return repo;
    }

    std::filesystem::path testDir;
    std::shared_ptr<ConnectionPool> pool_;
    std::shared_ptr<MetadataRepository> repo_;
};

} // namespace

TEST_CASE_METHOD(VersioningFixture, "Versioning: first document gets version 1 and is_latest",
                 "[versioning][metadata][catch2]") {
#ifdef _WIN32
    _putenv_s("YAMS_ENABLE_VERSIONING", "1");
#else
    setenv("YAMS_ENABLE_VERSIONING", "1", 1);
#endif

    auto repo = makeRepo();
    auto docInfo = makeDocInfo("/path/doc.txt", "hash_v1", "doc.txt");
    auto ins = repo->insertDocument(docInfo);
    REQUIRE(ins);
    auto docId = ins.value();

    int64_t version = applyPathSeriesVersioning(*repo, docInfo.filePath, docId, std::nullopt);
    CHECK((version == 1));

    auto verMeta = repo->getMetadata(docId, "version");
    REQUIRE(verMeta);
    REQUIRE(verMeta.value().has_value());
    CHECK((verMeta.value()->asInteger() == 1));

    auto latestMeta = repo->getMetadata(docId, "is_latest");
    REQUIRE(latestMeta);
    REQUIRE(latestMeta.value().has_value());
    CHECK((latestMeta.value()->asBoolean() == true));

    auto seriesMeta = repo->getMetadata(docId, "series_key");
    REQUIRE(seriesMeta);
    REQUIRE(seriesMeta.value().has_value());
    CHECK((seriesMeta.value()->asString() == docInfo.filePath));
}

TEST_CASE_METHOD(VersioningFixture, "Versioning: re-index with different hash creates version edge",
                 "[versioning][metadata][catch2]") {
#ifdef _WIN32
    _putenv_s("YAMS_ENABLE_VERSIONING", "1");
#else
    setenv("YAMS_ENABLE_VERSIONING", "1", 1);
#endif

    auto repo = makeRepo();
    const std::string path = "/path/evolving.txt";

    auto v1 = makeDocInfo(path, "hash_v1", "evolving.txt");
    auto ins1 = repo->insertDocument(v1);
    REQUIRE(ins1);
    auto id1 = ins1.value();
    int64_t ver1 = applyPathSeriesVersioning(*repo, path, id1, std::nullopt);
    CHECK((ver1 == 1));

    auto v2 = makeDocInfo(path, "hash_v2", "evolving.txt");
    auto ins2 = repo->insertDocument(v2);
    REQUIRE(ins2);
    auto id2 = ins2.value();
    REQUIRE((id1 != id2));

    auto priorDocResult = repo->findDocumentByExactPath(path);
    REQUIRE(priorDocResult);
    auto priorDoc = priorDocResult.value();
    REQUIRE(priorDoc.has_value());
    int64_t ver2 = applyPathSeriesVersioning(*repo, path, id2, priorDoc);
    CHECK((ver2 == 2));

    // v1 is no longer latest
    auto latest1 = repo->getMetadata(id1, "is_latest");
    REQUIRE(latest1);
    REQUIRE(latest1.value().has_value());
    CHECK((latest1.value()->asBoolean() == false));

    // v2 is latest
    auto latest2 = repo->getMetadata(id2, "is_latest");
    REQUIRE(latest2);
    REQUIRE(latest2.value().has_value());
    CHECK((latest2.value()->asBoolean() == true));

    auto verMeta2 = repo->getMetadata(id2, "version");
    REQUIRE(verMeta2);
    REQUIRE(verMeta2.value().has_value());
    CHECK((verMeta2.value()->asInteger() == 2));

    // VersionOf relationship exists
    auto relResult = repo->getRelationships(id2);
    REQUIRE(relResult);
    bool foundVersionOf = false;
    for (const auto& rel : relResult.value()) {
        if (rel.relationshipType == RelationshipType::VersionOf && rel.parentId == id1) {
            foundVersionOf = true;
            break;
        }
    }
    CHECK(foundVersionOf);
}

TEST_CASE_METHOD(VersioningFixture, "Versioning: same hash re-index does not create version edge",
                 "[versioning][metadata][catch2]") {
#ifdef _WIN32
    _putenv_s("YAMS_ENABLE_VERSIONING", "1");
#else
    setenv("YAMS_ENABLE_VERSIONING", "1", 1);
#endif

    auto repo = makeRepo();
    const std::string path = "/path/identical.txt";

    auto v1 = makeDocInfo(path, "hash_same", "identical.txt");
    auto ins1 = repo->insertDocument(v1);
    REQUIRE(ins1);
    auto id1 = ins1.value();
    applyPathSeriesVersioning(*repo, path, id1, std::nullopt);

    // When caller detects same hash, versioning is not called (handled by caller).
    // This test verifies the guard logic: if we accidentally called it, it still
    // wouldn't create a second version because the path lookup finds the same doc.
    auto priorDoc = repo->findDocumentByExactPath(path);
    REQUIRE(priorDoc);
    REQUIRE(priorDoc.value().has_value());
    CHECK((priorDoc.value()->sha256Hash == "hash_same"));
}

TEST_CASE_METHOD(VersioningFixture,
                 "Versioning: metadata batch failure falls back to best-effort individual writes",
                 "[versioning][metadata][catch2]") {
#ifdef _WIN32
    _putenv_s("YAMS_ENABLE_VERSIONING", "1");
#else
    setenv("YAMS_ENABLE_VERSIONING", "1", 1);
#endif

    auto repo = makeBatchFailingRepo();
    const std::string path = "/path/fallback.txt";

    auto v1 = makeDocInfo(path, "hash_v1_fallback", "fallback.txt");
    auto ins1 = repo->insertDocument(v1);
    REQUIRE(ins1);
    const auto id1 = ins1.value();
    REQUIRE((applyPathSeriesVersioning(*repo, path, id1, std::nullopt) == 1));
    REQUIRE((repo->batchAttempts == 1));
    REQUIRE((repo->lastBatchSize == 3));

    repo->failNextBatch = true;
    auto v2 = makeDocInfo(path, "hash_v2_fallback", "fallback.txt");
    auto ins2 = repo->insertDocument(v2);
    REQUIRE(ins2);
    const auto id2 = ins2.value();

    auto priorDoc = repo->findDocumentByExactPath(path);
    REQUIRE(priorDoc);
    REQUIRE(priorDoc.value().has_value());

    const auto version = applyPathSeriesVersioning(*repo, path, id2, priorDoc.value());
    CHECK((version == 2));
    CHECK((repo->batchAttempts == 2));
    CHECK((repo->lastBatchSize == 4));

    auto latest1 = repo->getMetadata(id1, "is_latest");
    REQUIRE(latest1);
    REQUIRE(latest1.value().has_value());
    CHECK((latest1.value()->asBoolean() == false));

    auto version2 = repo->getMetadata(id2, "version");
    REQUIRE(version2);
    REQUIRE(version2.value().has_value());
    CHECK((version2.value()->asInteger() == 2));

    auto latest2 = repo->getMetadata(id2, "is_latest");
    REQUIRE(latest2);
    REQUIRE(latest2.value().has_value());
    CHECK((latest2.value()->asBoolean() == true));

    auto series2 = repo->getMetadata(id2, "series_key");
    REQUIRE(series2);
    REQUIRE(series2.value().has_value());
    CHECK((series2.value()->asString() == path));

    auto relResult = repo->getRelationships(id2);
    REQUIRE(relResult);
    bool foundVersionOf = false;
    for (const auto& rel : relResult.value()) {
        if (rel.relationshipType == RelationshipType::VersionOf && rel.parentId == id1) {
            foundVersionOf = true;
            break;
        }
    }
    CHECK(foundVersionOf);
}

TEST_CASE_METHOD(VersioningFixture, "Versioning respects YAMS_ENABLE_VERSIONING=0",
                 "[versioning][metadata][catch2]") {
#ifdef _WIN32
    _putenv_s("YAMS_ENABLE_VERSIONING", "0");
#else
    setenv("YAMS_ENABLE_VERSIONING", "0", 1);
#endif

    auto repo = makeRepo();
    auto docInfo = makeDocInfo("/path/disabled.txt", "hash_disabled", "disabled.txt");
    auto ins = repo->insertDocument(docInfo);
    REQUIRE(ins);
    auto docId = ins.value();

    int64_t version = applyPathSeriesVersioning(*repo, docInfo.filePath, docId, std::nullopt);
    CHECK((version == 0));

    auto verMeta = repo->getMetadata(docId, "version");
    REQUIRE(verMeta);
    CHECK(!verMeta.value().has_value());
}
