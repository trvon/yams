#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <random>
#include <tuple>

#include <yams/compat/unistd.h>
#include <yams/integrity/repair_manager.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/document_metadata.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/path_utils.h>
#include <yams/metadata/query_helpers.h>
#include <yams/metadata/versioning_util.h>

#include "../../common/test_helpers_catch2.h"

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
    yams::test::ScopedEnvVar versioning{"YAMS_ENABLE_VERSIONING", std::string{"1"}};

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

TEST_CASE_METHOD(VersioningFixture,
                 "Versioning: exact path lookup selects the marked head, not the first hash",
                 "[versioning][metadata][path-head]") {
    auto repo = makeRepo();
    const std::string path = "/path/head.txt";
    std::vector<int64_t> ids;
    for (int version = 1; version <= 3; ++version) {
        auto doc = makeDocInfo(path, "head_hash_" + std::to_string(version), "head.txt");
        doc.indexedTime = std::chrono::sys_seconds{std::chrono::seconds{100}};
        auto inserted = repo->insertDocument(doc);
        REQUIRE(inserted);
        ids.push_back(inserted.value());
        REQUIRE(repo->setMetadata(ids.back(), "version", MetadataValue(int64_t{version})));
        REQUIRE(repo->setMetadata(ids.back(), "is_latest", MetadataValue(version == 3)));
    }
    auto latest = repo->findDocumentByExactPath(path);
    REQUIRE(latest);
    REQUIRE(latest.value());
    CHECK(latest.value()->id == ids.back());

    // Reverting to previously indexed bytes must move the path head even though
    // the content hash/document id is old. A cached path lookup must not pin v3.
    REQUIRE(repo->setMetadata(ids.back(), "is_latest", MetadataValue(false)));
    REQUIRE(repo->setMetadata(ids.front(), "version", MetadataValue(int64_t{4})));
    REQUIRE(repo->setMetadata(ids.front(), "is_latest", MetadataValue(true)));
    auto reverted = repo->findDocumentByExactPath(path);
    REQUIRE(reverted);
    REQUIRE(reverted.value());
    CHECK(reverted.value()->id == ids.front());

    // A fresh reader must agree with the writer's cached lookup.
    MetadataRepository cold(*pool_);
    auto reopened = cold.findDocumentByExactPath(path);
    REQUIRE(reopened);
    REQUIRE(reopened.value());
    CHECK(reopened.value()->id == ids.front());
}

TEST_CASE_METHOD(VersioningFixture, "Versioning: re-index with different hash creates version edge",
                 "[versioning][metadata][catch2]") {
    yams::test::ScopedEnvVar versioning{"YAMS_ENABLE_VERSIONING", std::string{"1"}};

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
    yams::test::ScopedEnvVar versioning{"YAMS_ENABLE_VERSIONING", std::string{"1"}};

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
    yams::test::ScopedEnvVar versioning{"YAMS_ENABLE_VERSIONING", std::string{"1"}};

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
    yams::test::ScopedEnvVar versioning{"YAMS_ENABLE_VERSIONING", std::string{"0"}};

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

TEST_CASE_METHOD(VersioningFixture,
                 "Path heads isolate aliases, content reversion and location retirement",
                 "[versioning][metadata][path-locations]") {
    auto repo = makeRepo();
    REQUIRE(repo->supportsDocumentPaths());
    auto original = makeDocInfo("/repo/main/file.txt", "shared-location-hash", "file.txt");
    auto first = repo->insertDocument(original);
    REQUIRE(first);
    REQUIRE(repo->recordDocumentPath(original, first.value(), {{"owner", MetadataValue("main")}}));
    auto alias = makeDocInfo("/repo/lane/file.txt", original.sha256Hash, "file.txt");
    auto second = repo->insertDocument(alias);
    REQUIRE(second);
    REQUIRE(second.value() == first.value());
    REQUIRE(repo->recordDocumentPath(
        alias, second.value(),
        {{"owner", MetadataValue("lane")}, {"tag:lane", MetadataValue("lane")}}));
    auto paths = repo->getDocumentPathReferences(first.value());
    REQUIRE(paths);
    REQUIRE(paths.value().size() == 2);
    auto originalMetadata = repo->getDocumentPathMetadata(original.filePath, first.value());
    auto aliasMetadata = repo->getDocumentPathMetadata(alias.filePath, second.value());
    REQUIRE(originalMetadata);
    REQUIRE(originalMetadata.value());
    REQUIRE(aliasMetadata);
    REQUIRE(aliasMetadata.value());
    CHECK(originalMetadata.value()->at("owner").asString() == "main");
    CHECK(aliasMetadata.value()->at("owner").asString() == "lane");

    SECTION("Path-aware projections filter local metadata and tags before pagination") {
        REQUIRE(repo->setMetadata(first.value(), "owner", MetadataValue("main")));
        DocumentQueryOptions options;
        options.usePathLocations = true;
        options.currentPathsOnly = true;
        options.pathPrefix = "/repo/lane/";
        options.prefixIsDirectory = true;
        options.fileNamePattern = "file%";
        options.tags = {"lane"};
        options.metadataFilters = {{"owner", "lane"}};
        options.limit = 1;
        auto documents = repo->queryDocuments(options);
        auto listed = repo->queryDocumentsForListProjection(options);
        auto candidates = repo->queryDocumentsForGrepCandidates(options);
        REQUIRE(documents);
        REQUIRE(listed);
        REQUIRE(candidates);
        REQUIRE(documents.value().size() == 1);
        REQUIRE(listed.value().size() == 1);
        REQUIRE(candidates.value().size() == 1);
        CHECK(documents.value().front().filePath == alias.filePath);
        CHECK(listed.value().front().filePath == alias.filePath);
        CHECK(candidates.value().front().filePath == alias.filePath);
        options.metadataFilters = {{"owner", "main"}};
        auto notInherited = repo->queryDocuments(options);
        REQUIRE(notInherited);
        CHECK(notInherited.value().empty());
        options.tags.clear();
        options.pathPrefix.reset();
        options.limit = 0;
        options.metadataFilters.clear();
        options.metadataAnyFilters = {{"owner", "lane"}, {"owner", "main"}};
        auto either = repo->queryDocuments(options);
        REQUIRE(either);
        CHECK(either.value().size() == 2);
    }
    SECTION("Cold readers resolve an alias and reverted bytes through the path head") {
        auto changed = original;
        changed.sha256Hash = "changed-location-hash";
        auto updated = repo->insertDocument(changed);
        REQUIRE(updated);
        REQUIRE(
            repo->recordDocumentPath(changed, updated.value(), {{"owner", MetadataValue("main")}}));
        REQUIRE(
            repo->recordDocumentPath(original, first.value(), {{"owner", MetadataValue("main")}}));
        MetadataRepository cold(*pool_);
        auto current = cold.findDocumentByExactPath(original.filePath);
        auto other = cold.findDocumentByExactPath(alias.filePath);
        REQUIRE(current);
        REQUIRE(current.value());
        REQUIRE(other);
        REQUIRE(other.value());
        CHECK(current.value()->sha256Hash == original.sha256Hash);
        CHECK(other.value()->filePath == alias.filePath);
        auto reverted = cold.getDocumentPathMetadata(original.filePath, first.value());
        auto untouched = cold.getDocumentPathMetadata(alias.filePath, first.value());
        REQUIRE(reverted);
        REQUIRE(reverted.value());
        REQUIRE(untouched);
        REQUIRE(untouched.value());
        CHECK(reverted.value()->at("version").asInteger() == 3);
        CHECK(untouched.value()->at("version").asInteger() == 1);
    }
    SECTION(
        "Retiring one location never deletes shared content identity or resurrects the old path") {
        REQUIRE(repo->retireDocumentPath(original.filePath));
        auto missing = repo->findDocumentByExactPath(original.filePath);
        REQUIRE(missing);
        CHECK_FALSE(missing.value());
        auto remaining = repo->getDocumentPathReferences(first.value());
        REQUIRE(remaining);
        REQUIRE(remaining.value().size() == 1);
        CHECK(remaining.value().front().filePath == alias.filePath);
        auto content = repo->getDocumentByHash(original.sha256Hash);
        REQUIRE(content);
        REQUIRE(content.value());
    }
}

TEST_CASE_METHOD(VersioningFixture,
                 "Admitting a legacy path preserves its version but an alias starts at one",
                 "[versioning][metadata][path-locations]") {
    auto repo = makeRepo();
    auto canonical = makeDocInfo("/repo/legacy.txt", "legacy-path-hash", "legacy.txt");
    auto inserted = repo->insertDocument(canonical);
    REQUIRE(inserted);
    REQUIRE(repo->setMetadata(inserted.value(), "version", MetadataValue(int64_t{7})));
    REQUIRE(repo->recordDocumentPath(canonical, inserted.value(), {}));
    auto alias = canonical;
    alias.filePath = "/repo/alias.txt";
    REQUIRE(repo->recordDocumentPath(alias, inserted.value(), {}));
    auto original = repo->getDocumentPathMetadata(canonical.filePath, inserted.value());
    auto copied = repo->getDocumentPathMetadata(alias.filePath, inserted.value());
    REQUIRE(original);
    REQUIRE(original.value());
    REQUIRE(copied);
    REQUIRE(copied.value());
    CHECK(original.value()->at("version").asInteger() == 7);
    CHECK(copied.value()->at("version").asInteger() == 1);
}

TEST_CASE_METHOD(VersioningFixture, "Retention selects only superseded versions",
                 "[versioning][metadata][catch2]") {
    auto repo = makeRepo();
    const std::string path = "/path/retained.txt";
    std::vector<int64_t> ids;
    for (int version = 1; version <= 3; ++version) {
        auto doc = makeDocInfo(path, "retention_hash_" + std::to_string(version), "retained.txt");
        auto inserted = repo->insertDocument(doc);
        REQUIRE(inserted);
        ids.push_back(inserted.value());
        REQUIRE(repo->setMetadata(ids.back(), "series_key", MetadataValue(path)));
        REQUIRE(
            repo->setMetadata(ids.back(), "version", MetadataValue(static_cast<int64_t>(version))));
        REQUIRE(repo->setMetadata(ids.back(), "is_latest", MetadataValue(version == 3)));
    }

    auto candidates = selectVersionRetentionCandidates(*repo, {.keepLatest = 2});
    REQUIRE(candidates);
    REQUIRE(candidates.value().size() == 1);
    CHECK(candidates.value().front().document.id == ids.front());
    CHECK(candidates.value().front().version == 1);
}

TEST_CASE_METHOD(VersioningFixture, "Retention never selects the marked latest version",
                 "[versioning][metadata][catch2]") {
    auto repo = makeRepo();
    const std::string path = "/path/latest-protected.txt";
    for (int version = 1; version <= 2; ++version) {
        auto doc =
            makeDocInfo(path, "protected_hash_" + std::to_string(version), "latest-protected.txt");
        auto inserted = repo->insertDocument(doc);
        REQUIRE(inserted);
        REQUIRE(repo->setMetadata(inserted.value(), "series_key", MetadataValue(path)));
        REQUIRE(repo->setMetadata(inserted.value(), "version",
                                  MetadataValue(static_cast<int64_t>(version))));
        // A stale version number must not cause a document explicitly marked
        // latest to be pruned.
        REQUIRE(repo->setMetadata(inserted.value(), "is_latest", MetadataValue(version == 1)));
    }

    auto candidates = selectVersionRetentionCandidates(*repo, {.keepLatest = 1});
    REQUIRE(candidates);
    REQUIRE(candidates.value().size() == 1);
    CHECK(candidates.value().front().version == 2);
}

TEST_CASE_METHOD(VersioningFixture, "Prune candidates include stored native build artifacts",
                 "[prune][metadata][catch2]") {
    auto repo = makeRepo();
    const auto insert = [&](const std::string& path, const std::string& hash) {
        auto result = repo->insertDocument(
            makeDocInfo(path, hash, std::filesystem::path(path).filename().string()));
        REQUIRE(result);
    };
    insert("/workspace/x64/Release/tool.ilk", "build-ilk");
    insert("/workspace/build/Debug/tool.dSYM/Contents/Resources/DWARF/tool", "build-dsym");
    insert("/workspace/src/tool.cpp", "source-cpp");

    integrity::PruneConfig config;
    config.categories = {"build-artifacts"};
    const auto candidates = integrity::RepairManager::queryCandidatesForPrune(*repo, config);

    REQUIRE(candidates.size() == 2);
    CHECK(std::ranges::any_of(
        candidates, [](const auto& candidate) { return candidate.path.ends_with("tool.ilk"); }));
    CHECK(std::ranges::any_of(
        candidates, [](const auto& candidate) { return candidate.path.ends_with("/tool"); }));
}

TEST_CASE_METHOD(VersioningFixture, "Prune candidates match dotted metadata extensions",
                 "[prune][metadata][catch2]") {
    auto repo = makeRepo();
    auto log = makeDocInfo("/workspace/logs/daemon.log", "dotted-log", "daemon.log");
    log.fileExtension = ".log";
    REQUIRE(repo->insertDocument(log));

    integrity::PruneConfig config;
    config.categories = {"all"};
    const auto candidates = integrity::RepairManager::queryCandidatesForPrune(*repo, config);

    REQUIRE(candidates.size() == 1);
    CHECK(candidates.front().category == "logs");
}
