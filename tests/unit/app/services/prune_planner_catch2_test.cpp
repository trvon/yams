// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (c) 2025 YAMS Contributors

#include <catch2/catch_test_macros.hpp>

#include <yams/app/services/prune_planner.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/document_metadata.h>
#include <yams/metadata/metadata_repository.h>

#include "../../../common/metadata_test_db.h"
#include "../../../common/metadata_test_documents.h"

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

using namespace yams;
using namespace yams::app::services;
using namespace yams::metadata;
using yams::test::makeDocInfo;

namespace {

struct PrunePlannerFixture {
    PrunePlannerFixture() {
        dbPath = yams::test::migrated_metadata_db_template().clone("yams_prune_planner_db_");

        ConnectionPoolConfig poolConfig;
        poolConfig.minConnections = 1;
        poolConfig.maxConnections = 2;
        pool = std::make_shared<ConnectionPool>(dbPath.string(), poolConfig);
        auto poolInitRes = pool->initialize();
        REQUIRE((poolInitRes.has_value()));

        repo = std::make_shared<MetadataRepository>(
            *pool, nullptr, MetadataRepository::SchemaBootstrapMode::AssumeReady);
    }

    ~PrunePlannerFixture() {
        repo.reset();
        pool.reset();
        yams::test::remove_sqlite_artifacts(dbPath);
    }

    void insert(const DocumentInfo& doc) {
        auto result = repo->insertDocument(doc);
        REQUIRE(result);
    }

    std::filesystem::path dbPath;
    std::shared_ptr<ConnectionPool> pool;
    std::shared_ptr<MetadataRepository> repo;
};

std::vector<std::string> sortedHashes(const std::vector<PruneCandidate>& candidates) {
    std::vector<std::string> hashes;
    hashes.reserve(candidates.size());
    for (const auto& candidate : candidates) {
        hashes.push_back(candidate.hash);
    }
    std::ranges::sort(hashes);
    return hashes;
}

} // namespace

TEST_CASE_METHOD(PrunePlannerFixture, "Prune candidates include stored native build artifacts",
                 "[prune][app][services][catch2]") {
    insert(makeDocInfo("/workspace/x64/Release/tool.ilk", "build-ilk"));
    insert(makeDocInfo("/workspace/build/Debug/tool.dSYM/Contents/Resources/DWARF/tool",
                       "build-dsym"));
    insert(makeDocInfo("/workspace/src/tool.cpp", "source-cpp"));

    PruneConfig config;
    config.categories = {"build-artifacts"};
    const auto result = queryPruneCandidates(*repo, config);
    REQUIRE(result);
    const auto& candidates = result.value();

    REQUIRE(candidates.size() == 2);
    CHECK(sortedHashes(candidates) == std::vector<std::string>{"build-dsym", "build-ilk"});
    for (const auto& candidate : candidates) {
        INFO(candidate.path);
        CHECK(candidate.category == "build-object");
        CHECK(candidate.fileSize == 100);
        if (candidate.hash == "build-ilk") {
            CHECK(candidate.path.ends_with("tool.ilk"));
        } else {
            CHECK(candidate.path.ends_with("/DWARF/tool"));
        }
    }
}

TEST_CASE_METHOD(PrunePlannerFixture, "Prune candidates match dotted metadata extensions",
                 "[prune][app][services][catch2]") {
    auto log = makeDocInfo("/workspace/logs/daemon.log", "dotted-log");
    log.fileExtension = ".log";
    insert(log);

    PruneConfig config;
    config.categories = {"all"};
    const auto result = queryPruneCandidates(*repo, config);
    REQUIRE(result);
    const auto& candidates = result.value();

    REQUIRE(candidates.size() == 1);
    CHECK(candidates.front().hash == "dotted-log");
    CHECK(candidates.front().path.ends_with("daemon.log"));
    CHECK(candidates.front().category == "logs");
    CHECK(candidates.front().fileSize == 100);
}

TEST_CASE_METHOD(PrunePlannerFixture, "Prune extension filter ignores case and a leading dot",
                 "[prune][app][services][catch2]") {
    auto upper = makeDocInfo("/workspace/logs/DAEMON.LOG", "upper-log");
    upper.fileExtension = ".LOG";
    insert(upper);
    auto bare = makeDocInfo("/workspace/logs/worker.log", "bare-log");
    bare.fileExtension = "log";
    insert(bare);
    insert(makeDocInfo("/workspace/notes/readme.txt", "notes-txt"));

    for (const std::string filter : {"log", ".log", "LOG", ".Log"}) {
        INFO("extension filter: " << filter);
        PruneConfig config;
        config.extensions = {filter};
        const auto result = queryPruneCandidates(*repo, config);
        REQUIRE(result);
        CHECK(sortedHashes(result.value()) == std::vector<std::string>{"bare-log", "upper-log"});
    }
}

namespace {

class QueryFailingMetadataRepository : public MetadataRepository {
public:
    explicit QueryFailingMetadataRepository(ConnectionPool& pool)
        : MetadataRepository(pool, nullptr, SchemaBootstrapMode::AssumeReady) {}

    Result<std::vector<DocumentInfo>> queryDocuments(const DocumentQueryOptions&) override {
        return Error{ErrorCode::DatabaseError, "injected query failure"};
    }
};

} // namespace

TEST_CASE_METHOD(PrunePlannerFixture, "Prune planning reports a failed metadata query",
                 "[prune][app][services][catch2]") {
    insert(makeDocInfo("/workspace/logs/daemon.log", "unreached-log"));
    QueryFailingMetadataRepository failing(*pool);

    const auto result = queryPruneCandidates(failing, PruneConfig{});

    REQUIRE_FALSE(result);
    CHECK(result.error().code == ErrorCode::DatabaseError);
    CHECK(result.error().message.find("injected query failure") != std::string::npos);
}
