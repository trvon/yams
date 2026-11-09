// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (c) 2025 YAMS Contributors

#include <catch2/catch_test_macros.hpp>

#include <yams/app/services/prune_planner.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/document_metadata.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/path_utils.h>

#include "../../../common/metadata_test_db.h"

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

using namespace yams;
using namespace yams::app::services;
using namespace yams::metadata;

namespace {

DocumentInfo makeDocInfo(const std::string& filePath, const std::string& hash) {
    DocumentInfo info;
    info.filePath = filePath;
    info.fileName = std::filesystem::path(filePath).filename().string();
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
    const auto candidates = queryPruneCandidates(*repo, config);

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
    const auto candidates = queryPruneCandidates(*repo, config);

    REQUIRE(candidates.size() == 1);
    CHECK(candidates.front().hash == "dotted-log");
    CHECK(candidates.front().path.ends_with("daemon.log"));
    CHECK(candidates.front().category == "logs");
    CHECK(candidates.front().fileSize == 100);
}
