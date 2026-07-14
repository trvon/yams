// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

#include <chrono>
#include <filesystem>
#include <future>
#include <memory>
#include <string>
#include <vector>

#include <catch2/catch_test_macros.hpp>

#include <yams/metadata/connection_pool.h>
#include <yams/metadata/content_index_writer.h>
#include <yams/metadata/database.h>
#include <yams/metadata/metadata_insert_writer.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/path_utils.h>

#include "../../common/sqlite_corruption.h"

using namespace yams;
using namespace yams::metadata;
using namespace yams::test;

// NOLINTBEGIN(bugprone-chained-comparison)
namespace {

constexpr std::chrono::seconds kFutureTimeout{30};

struct ArtifactGuard {
    explicit ArtifactGuard(std::filesystem::path p) : path(std::move(p)) {}
    ~ArtifactGuard() { remove_sqlite_artifacts(path); }
    std::filesystem::path path;
};

struct CorruptRepoFixture {
    CorruptRepoFixture() : dbPath_(migrated_metadata_db_template().clone("yams_writer_corrupt_")) {
        corruptPage(dbPath_, static_cast<std::uint64_t>(tableRootPage(dbPath_, "documents")));

        ConnectionPoolConfig config;
        config.minConnections = 1;
        config.maxConnections = 2;
        pool_ = std::make_unique<ConnectionPool>(dbPath_.string(), config);
        REQUIRE(pool_->initialize());
        repo_ = std::make_shared<MetadataRepository>(
            *pool_, nullptr, MetadataRepository::SchemaBootstrapMode::AssumeReady);
    }

    ~CorruptRepoFixture() {
        repo_.reset();
        pool_->shutdown();
        pool_.reset();
        remove_sqlite_artifacts(dbPath_);
    }

    std::filesystem::path dbPath_;
    std::unique_ptr<ConnectionPool> pool_;
    std::shared_ptr<MetadataRepository> repo_;
};

BatchDocumentInsert makeInsert(int i) {
    BatchDocumentInsert record;
    auto& info = record.info;
    info.filePath = "/corrupt/doc_" + std::to_string(i) + ".txt";
    info.fileName = "doc_" + std::to_string(i) + ".txt";
    info.fileExtension = ".txt";
    info.fileSize = 100 + i;
    info.sha256Hash = std::string(60, '0') + std::to_string(1000 + i);
    info.mimeType = "text/plain";
    info.createdTime = std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
    info.modifiedTime = info.createdTime;
    info.indexedTime = info.createdTime;
    populatePathDerivedFields(info);
    return record;
}

BatchContentEntry makeContentEntry(int64_t documentId) {
    BatchContentEntry entry;
    entry.documentId = documentId;
    entry.title = "title";
    entry.contentText = "content text";
    entry.mimeType = "text/plain";
    entry.extractionMethod = "test";
    entry.language = "en";
    return entry;
}

} // namespace

TEST_CASE("MetadataInsertWriter resolves all futures with errors on corrupt DB",
          "[unit][metadata][corruption][writer]") {
    CorruptRepoFixture fix;
    MetadataInsertWriter writer(fix.repo_);

    std::vector<std::future<Result<int64_t>>> futures;
    futures.reserve(8);
    for (int i = 0; i < 8; ++i) {
        futures.push_back(writer.submit(makeInsert(i)));
    }

    for (auto& future : futures) {
        REQUIRE(future.wait_for(kFutureTimeout) == std::future_status::ready);
        auto result = future.get();
        CHECK_FALSE(result);
    }

    REQUIRE(writer.flush());
    const auto metrics = writer.metricsSnapshot();
    CHECK(metrics.submittedItems == 8);
    CHECK(metrics.completedItems == 8);
    CHECK(metrics.failedBatches > 0);
    CHECK(metrics.fallbackItems == 8);
    writer.shutdown();
}

TEST_CASE("MetadataInsertWriter shutdown mid-queue abandons no promises",
          "[unit][metadata][corruption][writer]") {
    CorruptRepoFixture fix;
    MetadataInsertWriter writer(
        fix.repo_, MetadataInsertWriter::Options{.maxBatchCount = 2,
                                                 .maxDelay = std::chrono::microseconds{50000}});

    std::vector<std::future<Result<int64_t>>> futures;
    futures.reserve(16);
    for (int i = 0; i < 16; ++i) {
        futures.push_back(writer.submit(makeInsert(i)));
    }
    writer.shutdown();

    for (auto& future : futures) {
        REQUIRE(future.wait_for(kFutureTimeout) == std::future_status::ready);
        auto result = future.get();
        CHECK_FALSE(result);
    }

    auto rejected = writer.submit(makeInsert(99));
    REQUIRE(rejected.wait_for(kFutureTimeout) == std::future_status::ready);
    auto rejectedResult = rejected.get();
    REQUIRE_FALSE(rejectedResult);
    CHECK(rejectedResult.error().code == ErrorCode::InvalidState);

    const auto metrics = writer.metricsSnapshot();
    CHECK(metrics.submittedItems == 16);
    CHECK(metrics.completedItems == 16);
    CHECK(metrics.rejectedItems == 1);
}

TEST_CASE("ContentIndexWriter resolves all futures with errors on corrupt DB",
          "[unit][metadata][corruption][writer]") {
    CorruptRepoFixture fix;
    ContentIndexWriter writer(fix.repo_);

    std::vector<std::future<Result<void>>> futures;
    futures.reserve(4);
    for (int i = 0; i < 4; ++i) {
        futures.push_back(writer.submit({makeContentEntry(i + 1)}));
    }

    for (auto& future : futures) {
        REQUIRE(future.wait_for(kFutureTimeout) == std::future_status::ready);
        auto result = future.get();
        CHECK_FALSE(result);
    }

    writer.shutdown();
}
// NOLINTEND(bugprone-chained-comparison)
