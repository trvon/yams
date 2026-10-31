// RepairScanCursor: the startup repair scan resumes after hitting the queue cap, so an
// embedding-space migration re-embeds the whole corpus, not just the first maxPendingRepairs.

#include <catch2/catch_test_macros.hpp>

#include "../../common/test_helpers_catch2.h"

#include <yams/daemon/components/RepairService.h>
#include <yams/daemon/components/ResourceGovernor.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/metadata_repository.h>

#include <algorithm>
#include <memory>
#include <set>
#include <string>
#include <vector>

using yams::daemon::RepairScanCursor;

TEST_CASE("RepairScanCursor runs once when the scan completes", "[daemon][repair][scan][catch2]") {
    RepairScanCursor cursor;
    REQUIRE(cursor.shouldStart());
    CHECK(cursor.begin() == 0U);
    CHECK_FALSE(cursor.shouldStart()); // in flight
    cursor.finished();
    CHECK_FALSE(cursor.shouldStart()); // whole corpus scanned
}

TEST_CASE("RepairScanCursor resumes where a capped scan stopped",
          "[daemon][repair][scan][catch2]") {
    RepairScanCursor cursor;
    REQUIRE(cursor.shouldStart());
    CHECK(cursor.begin() == 0U);
    cursor.stoppedAt(1000);
    REQUIRE(cursor.shouldStart());
    CHECK(cursor.begin() == 1000U);
    cursor.stoppedAt(2000);
    REQUIRE(cursor.shouldStart());
    CHECK(cursor.begin() == 2000U);
    cursor.finished();
    CHECK_FALSE(cursor.shouldStart());
}

TEST_CASE("RepairScanCursor never starts two scans at once", "[daemon][repair][scan][catch2]") {
    RepairScanCursor cursor;
    REQUIRE(cursor.shouldStart());
    (void)cursor.begin();
    CHECK_FALSE(cursor.shouldStart());
    cursor.aborted(); // exception or shutdown mid-scan: allow a retry from the same offset
    REQUIRE(cursor.shouldStart());
    CHECK(cursor.begin() == 0U);
}

namespace {

using yams::daemon::RepairService;
using yams::daemon::RepairServiceContext;

// A metadata store with `n` documents that have content but no embeddings, and a RepairService
// over it with vectors enabled and no vector database (embedding state comes from metadata).
struct ScanHarness {
    yams::test::TempDirGuard tmp{"yams_repair_scan_"};
    std::unique_ptr<yams::metadata::ConnectionPool> pool;
    std::shared_ptr<yams::metadata::MetadataRepository> repo;
    std::vector<std::string> hashes;
    std::vector<int64_t> ids;
    std::unique_ptr<RepairService> service;
    yams::test::ScopedEnvVar vectorsOn{"YAMS_DISABLE_VECTORS", std::nullopt};

    explicit ScanHarness(int n, std::size_t maxPending = 1000) {
        yams::daemon::ResourceGovernor::instance().testing_setPressureState(
            yams::daemon::ResourcePressureLevel::Normal, std::chrono::steady_clock::now());
        yams::metadata::ConnectionPoolConfig pc;
        pc.minConnections = 1;
        pc.maxConnections = 2;
        pool = std::make_unique<yams::metadata::ConnectionPool>((tmp.path() / "yams.db").string(),
                                                                pc);
        REQUIRE(pool->initialize().has_value());
        repo = std::make_shared<yams::metadata::MetadataRepository>(*pool);
        for (int i = 0; i < n; ++i) {
            yams::metadata::DocumentInfo doc;
            doc.fileName = "doc" + std::to_string(i) + ".txt";
            doc.filePath = (tmp.path() / doc.fileName).string();
            doc.fileExtension = ".txt";
            doc.fileSize = 4;
            doc.sha256Hash = std::string(63, 'a') + static_cast<char>('a' + i);
            doc.mimeType = "text/plain";
            doc.contentExtracted = true;
            doc.extractionStatus = yams::metadata::ExtractionStatus::Skipped;
            auto id = repo->insertDocument(doc);
            REQUIRE(id.has_value());
            ids.push_back(id.value());
            hashes.push_back(doc.sha256Hash);
        }
        RepairServiceContext ctx;
        ctx.getMetadataRepo = [r = repo] { return r; };
        RepairService::Config cfg;
        cfg.enable = true;
        cfg.dataDir = tmp.path();
        cfg.maxPendingRepairs = maxPending;
        service = std::make_unique<RepairService>(
            ctx, nullptr, [] { return std::size_t{0}; }, cfg);
    }
    ~ScanHarness() {
        service.reset();
        repo.reset();
        pool->shutdown();
    }
    bool queuedIn(const std::vector<std::string>& q, const std::string& h) const {
        return std::find(q.begin(), q.end(), h) != q.end();
    }
};

} // namespace

// A daemon killed during a re-embed leaves the batches it had queued marked Processing. The next
// start owns no such work, so those documents must be scanned again.
TEST_CASE("Startup scan re-queues documents a previous run left Processing",
          "[daemon][repair][scan][migrate][catch2]") {
    ScanHarness h(3);
    REQUIRE(h.repo->updateDocumentRepairStatus(h.hashes[1],
                                               yams::metadata::RepairStatus::Processing));
    const auto queued = h.service->testing_runStartupScanPass();
    CHECK(h.queuedIn(queued, h.hashes[0]));
    CHECK(h.queuedIn(queued, h.hashes[1]));
    CHECK(h.queuedIn(queued, h.hashes[2]));
}

// Capped passes resume from a remembered position. Deleting documents the scan already passed
// must not shift the remaining ones past it.
TEST_CASE("Startup scan covers every document when earlier ones are deleted between passes",
          "[daemon][repair][scan][migrate][catch2]") {
    yams::test::ScopedEnvVar batch{"YAMS_REPAIR_STARTUP_BATCH", std::string{"2"}};
    ScanHarness h(6, /*maxPending=*/3);

    std::set<std::string> seen;
    auto first = h.service->testing_runStartupScanPass();
    seen.insert(first.begin(), first.end());
    REQUIRE(first.size() == 3U);           // capped
    REQUIRE(h.service->testing_startupScanDue());
    // The first three were embedded; the user then deletes two of them.
    for (int i = 0; i < 3; ++i)
        REQUIRE(h.repo->updateDocumentEmbeddingStatusByHash(h.hashes[i], true, "m"));
    REQUIRE(h.repo->deleteDocument(h.ids[0]));
    REQUIRE(h.repo->deleteDocument(h.ids[1]));

    for (int pass = 0; pass < 5 && h.service->testing_startupScanDue(); ++pass) {
        auto q = h.service->testing_runStartupScanPass();
        seen.insert(q.begin(), q.end());
    }
    for (int i = 3; i < 6; ++i) {
        INFO("document " << i);
        CHECK(seen.count(h.hashes[i]) == 1U);
    }
}

// Embedding can fail transiently right after a migration (provider still loading). Once the
// queue drains, documents that still have no vector must get another pass in the same run.
TEST_CASE("Startup scan runs again when documents are still unembedded after the queue drains",
          "[daemon][repair][scan][migrate][catch2]") {
    ScanHarness h(2);
    const auto queued = h.service->testing_runStartupScanPass();
    REQUIRE(queued.size() == 2U);
    // hashes[0] embedded; hashes[1] failed.
    REQUIRE(h.repo->updateDocumentEmbeddingStatusByHash(h.hashes[0], true, "m"));
    REQUIRE(h.repo->updateDocumentRepairStatus(h.hashes[1], yams::metadata::RepairStatus::Failed));
    REQUIRE(h.service->testing_startupScanDue());
    const auto retry = h.service->testing_runStartupScanPass();
    CHECK(retry == std::vector<std::string>{h.hashes[1]});
}
