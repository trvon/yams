// RepairScanCursor and the startup scan: the scan resumes after hitting the queue cap and retries
// documents that are still unembedded, so an embedding-space migration re-embeds the whole corpus.

#include <catch2/catch_test_macros.hpp>

#include "../../common/test_helpers_catch2.h"

#include <yams/daemon/components/RepairService.h>
#include <yams/daemon/components/ResourceGovernor.h>
#include <yams/extraction/content_extractor.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/metadata_repository.h>

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <memory>
#include <set>
#include <string>
#include <vector>

using yams::daemon::RepairScanCursor;

TEST_CASE("RepairScanCursor runs once when the scan completes", "[daemon][repair][scan][catch2]") {
    RepairScanCursor cursor;
    REQUIRE(cursor.shouldStart());
    CHECK(cursor.begin() == 0);
    CHECK_FALSE(cursor.shouldStart()); // in flight
    cursor.finished();
    CHECK_FALSE(cursor.shouldStart()); // whole corpus scanned
}

TEST_CASE("RepairScanCursor resumes where a capped scan stopped",
          "[daemon][repair][scan][catch2]") {
    RepairScanCursor cursor;
    REQUIRE(cursor.shouldStart());
    CHECK(cursor.begin() == 0);
    cursor.stoppedAt(1000);
    REQUIRE(cursor.shouldStart());
    CHECK(cursor.begin() == 1000);
    cursor.stoppedAt(2000);
    REQUIRE(cursor.shouldStart());
    CHECK(cursor.begin() == 2000);
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
    CHECK(cursor.begin() == 0);
}

TEST_CASE("RepairScanCursor retries a sweep that queued work, a bounded number of times",
          "[daemon][repair][scan][catch2]") {
    RepairScanCursor cursor;
    (void)cursor.begin();
    cursor.stoppedAt(500, /*queuedWork=*/true);
    CHECK_FALSE(cursor.retryDue()); // resuming, not retrying
    CHECK(cursor.begin() == 500);
    cursor.finished(); // the capped segment queued work earlier in the sweep
    REQUIRE(cursor.shouldStart());
    CHECK(cursor.retryDue());
    for (int i = 0; i < RepairScanCursor::kMaxRetrySweeps; ++i) {
        REQUIRE(cursor.shouldStart());
        CHECK(cursor.begin() == 0);
        cursor.finished(/*queuedWork=*/true);
    }
    CHECK_FALSE(cursor.shouldStart());
}

TEST_CASE("RepairScanCursor stops after a sweep that queued nothing",
          "[daemon][repair][scan][catch2]") {
    RepairScanCursor cursor;
    (void)cursor.begin();
    cursor.finished(/*queuedWork=*/true);
    REQUIRE(cursor.shouldStart());
    (void)cursor.begin();
    cursor.finished(/*queuedWork=*/false);
    CHECK_FALSE(cursor.shouldStart());
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
    std::vector<std::shared_ptr<yams::extraction::IContentExtractor>> extractors;
    // The unit lanes disable vectors through any of these; this harness needs embeddings on.
    yams::test::ScopedEnvVar vectorsOn{"YAMS_DISABLE_VECTORS", std::nullopt};
    yams::test::ScopedEnvVar vectorsOnAlias{"YAMS_DISABLE_VECTOR_DB", std::nullopt};
    yams::test::ScopedEnvVar vectorsOnSingular{"YAMS_DISABLE_VECTOR", std::nullopt};

    explicit ScanHarness(int n, std::size_t maxPending = 1000) {
        yams::daemon::ResourceGovernor::instance().testing_setPressureState(
            yams::daemon::ResourcePressureLevel::Normal, std::chrono::steady_clock::now());
        yams::metadata::ConnectionPoolConfig pc;
        pc.minConnections = 1;
        pc.maxConnections = 2;
        pool =
            std::make_unique<yams::metadata::ConnectionPool>((tmp.path() / "yams.db").string(), pc);
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
        ctx.getContentExtractors = [this] { return extractors; };
        RepairService::Config cfg;
        cfg.enable = true;
        cfg.dataDir = tmp.path();
        cfg.maxPendingRepairs = maxPending;
        // A data dir that already finished a sweep with no extractors; tests that need the
        // first-run retry remove it.
        std::ofstream(tmp.path() / "repair-extractor-set")
            << RepairService::extractorFingerprint({});
        service = std::make_unique<RepairService>(ctx, nullptr, [] { return std::size_t{0}; }, cfg);
    }
    ~ScanHarness() {
        service.reset();
        repo.reset();
        pool->shutdown();
    }
    // Extraction ran and produced nothing (a binary blob): no content, no embedding.
    void markExtractionFailed(int i) {
        REQUIRE(repo->updateDocumentExtractionStatus(
            ids[i], false, yams::metadata::ExtractionStatus::Failed, "No text extracted"));
        REQUIRE(repo->updateDocumentRepairStatus(hashes[i], yams::metadata::RepairStatus::Failed));
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
    REQUIRE(
        h.repo->updateDocumentRepairStatus(h.hashes[1], yams::metadata::RepairStatus::Processing));
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
    REQUIRE(first.size() == 3U); // capped
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

namespace {
// Claims every document, as a newly installed extractor plugin would for its formats.
class AcceptAllExtractor final : public yams::extraction::IContentExtractor {
public:
    bool supports(const std::string&, const std::string&) const override { return true; }
    std::optional<std::string> extractText(const std::vector<std::byte>&, const std::string&,
                                           const std::string&) override {
        return std::string{"text"};
    }
};
} // namespace

// A binary whose extraction failed has no content to embed or index. Re-queuing it every sweep
// failed the same ~1.6k documents 4 times per daemon run on the dev corpus and churned their
// status rows; it is terminal until something that could extract it changes.
TEST_CASE("Startup scan skips documents whose extraction failed with no content",
          "[daemon][repair][scan][terminal][catch2]") {
    ScanHarness h(3);
    h.markExtractionFailed(1);
    const auto queued = h.service->testing_runStartupScanPass();
    CHECK(h.queuedIn(queued, h.hashes[0]));
    CHECK_FALSE(h.queuedIn(queued, h.hashes[1]));
    CHECK(h.queuedIn(queued, h.hashes[2]));
}

TEST_CASE("Startup scan does not retry a sweep whose only candidates are terminal",
          "[daemon][repair][scan][terminal][catch2]") {
    ScanHarness h(2);
    h.markExtractionFailed(0);
    h.markExtractionFailed(1);
    CHECK(h.service->testing_runStartupScanPass().empty());
    CHECK_FALSE(h.service->testing_startupScanDue());
}

// Documents the per-document check would drop (already embedded, extraction pending, a format
// nothing can extract) must not be queued either, or every sweep counts as "queued work" and
// re-arms a retry for nothing.
TEST_CASE("Startup scan only queues documents the repair batch would act on",
          "[daemon][repair][scan][terminal][catch2]") {
    ScanHarness h(1);
    REQUIRE(h.repo->updateDocumentEmbeddingStatusByHash(h.hashes[0], true, "m"));
    auto doc = h.repo->getDocumentByHash(h.hashes[0]);
    REQUIRE(doc.has_value());
    REQUIRE(doc.value().has_value());
    auto info = *doc.value();
    info.mimeType = "application/x-unextractable";
    info.fileExtension = ".bin";
    REQUIRE(h.repo->updateDocument(info));
    REQUIRE(h.repo->updateDocumentExtractionStatus(h.ids[0], false,
                                                   yams::metadata::ExtractionStatus::Pending));
    CHECK(h.service->testing_runStartupScanPass().empty());
    CHECK_FALSE(h.service->testing_startupScanDue());
}

TEST_CASE("Startup scan retries terminal documents once the extractor set changes",
          "[daemon][repair][scan][terminal][catch2]") {
    ScanHarness h(1);
    h.markExtractionFailed(0);
    REQUIRE(h.service->testing_runStartupScanPass().empty());

    // A new extractor plugin is installed; the next daemon run scans from the start.
    h.extractors.push_back(std::make_shared<AcceptAllExtractor>());
    RepairService::Config cfg;
    cfg.enable = true;
    cfg.dataDir = h.tmp.path();
    RepairServiceContext ctx;
    ctx.getMetadataRepo = [r = h.repo] { return r; };
    ctx.getContentExtractors = [&h] { return h.extractors; };
    h.service = std::make_unique<RepairService>(ctx, nullptr, [] { return std::size_t{0}; }, cfg);
    const auto queued = h.service->testing_runStartupScanPass();
    CHECK(h.queuedIn(queued, h.hashes[0]));

    // Once that sweep finished, the same extractor set does not retry it again.
    h.service = std::make_unique<RepairService>(ctx, nullptr, [] { return std::size_t{0}; }, cfg);
    CHECK(h.service->testing_runStartupScanPass().empty());
}

// The extractor set is recorded only when a sweep finishes. A first sweep that stops at the queue
// cap must leave no marker, or documents after the stop point would lose their retry.
TEST_CASE("Startup scan records the extractor set only after a finished sweep",
          "[daemon][repair][scan][terminal][catch2]") {
    yams::test::ScopedEnvVar batch{"YAMS_REPAIR_STARTUP_BATCH", std::string{"1"}};
    ScanHarness h(3, /*maxPending=*/1);
    const auto marker = h.tmp.path() / "repair-extractor-set";
    std::filesystem::remove(marker); // a data dir from before the marker existed
    REQUIRE(h.service->testing_runStartupScanPass().size() == 1U); // capped
    CHECK_FALSE(std::filesystem::exists(marker));
    for (int pass = 0; pass < 6 && h.service->testing_startupScanDue(); ++pass) {
        (void)h.service->testing_runStartupScanPass();
    }
    CHECK(std::filesystem::exists(marker));
}

// Ingest and explicit requests also feed the repair queue. Documents handled before the scan has
// queued anything must not count toward scan documents queued later.
TEST_CASE("Repair progress does not pre-count documents outside the scan backlog",
          "[daemon][repair][progress][catch2]") {
    yams::test::ScopedEnvVar batch{"YAMS_REPAIR_STARTUP_BATCH", std::string{"2"}};
    ScanHarness h(6, /*maxPending=*/3);
    REQUIRE(h.service->testing_runStartupScanPass().size() == 3U); // capped
    h.service->testing_noteRepairResolved(3);
    CHECK(h.service->testing_repairProgress() == std::pair<std::uint64_t, std::uint64_t>{3, 3});

    // Freshly ingested documents are handled before the scan resumes.
    h.service->testing_noteRepairResolved(2);
    REQUIRE(h.service->testing_startupScanDue());
    REQUIRE(h.service->testing_runStartupScanPass().size() == 3U); // resumed pass
    CHECK(h.service->testing_repairProgress() == std::pair<std::uint64_t, std::uint64_t>{6, 3});
}

// A data dir from before the marker existed: documents that failed extraction under unknown rules
// (here, PEM files that were not yet treated as text) get one retry sweep, then stay terminal.
TEST_CASE("Startup scan retries terminal documents once when no extractor set is recorded",
          "[daemon][repair][scan][terminal][catch2]") {
    ScanHarness h(1);
    std::filesystem::remove(h.tmp.path() / "repair-extractor-set");
    h.markExtractionFailed(0);
    RepairServiceContext ctx;
    ctx.getMetadataRepo = [r = h.repo] { return r; };
    RepairService::Config cfg;
    cfg.enable = true;
    cfg.dataDir = h.tmp.path();
    h.service = std::make_unique<RepairService>(ctx, nullptr, [] { return std::size_t{0}; }, cfg);
    CHECK(h.queuedIn(h.service->testing_runStartupScanPass(), h.hashes[0]));

    h.service = std::make_unique<RepairService>(ctx, nullptr, [] { return std::size_t{0}; }, cfg);
    CHECK(h.service->testing_runStartupScanPass().empty());
}
