#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <filesystem>
#include <functional>
#include <optional>
#include <span>
#include <string>
#include <thread>

#include <cstdlib>

#include "../../common/test_helpers_catch2.h"

#include <yams/daemon/components/DaemonLifecycleFsm.h>
#include <yams/daemon/components/InternalEventBus.h>
#include <yams/daemon/components/RepairService.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/daemon.h>

using namespace yams;
using namespace yams::daemon;

namespace {

template <typename T> void drainQueue(const std::shared_ptr<SpscQueue<T>>& q) {
    if (!q)
        return;
    T tmp{};
    while (q->try_pop(tmp)) {
    }
}

struct ServiceManagerFixture {
    DaemonConfig config_;
    StateComponent state_;
    DaemonLifecycleFsm lifecycleFsm_;
    std::filesystem::path testDir_;

    ServiceManagerFixture() {
        namespace fs = std::filesystem;
        testDir_ = fs::temp_directory_path() /
                   ("repair_service_test_" +
                    std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        fs::create_directories(testDir_);

        config_.dataDir = testDir_ / "data";
        config_.socketPath = testDir_ / "daemon.sock";
        config_.pidFile = testDir_ / "daemon.pid";
        config_.logFile = testDir_ / "daemon.log";
        fs::create_directories(config_.dataDir);
    }

    ~ServiceManagerFixture() {
        namespace fs = std::filesystem;
        if (fs::exists(testDir_)) {
            std::error_code ec;
            fs::remove_all(testDir_, ec);
        }
    }
};

std::optional<RepairOperationResult> findOperationResult(const RepairResponse& response,
                                                         std::string_view operation) {
    for (const auto& result : response.operationResults) {
        if (result.operation == operation) {
            return result;
        }
    }
    return std::nullopt;
}

bool waitForCondition(std::chrono::milliseconds timeout, const std::function<bool()>& predicate) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return predicate();
}

} // namespace

TEST_CASE_METHOD(ServiceManagerFixture,
                 "RepairService: stuck-doc recovery enqueues to PostIngestQueue channel",
                 "[daemon][repair][stuck_docs][bus]") {
    // Keep this test isolated from other bus tests (singleton channels).
    auto postIngest =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::PostIngestTask>(
            "post_ingest", 64);
    auto postIngestRpc =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::PostIngestTask>(
            "post_ingest_rpc", 64);
    auto postIngestTasks =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::PostIngestTask>(
            "post_ingest_tasks", 64);
    drainQueue(postIngest);
    drainQueue(postIngestRpc);
    drainQueue(postIngestTasks);

    // Ensure optional subsystems don't trigger heavy init in this unit test.
    yams::test::ScopedEnvVar disableVectors("YAMS_DISABLE_VECTORS",
                                            std::optional<std::string>{"1"});
    yams::test::ScopedEnvVar disableVectorDb("YAMS_DISABLE_VECTOR_DB",
                                             std::optional<std::string>{"1"});
    yams::test::ScopedEnvVar skipModelLoading("YAMS_SKIP_MODEL_LOADING",
                                              std::optional<std::string>{"1"});
    yams::test::ScopedEnvVar safeSingleInstance("YAMS_TEST_SAFE_SINGLE_INSTANCE",
                                                std::optional<std::string>{"1"});

    auto sm = std::make_shared<ServiceManager>(config_, state_, lifecycleFsm_);
    REQUIRE(sm->initialize());
    sm->startAsyncInit();

    // Wait for async initialization to settle before reading optional subsystem pointers.
    auto smSnap = sm->waitForServiceManagerTerminalState(30);
    REQUIRE(smSnap.state == ServiceManagerState::Ready);

    // Async init can race this test; wait briefly for repo availability.
    auto meta = sm->getMetadataRepo();
    for (int i = 0; i < 100 && meta == nullptr; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        meta = sm->getMetadataRepo();
    }
    REQUIRE(meta != nullptr);

    // PostIngestQueue is started asynchronously and can consume tasks quickly.
    // Pause it to make channel assertions deterministic.
    auto piq = sm->getPostIngestQueue();
    for (int i = 0; i < 200 && piq == nullptr; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        piq = sm->getPostIngestQueue();
    }
    REQUIRE(piq != nullptr);
    for (int i = 0; i < 200 && !piq->started(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    REQUIRE(piq->started());
    piq->pauseAll();

    // Clear any background noise after pause.
    drainQueue(postIngest);
    drainQueue(postIngestRpc);
    drainQueue(postIngestTasks);

    metadata::DocumentInfo doc{};
    doc.fileName = "stuck.txt";
    doc.filePath = (config_.dataDir / "stuck.txt").string();
    doc.fileExtension = "txt";
    doc.fileSize = 1;
    doc.sha256Hash = std::string(64, 'a');
    doc.mimeType = "text/plain";
    doc.setCreatedTime(1);
    doc.setModifiedTime(1);
    doc.setIndexedTime(1);

    auto idRes = meta->insertDocument(doc);
    REQUIRE(idRes.has_value());
    const int64_t docId = idRes.value();

    // Mark as failed so it is always detected as "stuck".
    auto st = meta->updateDocumentExtractionStatus(docId, false, metadata::ExtractionStatus::Failed,
                                                   "test stuck");
    REQUIRE(st.has_value());

    auto rs =
        meta->batchUpdateDocumentRepairStatuses({doc.sha256Hash}, metadata::RepairStatus::Pending);
    REQUIRE(rs.has_value());

    RepairService::Config cfg;
    cfg.enable = false; // executeRepair() is synchronous; don't start the background loop here
    cfg.maxRetries = 3;

    RepairService repair(sm.get(), &state_, []() -> size_t { return 0; }, cfg);
    RepairRequest req;
    req.repairStuckDocs = true;
    req.dryRun = false;
    req.maxRetries = 3;

    (void)repair.executeRepair(req, nullptr);

    // Correct behavior: tasks land on the high-priority channel PostIngestQueue consumes first
    // ("post_ingest_rpc").
    REQUIRE(postIngestRpc->size_approx() == 1);
    REQUIRE(postIngest->size_approx() == 0);
    REQUIRE(postIngestTasks->size_approx() == 0);

    drainQueue(postIngest);
    drainQueue(postIngestRpc);
    drainQueue(postIngestTasks);

    piq->resumeAll();

    sm->shutdown();
}

TEST_CASE_METHOD(ServiceManagerFixture,
                 "RepairService: post-ingest success should mark repair status completed",
                 "[daemon][repair][regression][post-ingest]") {
    yams::test::ScopedEnvVar disableVectors("YAMS_DISABLE_VECTORS",
                                            std::optional<std::string>{"1"});
    yams::test::ScopedEnvVar disableVectorDb("YAMS_DISABLE_VECTOR_DB",
                                             std::optional<std::string>{"1"});
    yams::test::ScopedEnvVar skipModelLoading("YAMS_SKIP_MODEL_LOADING",
                                              std::optional<std::string>{"1"});
    yams::test::ScopedEnvVar safeSingleInstance("YAMS_TEST_SAFE_SINGLE_INSTANCE",
                                                std::optional<std::string>{"1"});

    auto sm = std::make_shared<ServiceManager>(config_, state_, lifecycleFsm_);
    REQUIRE(sm->initialize());
    sm->startAsyncInit();

    auto smSnap = sm->waitForServiceManagerTerminalState(30);
    REQUIRE(smSnap.state == ServiceManagerState::Ready);

    auto meta = sm->getMetadataRepo();
    auto store = sm->getContentStore();
    REQUIRE(meta != nullptr);
    REQUIRE(store != nullptr);

    auto piq = sm->getPostIngestQueue();
    for (int i = 0; i < 200 && piq == nullptr; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        piq = sm->getPostIngestQueue();
    }
    REQUIRE(piq != nullptr);
    for (int i = 0; i < 200 && !piq->started(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    REQUIRE(piq->started());
    piq->resumeAll();

    auto postIngestRpc =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::PostIngestTask>(
            "post_ingest_rpc", 64);
    REQUIRE(postIngestRpc != nullptr);
    drainQueue(postIngestRpc);

    const std::string text = "repair service regression test content";
    const auto textBytes = std::as_bytes(std::span<const char>(text.data(), text.size()));
    auto storeRes = store->storeBytes(textBytes);
    REQUIRE(storeRes.has_value());
    const auto hash = storeRes.value().contentHash;

    metadata::DocumentInfo doc{};
    doc.fileName = "regression.txt";
    doc.filePath = (config_.dataDir / "regression.txt").string();
    doc.fileExtension = "txt";
    doc.fileSize = text.size();
    doc.sha256Hash = hash;
    doc.mimeType = "text/plain";
    const auto now =
        std::chrono::time_point_cast<std::chrono::seconds>(std::chrono::system_clock::now());
    doc.modifiedTime = now;
    doc.indexedTime = now;

    auto idRes = meta->insertDocument(doc);
    REQUIRE(idRes.has_value());
    const int64_t docId = idRes.value();

    auto statusRes =
        meta->batchUpdateDocumentRepairStatuses({hash}, metadata::RepairStatus::Processing);
    REQUIRE(statusRes.has_value());

    InternalEventBus::PostIngestTask task;
    task.hash = hash;
    task.mime = "text/plain";
    REQUIRE(postIngestRpc->try_push(task));

    const bool completed = waitForCondition(std::chrono::seconds(5), [&]() {
        auto docRes = meta->getDocument(docId);
        if (!docRes || !docRes.value().has_value()) {
            return false;
        }
        const auto& current = docRes.value().value();
        return current.repairStatus == metadata::RepairStatus::Completed;
    });
    REQUIRE(completed);

    auto finalDocRes = meta->getDocument(docId);
    REQUIRE(finalDocRes.has_value());
    REQUIRE(finalDocRes.value().has_value());
    CHECK(finalDocRes.value()->repairStatus == metadata::RepairStatus::Completed);

    sm->shutdown();
}

TEST_CASE_METHOD(ServiceManagerFixture,
                 "RepairService: startup scan should ignore extraction skipped docs",
                 "[daemon][repair][regression][startup-scan]") {
    yams::test::ScopedEnvVar disableVectors("YAMS_DISABLE_VECTORS",
                                            std::optional<std::string>{"1"});
    yams::test::ScopedEnvVar disableVectorDb("YAMS_DISABLE_VECTOR_DB",
                                             std::optional<std::string>{"1"});
    yams::test::ScopedEnvVar skipModelLoading("YAMS_SKIP_MODEL_LOADING",
                                              std::optional<std::string>{"1"});
    yams::test::ScopedEnvVar safeSingleInstance("YAMS_TEST_SAFE_SINGLE_INSTANCE",
                                                std::optional<std::string>{"1"});

    auto sm = std::make_shared<ServiceManager>(config_, state_, lifecycleFsm_);
    REQUIRE(sm->initialize());
    sm->startAsyncInit();

    auto smSnap = sm->waitForServiceManagerTerminalState(30);
    REQUIRE(smSnap.state == ServiceManagerState::Ready);

    auto meta = sm->getMetadataRepo();
    REQUIRE(meta != nullptr);

    metadata::DocumentInfo doc{};
    doc.fileName = "binary.jpg";
    doc.filePath = (config_.dataDir / "binary.jpg").string();
    doc.fileExtension = "jpg";
    doc.fileSize = 123;
    doc.sha256Hash = std::string(64, 'b');
    doc.mimeType = "image/jpeg";
    const auto now =
        std::chrono::time_point_cast<std::chrono::seconds>(std::chrono::system_clock::now());
    doc.modifiedTime = now;
    doc.indexedTime = now;

    auto idRes = meta->insertDocument(doc);
    REQUIRE(idRes.has_value());
    const int64_t docId = idRes.value();

    auto extractionRes = meta->updateDocumentExtractionStatus(
        docId, false, metadata::ExtractionStatus::Skipped, "test skipped");
    REQUIRE(extractionRes.has_value());
    auto repairRes =
        meta->batchUpdateDocumentRepairStatuses({doc.sha256Hash}, metadata::RepairStatus::Pending);
    REQUIRE(repairRes.has_value());

    state_.stats.repairTotalBacklog.store(0, std::memory_order_relaxed);

    RepairService::Config cfg;
    cfg.enable = true;
    cfg.maxBatch = 16;
    RepairService repair(sm.get(), &state_, []() -> size_t { return 0; }, cfg);
    repair.start();

    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    repair.stop();

    CHECK(state_.stats.repairTotalBacklog.load(std::memory_order_relaxed) == 0u);

    sm->shutdown();
}

TEST_CASE_METHOD(ServiceManagerFixture,
                 "RepairService: stalled pending detection should use indexed age",
                 "[daemon][repair][regression][stuck-docs]") {
    yams::test::ScopedEnvVar disableVectors("YAMS_DISABLE_VECTORS",
                                            std::optional<std::string>{"1"});
    yams::test::ScopedEnvVar disableVectorDb("YAMS_DISABLE_VECTOR_DB",
                                             std::optional<std::string>{"1"});
    yams::test::ScopedEnvVar skipModelLoading("YAMS_SKIP_MODEL_LOADING",
                                              std::optional<std::string>{"1"});
    yams::test::ScopedEnvVar safeSingleInstance("YAMS_TEST_SAFE_SINGLE_INSTANCE",
                                                std::optional<std::string>{"1"});

    auto sm = std::make_shared<ServiceManager>(config_, state_, lifecycleFsm_);
    REQUIRE(sm->initialize());
    sm->startAsyncInit();

    auto smSnap = sm->waitForServiceManagerTerminalState(30);
    REQUIRE(smSnap.state == ServiceManagerState::Ready);

    auto meta = sm->getMetadataRepo();
    REQUIRE(meta != nullptr);

    metadata::DocumentInfo doc{};
    doc.fileName = "pending.txt";
    doc.filePath = (config_.dataDir / "pending.txt").string();
    doc.fileExtension = "txt";
    doc.fileSize = 17;
    doc.sha256Hash = std::string(64, 'c');
    doc.mimeType = "text/plain";
    doc.setModifiedTime(1); // very old file mtime
    const auto recent =
        std::chrono::time_point_cast<std::chrono::seconds>(std::chrono::system_clock::now());
    doc.indexedTime = recent; // recently indexed, should not be stalled

    auto idRes = meta->insertDocument(doc);
    REQUIRE(idRes.has_value());
    const int64_t docId = idRes.value();

    auto extractionRes = meta->updateDocumentExtractionStatus(
        docId, false, metadata::ExtractionStatus::Pending, "pending test");
    REQUIRE(extractionRes.has_value());
    auto repairRes =
        meta->batchUpdateDocumentRepairStatuses({doc.sha256Hash}, metadata::RepairStatus::Pending);
    REQUIRE(repairRes.has_value());

    RepairService::Config cfg;
    cfg.enable = false;
    cfg.stalledThreshold = std::chrono::seconds(60);
    RepairService repair(sm.get(), &state_, []() -> size_t { return 0; }, cfg);

    RepairRequest req;
    req.repairStuckDocs = true;
    req.dryRun = true;
    req.maxRetries = 3;

    auto resp = repair.executeRepair(req, nullptr);
    auto stuckDocsResult = findOperationResult(resp, "stuck_docs");
    REQUIRE(stuckDocsResult.has_value());
    CHECK(stuckDocsResult->processed == 0u);

    sm->shutdown();
}

// ---------------------------------------------------------------------------
// FTS5 blind-spot regression tests
// ---------------------------------------------------------------------------
// These tests verify that documents marked as successfully extracted but
// missing their FTS5 index entries are correctly detected and repaired.
// Before the fix, all three code paths (rebuildFts5Index, spawnInitialScan,
// detectMissingWork) would silently skip these documents.

TEST_CASE_METHOD(
    ServiceManagerFixture,
    "RepairService: rebuildFts5Index catches extraction-success docs with missing FTS5",
    "[daemon][repair][fts5][regression]") {
    yams::test::ScopedEnvVar disableVectors("YAMS_DISABLE_VECTORS",
                                            std::optional<std::string>{"1"});
    yams::test::ScopedEnvVar disableVectorDb("YAMS_DISABLE_VECTOR_DB",
                                             std::optional<std::string>{"1"});
    yams::test::ScopedEnvVar skipModelLoading("YAMS_SKIP_MODEL_LOADING",
                                              std::optional<std::string>{"1"});
    yams::test::ScopedEnvVar safeSingleInstance("YAMS_TEST_SAFE_SINGLE_INSTANCE",
                                                std::optional<std::string>{"1"});

    auto sm = std::make_shared<ServiceManager>(config_, state_, lifecycleFsm_);
    REQUIRE(sm->initialize());
    sm->startAsyncInit();

    auto smSnap = sm->waitForServiceManagerTerminalState(30);
    REQUIRE(smSnap.state == ServiceManagerState::Ready);

    auto meta = sm->getMetadataRepo();
    auto store = sm->getContentStore();
    REQUIRE(meta != nullptr);
    REQUIRE(store != nullptr);

    // Store real content so the extractor can find it.
    const std::string text = "hello world searchable content for fts5 repair test";
    const auto textBytes = std::as_bytes(std::span<const char>(text.data(), text.size()));
    auto storeRes = store->storeBytes(textBytes);
    REQUIRE(storeRes.has_value());
    const auto hash = storeRes.value().contentHash;

    // Insert document marked as successfully extracted.
    metadata::DocumentInfo doc{};
    doc.fileName = "fts5_missing.txt";
    doc.filePath = (config_.dataDir / "fts5_missing.txt").string();
    doc.fileExtension = "txt";
    doc.fileSize = static_cast<int64_t>(text.size());
    doc.sha256Hash = hash;
    doc.mimeType = "text/plain";
    const auto now =
        std::chrono::time_point_cast<std::chrono::seconds>(std::chrono::system_clock::now());
    doc.modifiedTime = now;
    doc.indexedTime = now;

    auto idRes = meta->insertDocument(doc);
    REQUIRE(idRes.has_value());
    const int64_t docId = idRes.value();

    // Mark extraction as successful at the DB level.
    auto statusRes =
        meta->updateDocumentExtractionStatus(docId, true, metadata::ExtractionStatus::Success);
    REQUIRE(statusRes.has_value());

    // Insert a content row so the ghost-success loop doesn't reset the status.
    metadata::DocumentContent content;
    content.documentId = docId;
    content.contentText = text;
    content.contentLength = static_cast<int64_t>(text.size());
    content.extractionMethod = "test";
    content.language = "en";
    auto contentRes = meta->insertContent(content);
    REQUIRE(contentRes.has_value());

    // Crucially: do NOT call indexDocumentContent(), so no FTS5 entry exists.
    auto hasFts = meta->hasFtsEntry(docId);
    REQUIRE(hasFts.has_value());
    REQUIRE_FALSE(hasFts.value());

    // Run repair with fts5 only.
    RepairService::Config cfg;
    cfg.enable = false;
    RepairService repair(sm.get(), &state_, []() -> size_t { return 0; }, cfg);
    RepairRequest req;
    req.repairFts5 = true;
    req.dryRun = false;

    auto resp = repair.executeRepair(req, nullptr);

    // The document should have been rebuilt, not skipped.
    auto ftsResult = findOperationResult(resp, "fts5");
    REQUIRE(ftsResult.has_value());
    CHECK(ftsResult->succeeded >= 1u);

    // Verify an FTS5 entry now exists.
    auto hasFtsAfter = meta->hasFtsEntry(docId);
    REQUIRE(hasFtsAfter.has_value());
    CHECK(hasFtsAfter.value());

    sm->shutdown();
}

TEST_CASE_METHOD(ServiceManagerFixture,
                 "RepairService: startup scan detects extraction-success docs with missing FTS5",
                 "[daemon][repair][fts5][startup-scan][regression]") {
    yams::test::ScopedEnvVar disableVectors("YAMS_DISABLE_VECTORS",
                                            std::optional<std::string>{"1"});
    yams::test::ScopedEnvVar disableVectorDb("YAMS_DISABLE_VECTOR_DB",
                                             std::optional<std::string>{"1"});
    yams::test::ScopedEnvVar skipModelLoading("YAMS_SKIP_MODEL_LOADING",
                                              std::optional<std::string>{"1"});
    yams::test::ScopedEnvVar safeSingleInstance("YAMS_TEST_SAFE_SINGLE_INSTANCE",
                                                std::optional<std::string>{"1"});

    auto sm = std::make_shared<ServiceManager>(config_, state_, lifecycleFsm_);
    REQUIRE(sm->initialize());
    sm->startAsyncInit();

    auto smSnap = sm->waitForServiceManagerTerminalState(30);
    REQUIRE(smSnap.state == ServiceManagerState::Ready);

    auto meta = sm->getMetadataRepo();
    REQUIRE(meta != nullptr);

    // Insert document marked as successfully extracted but with no FTS5 entry.
    metadata::DocumentInfo doc{};
    doc.fileName = "fts5_scan.txt";
    doc.filePath = (config_.dataDir / "fts5_scan.txt").string();
    doc.fileExtension = "txt";
    doc.fileSize = 42;
    doc.sha256Hash = std::string(64, 'f');
    doc.mimeType = "text/plain";
    const auto now =
        std::chrono::time_point_cast<std::chrono::seconds>(std::chrono::system_clock::now());
    doc.modifiedTime = now;
    doc.indexedTime = now;

    auto idRes = meta->insertDocument(doc);
    REQUIRE(idRes.has_value());
    const int64_t docId = idRes.value();

    // Mark as successfully extracted.
    auto statusRes =
        meta->updateDocumentExtractionStatus(docId, true, metadata::ExtractionStatus::Success);
    REQUIRE(statusRes.has_value());

    // Ensure repair status is Pending (eligible for scan).
    auto repairRes =
        meta->batchUpdateDocumentRepairStatuses({doc.sha256Hash}, metadata::RepairStatus::Pending);
    REQUIRE(repairRes.has_value());

    // Verify NO FTS5 entry exists.
    auto hasFts = meta->hasFtsEntry(docId);
    REQUIRE(hasFts.has_value());
    REQUIRE_FALSE(hasFts.value());

    // Clear backlog counter.
    state_.stats.repairTotalBacklog.store(0, std::memory_order_relaxed);

    // Start RepairService (triggers spawnInitialScan which should detect the gap).
    RepairService::Config cfg;
    cfg.enable = true;
    cfg.maxBatch = 16;
    RepairService repair(sm.get(), &state_, []() -> size_t { return 0; }, cfg);
    repair.start();

    // spawnInitialScan is deferred by minDeferTicks (50 ticks * 100ms = ~5s).
    // Poll until the backlog counter is incremented or a generous timeout expires.
    const bool detected = waitForCondition(std::chrono::seconds(15), [&]() {
        return state_.stats.repairTotalBacklog.load(std::memory_order_relaxed) >= 1u;
    });
    repair.stop();

    // The document should have been enqueued (backlog > 0).
    CHECK(detected);

    sm->shutdown();
}
