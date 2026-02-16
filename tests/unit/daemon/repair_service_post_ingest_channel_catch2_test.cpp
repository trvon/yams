#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <filesystem>
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
    PostIngestQueue* piq = sm->getPostIngestQueue();
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

    sm->shutdown();
}
