/**
 * @file post_ingest_queue_validation_test.cpp
 * @brief Integration tests to validate PostIngestQueue and repair indexing service
 *
 * These tests verify that:
 * 1. PostIngestQueue is properly initialized
 * 2. Documents are actually enqueued after ingestion
 * 3. Background indexing (FTS5, KG, embeddings) happens
 * 4. Background repair detects and fixes missing indexes
 */

#include <atomic>
#include <chrono>
#include <filesystem>
#include <future>
#include <memory>
#include <thread>
#include <catch2/catch_test_macros.hpp>
#include <yams/compat/unistd.h>

#include <yams/app/services/services.hpp>
#include <yams/core/types.h>
#include <yams/daemon/components/DaemonLifecycleFsm.h>
#include <yams/daemon/components/PostIngestQueue.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/daemon.h>

#include "common/fixture_manager.h"
#include "common/test_helpers_catch2.h"
#include "service_manager_test_helper.h"

// Windows daemon tests are currently unstable - ServiceManager async initialization
// has socket path length issues and shutdown race conditions
#ifdef _WIN32
#define SKIP_DAEMON_TEST_ON_WINDOWS()                                                              \
    SKIP("Daemon tests unstable on Windows - see windows-daemon-ipc-plan.md")
#else
#define SKIP_DAEMON_TEST_ON_WINDOWS() ((void)0)
#endif

namespace fs = std::filesystem;
using namespace yams;
using namespace yams::daemon;
using namespace yams::app::services;

namespace {

class WorkCoordinatorThreadsGuard {
public:
    explicit WorkCoordinatorThreadsGuard(std::uint32_t threads)
        : previous_(TuneAdvisor::workCoordinatorThreads()) {
        TuneAdvisor::setWorkCoordinatorThreads(threads);
    }
    ~WorkCoordinatorThreadsGuard() { TuneAdvisor::setWorkCoordinatorThreads(previous_); }

private:
    std::uint32_t previous_;
};

struct DrainSignal {
    std::promise<void> promise;
    std::atomic<bool> signalled{false};

    void notify() {
        if (!signalled.exchange(true, std::memory_order_acq_rel)) {
            promise.set_value();
        }
    }
};

/**
 * @brief Test fixture for PostIngestQueue validation
 */
class PostIngestQueueFixture {
public:
    PostIngestQueueFixture() {
        setupTestEnvironment();
        setupDaemonComponents();
    }

    ~PostIngestQueueFixture() {
        cleanupDaemonComponents();
        cleanupTestEnvironment();
    }

    void setupTestEnvironment() {
        auto pid = std::to_string(::getpid());
        auto timestamp =
            std::to_string(std::chrono::system_clock::now().time_since_epoch().count());
        testDir_ = fs::temp_directory_path() / ("post_ingest_test_" + pid + "_" + timestamp);
        fs::create_directories(testDir_);

        // Keep all daemon/session state inside the temp test directory and restore the exact host
        // values after every fixture.
        environment_.emplace_back("YAMS_STORAGE", testDir_.string());
        environment_.emplace_back("XDG_DATA_HOME", testDir_.string());
        environment_.emplace_back("XDG_STATE_HOME", testDir_.string());
        environment_.emplace_back("HOME", testDir_.string());

        fixtureManager_ = std::make_shared<yams::test::FixtureManager>(testDir_);
    }

    void setupDaemonComponents() {
        // Create daemon config
        config_.dataDir = testDir_;
        config_.socketPath = testDir_ / "yams.sock";

        // Enable post-ingest queue with minimal threads for testing
        config_.tuning.postIngestThreadsMin = 2;
        config_.tuning.postIngestThreadsMax = 4;
        config_.tuning.postIngestCapacity = 4;

        // Create state and lifecycle components
        state_ = std::make_unique<StateComponent>();
        lifecycleFsm_ = std::make_unique<DaemonLifecycleFsm>();

        // Create service manager
        serviceManager_ = std::make_shared<ServiceManager>(config_, *state_, *lifecycleFsm_);

        // Initialize service manager (both sync and async phases)
        bool initialized = yams::test::initializeServiceManagerFully(serviceManager_);
        REQUIRE(initialized);
    }

    void cleanupDaemonComponents() {
        if (serviceManager_) {
            serviceManager_->shutdown();
            serviceManager_.reset();
        }
        lifecycleFsm_.reset();
        state_.reset();
    }

    void cleanupTestEnvironment() {
        fixtureManager_.reset();
        environment_.clear();
        if (fs::exists(testDir_)) {
            fs::remove_all(testDir_);
        }
    }

    // Helper: Wait for post-ingest queue to drain (both channel AND in-flight work)
    bool waitForQueueDrain(std::chrono::milliseconds timeout = std::chrono::seconds(30)) {
        auto start = std::chrono::steady_clock::now();
        while (std::chrono::steady_clock::now() - start < timeout) {
            if (auto queue = serviceManager_->getPostIngestQueue()) {
                auto size = queue->size();
                auto inFlight = queue->totalInFlight();

                spdlog::info("PostIngestQueue status: size={}, inFlight={}", size, inFlight);

                // Must wait for BOTH channel to be empty AND all in-flight work to complete
                if (size == 0 && inFlight == 0) {
                    // Extra delay for FTS5 index to finalize writes
                    std::this_thread::sleep_for(std::chrono::milliseconds(500));
                    return true;
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        return false;
    }

    bool waitForProcessed(std::size_t target,
                          std::chrono::milliseconds timeout = std::chrono::seconds(30)) {
        auto start = std::chrono::steady_clock::now();
        while (std::chrono::steady_clock::now() - start < timeout) {
            if (auto queue = serviceManager_->getPostIngestQueue()) {
                if (queue->processed() >= target) {
                    return true;
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        return false;
    }

    bool waitForKgQueued(std::uint64_t target,
                         std::chrono::milliseconds timeout = std::chrono::seconds(30)) {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            if (InternalEventBus::instance().kgQueued() >= target) {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        return false;
    }

    bool waitForFailed(std::size_t target,
                       std::chrono::milliseconds timeout = std::chrono::seconds(30)) {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            if (auto queue = serviceManager_->getPostIngestQueue();
                queue && queue->failed() >= target) {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        return false;
    }

    bool waitForExtractionIdle(std::chrono::milliseconds timeout = std::chrono::seconds(30)) {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            if (auto queue = serviceManager_->getPostIngestQueue();
                queue && queue->size() == 0 && queue->totalInFlight() == 0) {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        return false;
    }

    // Helper: Create and store a document
    std::string storeDocument(const std::string& filename, const std::string& content) {
        auto fixture = fixtureManager_->createTextFixture(filename, content);

        // Get document service from service manager's app context
        auto appContext = serviceManager_->getAppContext();
        auto docService = makeDocumentService(appContext);
        REQUIRE(docService);

        StoreDocumentRequest req;
        req.path = fixture.path.string();
        auto result = docService->store(req);
        REQUIRE(result);

        return result.value().hash;
    }

    fs::path testDir_;
    std::shared_ptr<yams::test::FixtureManager> fixtureManager_;
    std::vector<yams::test::ScopedEnvVar> environment_;

    DaemonConfig config_;
    std::unique_ptr<StateComponent> state_;
    std::unique_ptr<DaemonLifecycleFsm> lifecycleFsm_;
    std::shared_ptr<ServiceManager> serviceManager_;
};

} // anonymous namespace

// ============================================================================
// PostIngestQueue Initialization Tests
// ============================================================================

TEST_CASE("PostIngestQueue - Initialization", "[daemon][post-ingest][init]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();
    PostIngestQueueFixture fixture;

    SECTION("PostIngestQueue is created") {
        auto queue = fixture.serviceManager_->getPostIngestQueue();
        REQUIRE((queue != nullptr));

        INFO("Queue should have capacity");
        REQUIRE((queue->capacity() > 0));
    }

    SECTION("PostIngestQueue metrics are accessible") {
        auto queue = fixture.serviceManager_->getPostIngestQueue();
        REQUIRE((queue != nullptr));

        INFO("Fresh queue metrics should be empty");
        CHECK((queue->size() == 0));
        CHECK((queue->processed() == 0));
        CHECK((queue->failed() == 0));
    }
}

// ============================================================================
// Document Ingestion and Queueing Tests
// ============================================================================

TEST_CASE("PostIngestQueue - Document Enqueuing", "[daemon][post-ingest][enqueue]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();
    PostIngestQueueFixture fixture;

    SECTION("Documents are enqueued after storage") {
        auto queue = fixture.serviceManager_->getPostIngestQueue();
        REQUIRE((queue != nullptr));

        const auto initialProcessed = queue->processed();

        // Store a document
        auto hash = fixture.storeDocument("test.txt", "Hello World");
        REQUIRE(!hash.empty());

        // Manually enqueue to post-ingest (simulating what IngestService does)
        fixture.serviceManager_->enqueuePostIngest(hash, "text/plain");

        INFO("PostIngestQueue should process the document before the deadline");
        REQUIRE(fixture.waitForProcessed(initialProcessed + 1, std::chrono::seconds(30)));
    }

    SECTION("Multiple documents are processed") {
        auto queue = fixture.serviceManager_->getPostIngestQueue();
        REQUIRE((queue != nullptr));

        auto initialProcessed = queue->processed();

        // Store multiple documents
        std::vector<std::string> hashes;
        for (int i = 0; i < 5; i++) {
            auto hash = fixture.storeDocument("doc" + std::to_string(i) + ".txt",
                                              "Content " + std::to_string(i));
            hashes.push_back(hash);
            fixture.serviceManager_->enqueuePostIngest(hash, "text/plain");
        }

        INFO("All documents should be processed");
        REQUIRE(fixture.waitForProcessed(initialProcessed + 5, std::chrono::seconds(30)));
    }
}

// ============================================================================
// FTS5 Indexing Verification Tests
// ============================================================================

TEST_CASE("PostIngestQueue - FTS5 Indexing", "[daemon][post-ingest][fts5]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();
    PostIngestQueueFixture fixture;

    SECTION("Stored document is searchable after post-ingest") {
        // Store a document with unique content
        // NOTE: Use spaces, not underscores! FTS5 tokenizer treats underscores as token chars
        // (tokenchars '_-'), so "xyzzy_foo" is ONE token. Spaces create separate tokens.
        const std::string uniqueContent = "xyzzy unique test content 12345";
        auto hash = fixture.storeDocument("searchable.txt", uniqueContent);

        // Enqueue for post-ingest processing
        fixture.serviceManager_->enqueuePostIngest(hash, "text/plain");

        // Wait for processing
        bool drained = fixture.waitForQueueDrain(std::chrono::seconds(30));
        REQUIRE(drained);

        // Try to search for the document
        auto appContext = fixture.serviceManager_->getAppContext();
        auto searchService = makeSearchService(appContext);
        REQUIRE(searchService);

        app::services::SearchRequest req;
        req.query = "xyzzy"; // Search for unique token
        req.type = "keyword";
        req.limit = 10;
        req.showHash = true;     // REQUIRED: populate hash field in results
        req.globalSearch = true; // Bypass session isolation for test

        // Run search using async helper
        boost::asio::io_context ioc;
        auto fut = boost::asio::co_spawn(ioc, searchService->search(req), boost::asio::use_future);
        ioc.run();
        auto result = fut.get();

        REQUIRE(result);
        INFO("Document should be found in FTS5 search");
        REQUIRE((result.value().results.size() > 0));

        // Verify the document was actually indexed
        bool found = false;
        for (const auto& item : result.value().results) {
            if (item.hash == hash) {
                found = true;
                break;
            }
        }
        REQUIRE(found);
    }
}

// ============================================================================
// Sync Indexing Tests (PBI-040 feature)
// ============================================================================

TEST_CASE("PostIngestQueue - Synchronous Indexing", "[daemon][post-ingest][sync]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();
    PostIngestQueueFixture fixture;

    SECTION("Documents are indexed via async channel") {
        auto queue = fixture.serviceManager_->getPostIngestQueue();
        REQUIRE((queue != nullptr));

        auto hash = fixture.storeDocument("sync_test.txt", "Sync content for async test");

        // Enqueue for post-ingest processing
        fixture.serviceManager_->enqueuePostIngest(hash, "text/plain");

        REQUIRE(fixture.waitForQueueDrain(std::chrono::seconds(30)));

        auto appContext = fixture.serviceManager_->getAppContext();
        auto searchService = makeSearchService(appContext);
        REQUIRE(searchService);

        app::services::SearchRequest req;
        req.query = "Sync";
        req.type = "keyword";
        req.limit = 10;
        req.globalSearch = true; // Bypass session isolation for test

        boost::asio::io_context ioc;
        auto fut = boost::asio::co_spawn(ioc, searchService->search(req), boost::asio::use_future);
        ioc.run();
        auto result = fut.get();

        REQUIRE(result);
        REQUIRE((result.value().results.size() > 0));
    }
}

TEST_CASE("PostIngestQueue - continues processing when KG stage is paused",
          "[daemon][post-ingest][sync][kg-paused]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();
    PostIngestQueueFixture fixture;

    auto queue = fixture.serviceManager_->getPostIngestQueue();
    REQUIRE((queue != nullptr));

    auto& bus = InternalEventBus::instance();
    const auto processedBefore = queue->processed();
    const auto kgQueuedBefore = bus.kgQueued();
    const auto kgConsumedBefore = bus.kgConsumed();
    const auto kgDroppedBefore = bus.kgDropped();

    queue->pauseStage(PostIngestQueue::Stage::KnowledgeGraph);

    auto hash = fixture.storeDocument("kg_paused_test.txt",
                                      "Pipeline should still index this content when KG is paused");
    fixture.serviceManager_->enqueuePostIngest(hash, "text/plain");

    REQUIRE(fixture.waitForProcessed(processedBefore + 1, std::chrono::seconds(30)));
    REQUIRE(fixture.waitForKgQueued(kgQueuedBefore + 1, std::chrono::seconds(30)));
    CHECK((bus.kgQueued() == kgQueuedBefore + 1));
    CHECK((bus.kgConsumed() == kgConsumedBefore));
    CHECK((bus.kgDropped() == kgDroppedBefore));

    auto appContext = fixture.serviceManager_->getAppContext();
    auto searchService = makeSearchService(appContext);
    REQUIRE(searchService);

    app::services::SearchRequest req;
    req.query = "Pipeline should still index";
    req.type = "keyword";
    req.limit = 10;
    req.globalSearch = true;

    boost::asio::io_context ioc;
    auto fut = boost::asio::co_spawn(ioc, searchService->search(req), boost::asio::use_future);
    ioc.run();
    auto result = fut.get();

    REQUIRE(result);
    REQUIRE((result.value().results.size() > 0));

    auto drainSignal = std::make_shared<DrainSignal>();
    auto drainedFuture = drainSignal->promise.get_future();
    queue->setDrainCallback([drainSignal]() { drainSignal->notify(); });
    queue->resumeStage(PostIngestQueue::Stage::KnowledgeGraph);
    REQUIRE((drainedFuture.wait_for(std::chrono::seconds(10)) == std::future_status::ready));
    queue->setDrainCallback({});

    CHECK((bus.kgConsumed() == kgConsumedBefore + 1));
    CHECK((bus.kgDropped() == kgDroppedBefore));
}

// ============================================================================
// Queue Capacity and Backpressure Tests
// ============================================================================

TEST_CASE("PostIngestQueue - Capacity and Backpressure", "[daemon][post-ingest][capacity]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();
    PostIngestQueueFixture fixture;

    SECTION("Runtime tuning does not misreport construction-time channel capacity") {
        auto queue = fixture.serviceManager_->getPostIngestQueue();
        REQUIRE((queue != nullptr));
        const auto constructedCapacity = queue->capacity();

        auto tuning = fixture.serviceManager_->getTuningConfig();
        tuning.postIngestCapacity = static_cast<std::uint32_t>(constructedCapacity + 32);
        fixture.serviceManager_->setTuningConfig(tuning);

        CHECK((queue->capacity() == constructedCapacity));
        CHECK(
            (fixture.serviceManager_->getTuningConfig().postIngestCapacity == constructedCapacity));
    }

    SECTION("Paused extraction applies deterministic bounded backpressure") {
        auto queue = fixture.serviceManager_->getPostIngestQueue();
        REQUIRE((queue != nullptr));

        queue->pauseStage(PostIngestQueue::Stage::Extraction);
        REQUIRE(queue->isStagePaused(PostIngestQueue::Stage::Extraction));

        const auto capacity = queue->capacity();
        INFO("Queue capacity: " << capacity);
        REQUIRE((capacity > 1));

        std::size_t enqueued = 0;
        std::size_t rejected = 0;
        for (std::size_t i = 0; i < capacity + 10; ++i) {
            PostIngestQueue::Task task;
            task.hash = "backpressure_hash_" + std::to_string(i);
            task.mime = "text/plain";
            if (queue->tryEnqueue(std::move(task))) {
                ++enqueued;
            } else {
                ++rejected;
            }
        }

        INFO("Enqueued: " << enqueued << ", Rejected: " << rejected);
        CHECK((enqueued == capacity - 1));
        CHECK((rejected == 11));
        CHECK((queue->size() == enqueued));

        const auto failedBefore = queue->failed();
        queue->resumeStage(PostIngestQueue::Stage::Extraction);
        REQUIRE(fixture.waitForFailed(failedBefore + enqueued, std::chrono::seconds(10)));
        CHECK((queue->size() == 0));
    }
}

TEST_CASE("PostIngestQueue - KG disable and saturation semantics",
          "[daemon][post-ingest][backpressure]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();
    WorkCoordinatorThreadsGuard workerGuard(1);
    PostIngestQueueFixture fixture;
    auto queue = fixture.serviceManager_->getPostIngestQueue();
    REQUIRE((queue != nullptr));
    auto& bus = InternalEventBus::instance();

    SECTION("Explicit disable is outside the pipeline and does not count as a drop") {
        const auto processedBefore = queue->processed();
        const auto queuedBefore = bus.kgQueued();
        const auto consumedBefore = bus.kgConsumed();
        const auto droppedBefore = bus.kgDropped();
        queue->setKnowledgeGraphEnabled(false);

        const auto hash = fixture.storeDocument("kg-disabled.txt", "explicitly disabled KG");
        REQUIRE(queue->tryEnqueue(PostIngestQueue::Task{
            .hash = hash, .mime = "text/plain", .filePath = "kg-disabled.txt"}));
        REQUIRE(fixture.waitForProcessed(processedBefore + 1, std::chrono::seconds(10)));

        CHECK((bus.kgQueued() == queuedBefore));
        CHECK((bus.kgConsumed() == consumedBefore));
        CHECK((bus.kgDropped() == droppedBefore));
    }

    SECTION("Disable racing a saturated dispatch cancels without recording a drop") {
        const auto queuedBefore = bus.kgQueued();
        const auto consumedBefore = bus.kgConsumed();
        const auto droppedBefore = bus.kgDropped();
        queue->pauseStage(PostIngestQueue::Stage::KnowledgeGraph);

        std::size_t accepted = 0;
        for (std::size_t i = 0; i < 32; ++i) {
            const auto name = "kg-disable-race-" + std::to_string(i) + ".txt";
            const auto hash = fixture.storeDocument(name, "KG disable race " + std::to_string(i));
            if (queue->tryEnqueue(
                    PostIngestQueue::Task{.hash = hash, .mime = "text/plain", .filePath = name})) {
                ++accepted;
            }
        }
        REQUIRE((accepted > 3));
        REQUIRE((accepted < 32));
        REQUIRE(fixture.waitForKgQueued(queuedBefore + 3, std::chrono::seconds(10)));

        queue->setKnowledgeGraphEnabled(false);
        REQUIRE(fixture.waitForExtractionIdle(std::chrono::seconds(10)));
        CHECK((bus.kgDropped() == droppedBefore));

        const auto queuedAfterDisable = bus.kgQueued();
        queue->setKnowledgeGraphEnabled(true);
        auto drainSignal = std::make_shared<DrainSignal>();
        auto drainedFuture = drainSignal->promise.get_future();
        queue->setDrainCallback([drainSignal]() { drainSignal->notify(); });
        queue->resumeStage(PostIngestQueue::Stage::KnowledgeGraph);
        REQUIRE((drainedFuture.wait_for(std::chrono::seconds(30)) == std::future_status::ready));
        queue->setDrainCallback({});

        CHECK((bus.kgConsumed() == consumedBefore + (queuedAfterDisable - queuedBefore)));
        CHECK((bus.kgDropped() == droppedBefore));
    }

    SECTION("Saturated paused KG preserves every accepted job with one requested worker") {
        const auto processedBefore = queue->processed();
        const auto queuedBefore = bus.kgQueued();
        const auto consumedBefore = bus.kgConsumed();
        const auto droppedBefore = bus.kgDropped();
        queue->pauseStage(PostIngestQueue::Stage::KnowledgeGraph);

        std::size_t accepted = 0;
        for (std::size_t i = 0; i < 32; ++i) {
            const auto name = "kg-saturated-" + std::to_string(i) + ".txt";
            const auto hash =
                fixture.storeDocument(name, "saturated KG document " + std::to_string(i));
            if (queue->tryEnqueue(
                    PostIngestQueue::Task{.hash = hash, .mime = "text/plain", .filePath = name})) {
                ++accepted;
            }
        }
        REQUIRE((accepted > 0));
        REQUIRE((accepted < 32));
        REQUIRE(fixture.waitForKgQueued(queuedBefore + 1, std::chrono::seconds(10)));
        CHECK((bus.kgDropped() == droppedBefore));

        auto drainSignal = std::make_shared<DrainSignal>();
        auto drainedFuture = drainSignal->promise.get_future();
        queue->setDrainCallback([drainSignal]() { drainSignal->notify(); });
        queue->resumeStage(PostIngestQueue::Stage::KnowledgeGraph);

        REQUIRE((drainedFuture.wait_for(std::chrono::seconds(30)) == std::future_status::ready));
        queue->setDrainCallback({});
        REQUIRE(fixture.waitForProcessed(processedBefore + accepted, std::chrono::seconds(10)));
        CHECK((bus.kgQueued() == queuedBefore + accepted));
        CHECK((bus.kgConsumed() == consumedBefore + accepted));
        CHECK((bus.kgDropped() == droppedBefore));
    }
}

// ============================================================================
// Error Handling Tests
// ============================================================================

TEST_CASE("PostIngestQueue - Error Handling", "[daemon][post-ingest][errors]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();
    PostIngestQueueFixture fixture;

    SECTION("Missing content records extraction failure before the queue drains") {
        auto queue = fixture.serviceManager_->getPostIngestQueue();
        REQUIRE((queue != nullptr));

        queue->pauseStage(PostIngestQueue::Stage::Extraction);
        REQUIRE(queue->isStagePaused(PostIngestQueue::Stage::Extraction));

        const auto failedBefore = queue->failed();
        PostIngestQueue::Task task;
        task.hash = "nonexistent_hash_12345";
        task.mime = "text/plain";
        REQUIRE(queue->tryEnqueue(std::move(task)));

        queue->resumeStage(PostIngestQueue::Stage::Extraction);
        REQUIRE(fixture.waitForFailed(failedBefore + 1, std::chrono::seconds(10)));

        INFO("failed before=" << failedBefore << ", after=" << queue->failed());
        CHECK((queue->failed() == failedBefore + 1));
        CHECK((queue->size() == 0));
        CHECK((queue->totalInFlight() == 0));
    }
}

// ============================================================================
// Thread Scaling Tests
// ============================================================================
// NOTE: Thread scaling test removed - resizePostIngestThreads API does not exist
// in the current ServiceManager implementation. If this functionality is added
// in the future, tests should be re-enabled.
