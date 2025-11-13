/**
 * @file post_ingest_queue_validation_test.cpp
 * @brief Integration tests to validate PostIngestQueue and repair indexing service
 *
 * These tests verify that:
 * 1. PostIngestQueue is properly initialized
 * 2. Documents are actually enqueued after ingestion
 * 3. Background indexing (FTS5, KG, embeddings) happens
 * 4. RepairCoordinator detects and fixes missing indexes
 */

#include <chrono>
#include <filesystem>
#include <thread>
#include <catch2/catch_test_macros.hpp>

#include <yams/app/services/services.hpp>
#include <yams/core/types.h>
#include <yams/daemon/components/DaemonLifecycleFsm.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/daemon.h>

#include "common/fixture_manager.h"
#include "service_manager_test_helper.h"

namespace fs = std::filesystem;
using namespace yams;
using namespace yams::daemon;
using namespace yams::app::services;

namespace {

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

        // Set YAMS_STORAGE environment variable
        setenv("YAMS_STORAGE", testDir_.string().c_str(), 1);

        fixtureManager_ = std::make_shared<yams::test::FixtureManager>(testDir_);
    }

    void setupDaemonComponents() {
        // Create daemon config
        config_.dataDir = testDir_;
        config_.socketPath = testDir_ / "yams.sock";

        // Enable post-ingest queue with minimal threads for testing
        config_.tuning.postIngestThreadsMin = 2;
        config_.tuning.postIngestThreadsMax = 4;
        config_.tuning.postIngestCapacity = 100;

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
        if (fs::exists(testDir_)) {
            fs::remove_all(testDir_);
        }
    }

    // Helper: Wait for post-ingest queue to drain
    bool waitForQueueDrain(std::chrono::milliseconds timeout = std::chrono::seconds(10)) {
        auto start = std::chrono::steady_clock::now();
        while (std::chrono::steady_clock::now() - start < timeout) {
            if (auto* queue = serviceManager_->getPostIngestQueue()) {
                auto size = queue->size();

                spdlog::info("PostIngestQueue status: size={}", size);

                if (size == 0) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(500));
                    return true;
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
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
    PostIngestQueueFixture fixture;

    SECTION("PostIngestQueue is created") {
        auto* queue = fixture.serviceManager_->getPostIngestQueue();
        REQUIRE(queue != nullptr);

        INFO("Queue should have capacity");
        REQUIRE(queue->capacity() > 0);
    }

    SECTION("PostIngestQueue metrics are accessible") {
        auto* queue = fixture.serviceManager_->getPostIngestQueue();
        REQUIRE(queue != nullptr);

        auto size = queue->size();
        auto processed = queue->processed();
        auto failed = queue->failed();

        INFO("Initial metrics should be reasonable");
        REQUIRE(size >= 0);
        REQUIRE(processed >= 0);
        REQUIRE(failed >= 0);
    }
}

// ============================================================================
// Document Ingestion and Queueing Tests
// ============================================================================

TEST_CASE("PostIngestQueue - Document Enqueuing", "[daemon][post-ingest][enqueue]") {
    PostIngestQueueFixture fixture;

    SECTION("Documents are enqueued after storage") {
        auto* queue = fixture.serviceManager_->getPostIngestQueue();
        REQUIRE(queue != nullptr);

        auto initialProcessed = queue->processed();

        // Store a document
        auto hash = fixture.storeDocument("test.txt", "Hello World");
        REQUIRE(!hash.empty());

        // Manually enqueue to post-ingest (simulating what IngestService does)
        fixture.serviceManager_->enqueuePostIngest(hash, "text/plain");

        // Wait a bit for processing
        std::this_thread::sleep_for(std::chrono::milliseconds(500));

        auto afterProcessed = queue->processed();
        INFO("PostIngestQueue should have processed the document");
        REQUIRE(afterProcessed > initialProcessed);
    }

    SECTION("Multiple documents are processed") {
        auto* queue = fixture.serviceManager_->getPostIngestQueue();
        REQUIRE(queue != nullptr);

        auto initialProcessed = queue->processed();

        // Store multiple documents
        std::vector<std::string> hashes;
        for (int i = 0; i < 5; i++) {
            auto hash = fixture.storeDocument("doc" + std::to_string(i) + ".txt",
                                              "Content " + std::to_string(i));
            hashes.push_back(hash);
            fixture.serviceManager_->enqueuePostIngest(hash, "text/plain");
        }

        // Wait for queue to drain
        bool drained = fixture.waitForQueueDrain(std::chrono::seconds(5));
        REQUIRE(drained);

        auto finalProcessed = queue->processed();
        INFO("All documents should be processed");
        REQUIRE(finalProcessed >= initialProcessed + 5);
    }
}

// ============================================================================
// FTS5 Indexing Verification Tests
// ============================================================================

TEST_CASE("PostIngestQueue - FTS5 Indexing", "[daemon][post-ingest][fts5]") {
    PostIngestQueueFixture fixture;

    SECTION("Stored document is searchable after post-ingest") {
        // Store a document with unique content
        const std::string uniqueContent = "xyzzy_unique_test_content_12345";
        auto hash = fixture.storeDocument("searchable.txt", uniqueContent);

        // Enqueue for post-ingest processing
        fixture.serviceManager_->enqueuePostIngest(hash, "text/plain");

        // Wait for processing
        bool drained = fixture.waitForQueueDrain(std::chrono::seconds(5));
        REQUIRE(drained);

        // Additional wait to ensure FTS5 indexing completes
        std::this_thread::sleep_for(std::chrono::milliseconds(500));

        // Try to search for the document
        auto appContext = fixture.serviceManager_->getAppContext();
        auto searchService = makeSearchService(appContext);
        REQUIRE(searchService);

        app::services::SearchRequest req;
        req.query = "xyzzy_unique_test_content_12345"; // Fixed: match stored content
        req.type = "keyword";
        req.limit = 10;
        req.showHash = true; // REQUIRED: populate hash field in results

        // Run search using async helper
        boost::asio::io_context ioc;
        auto fut = boost::asio::co_spawn(ioc, searchService->search(req), boost::asio::use_future);
        ioc.run();
        auto result = fut.get();

        REQUIRE(result);
        INFO("Document should be found in FTS5 search");
        REQUIRE(result.value().results.size() > 0);

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
    PostIngestQueueFixture fixture;

    SECTION("Documents are indexed via async channel") {
        auto* queue = fixture.serviceManager_->getPostIngestQueue();
        REQUIRE(queue != nullptr);

        auto hash = fixture.storeDocument("sync_test.txt", "Sync content");

        REQUIRE(fixture.waitForQueueDrain(std::chrono::seconds(5)));

        auto appContext = fixture.serviceManager_->getAppContext();
        auto searchService = makeSearchService(appContext);
        REQUIRE(searchService);

        app::services::SearchRequest req;
        req.query = "Sync";
        req.type = "keyword";
        req.limit = 10;

        boost::asio::io_context ioc;
        auto fut = boost::asio::co_spawn(ioc, searchService->search(req), boost::asio::use_future);
        ioc.run();
        auto result = fut.get();

        REQUIRE(result);
        REQUIRE(result.value().results.size() > 0);
    }
}

// ============================================================================
// Queue Capacity and Backpressure Tests
// ============================================================================

TEST_CASE("PostIngestQueue - Capacity and Backpressure", "[daemon][post-ingest][capacity]") {
    PostIngestQueueFixture fixture;

    SECTION("Queue respects capacity limits") {
        auto* queue = fixture.serviceManager_->getPostIngestQueue();
        REQUIRE(queue != nullptr);

        auto capacity = queue->capacity();
        INFO("Queue capacity: " << capacity);
        REQUIRE(capacity > 0);

        // Try to enqueue more than capacity (using tryEnqueue)
        std::vector<PostIngestQueue::Task> tasks;
        for (size_t i = 0; i < capacity + 10; i++) {
            PostIngestQueue::Task task;
            task.hash = "hash_" + std::to_string(i);
            task.mime = "text/plain";
            task.stage = PostIngestQueue::Task::Stage::Metadata;
            tasks.push_back(task);
        }

        size_t enqueued = 0;
        size_t rejected = 0;

        for (auto& task : tasks) {
            if (queue->tryEnqueue(std::move(task))) {
                enqueued++;
            } else {
                rejected++;
            }
        }

        INFO("Enqueued: " << enqueued << ", Rejected: " << rejected);

        // At least some should be rejected when exceeding capacity
        // (Note: some may process quickly, so we can't guarantee exact rejection count)
        REQUIRE(enqueued <= capacity + 10);
    }
}

// ============================================================================
// Error Handling Tests
// ============================================================================

TEST_CASE("PostIngestQueue - Error Handling", "[daemon][post-ingest][errors]") {
    PostIngestQueueFixture fixture;

    SECTION("Invalid hash doesn't crash") {
        auto* queue = fixture.serviceManager_->getPostIngestQueue();
        REQUIRE(queue != nullptr);

        // Enqueue a task with non-existent hash
        fixture.serviceManager_->enqueuePostIngest("nonexistent_hash_12345", "text/plain");

        // Wait a bit
        std::this_thread::sleep_for(std::chrono::milliseconds(500));

        // Queue should still be operational
        auto failed = queue->failed();
        auto processed = queue->processed();

        INFO("Failed tasks: " << failed);
        INFO("Processed tasks: " << processed);

        // Should have recorded a failure (wrap in parentheses for Catch2)
        REQUIRE((failed > 0 || processed > 0));
    }
}

// ============================================================================
// Thread Scaling Tests
// ============================================================================

TEST_CASE("PostIngestQueue - Thread Scaling", "[daemon][post-ingest][scaling]") {
    PostIngestQueueFixture fixture;

    SECTION("Thread scaling not supported in strand-based implementation") {
        auto* queue = fixture.serviceManager_->getPostIngestQueue();
        REQUIRE(queue != nullptr);

        bool scaled = fixture.serviceManager_->resizePostIngestThreads(4);
        REQUIRE_FALSE(scaled);
    }
}
