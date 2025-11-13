/**
 * @file post_ingest_trace.cpp
 * @brief Diagnostic tool to trace PostIngestQueue execution
 *
 * This creates a minimal daemon setup and traces the flow of documents
 * through the PostIngestQueue to identify where things might be failing.
 */

#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <thread>

#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include <yams/app/services/services.hpp>
#include <yams/daemon/components/DaemonLifecycleFsm.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/daemon.h>

#include "service_manager_test_helper.h"

namespace fs = std::filesystem;
using namespace yams;
using namespace yams::daemon;
using namespace yams::app::services;

int main() {
    // Set up logging
    spdlog::set_level(spdlog::level::debug);
    spdlog::set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%^%l%$] %v");

    std::cout << "=== PostIngestQueue Trace Diagnostic ===" << std::endl;
    std::cout << std::endl;

    // Create test directory
    auto testDir = fs::temp_directory_path() / "post_ingest_trace";
    if (fs::exists(testDir)) {
        fs::remove_all(testDir);
    }
    fs::create_directories(testDir);
    std::cout << "Test directory: " << testDir << std::endl;

    // Set environment
    setenv("YAMS_STORAGE", testDir.string().c_str(), 1);

    // Create daemon components
    DaemonConfig config;
    config.dataDir = testDir;
    config.socketPath = testDir / "yams.sock";
    config.tuning.postIngestThreadsMin = 2;
    config.tuning.postIngestThreadsMax = 4;
    config.tuning.postIngestCapacity = 100;

    StateComponent state;
    DaemonLifecycleFsm lifecycleFsm;

    std::cout << "\n1. Creating ServiceManager..." << std::endl;
    auto serviceManager = std::make_shared<ServiceManager>(config, state, lifecycleFsm);

    std::cout << "2. Initializing ServiceManager (sync + async)..." << std::endl;
    bool initialized = yams::test::initializeServiceManagerFully(serviceManager);
    if (!initialized) {
        std::cerr << "ERROR: ServiceManager initialization failed" << std::endl;
        return 1;
    }
    std::cout << "   ✓ ServiceManager fully initialized" << std::endl;

    std::cout << "\n3. Checking PostIngestQueue..." << std::endl;
    auto* queue = serviceManager->getPostIngestQueue();
    if (!queue) {
        std::cerr << "ERROR: PostIngestQueue is NULL!" << std::endl;
        return 1;
    }
    std::cout << "   ✓ PostIngestQueue exists" << std::endl;
    std::cout << "   - Capacity: " << queue->capacity() << std::endl;
    std::cout << "   - Size: " << queue->size() << std::endl;
    std::cout << "   - Processed: " << queue->processed() << std::endl;
    std::cout << "   - Failed: " << queue->failed() << std::endl;

    std::cout << "\n4. Creating test document..." << std::endl;
    auto testFile = testDir / "test.txt";
    {
        std::ofstream out(testFile);
        out << "Test content for PostIngestQueue tracing\n";
        out << "This document should be indexed by FTS5\n";
    }
    std::cout << "   ✓ Created " << testFile << std::endl;

    std::cout << "\n5. Storing document via DocumentService..." << std::endl;
    auto appContext = serviceManager->getAppContext();
    auto docService = makeDocumentService(appContext);
    if (!docService) {
        std::cerr << "ERROR: Could not create DocumentService!" << std::endl;
        return 1;
    }

    StoreDocumentRequest storeReq;
    storeReq.path = testFile.string();
    auto storeResult = docService->store(storeReq);
    if (!storeResult) {
        std::cerr << "ERROR: Document storage failed: " << storeResult.error().message << std::endl;
        return 1;
    }

    const auto& hash = storeResult.value().hash;
    std::cout << "   ✓ Document stored with hash: " << hash << std::endl;
    std::cout << "   - Bytes stored: " << storeResult.value().bytesStored << std::endl;
    std::cout << "   - Bytes deduped: " << storeResult.value().bytesDeduped << std::endl;

    std::cout << "\n6. Manually enqueuing to PostIngestQueue..." << std::endl;
    auto initialProcessed = queue->processed();
    auto initialFailed = queue->failed();

    serviceManager->enqueuePostIngest(hash, "text/plain");
    std::cout << "   ✓ Enqueued document" << std::endl;

    // Give some time to observe if workers are active
    std::cout << "\n7. Monitoring PostIngestQueue for 5 seconds..." << std::endl;
    for (int i = 0; i < 10; i++) {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));

        auto size = queue->size();
        auto processed = queue->processed();
        auto failed = queue->failed();

        std::cout << "   [" << (i * 0.5) << "s] "
                  << "size=" << size << " "
                  << "processed=" << processed << " "
                  << "failed=" << failed;

        if (processed > initialProcessed) {
            std::cout << " ✓ PROCESSING OCCURRED!";
        } else if (failed > initialFailed) {
            std::cout << " ✗ FAILURE OCCURRED!";
        }

        std::cout << std::endl;

        if (size == 0 && (processed > initialProcessed || failed > initialFailed)) {
            std::cout << "   Queue drained, processing complete." << std::endl;
            break;
        }
    }

    auto finalProcessed = queue->processed();
    auto finalFailed = queue->failed();

    std::cout << "\n8. Verifying FTS5 indexing..." << std::endl;
    std::cout << "   Waiting additional 1 second for FTS5..." << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(1));

    auto searchService = makeSearchService(appContext);
    if (!searchService) {
        std::cerr << "ERROR: Could not create SearchService!" << std::endl;
        return 1;
    }

    app::services::SearchRequest searchReq;
    searchReq.query = "PostIngestQueue";
    searchReq.type = "keyword";
    searchReq.limit = 10;

    boost::asio::io_context ioc;
    auto searchFut =
        boost::asio::co_spawn(ioc, searchService->search(searchReq), boost::asio::use_future);
    ioc.run();
    auto searchResult = searchFut.get();

    if (searchResult) {
        std::cout << "   ✓ Search succeeded" << std::endl;
        std::cout << "   - Results found: " << searchResult.value().results.size() << std::endl;

        bool foundOurDoc = false;
        for (const auto& item : searchResult.value().results) {
            if (item.hash == hash) {
                foundOurDoc = true;
                std::cout << "   ✓ Our document was indexed and searchable!" << std::endl;
                break;
            }
        }

        if (!foundOurDoc) {
            std::cout << "   ✗ Our document was NOT found in search results" << std::endl;
        }
    } else {
        std::cout << "   ✗ Search failed: " << searchResult.error().message << std::endl;
    }

    // Summary
    std::cout << "\n=== SUMMARY ===" << std::endl;
    std::cout << "PostIngestQueue Status:" << std::endl;
    std::cout << "  - Initial processed: " << initialProcessed << std::endl;
    std::cout << "  - Final processed: " << finalProcessed << std::endl;
    std::cout << "  - Documents processed: " << (finalProcessed - initialProcessed) << std::endl;
    std::cout << "  - Initial failed: " << initialFailed << std::endl;
    std::cout << "  - Final failed: " << finalFailed << std::endl;
    std::cout << "  - Documents failed: " << (finalFailed - initialFailed) << std::endl;

    if (finalProcessed > initialProcessed) {
        std::cout << "\n✓ PostIngestQueue IS WORKING - document was processed" << std::endl;
    } else if (finalFailed > initialFailed) {
        std::cout << "\n✗ PostIngestQueue FAILED - document processing failed" << std::endl;
    } else {
        std::cout << "\n✗ PostIngestQueue NOT WORKING - no processing occurred" << std::endl;
    }

    // Cleanup
    std::cout << "\n9. Shutting down..." << std::endl;
    serviceManager->shutdown();
    serviceManager.reset();

    std::cout << "   ✓ Cleanup complete" << std::endl;
    std::cout << "\n=== Diagnostic Complete ===" << std::endl;

    return (finalProcessed > initialProcessed) ? 0 : 1;
}
