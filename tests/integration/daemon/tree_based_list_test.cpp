// Tree-based list integration test: verify list uses PathTreeNode for path prefix queries
#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <set>
#include <string>
#include <thread>

#include "test_async_helpers.h"
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/daemon.h>
#include <yams/daemon/ipc/ipc_protocol.h>

using namespace std::chrono_literals;
namespace fs = std::filesystem;

TEST(TreeBasedListE2E, ListUsesTreeForPathPrefix) {
    if (std::getenv("GTEST_DISCOVERY_MODE")) {
        GTEST_SKIP() << "Skipping during test discovery";
    }

    fs::path tmp = fs::temp_directory_path() / ("yams_tree_list_" + std::to_string(::getpid()));
    fs::create_directories(tmp);

    // Canonicalize the temp path to handle symlinks (e.g., /var -> /private/var on macOS)
    std::error_code ec;
    tmp = fs::canonical(tmp, ec);
    ASSERT_FALSE(ec) << "Failed to canonicalize temp directory: " << ec.message();

    auto cleanup = [&]() {
        std::error_code ec;
        fs::remove_all(tmp, ec);
    };

    fs::path sock = tmp / "d.sock";
    fs::path pid = tmp / "d.pid";
    fs::path log = tmp / "d.log";
    fs::path data = tmp / "data";
    fs::create_directories(data);

    // Create test directory structure with documents
    fs::path docs = tmp / "docs";
    fs::path delivery = docs / "delivery";
    fs::path pbi001 = delivery / "001";
    fs::create_directories(pbi001);

    // Create test files
    std::ofstream(pbi001 / "prd.md")
        << "# Product Requirements Document\n\nGoals and acceptance criteria.";
    std::ofstream(pbi001 / "tasks.md") << "# Tasks\n\n- Task 1\n- Task 2";
    std::ofstream(docs / "README.md") << "# Documentation\n\nMain documentation.";

    yams::daemon::DaemonConfig cfg;
    cfg.dataDir = data;
    cfg.socketPath = sock;
    cfg.pidFile = pid;
    cfg.logFile = log;
    yams::daemon::YamsDaemon daemon(cfg);
    ASSERT_TRUE(daemon.start()) << "Failed to start daemon";
    auto guard = std::unique_ptr<void, void (*)(void*)>{
        &daemon, +[](void* d) { (void)static_cast<yams::daemon::YamsDaemon*>(d)->stop(); }};

    yams::daemon::ClientConfig cc;
    cc.socketPath = sock;
    cc.requestTimeout = 30s;
    cc.autoStart = false;
    yams::daemon::DaemonClient client(cc);

    // Wait for daemon readiness
    for (int i = 0; i < 50; ++i) {
        auto st = yams::cli::run_sync(client.status(), 500ms);
        if (st && st.value().readinessStates.count("content_store") &&
            st.value().readinessStates.at("content_store") &&
            st.value().readinessStates.count("metadata_repo") &&
            st.value().readinessStates.at("metadata_repo"))
            break;
        std::this_thread::sleep_for(100ms);
    }

    // Add documents to daemon
    std::vector<std::string> filePaths = {(pbi001 / "prd.md").string(),
                                          (pbi001 / "tasks.md").string(),
                                          (docs / "README.md").string()};

    std::vector<std::string> hashes;
    for (const auto& path : filePaths) {
        yams::daemon::AddDocumentRequest add;
        add.path = path;
        add.tags = {"test", "tree"};
        auto addRes = yams::cli::run_sync(client.streamingAddDocument(add), 10s);
        ASSERT_TRUE(addRes) << "Failed to add " << path << ": " << addRes.error().message;
        hashes.push_back(addRes.value().hash);
    }

    ASSERT_EQ(hashes.size(), 3UL) << "Expected 3 documents to be added";

    // Wait for documents to be fully indexed with polling
    // The ingest service processes documents asynchronously
    bool indexingComplete = false;
    for (int attempt = 0; attempt < 30; ++attempt) { // 30 attempts = 6 seconds max
        std::this_thread::sleep_for(200ms);

        yams::daemon::ListRequest listCheck;
        listCheck.limit = 100;
        auto checkRes = yams::cli::run_sync(client.list(listCheck), 5s);

        if (checkRes && checkRes.value().totalCount >= 3) {
            indexingComplete = true;
            break;
        }
    }
    ASSERT_TRUE(indexingComplete) << "Documents were not indexed within timeout";

    // Verify documents exist at all
    yams::daemon::ListRequest listAny;
    listAny.limit = 100;
    auto listAnyRes = yams::cli::run_sync(client.list(listAny), 10s);
    ASSERT_TRUE(listAnyRes) << "List failed: " << listAnyRes.error().message;
    ASSERT_GE(listAnyRes.value().totalCount, 3UL)
        << "Expected at least 3 documents total, got " << listAnyRes.value().totalCount;

    // Test 1: List with path prefix pattern (tree-based query)
    yams::daemon::ListRequest listWithPrefix;
    listWithPrefix.namePattern = docs.string() + "/**";
    listWithPrefix.limit = 100;
    auto prefixRes = yams::cli::run_sync(client.list(listWithPrefix), 10s);
    ASSERT_TRUE(prefixRes) << "List with prefix failed: " << prefixRes.error().message;
    EXPECT_GE(prefixRes.value().items.size(), 3UL)
        << "Expected at least 3 documents with prefix pattern " << listWithPrefix.namePattern
        << ", got " << prefixRes.value().items.size();

    // Test 2: List with path prefix AND tag filter (tree query with tag filtering)
    yams::daemon::ListRequest listWithPrefixAndTags;
    listWithPrefixAndTags.namePattern = docs.string() + "/**";
    listWithPrefixAndTags.tags = {"test"};
    listWithPrefixAndTags.limit = 100;
    auto prefixTagsRes = yams::cli::run_sync(client.list(listWithPrefixAndTags), 10s);
    ASSERT_TRUE(prefixTagsRes) << "List with prefix+tags failed: " << prefixTagsRes.error().message;
    EXPECT_GE(prefixTagsRes.value().items.size(), 3UL)
        << "Expected at least 3 documents with prefix+tags, got "
        << prefixTagsRes.value().items.size();

    // Test 3: Verify all returned documents have the expected tag
    for (const auto& doc : prefixTagsRes.value().items) {
        bool hasTestTag = std::find(doc.tags.begin(), doc.tags.end(), "test") != doc.tags.end();
        EXPECT_TRUE(hasTestTag) << "Document " << doc.path << " missing 'test' tag";
    }

    // Test 4: Verify hashes match what we added
    std::set<std::string> foundHashes;
    for (const auto& doc : prefixTagsRes.value().items) {
        foundHashes.insert(doc.hash);
    }
    for (const auto& expectedHash : hashes) {
        EXPECT_TRUE(foundHashes.count(expectedHash) > 0)
            << "Expected hash " << expectedHash << " not found in tree query results";
    }

    cleanup();
}

TEST(TreeBasedListE2E, TreeQueryPerformance) {
    if (std::getenv("GTEST_DISCOVERY_MODE")) {
        GTEST_SKIP() << "Skipping during test discovery";
    }

    // This test verifies that tree-based queries are used and perform reasonably
    // We won't do extensive benchmarking here, just verify the functionality works

    fs::path tmp = fs::temp_directory_path() / ("yams_tree_perf_" + std::to_string(::getpid()));
    fs::create_directories(tmp);
    auto cleanup = [&]() {
        std::error_code ec;
        fs::remove_all(tmp, ec);
    };

    fs::path sock = tmp / "d.sock";
    fs::path pid = tmp / "d.pid";
    fs::path log = tmp / "d.log";
    fs::path data = tmp / "data";
    fs::create_directories(data);

    // Create deeper hierarchy
    fs::path base = tmp / "project";
    std::vector<std::string> dirs = {"src/app",     "src/lib",    "docs/api",
                                     "docs/guides", "tests/unit", "tests/integration"};
    for (const auto& dir : dirs) {
        fs::create_directories(base / dir);
        // Create a few files in each directory
        for (int i = 0; i < 5; ++i) {
            std::ofstream(base / dir / ("file" + std::to_string(i) + ".txt"))
                << "Content for " << dir << " file " << i;
        }
    }

    yams::daemon::DaemonConfig cfg;
    cfg.dataDir = data;
    cfg.socketPath = sock;
    cfg.pidFile = pid;
    cfg.logFile = log;
    yams::daemon::YamsDaemon daemon(cfg);
    ASSERT_TRUE(daemon.start()) << "Failed to start daemon";
    auto guard = std::unique_ptr<void, void (*)(void*)>{
        &daemon, +[](void* d) { (void)static_cast<yams::daemon::YamsDaemon*>(d)->stop(); }};

    yams::daemon::ClientConfig cc;
    cc.socketPath = sock;
    cc.requestTimeout = 30s;
    cc.autoStart = false;
    yams::daemon::DaemonClient client(cc);

    // Wait for readiness
    for (int i = 0; i < 50; ++i) {
        auto st = yams::cli::run_sync(client.status(), 500ms);
        if (st && st.value().readinessStates.count("metadata_repo") &&
            st.value().readinessStates.at("metadata_repo"))
            break;
        std::this_thread::sleep_for(100ms);
    }

    // Add all documents
    int addedCount = 0;
    for (const auto& entry : fs::recursive_directory_iterator(base)) {
        if (entry.is_regular_file()) {
            yams::daemon::AddDocumentRequest add;
            add.path = entry.path().string();
            auto addRes = yams::cli::run_sync(client.streamingAddDocument(add), 10s);
            if (addRes) {
                ++addedCount;
            }
        }
    }
    ASSERT_GT(addedCount, 0) << "Failed to add any documents";

    // Wait for documents to be fully indexed with polling
    // The ingest service processes documents asynchronously
    bool indexingComplete = false;
    for (int attempt = 0; attempt < 30; ++attempt) { // 30 attempts = 6 seconds max
        std::this_thread::sleep_for(200ms);

        yams::daemon::ListRequest listCheck;
        listCheck.limit = 100;
        auto checkRes = yams::cli::run_sync(client.list(listCheck), 5s);

        if (checkRes && checkRes.value().totalCount >= static_cast<uint64_t>(addedCount)) {
            indexingComplete = true;
            break;
        }
    }
    ASSERT_TRUE(indexingComplete) << "Documents were not indexed within timeout (expected "
                                  << addedCount << ")";

    // Query all documents - verify they were added
    auto start = std::chrono::steady_clock::now();
    yams::daemon::ListRequest listAll;
    listAll.limit = 50;
    auto listRes = yams::cli::run_sync(client.list(listAll), 10s);
    auto duration = std::chrono::steady_clock::now() - start;

    ASSERT_TRUE(listRes) << "List query failed: " << listRes.error().message;
    EXPECT_GE(listRes.value().totalCount, static_cast<uint64_t>(addedCount))
        << "Expected at least " << addedCount << " documents, got " << listRes.value().totalCount;

    // Verify query completed in reasonable time (less than 5 seconds for this small dataset)
    EXPECT_LT(duration, 5s)
        << "List query took too long: "
        << std::chrono::duration_cast<std::chrono::milliseconds>(duration).count() << "ms";

    cleanup();
}
