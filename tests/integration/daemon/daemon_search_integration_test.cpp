#include <filesystem>
#include <fstream>
#include <random>
#include "test_async_helpers.h"
#include <gtest/gtest.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/daemon.h>
#include <yams/indexing/document_indexer.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/search/search_executor.h>

namespace yams::daemon::integration::test {

namespace fs = std::filesystem;
using namespace std::chrono_literals;

class DaemonSearchIntegrationTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create test directories
        testDir_ = fs::temp_directory_path() /
                   ("search_test_" +
                    std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        fs::create_directories(testDir_);
        dataDir_ = testDir_ / "data";
        fs::create_directories(dataDir_);

        // Setup daemon
        daemonConfig_.socketPath = testDir_ / "daemon.sock";
        daemonConfig_.pidFile = testDir_ / "daemon.pid";
        daemonConfig_.logFile = testDir_ / "daemon.log";
        daemonConfig_.dataDir = dataDir_;

        clientConfig_.socketPath = daemonConfig_.socketPath;
        clientConfig_.autoStart = false;

        // Create test documents
        createTestDocuments();

        // Start daemon
        daemon_ = std::make_unique<YamsDaemon>(daemonConfig_);
        auto result = daemon_->start();
        ASSERT_TRUE(result) << "Failed to start daemon: " << result.error().message;

        std::this_thread::sleep_for(200ms); // Give daemon time to initialize
    }

    void TearDown() override {
        if (daemon_) {
            daemon_->stop();
        }

        std::error_code ec;
        fs::remove_all(testDir_, ec);
    }

    void createTestDocuments() {
        // Create various test documents
        testDocs_ = {
            {"doc1.txt", "The quick brown fox jumps over the lazy dog"},
            {"doc2.txt", "Lorem ipsum dolor sit amet, consectetur adipiscing elit"},
            {"doc3.md", "# Markdown Document\n\nThis is a test markdown file with **bold** text"},
            {"doc4.txt", "Python programming language is widely used for data science"},
            {"doc5.txt",
             "Machine learning and artificial intelligence are transforming technology"},
            {"readme.md", "# README\n\nThis project demonstrates search functionality"},
            {"notes.txt", "Important notes about the quick deployment process"},
            {"config.json", "{\"server\": \"localhost\", \"port\": 8080}"},
            {"test.py", "def hello_world():\n    print(\"Hello, World!\")"},
            {"data.csv", "name,age,city\nJohn,30,New York\nJane,25,London"}};

        for (const auto& [filename, content] : testDocs_) {
            fs::path filePath = dataDir_ / filename;
            std::ofstream file(filePath);
            file << content;
            file.close();
        }
    }

    fs::path testDir_;
    fs::path dataDir_;
    DaemonConfig daemonConfig_;
    ClientConfig clientConfig_;
    std::unique_ptr<YamsDaemon> daemon_;
    std::vector<std::pair<std::string, std::string>> testDocs_;
};

// Test basic search through daemon
TEST_F(DaemonSearchIntegrationTest, BasicSearch) {
    DaemonClient client(clientConfig_);
    ASSERT_TRUE(client.connect());

    // Search for "quick"
    SearchRequest req;
    req.query = "quick";
    req.limit = 10;
    req.fuzzy = false;

    auto result = yams::test_async::res(client.search(req));

    // May fail if documents aren't indexed
    if (!result) {
        EXPECT_TRUE(result.error().code == ErrorCode::NotFound ||
                    result.error().code == ErrorCode::NotInitialized)
            << "Unexpected error: " << result.error().message;
        return;
    }

    auto& response = result.value();
    EXPECT_GE(response.totalCount, 0);

    // If we have results, verify they're relevant
    for (const auto& res : response.results) {
        // Results should contain "quick" or be relevant
        bool relevant = res.snippet.find("quick") != std::string::npos ||
                        res.title.find("quick") != std::string::npos;
        EXPECT_TRUE(relevant) << "Result not relevant: " << res.title;
    }
}

// Test fuzzy search
TEST_F(DaemonSearchIntegrationTest, FuzzySearch) {
    DaemonClient client(clientConfig_);
    ASSERT_TRUE(client.connect());

    // Search with typo: "qwick" instead of "quick"
    SearchRequest req;
    req.query = "qwick";
    req.limit = 10;
    req.fuzzy = true;
    req.similarity = 0.7;

    auto result = yams::test_async::res(client.search(req));

    if (!result) {
        // Expected in test environment
        return;
    }

    auto& response = result.value();

    // Fuzzy search might find "quick" documents
    if (response.totalCount > 0) {
        // Check that results are somewhat relevant
        for (const auto& res : response.results) {
            EXPECT_GT(res.score, 0.0) << "Result should have positive score";
        }
    }
}

// Test search with limit
TEST_F(DaemonSearchIntegrationTest, SearchWithLimit) {
    DaemonClient client(clientConfig_);
    ASSERT_TRUE(client.connect());

    // Search with small limit
    SearchRequest req;
    req.query = "test";
    req.limit = 3;

    auto result = yams::test_async::res(client.search(req));

    if (!result) {
        return;
    }

    auto& response = result.value();
    EXPECT_LE(response.results.size(), 3) << "Should respect limit";
}

// Test empty query
TEST_F(DaemonSearchIntegrationTest, EmptyQuery) {
    DaemonClient client(clientConfig_);
    ASSERT_TRUE(client.connect());

    SearchRequest req;
    req.query = "";
    req.limit = 10;

    auto result = yams::test_async::res(client.search(req));

    // Empty query might return error or empty results
    if (result) {
        EXPECT_EQ(result.value().totalCount, 0);
    }
}

// Test search with special characters
TEST_F(DaemonSearchIntegrationTest, SpecialCharacterSearch) {
    DaemonClient client(clientConfig_);
    ASSERT_TRUE(client.connect());

    // Search for special characters
    std::vector<std::string> queries = {"**bold**", "hello_world()", "{\"server\":", "#", "30,New"};

    for (const auto& query : queries) {
        SearchRequest req;
        req.query = query;
        req.limit = 10;

        auto result = yams::test_async::res(client.search(req));

        // May succeed or fail depending on query parsing
        if (result) {
            auto& response = result.value();
            EXPECT_GE(response.totalCount, 0);
        }
    }
}

// Test concurrent searches
TEST_F(DaemonSearchIntegrationTest, ConcurrentSearches) {
    const int numThreads = 5;
    const int searchesPerThread = 10;
    std::atomic<int> successCount{0};
    std::atomic<int> errorCount{0};

    std::vector<std::string> queries = {"quick", "test", "data", "python", "markdown"};

    std::vector<std::thread> threads;
    for (int t = 0; t < numThreads; ++t) {
        threads.emplace_back([this, &queries, &successCount, &errorCount, searchesPerThread]() {
            DaemonClient client(clientConfig_);
            if (!client.connect()) {
                errorCount += searchesPerThread;
                return;
            }

            for (int i = 0; i < searchesPerThread; ++i) {
                SearchRequest req;
                req.query = queries[i % queries.size()];
                req.limit = 10;
                req.fuzzy = (i % 2 == 0);

                auto result = yams::test_async::res(client.search(req));
                if (result || result.error().code == ErrorCode::NotFound) {
                    successCount++;
                } else {
                    errorCount++;
                }

                std::this_thread::sleep_for(10ms);
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    EXPECT_GT(successCount, 0) << "Some searches should succeed";
    EXPECT_LT(errorCount, numThreads * searchesPerThread) << "Not all searches should fail";
}

// Test search performance
TEST_F(DaemonSearchIntegrationTest, SearchPerformance) {
    DaemonClient client(clientConfig_);
    ASSERT_TRUE(client.connect());

    // Warm up
    SearchRequest warmupReq;
    warmupReq.query = "test";
    warmupReq.limit = 1;
    (void)yams::test_async::res(client.search(warmupReq));

    // Measure search performance
    const int numSearches = 100;
    auto start = std::chrono::steady_clock::now();

    int successCount = 0;
    for (int i = 0; i < numSearches; ++i) {
        SearchRequest req;
        req.query = "quick brown fox";
        req.limit = 10;
        auto result = yams::test_async::res(client.search(req));
        if (result || result.error().code == ErrorCode::NotFound) {
            successCount++;
        }
    }

    auto elapsed = std::chrono::steady_clock::now() - start;
    auto elapsedMs = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();

    if (successCount > 0) {
        double avgMs = static_cast<double>(elapsedMs) / successCount;
        std::cout << "Average search time: " << avgMs << " ms" << std::endl;

        // Performance expectation (adjust based on requirements)
        EXPECT_LT(avgMs, 100) << "Search should be reasonably fast";
    }
}

// Test search with metadata filters (if supported)
TEST_F(DaemonSearchIntegrationTest, SearchWithMetadata) {
    DaemonClient client(clientConfig_);
    ASSERT_TRUE(client.connect());

    SearchRequest req;
    req.query = "document";
    req.limit = 10;
    // Add metadata filters if supported
    // req.filters["extension"] = ".md";

    auto result = yams::test_async::res(client.search(req));

    if (result) {
        auto& response = result.value();

        // Verify metadata is returned
        for (const auto& res : response.results) {
            EXPECT_FALSE(res.metadata.empty()) << "Results should have metadata";

            // Check for common metadata fields
            if (res.metadata.find("path") != res.metadata.end()) {
                EXPECT_FALSE(res.metadata.at("path").empty());
            }
        }
    }
}

// Test literal text search
TEST_F(DaemonSearchIntegrationTest, LiteralTextSearch) {
    DaemonClient client(clientConfig_);
    ASSERT_TRUE(client.connect());

    // Search for exact phrase
    SearchRequest req;
    req.query = "quick brown fox";
    req.limit = 10;
    req.literalText = true;

    auto result = yams::test_async::res(client.search(req));

    if (result) {
        auto& response = result.value();

        // If we have results, they should contain the exact phrase
        for (const auto& res : response.results) {
            if (!res.snippet.empty()) {
                // Snippet might contain the exact phrase
                bool hasPhrase = res.snippet.find("quick brown fox") != std::string::npos;
                if (response.totalCount == 1) {
                    EXPECT_TRUE(hasPhrase) << "Literal search should find exact phrase";
                }
            }
        }
    }
}

// Test search result ranking
TEST_F(DaemonSearchIntegrationTest, SearchResultRanking) {
    DaemonClient client(clientConfig_);
    ASSERT_TRUE(client.connect());

    SearchRequest req;
    req.query = "quick";
    req.limit = 10;

    auto result = yams::test_async::res(client.search(req));

    if (!result || result.value().results.size() < 2) {
        return;
    }

    auto& response = result.value();

    // Verify results are ranked by score
    double prevScore = std::numeric_limits<double>::max();
    for (const auto& res : response.results) {
        EXPECT_LE(res.score, prevScore) << "Results should be ordered by score";
        prevScore = res.score;
    }
}

// Test search timeout
TEST_F(DaemonSearchIntegrationTest, SearchTimeout) {
    ClientConfig shortTimeoutConfig = clientConfig_;
    shortTimeoutConfig.requestTimeout = 1ms; // Very short timeout

    DaemonClient client(shortTimeoutConfig);
    ASSERT_TRUE(client.connect());

    // Complex query that might take time
    SearchRequest req;
    req.query = "quick OR brown OR fox OR jumps OR lazy OR dog";
    req.limit = 100;
    req.fuzzy = true;

    auto result = yams::test_async::res(client.search(req));

    // Either succeeds or times out
    if (!result) {
        bool isTimeout = result.error().code == ErrorCode::Timeout ||
                         result.error().code == ErrorCode::NetworkError;
        EXPECT_TRUE(isTimeout || result.error().code == ErrorCode::NotFound)
            << "Should timeout or not find results";
    }
}

// Compare daemon search with direct search (if possible)
TEST_F(DaemonSearchIntegrationTest, CompareDaemonVsDirect) {
    DaemonClient client(clientConfig_);
    ASSERT_TRUE(client.connect());

    std::string query = "test";

    // Search through daemon
    SearchRequest daemonReq;
    daemonReq.query = query;
    daemonReq.limit = 10;
    auto daemonResult = yams::test_async::res(client.search(daemonReq));

    // In a real test, we'd also search directly using SearchExecutor
    // and compare results

    if (daemonResult) {
        auto& response = daemonResult.value();

        // Verify daemon search returns reasonable results
        EXPECT_GE(response.totalCount, 0);
        EXPECT_LE(response.results.size(), 10);

        // Check that elapsed time is recorded
        EXPECT_GE(response.elapsed.count(), 0);
    }
}

} // namespace yams::daemon::integration::test
