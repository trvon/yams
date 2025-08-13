#include <gtest/gtest.h>
#include <yams/api/content_store.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/database.h>
#include "../../common/test_data_generator.h"
#include <filesystem>
#include <thread>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <random>
#include <barrier>

using namespace yams;
using namespace yams::api;
using namespace yams::metadata;
using namespace yams::test;

class ConcurrencyTest : public ::testing::Test {
protected:
    void SetUp() override {
        testDir_ = std::filesystem::temp_directory_path() / "yams_concurrency_test";
        std::filesystem::create_directories(testDir_);
        
        initializeYAMS();
        generator_ = std::make_unique<TestDataGenerator>();
    }
    
    void TearDown() override {
        contentStore_.reset();
        metadataRepo_.reset();
        database_.reset();
        std::filesystem::remove_all(testDir_);
    }
    
    void initializeYAMS() {
        auto dbPath = testDir_ / "yams.db";
        database_ = std::make_shared<Database>(dbPath.string());
        database_->initialize();
        metadataRepo_ = std::make_shared<MetadataRepository>(database_);
        contentStore_ = std::make_unique<ContentStore>(testDir_.string());
    }
    
    std::filesystem::path testDir_;
    std::shared_ptr<Database> database_;
    std::shared_ptr<MetadataRepository> metadataRepo_;
    std::unique_ptr<ContentStore> contentStore_;
    std::unique_ptr<TestDataGenerator> generator_;
};

TEST_F(ConcurrencyTest, ConcurrentDocumentAddition) {
    const size_t numThreads = 8;
    const size_t docsPerThread = 10;
    
    std::atomic<size_t> successCount{0};
    std::atomic<size_t> failCount{0};
    std::vector<std::string> allHashes;
    std::mutex hashMutex;
    
    std::vector<std::thread> threads;
    
    for (size_t t = 0; t < numThreads; ++t) {
        threads.emplace_back([&, t]() {
            TestDataGenerator localGen(t); // Thread-local generator with unique seed
            
            for (size_t i = 0; i < docsPerThread; ++i) {
                auto content = localGen.generateTextDocument(1024 + i * 100);
                auto file = testDir_ / ("thread_" + std::to_string(t) + "_doc_" + std::to_string(i) + ".txt");
                
                std::ofstream outFile(file);
                outFile << content;
                outFile.close();
                
                auto result = contentStore_->addFile(file, {"concurrent", "thread_" + std::to_string(t)});
                
                if (result.has_value()) {
                    successCount++;
                    std::lock_guard<std::mutex> lock(hashMutex);
                    allHashes.push_back(result->contentHash);
                } else {
                    failCount++;
                }
            }
        });
    }
    
    for (auto& thread : threads) {
        thread.join();
    }
    
    // Verify results
    EXPECT_EQ(successCount.load(), numThreads * docsPerThread) << "Some additions failed";
    EXPECT_EQ(failCount.load(), 0) << "There were failures";
    EXPECT_EQ(allHashes.size(), numThreads * docsPerThread) << "Hash count mismatch";
    
    // Verify all documents are retrievable
    for (const auto& hash : allHashes) {
        auto content = contentStore_->getContent(hash);
        EXPECT_TRUE(content.has_value()) << "Failed to retrieve document: " << hash;
    }
}

TEST_F(ConcurrencyTest, ConcurrentSearchOperations) {
    // First, add some documents
    const size_t numDocs = 100;
    std::vector<std::string> searchTerms = {"alpha", "beta", "gamma", "delta", "epsilon"};
    
    for (size_t i = 0; i < numDocs; ++i) {
        auto content = generator_->generateTextDocument(512);
        content += " " + searchTerms[i % searchTerms.size()];
        
        auto file = testDir_ / ("search_doc_" + std::to_string(i) + ".txt");
        std::ofstream(file) << content;
        
        contentStore_->addFile(file, {"searchable"});
    }
    
    // Concurrent searches
    const size_t numSearchThreads = 10;
    const size_t searchesPerThread = 20;
    std::atomic<size_t> totalResults{0};
    std::atomic<size_t> searchFailures{0};
    
    std::vector<std::thread> searchThreads;
    
    for (size_t t = 0; t < numSearchThreads; ++t) {
        searchThreads.emplace_back([&, t]() {
            std::mt19937 rng(t);
            std::uniform_int_distribution<size_t> dist(0, searchTerms.size() - 1);
            
            for (size_t i = 0; i < searchesPerThread; ++i) {
                const auto& term = searchTerms[dist(rng)];
                
                auto results = contentStore_->search(term);
                if (results.has_value()) {
                    totalResults++;
                } else {
                    searchFailures++;
                }
                
                // Also test with options
                search::SearchOptions options;
                options.tags = {"searchable"};
                options.limit = 10;
                
                auto taggedResults = contentStore_->searchWithOptions(term, options);
                totalResults += taggedResults.size();
            }
        });
    }
    
    for (auto& thread : searchThreads) {
        thread.join();
    }
    
    EXPECT_GT(totalResults.load(), 0) << "No search results found";
    EXPECT_EQ(searchFailures.load(), 0) << "Some searches failed";
}

TEST_F(ConcurrencyTest, ConcurrentMetadataUpdates) {
    // Add documents first
    const size_t numDocs = 50;
    std::vector<std::string> docIds;
    
    for (size_t i = 0; i < numDocs; ++i) {
        auto content = generator_->generateTextDocument(512);
        auto file = testDir_ / ("metadata_doc_" + std::to_string(i) + ".txt");
        std::ofstream(file) << content;
        
        auto result = contentStore_->addFile(file);
        ASSERT_TRUE(result.has_value());
        
        auto docResult = metadataRepo_->getDocumentByHash(result->contentHash);
        ASSERT_TRUE(docResult.has_value() && docResult->has_value());
        docIds.push_back(docResult->value()->id);
    }
    
    // Concurrent metadata updates
    const size_t numUpdateThreads = 8;
    std::atomic<size_t> updateSuccess{0};
    std::atomic<size_t> updateFailure{0};
    
    std::vector<std::thread> updateThreads;
    
    for (size_t t = 0; t < numUpdateThreads; ++t) {
        updateThreads.emplace_back([&, t]() {
            std::mt19937 rng(t);
            std::uniform_int_distribution<size_t> docDist(0, docIds.size() - 1);
            
            for (size_t i = 0; i < 20; ++i) {
                size_t docIdx = docDist(rng);
                const auto& docId = docIds[docIdx];
                
                std::string key = "thread_" + std::to_string(t) + "_field_" + std::to_string(i);
                std::string value = "value_" + std::to_string(i);
                
                auto result = metadataRepo_->setMetadata(docId, key, MetadataValue(value));
                
                if (result.isOk()) {
                    updateSuccess++;
                } else {
                    updateFailure++;
                }
            }
        });
    }
    
    for (auto& thread : updateThreads) {
        thread.join();
    }
    
    EXPECT_GT(updateSuccess.load(), 0) << "No successful updates";
    EXPECT_EQ(updateFailure.load(), 0) << "Some updates failed";
    
    // Verify metadata persistence
    for (const auto& docId : docIds) {
        auto allMetadata = metadataRepo_->getAllMetadata(docId);
        EXPECT_TRUE(allMetadata.has_value()) << "Failed to get metadata for " << docId;
    }
}

TEST_F(ConcurrencyTest, RaceConditionDetection) {
    // Test for race conditions in critical sections
    
    const size_t numThreads = 10;
    std::atomic<int> sharedCounter{0};
    std::vector<int> threadResults(numThreads, 0);
    
    // Add a single document that all threads will try to update
    auto content = generator_->generateTextDocument(1024);
    auto file = testDir_ / "race_test.txt";
    std::ofstream(file) << content;
    
    auto result = contentStore_->addFile(file);
    ASSERT_TRUE(result.has_value());
    
    auto docResult = metadataRepo_->getDocumentByHash(result->contentHash);
    ASSERT_TRUE(docResult.has_value() && docResult->has_value());
    std::string docId = docResult->value()->id;
    
    // Use a barrier to ensure all threads start simultaneously
    std::barrier sync_point(numThreads);
    
    std::vector<std::thread> threads;
    
    for (size_t t = 0; t < numThreads; ++t) {
        threads.emplace_back([&, t]() {
            sync_point.arrive_and_wait();
            
            // Try to increment a shared counter through metadata
            for (int i = 0; i < 100; ++i) {
                // Read current value
                auto currentMeta = metadataRepo_->getMetadata(docId, "counter");
                int currentValue = 0;
                if (currentMeta.has_value()) {
                    currentValue = static_cast<int>(currentMeta->toDouble());
                }
                
                // Increment and write back
                currentValue++;
                auto updateResult = metadataRepo_->setMetadata(
                    docId, 
                    "counter", 
                    MetadataValue(static_cast<double>(currentValue))
                );
                
                if (updateResult.isOk()) {
                    threadResults[t]++;
                }
                
                sharedCounter++;
            }
        });
    }
    
    for (auto& thread : threads) {
        thread.join();
    }
    
    // Check final counter value
    auto finalMeta = metadataRepo_->getMetadata(docId, "counter");
    ASSERT_TRUE(finalMeta.has_value());
    int finalValue = static_cast<int>(finalMeta->toDouble());
    
    // Due to race conditions, final value may be less than total operations
    // This is expected and demonstrates the need for proper synchronization
    EXPECT_LE(finalValue, numThreads * 100) << "Counter exceeded maximum possible value";
    EXPECT_GT(finalValue, 0) << "Counter was not updated at all";
    
    // Verify shared counter
    EXPECT_EQ(sharedCounter.load(), numThreads * 100) << "Shared counter mismatch";
}

TEST_F(ConcurrencyTest, DataIntegrityUnderLoad) {
    // Test data integrity with mixed concurrent operations
    
    const size_t numOperations = 200;
    std::atomic<size_t> addCount{0};
    std::atomic<size_t> searchCount{0};
    std::atomic<size_t> updateCount{0};
    std::atomic<size_t> getCount{0};
    
    std::vector<std::string> documentHashes;
    std::mutex hashesMutex;
    
    // Mixed operation threads
    std::vector<std::thread> threads;
    
    for (int t = 0; t < 8; ++t) {
        threads.emplace_back([&, t]() {
            std::mt19937 rng(t);
            std::uniform_int_distribution<int> opDist(0, 3);
            TestDataGenerator localGen(t);
            
            for (size_t i = 0; i < numOperations / 8; ++i) {
                int operation = opDist(rng);
                
                switch (operation) {
                    case 0: { // Add
                        auto content = localGen.generateTextDocument(512);
                        auto file = testDir_ / ("integrity_" + std::to_string(t) + "_" + std::to_string(i) + ".txt");
                        std::ofstream(file) << content;
                        
                        auto result = contentStore_->addFile(file);
                        if (result.has_value()) {
                            addCount++;
                            std::lock_guard<std::mutex> lock(hashesMutex);
                            documentHashes.push_back(result->contentHash);
                        }
                        break;
                    }
                    
                    case 1: { // Search
                        auto results = contentStore_->search("test");
                        searchCount++;
                        break;
                    }
                    
                    case 2: { // Update metadata
                        std::lock_guard<std::mutex> lock(hashesMutex);
                        if (!documentHashes.empty()) {
                            std::uniform_int_distribution<size_t> hashDist(0, documentHashes.size() - 1);
                            const auto& hash = documentHashes[hashDist(rng)];
                            
                            auto docResult = metadataRepo_->getDocumentByHash(hash);
                            if (docResult.has_value() && docResult->has_value()) {
                                auto updateResult = metadataRepo_->setMetadata(
                                    docResult->value()->id,
                                    "thread_" + std::to_string(t),
                                    MetadataValue("update_" + std::to_string(i))
                                );
                                if (updateResult.isOk()) {
                                    updateCount++;
                                }
                            }
                        }
                        break;
                    }
                    
                    case 3: { // Get
                        std::lock_guard<std::mutex> lock(hashesMutex);
                        if (!documentHashes.empty()) {
                            std::uniform_int_distribution<size_t> hashDist(0, documentHashes.size() - 1);
                            const auto& hash = documentHashes[hashDist(rng)];
                            
                            auto content = contentStore_->getContent(hash);
                            if (content.has_value()) {
                                getCount++;
                                // Verify content is not corrupted
                                EXPECT_GT(content->size(), 0) << "Empty content retrieved";
                            }
                        }
                        break;
                    }
                }
            }
        });
    }
    
    for (auto& thread : threads) {
        thread.join();
    }
    
    // Verify operation counts
    std::cout << "Operations completed - Add: " << addCount.load() 
              << ", Search: " << searchCount.load()
              << ", Update: " << updateCount.load()
              << ", Get: " << getCount.load() << std::endl;
    
    EXPECT_GT(addCount.load(), 0) << "No documents added";
    EXPECT_GT(searchCount.load(), 0) << "No searches performed";
    EXPECT_GT(updateCount.load(), 0) << "No updates performed";
    EXPECT_GT(getCount.load(), 0) << "No gets performed";
    
    // Final integrity check - all documents should be retrievable
    for (const auto& hash : documentHashes) {
        auto content = contentStore_->getContent(hash);
        EXPECT_TRUE(content.has_value()) << "Document lost: " << hash;
        if (content.has_value()) {
            EXPECT_GT(content->size(), 0) << "Document corrupted: " << hash;
        }
    }
}

TEST_F(ConcurrencyTest, DeadlockPrevention) {
    // Test that operations don't deadlock
    
    const size_t numThreads = 4;
    std::atomic<bool> deadlockDetected{false};
    
    // Add some initial documents
    std::vector<std::string> hashes;
    for (int i = 0; i < 10; ++i) {
        auto content = generator_->generateTextDocument(512);
        auto file = testDir_ / ("deadlock_test_" + std::to_string(i) + ".txt");
        std::ofstream(file) << content;
        
        auto result = contentStore_->addFile(file);
        ASSERT_TRUE(result.has_value());
        hashes.push_back(result->contentHash);
    }
    
    // Threads that perform operations in different orders
    std::vector<std::thread> threads;
    
    // Thread pattern 1: Add -> Search -> Update
    threads.emplace_back([&]() {
        for (int i = 0; i < 50; ++i) {
            auto content = generator_->generateTextDocument(256);
            auto file = testDir_ / ("pattern1_" + std::to_string(i) + ".txt");
            std::ofstream(file) << content;
            
            contentStore_->addFile(file);
            contentStore_->search("test");
            
            if (!hashes.empty()) {
                auto docResult = metadataRepo_->getDocumentByHash(hashes[0]);
                if (docResult.has_value() && docResult->has_value()) {
                    metadataRepo_->setMetadata(
                        docResult->value()->id,
                        "pattern1",
                        MetadataValue(i)
                    );
                }
            }
        }
    });
    
    // Thread pattern 2: Update -> Add -> Search
    threads.emplace_back([&]() {
        for (int i = 0; i < 50; ++i) {
            if (!hashes.empty()) {
                auto docResult = metadataRepo_->getDocumentByHash(hashes[0]);
                if (docResult.has_value() && docResult->has_value()) {
                    metadataRepo_->setMetadata(
                        docResult->value()->id,
                        "pattern2",
                        MetadataValue(i)
                    );
                }
            }
            
            auto content = generator_->generateTextDocument(256);
            auto file = testDir_ / ("pattern2_" + std::to_string(i) + ".txt");
            std::ofstream(file) << content;
            
            contentStore_->addFile(file);
            contentStore_->search("test");
        }
    });
    
    // Thread pattern 3: Search -> Update -> Add
    threads.emplace_back([&]() {
        for (int i = 0; i < 50; ++i) {
            contentStore_->search("test");
            
            if (!hashes.empty()) {
                auto docResult = metadataRepo_->getDocumentByHash(hashes[0]);
                if (docResult.has_value() && docResult->has_value()) {
                    metadataRepo_->setMetadata(
                        docResult->value()->id,
                        "pattern3",
                        MetadataValue(i)
                    );
                }
            }
            
            auto content = generator_->generateTextDocument(256);
            auto file = testDir_ / ("pattern3_" + std::to_string(i) + ".txt");
            std::ofstream(file) << content;
            
            contentStore_->addFile(file);
        }
    });
    
    // Deadlock detection thread
    threads.emplace_back([&]() {
        auto startTime = std::chrono::steady_clock::now();
        const auto timeout = std::chrono::seconds(30);
        
        while (true) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            
            auto now = std::chrono::steady_clock::now();
            if (now - startTime > timeout) {
                deadlockDetected = true;
                break;
            }
            
            // Check if other threads are done
            bool allDone = true;
            for (size_t i = 0; i < threads.size() - 1; ++i) {
                if (threads[i].joinable()) {
                    allDone = false;
                    break;
                }
            }
            
            if (allDone) break;
        }
    });
    
    // Join all threads
    for (auto& thread : threads) {
        if (thread.joinable()) {
            thread.join();
        }
    }
    
    EXPECT_FALSE(deadlockDetected.load()) << "Potential deadlock detected (timeout)";
}