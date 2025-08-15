#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <yams/integrity/verifier.h>

#include <chrono>
#include <random>
#include <thread>
#include <vector>

using namespace yams::integrity;
using namespace testing;

class VerificationSchedulerTest : public ::testing::Test {
protected:
    void SetUp() override {
        scheduler = std::make_unique<VerificationScheduler>(SchedulingStrategy::Balanced);
    }

    void TearDown() override { scheduler.reset(); }

    BlockInfo createTestBlock(const std::string& hash,
                              std::chrono::hours age = std::chrono::hours{0},
                              uint32_t failureCount = 0, uint64_t size = 1024,
                              uint32_t accessCount = 0) {
        BlockInfo block;
        block.hash = hash;
        block.lastVerified = std::chrono::system_clock::now() - age;
        block.failureCount = failureCount;
        block.size = size;
        block.accessCount = accessCount;
        return block;
    }

    std::unique_ptr<VerificationScheduler> scheduler;
};

TEST_F(VerificationSchedulerTest, ConstructorAndInitialState) {
    EXPECT_EQ(scheduler->getQueueSize(), 0);

    auto nextBlock = scheduler->getNextBlock();
    EXPECT_FALSE(nextBlock.has_value());
}

TEST_F(VerificationSchedulerTest, AddSingleBlock) {
    auto block = createTestBlock("hash1");

    scheduler->addBlock(block);

    EXPECT_EQ(scheduler->getQueueSize(), 1);

    auto nextBlock = scheduler->getNextBlock();
    ASSERT_TRUE(nextBlock.has_value());
    EXPECT_EQ(nextBlock->hash, "hash1");

    EXPECT_EQ(scheduler->getQueueSize(), 0);
}

TEST_F(VerificationSchedulerTest, AddMultipleBlocks) {
    std::vector<BlockInfo> blocks = {createTestBlock("hash1"), createTestBlock("hash2"),
                                     createTestBlock("hash3")};

    scheduler->addBlocks(blocks);

    EXPECT_EQ(scheduler->getQueueSize(), 3);

    // Get all blocks
    std::vector<std::string> retrievedHashes;
    while (auto block = scheduler->getNextBlock()) {
        retrievedHashes.push_back(block->hash);
    }

    EXPECT_EQ(retrievedHashes.size(), 3);
    EXPECT_THAT(retrievedHashes, UnorderedElementsAre("hash1", "hash2", "hash3"));
}

TEST_F(VerificationSchedulerTest, PriorityOrdering) {
    // Create blocks with different priorities
    auto lowPriority =
        createTestBlock("hash_low", std::chrono::hours{1}, 0, 1024, 0); // Age: 1 hour
    auto mediumPriority = createTestBlock("hash_med", std::chrono::hours{24}, 1, 1024,
                                          5); // Age: 1 day, 1 failure, some access
    auto highPriority = createTestBlock("hash_high", std::chrono::hours{48}, 3, 1024,
                                        10); // Age: 2 days, 3 failures, high access

    // Add in random order
    scheduler->addBlock(lowPriority);
    scheduler->addBlock(highPriority);
    scheduler->addBlock(mediumPriority);

    EXPECT_EQ(scheduler->getQueueSize(), 3);

    // Should get highest priority first
    auto first = scheduler->getNextBlock();
    ASSERT_TRUE(first.has_value());
    EXPECT_EQ(first->hash, "hash_high");

    auto second = scheduler->getNextBlock();
    ASSERT_TRUE(second.has_value());
    EXPECT_EQ(second->hash, "hash_med");

    auto third = scheduler->getNextBlock();
    ASSERT_TRUE(third.has_value());
    EXPECT_EQ(third->hash, "hash_low");

    EXPECT_EQ(scheduler->getQueueSize(), 0);
}

TEST_F(VerificationSchedulerTest, BlockInfoPriorityCalculation) {
    auto now = std::chrono::system_clock::now();

    // Test age priority
    BlockInfo oldBlock;
    oldBlock.hash = "old";
    oldBlock.lastVerified = now - std::chrono::hours{24 * 7}; // 1 week old
    oldBlock.failureCount = 0;
    oldBlock.accessCount = 0;

    BlockInfo newBlock;
    newBlock.hash = "new";
    newBlock.lastVerified = now - std::chrono::hours{1}; // 1 hour old
    newBlock.failureCount = 0;
    newBlock.accessCount = 0;

    EXPECT_GT(oldBlock.getPriority(), newBlock.getPriority());

    // Test failure count priority
    BlockInfo failedBlock;
    failedBlock.hash = "failed";
    failedBlock.lastVerified = now - std::chrono::hours{1};
    failedBlock.failureCount = 5;
    failedBlock.accessCount = 0;

    EXPECT_GT(failedBlock.getPriority(), newBlock.getPriority());

    // Test access count priority
    BlockInfo accessedBlock;
    accessedBlock.hash = "accessed";
    accessedBlock.lastVerified = now - std::chrono::hours{1};
    accessedBlock.failureCount = 0;
    accessedBlock.accessCount = 100;

    EXPECT_GT(accessedBlock.getPriority(), newBlock.getPriority());
}

TEST_F(VerificationSchedulerTest, StrategyChange) {
    auto block1 = createTestBlock("hash1", std::chrono::hours{48}, 0, 2048, 0); // Old, large
    auto block2 =
        createTestBlock("hash2", std::chrono::hours{1}, 3, 512, 0); // New, small, failures

    scheduler->addBlock(block1);
    scheduler->addBlock(block2);

    // With balanced strategy, failure count should take precedence
    auto first = scheduler->getNextBlock();
    EXPECT_EQ(first->hash, "hash2"); // Higher failure count

    scheduler->clearQueue();

    // Change strategy and test again
    scheduler->setStrategy(SchedulingStrategy::ByAge);
    scheduler->addBlock(block1);
    scheduler->addBlock(block2);

    // Note: The actual strategy implementation may vary in the real system
    // This test primarily ensures the strategy can be changed without errors
    EXPECT_NO_THROW(scheduler->getNextBlock());
}

TEST_F(VerificationSchedulerTest, UpdateBlockInfo) {
    auto block = createTestBlock("hash1", std::chrono::hours{1}, 0);
    scheduler->addBlock(block);

    // Create verification result indicating failure
    VerificationResult result;
    result.blockHash = "hash1";
    result.status = VerificationStatus::Failed;
    result.timestamp = std::chrono::system_clock::now();

    // Update block info based on result
    EXPECT_NO_THROW(scheduler->updateBlockInfo("hash1", result));

    // The scheduler should track this information internally
    // Specific behavior depends on implementation details
}

TEST_F(VerificationSchedulerTest, ClearQueue) {
    std::vector<BlockInfo> blocks = {createTestBlock("hash1"), createTestBlock("hash2"),
                                     createTestBlock("hash3")};

    scheduler->addBlocks(blocks);
    EXPECT_EQ(scheduler->getQueueSize(), 3);

    scheduler->clearQueue();
    EXPECT_EQ(scheduler->getQueueSize(), 0);

    auto nextBlock = scheduler->getNextBlock();
    EXPECT_FALSE(nextBlock.has_value());
}

TEST_F(VerificationSchedulerTest, EmptyQueueOperations) {
    // Operations on empty queue should not crash
    EXPECT_EQ(scheduler->getQueueSize(), 0);

    auto nextBlock = scheduler->getNextBlock();
    EXPECT_FALSE(nextBlock.has_value());

    EXPECT_NO_THROW(scheduler->clearQueue());

    EXPECT_NO_THROW(scheduler->setStrategy(SchedulingStrategy::BySize));

    VerificationResult result;
    result.blockHash = "nonexistent";
    result.status = VerificationStatus::Passed;
    EXPECT_NO_THROW(scheduler->updateBlockInfo("nonexistent", result));
}

TEST_F(VerificationSchedulerTest, AllSchedulingStrategies) {
    // Test that all scheduling strategies can be set without errors
    EXPECT_NO_THROW(scheduler->setStrategy(SchedulingStrategy::ByAge));
    EXPECT_NO_THROW(scheduler->setStrategy(SchedulingStrategy::BySize));
    EXPECT_NO_THROW(scheduler->setStrategy(SchedulingStrategy::ByFailures));
    EXPECT_NO_THROW(scheduler->setStrategy(SchedulingStrategy::ByAccess));
    EXPECT_NO_THROW(scheduler->setStrategy(SchedulingStrategy::Balanced));
}

TEST_F(VerificationSchedulerTest, LargeNumberOfBlocks) {
    const size_t numBlocks = 1000;
    std::vector<BlockInfo> blocks;
    blocks.reserve(numBlocks);

    // Create many blocks with random priorities
    std::mt19937 rng(42);                               // Fixed seed for reproducible tests
    std::uniform_int_distribution<int> ageDist(1, 168); // 1 to 168 hours
    std::uniform_int_distribution<int> failureDist(0, 10);
    std::uniform_int_distribution<int> accessDist(0, 1000);

    for (size_t i = 0; i < numBlocks; ++i) {
        blocks.push_back(createTestBlock("hash" + std::to_string(i),
                                         std::chrono::hours{ageDist(rng)}, failureDist(rng), 1024,
                                         accessDist(rng)));
    }

    scheduler->addBlocks(blocks);
    EXPECT_EQ(scheduler->getQueueSize(), numBlocks);

    // Retrieve all blocks (should be ordered by priority)
    std::vector<BlockInfo> retrieved;
    while (auto block = scheduler->getNextBlock()) {
        retrieved.push_back(*block);
    }

    EXPECT_EQ(retrieved.size(), numBlocks);
    EXPECT_EQ(scheduler->getQueueSize(), 0);

    // Verify ordering (each block should have >= priority than the next)
    for (size_t i = 1; i < retrieved.size(); ++i) {
        EXPECT_GE(retrieved[i - 1].getPriority(), retrieved[i].getPriority())
            << "Priority ordering violated at position " << i;
    }
}

TEST_F(VerificationSchedulerTest, ConcurrentAccess) {
    // This test ensures thread safety of the scheduler
    // Add blocks from multiple threads

    const int numThreads = 4;
    const int blocksPerThread = 250;

    std::vector<std::thread> threads;

    // Add blocks concurrently
    for (int t = 0; t < numThreads; ++t) {
        threads.emplace_back([this, t, blocksPerThread]() {
            for (int i = 0; i < blocksPerThread; ++i) {
                auto block =
                    createTestBlock("thread" + std::to_string(t) + "_block" + std::to_string(i),
                                    std::chrono::hours{i + 1});
                scheduler->addBlock(block);
            }
        });
    }

    // Wait for all threads to finish
    for (auto& thread : threads) {
        thread.join();
    }

    EXPECT_EQ(scheduler->getQueueSize(), numThreads * blocksPerThread);

    // Retrieve all blocks concurrently
    std::atomic<int> retrievedCount{0};
    threads.clear();

    for (int t = 0; t < numThreads; ++t) {
        threads.emplace_back([this, &retrievedCount]() {
            while (auto block = scheduler->getNextBlock()) {
                retrievedCount++;
            }
        });
    }

    // Wait for all threads to finish
    for (auto& thread : threads) {
        thread.join();
    }

    EXPECT_EQ(retrievedCount.load(), numThreads * blocksPerThread);
    EXPECT_EQ(scheduler->getQueueSize(), 0);
}