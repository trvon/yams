#include <catch2/catch_test_macros.hpp>

#include <yams/integrity/verifier.h>

#include <chrono>

using namespace yams::integrity;

namespace {

struct SchedulerFixture {
    VerificationScheduler scheduler;

    BlockInfo makeBlock(const std::string& hash, uint32_t failureCount = 0) {
        BlockInfo block;
        block.hash = hash;
        block.failureCount = failureCount;
        block.lastVerified = std::chrono::system_clock::now();
        block.size = 1024;
        block.accessCount = 1;
        return block;
    }
};

} // namespace

TEST_CASE_METHOD(SchedulerFixture, "VerificationScheduler block management",
                 "[integrity][scheduler]") {
    SECTION("empty scheduler returns nullopt") {
        CHECK_FALSE(scheduler.getNextBlock().has_value());
    }

    SECTION("adds and retrieves single block") {
        auto block = makeBlock("hash_1");
        scheduler.addBlock(block);

        auto next = scheduler.getNextBlock();
        REQUIRE(next.has_value());
        CHECK(next->hash == "hash_1");
    }

    SECTION("queue size is tracked") {
        CHECK(scheduler.getQueueSize() == 0u);

        scheduler.addBlock(makeBlock("a"));
        CHECK(scheduler.getQueueSize() == 1u);

        scheduler.addBlock(makeBlock("b"));
        CHECK(scheduler.getQueueSize() == 2u);
    }

    SECTION("clearQueue removes all blocks") {
        scheduler.addBlock(makeBlock("x"));
        scheduler.addBlock(makeBlock("y"));
        scheduler.addBlock(makeBlock("z"));

        scheduler.clearQueue();
        CHECK(scheduler.getQueueSize() == 0u);
        CHECK_FALSE(scheduler.getNextBlock().has_value());
    }
}

TEST_CASE_METHOD(SchedulerFixture, "VerificationScheduler batch operations",
                 "[integrity][scheduler]") {
    SECTION("adds multiple blocks at once") {
        std::vector<BlockInfo> blocks;
        blocks.push_back(makeBlock("block_1"));
        blocks.push_back(makeBlock("block_2"));
        blocks.push_back(makeBlock("block_3"));

        scheduler.addBlocks(blocks);
        CHECK(scheduler.getQueueSize() == 3u);
    }
}

TEST_CASE("VerificationScheduler strategy", "[integrity][scheduler]") {
    SECTION("can change scheduling strategy") {
        VerificationScheduler scheduler(SchedulingStrategy::ByAge);
        // Just verify it doesn't throw
        scheduler.setStrategy(SchedulingStrategy::BySize);
        scheduler.setStrategy(SchedulingStrategy::ByFailures);
        scheduler.setStrategy(SchedulingStrategy::Balanced);
        CHECK(true); // If we got here, strategies can be changed
    }
}
