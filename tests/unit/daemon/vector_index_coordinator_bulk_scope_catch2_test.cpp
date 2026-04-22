// Tests: BulkScope ref-counting — beginBulkLoad once, finalizeBulkLoad once on last release.
// Verified via observable effects (activeBulkScopes and rebuildEpoch).
// Compile with -DYAMS_TESTING=1

#include <catch2/catch_test_macros.hpp>
#include <yams/daemon/components/VectorIndexCoordinator.h>
#include <boost/asio/io_context.hpp>

using namespace yams::daemon;

TEST_CASE("BulkScope: beginBulkLoad once, finalizeBulkLoad once on last release",
          "[coordinator][bulk_scope]") {
    boost::asio::io_context io;
    // VDB nullptr — coordinator handles it gracefully; epoch still advances on finalize.
    VectorIndexCoordinator coord(io.get_executor(), nullptr, nullptr);

    REQUIRE(coord.testing_activeScopes() == 0u);
    REQUIRE(coord.testing_rebuildEpoch() == 0u);

    SECTION("two scopes: single begin mode, single finalize on last release") {
        {
            auto scope1 = coord.beginBulkIngest(RebuildReason::EmbeddingBatch);
            REQUIRE(coord.testing_activeScopes() == 1u);
            REQUIRE(coord.testing_rebuildEpoch() == 0u);
            {
                auto scope2 = coord.beginBulkIngest(RebuildReason::EmbeddingBatch);
                REQUIRE(coord.testing_activeScopes() == 2u);
                REQUIRE(coord.testing_rebuildEpoch() == 0u);
            }
            // scope2 gone → 2→1, no finalize
            REQUIRE(coord.testing_activeScopes() == 1u);
            REQUIRE(coord.testing_rebuildEpoch() == 0u);
        }
        // scope1 gone → 1→0, finalize posted
        REQUIRE(coord.testing_activeScopes() == 0u);
        io.run();
        // Finalize ran → epoch bumped exactly once
        REQUIRE(coord.testing_rebuildEpoch() == 1u);
        REQUIRE(coord.snapshot().ready == true);
    }

    SECTION("single scope: finalize on last release") {
        {
            auto scope = coord.beginBulkIngest(RebuildReason::Manual);
        }
        io.run();
        REQUIRE(coord.testing_rebuildEpoch() == 1u);
        REQUIRE(coord.testing_activeScopes() == 0u);
    }

    SECTION("move semantics: moved scope still triggers single finalize") {
        VectorIndexCoordinator::BulkScope outer;
        {
            auto inner = coord.beginBulkIngest(RebuildReason::EmbeddingBatch);
            REQUIRE(coord.testing_activeScopes() == 1u);
            outer = std::move(inner);
        }
        // inner empty, outer holds scope
        REQUIRE(coord.testing_activeScopes() == 1u);
        outer = {}; // reset → posts finalize
        io.run();
        REQUIRE(coord.testing_rebuildEpoch() == 1u);
    }

    SECTION("no scopes: no finalize posted") {
        io.run();
        REQUIRE(coord.testing_rebuildEpoch() == 0u);
    }
}
