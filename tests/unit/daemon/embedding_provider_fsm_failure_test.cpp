#include <gtest/gtest.h>
#include <yams/daemon/components/EmbeddingProviderFsm.h>

using namespace yams::daemon;

TEST(EmbeddingProviderFsmFailure, LoadFailureTransitionsToFailed) {
    EmbeddingProviderFsm fsm;
    // Immediately fail without prior adopt/load to simulate early failure
    fsm.dispatch(LoadFailureEvent{"init failure"});
    auto snap = fsm.snapshot();
    // Should not be ready; state should be Failed
    EXPECT_FALSE(fsm.isReady());
    // Dimension should remain zero
    EXPECT_EQ(fsm.dimension(), static_cast<std::size_t>(0));
}
