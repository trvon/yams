// Validates degraded->ready recovery via ServiceManager's FSM helper.
#include <gtest/gtest.h>

#include <yams/daemon/components/DaemonLifecycleFsm.h>
#include <yams/daemon/components/EmbeddingProviderFsm.h>
#include <yams/daemon/components/ServiceManager.h>

using namespace yams::daemon;

TEST(EmbeddingProviderRecovery, DegradedThenRecoveredToReady) {
    DaemonConfig cfg; // defaults are fine; no plugins needed
    StateComponent state{};
    DaemonLifecycleFsm lifecycleFsm;

    ServiceManager sm(cfg, state, lifecycleFsm);

    // Simulate degraded provider
    sm.__test_setModelProviderDegraded(true, "unit-test degraded");
    auto s1 = sm.getEmbeddingProviderFsmSnapshot();
    EXPECT_EQ(s1.state, EmbeddingProviderState::Degraded);
    EXPECT_NE(s1.lastError.find("degraded"), std::string::npos);

    // Simulate recovery (helper dispatches ModelLoadedEvent with dimension 0)
    sm.__test_setModelProviderDegraded(false, "");
    auto s2 = sm.getEmbeddingProviderFsmSnapshot();
    EXPECT_EQ(s2.state, EmbeddingProviderState::ModelReady);
    EXPECT_EQ(s2.embeddingDimension, 0u);
}
