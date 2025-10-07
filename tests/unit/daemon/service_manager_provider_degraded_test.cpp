// Minimal unit covering degraded transition when no provider can be adopted.
#include <gtest/gtest.h>

#include <yams/daemon/components/EmbeddingProviderFsm.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/daemon.h>

using namespace yams::daemon;

TEST(ServiceManagerProviderDegradedTest, DegradesWhenNoProviderAvailable) {
    DaemonConfig cfg;       // defaults: autoLoadPlugins=false; no pluginDir
    StateComponent state{}; // empty readiness snapshot

    ServiceManager sm(cfg, state);

    auto res = sm.adoptModelProviderFromHosts();
    ASSERT_TRUE(res.has_value());
    EXPECT_FALSE(res.value());

    auto snap = sm.getEmbeddingProviderFsmSnapshot();
    // Expect degraded since we explicitly dispatch when adoption fails.
    EXPECT_EQ(snap.state, EmbeddingProviderState::Degraded);
}
