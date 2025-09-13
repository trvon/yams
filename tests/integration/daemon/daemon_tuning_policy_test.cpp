#include <gtest/gtest.h>
#include <yams/daemon/components/TuneAdvisor.h>

using yams::daemon::TuneAdvisor;

TEST(DaemonTuning, AutoEmbedPolicySetters) {
    // Verify setter/getter roundtrip for AutoEmbedPolicy
    TuneAdvisor::setAutoEmbedPolicy(TuneAdvisor::AutoEmbedPolicy::Never);
    EXPECT_EQ(TuneAdvisor::autoEmbedPolicy(), TuneAdvisor::AutoEmbedPolicy::Never);

    TuneAdvisor::setAutoEmbedPolicy(TuneAdvisor::AutoEmbedPolicy::Always);
    EXPECT_EQ(TuneAdvisor::autoEmbedPolicy(), TuneAdvisor::AutoEmbedPolicy::Always);

    TuneAdvisor::setAutoEmbedPolicy(TuneAdvisor::AutoEmbedPolicy::Idle);
    EXPECT_EQ(TuneAdvisor::autoEmbedPolicy(), TuneAdvisor::AutoEmbedPolicy::Idle);
}

TEST(DaemonTuning, EmbeddingBatchKnobsClamp) {
    // Safety clamps to [0.5, 0.95]
    TuneAdvisor::setEmbedSafety(0.1);
    EXPECT_GE(TuneAdvisor::embedSafety(), 0.5);
    TuneAdvisor::setEmbedSafety(0.99);
    EXPECT_LE(TuneAdvisor::embedSafety(), 0.95);

    // Doc cap and pause are pass-through
    TuneAdvisor::setEmbedDocCap(123);
    EXPECT_EQ(TuneAdvisor::embedDocCap(), 123u);
    TuneAdvisor::setEmbedPauseMs(77);
    EXPECT_EQ(TuneAdvisor::embedPauseMs(), 77u);
}
