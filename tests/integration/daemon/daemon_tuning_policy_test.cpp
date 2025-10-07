#include <cstdlib>
#include <optional>
#include <string>

#include <gtest/gtest.h>
#include <yams/daemon/components/TuneAdvisor.h>

namespace {
struct ScopedUnsetEnv {
    explicit ScopedUnsetEnv(const char* key) : key_(key) {
        if (const char* cur = std::getenv(key))
            old_ = std::string(cur);
#ifdef _WIN32
        _putenv_s(key, "");
#else
        ::unsetenv(key);
#endif
    }

    ~ScopedUnsetEnv() {
        if (old_) {
#ifdef _WIN32
            _putenv_s(key_.c_str(), old_->c_str());
#else
            ::setenv(key_.c_str(), old_->c_str(), 1);
#endif
        } else {
#ifdef _WIN32
            _putenv_s(key_.c_str(), "");
#else
            ::unsetenv(key_.c_str());
#endif
        }
    }

private:
    std::string key_;
    std::optional<std::string> old_;
};
} // namespace

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

TEST(DaemonTuning, ProfileDefaultScaling) {
    ScopedUnsetEnv cpuBudget{"YAMS_CPU_BUDGET_PERCENT"};
    ScopedUnsetEnv workerQueue{"YAMS_MAX_WORKER_QUEUE"};
    ScopedUnsetEnv muxBytes{"YAMS_MAX_MUX_BYTES"};
    ScopedUnsetEnv poolStep{"YAMS_POOL_SCALE_STEP"};
    ScopedUnsetEnv poolCooldown{"YAMS_POOL_COOLDOWN_MS"};

    TuneAdvisor::setPoolScaleStep(0);
    TuneAdvisor::setPoolCooldownMs(0);

    TuneAdvisor::setTuningProfile(TuneAdvisor::Profile::Balanced);
    EXPECT_EQ(TuneAdvisor::cpuBudgetPercent(), 50u);
    EXPECT_EQ(TuneAdvisor::poolScaleStep(), 1);
    EXPECT_EQ(TuneAdvisor::poolCooldownMs(), 500u);
    EXPECT_EQ(TuneAdvisor::maxWorkerQueue(8), 16u);
    EXPECT_EQ(TuneAdvisor::maxMuxBytes(), 256ull * 1024ull * 1024ull);

    TuneAdvisor::setTuningProfile(TuneAdvisor::Profile::Aggressive);
    TuneAdvisor::setPoolScaleStep(0);
    TuneAdvisor::setPoolCooldownMs(0);
    EXPECT_EQ(TuneAdvisor::cpuBudgetPercent(), 80u);
    EXPECT_EQ(TuneAdvisor::poolScaleStep(), 1);
    EXPECT_EQ(TuneAdvisor::poolCooldownMs(), 250u);
    EXPECT_EQ(TuneAdvisor::maxWorkerQueue(8), 24u);
    EXPECT_EQ(TuneAdvisor::maxMuxBytes(), 384ull * 1024ull * 1024ull);

    TuneAdvisor::setTuningProfile(TuneAdvisor::Profile::Efficient);
    TuneAdvisor::setPoolScaleStep(0);
    TuneAdvisor::setPoolCooldownMs(0);
    EXPECT_EQ(TuneAdvisor::cpuBudgetPercent(), 40u);
    EXPECT_EQ(TuneAdvisor::poolScaleStep(), 1);
    EXPECT_EQ(TuneAdvisor::poolCooldownMs(), 750u);
    EXPECT_EQ(TuneAdvisor::maxWorkerQueue(8), 12u);
    EXPECT_EQ(TuneAdvisor::maxMuxBytes(), 192ull * 1024ull * 1024ull);

    TuneAdvisor::setTuningProfile(TuneAdvisor::Profile::Balanced);
    TuneAdvisor::setPoolScaleStep(0);
    TuneAdvisor::setPoolCooldownMs(0);
}
