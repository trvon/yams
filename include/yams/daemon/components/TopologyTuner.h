// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

#pragma once

#include <yams/core/types.h>
#include <yams/daemon/components/TopologyManager.h>
#include <yams/search/tuner_mab.h>

#include <nlohmann/json.hpp>

#include <chrono>
#include <cstddef>
#include <filesystem>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace yams::daemon {

// One arm in the topology bandit's grid: an (engine, params) tuple to be
// evaluated against intrinsic cluster-quality reward. Caller-supplied id
// is the canonical key the bandit uses to attribute reward back to this arm.
struct TopologyArm {
    std::string id;
    std::string engine;
    std::size_t hdbscanMinClusterSize{0};
    std::size_t hdbscanMinPoints{0};
    std::size_t featureSmoothingHops{0};
};

// Weights for the intrinsic-reward formula. The reward is clamped to [0, 1].
// Singleton and giant-cluster penalties are applied QUADRATICALLY — small
// values (well-clustered corpus) have negligible penalty, pathological values
// (singleton=1 or giant=1) take the full α/β hit. Gini deviation from 0.4 is
// applied linearly; intra-cluster cohesion adds a small bonus.
struct IntrinsicRewardWeights {
    double alphaSingleton{0.7};
    double betaGiantCluster{0.7};
    double gammaGiniDeviation{0.15};
    double deltaIntraEdge{0.15};
};

// Reward computation mode for the topology tuner. Phase H-TDA ablation.
//   Geometric  — use the IntrinsicRewardWeights formula (singleton/giant/gini/intra)
//   Persistence — use H_0 persistence on a sample of embeddings as the reward
//   Hybrid     — 0.5 × geometric + 0.5 × normalized(persistence)
enum class TunerRewardMode : std::uint8_t {
    Geometric = 0,
    Persistence = 1,
    Hybrid = 2,
};

struct TopologyTunerConfig {
    bool enabled{false};
    std::chrono::minutes cooldown{10};
    std::size_t docCountDelta{100};
    IntrinsicRewardWeights weights;
    TunerRewardMode rewardMode{TunerRewardMode::Geometric};
    // Phase H-TDA: number of embeddings to sample for H_0 persistence
    // computation when rewardMode != Geometric. ~512 keeps O(n²) cost
    // manageable on rebuild critical path; bigger improves signal stability.
    std::size_t persistenceSampleSize{512};
    // When set, the tuner persists its MAB state to this file after each
    // observeRebuildStats call, and loads from it on construction. Lets
    // UCB1 accumulate arm-pull history across daemon restarts.
    std::optional<std::filesystem::path> statePath;
};

[[nodiscard]] double computeIntrinsicReward(const TopologyManager::RebuildStats& stats,
                                            const IntrinsicRewardWeights& weights) noexcept;

// Build the default arm grid for a corpus of `corpusDocCount` documents.
// The HDBSCAN parameter values scale with corpus size (log2(n), sqrt(n))
// so the same grid is meaningful at 1k and 50k docs.
[[nodiscard]] std::vector<TopologyArm> defaultArmGrid(std::size_t corpusDocCount);

class TopologyTuner {
public:
    explicit TopologyTuner(TopologyTunerConfig cfg);

    void setArms(std::vector<TopologyArm> arms);

    // Rebuild the arm grid for a new corpus size. If the resulting grid has
    // the same arm IDs as the current one, this is a no-op (preserves MAB
    // state). Otherwise, MAB state is reset (existing arm rewards are dropped
    // because the underlying parameter values changed). Returns true if the
    // grid was actually rebuilt.
    bool rebuildArmGridForCorpusSize(std::size_t corpusDocCount);

    [[nodiscard]] std::optional<TopologyArm> selectArm();

    void observeRebuildStats(std::string_view armId, const TopologyManager::RebuildStats& stats);

    // Phase H-TDA: alternative reward modes. When rewardMode != Geometric,
    // the caller supplies the extrinsic persistence signal (e.g. H_0
    // persistence over a sample of cluster embeddings). The reward actually
    // recorded to the MAB is:
    //   Geometric:   geometric only (persistence ignored; equivalent to observeRebuildStats)
    //   Persistence: clamp01(persistence / persistenceScale)
    //   Hybrid:      0.5 × geometric + 0.5 × clamp01(persistence / persistenceScale)
    // `persistenceScale` normalizes the raw persistence score into [0, 1];
    // reasonable default is (corpus_sample_count − 1) which is the upper bound
    // of H_0 total persistence under the percentile-normalized formula.
    void observeRebuildStatsWithPersistence(std::string_view armId,
                                            const TopologyManager::RebuildStats& stats,
                                            double persistenceSignal, double persistenceScale);

    // Phase G shadow gate: feeds an EXTRINSIC reward signal (e.g. nDCG over
    // recent labeled queries) for the most recently observed arm so the tuner
    // can detect when its intrinsic preference diverges from retrieval quality.
    // Maintains a sliding window of (intrinsic, extrinsic) deltas; logs a
    // warning when intrinsic improves while extrinsic drops by >0.02 across
    // 3 consecutive observations. Observability only — bandit still optimizes
    // intrinsic reward.
    void observeShadowExtrinsic(double extrinsicSignal);

    [[nodiscard]] std::size_t divergenceWarningCount() const noexcept {
        std::lock_guard<std::mutex> lock(mutex_);
        return divergenceWarningCount_;
    }

    [[nodiscard]] bool canPullArm(std::chrono::steady_clock::time_point now,
                                  std::chrono::steady_clock::time_point lastPullTime,
                                  std::chrono::milliseconds lastDuration,
                                  std::size_t corpusDocCount,
                                  std::size_t lastPullDocCount) const noexcept;

    [[nodiscard]] std::optional<std::string> bestArmId() const;
    [[nodiscard]] const TopologyTunerConfig& config() const noexcept { return cfg_; }
    [[nodiscard]] std::vector<TopologyArm> arms() const;

    [[nodiscard]] nlohmann::json toJson() const;
    [[nodiscard]] Result<void> fromJson(const nlohmann::json& payload);

    // Load/save MAB state to disk. Fail-soft on missing/corrupt files.
    [[nodiscard]] Result<void> loadState(const std::filesystem::path& path);
    [[nodiscard]] Result<void> saveState(const std::filesystem::path& path) const;

private:
    mutable std::mutex mutex_;
    TopologyTunerConfig cfg_;
    std::vector<TopologyArm> arms_;
    yams::search::TunerMAB mab_;

    // Shadow-divergence tracking: ring buffer of (intrinsic, extrinsic) pairs
    // keyed by arm pull. Window size is hardcoded at 3 (3 consecutive
    // observations of intrinsic-up + extrinsic-down trips the warning).
    static constexpr std::size_t kDivergenceWindow = 3;
    static constexpr double kDivergenceExtrinsicDropThreshold = 0.02;
    std::vector<std::pair<double, double>> shadowHistory_;
    double lastIntrinsicReward_{0.0};
    bool haveLastIntrinsic_{false};
    std::size_t divergenceWarningCount_{0};
};

} // namespace yams::daemon
