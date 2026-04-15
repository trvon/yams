#pragma once

#include <yams/core/types.h>

#include <nlohmann/json.hpp>

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace yams::search {

// P6: Minimal UCB1 bandit over a discrete arm set.
//
// This is the first step toward the hierarchical MAB described in the plan.
// The v1 implementation exposes a single arm-group (e.g. `rrfK`) with a fixed
// discrete candidate list, tracks rewards per arm, and selects arms via UCB1.
//
// Design constraints (from the tuning plan):
//   * Never authoritative until the rule-based EWMA tuner reports
//     `SearchTuner::hasConverged()`.
//   * Reward source is selectable: user-labeled (primary) or proxy (fallback).
//   * Persistent state survives restarts via JSON; schema mismatch is
//     non-fatal (state is discarded, MAB resets, EWMA continues unaffected).
//
// This class is intentionally small and standalone; `SearchTuner` composes it
// optionally rather than inheriting from it.
class TunerMAB {
public:
    enum class RewardSource : std::uint8_t { Unknown, Labels, Proxy };

    struct ArmStats {
        std::uint64_t pulls = 0;
        double rewardSum = 0.0; // Clamped reward contributions (each in [0,1]).
    };

    struct Arm {
        std::string id;   // Canonical key (e.g. "rrfK=12").
        double value = 0; // The numeric value the arm represents.
        ArmStats stats;
    };

    // `explorationC` is the UCB1 exploration constant (sqrt(2) is standard).
    // `schemaVersion` is written to persistent state and checked on load — a
    // mismatch means the stored bandit is for a different arm layout and must
    // be discarded.
    static constexpr std::uint32_t kSchemaVersion = 1;

    TunerMAB();

    // Replace the arm set. Zero pulls and reward sums for every arm. Intended
    // to be called once at construction; subsequent calls reset the bandit.
    void setArms(std::vector<Arm> arms);

    // Pick the next arm to try. Uses UCB1 with tie-breaking by id. Returns
    // the arm's index in the configured arm list, or std::nullopt if no arms
    // are configured.
    std::optional<std::size_t> selectArm();

    // Record a reward for the arm at `armIndex`. Rewards are clamped to [0,1].
    // `source` is stored in telemetry so downstream consumers (e.g. debugStats)
    // can surface whether the decision was label-driven or proxy-driven.
    void recordReward(std::size_t armIndex, double reward, RewardSource source);

    [[nodiscard]] const std::vector<Arm>& arms() const noexcept { return arms_; }
    [[nodiscard]] std::uint64_t totalPulls() const noexcept { return totalPulls_; }
    [[nodiscard]] RewardSource lastRewardSource() const noexcept { return lastRewardSource_; }
    [[nodiscard]] double explorationC() const noexcept { return explorationC_; }

    // Arm id that is currently the best exploitation choice (highest mean reward).
    // Returns std::nullopt if no arm has been pulled yet.
    [[nodiscard]] std::optional<std::string> bestArmId() const;

    [[nodiscard]] nlohmann::json toJson() const;
    // Loads a bandit snapshot. If the JSON payload has a mismatched schema
    // version, or lacks the expected shape, the bandit is reset and the call
    // returns an error Result but the bandit remains usable (fail-soft).
    [[nodiscard]] Result<void> fromJson(const nlohmann::json& payload);

    // Convenience: string form of RewardSource for telemetry.
    [[nodiscard]] static std::string_view rewardSourceToString(RewardSource src) noexcept;

private:
    double explorationC_;
    std::vector<Arm> arms_;
    std::uint64_t totalPulls_ = 0;
    RewardSource lastRewardSource_ = RewardSource::Unknown;
};

} // namespace yams::search
