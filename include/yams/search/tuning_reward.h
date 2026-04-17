#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string_view>

namespace yams::search {

/**
 * @brief Source of the reward signal returned by `combineReward()`. Used for
 *        explainability (debugStats["mab_reward_source"]) and replay-harness
 *        diagnostics so a user can see which channel drove a given tuning
 *        observation.
 */
enum class RewardSource : std::uint8_t {
    Labels = 0, // User-provided labels (yams tune) dominated.
    Proxy = 1,  // No labels; proxy reward (KG utility + freshness + agreement) used.
    Mixed = 2,  // Labels present but downweighted; proxy contributed non-trivially.
};

// Reserved for R5 debugStats["mab_reward_source"] emission and the R6
// replay-harness diagnostics. Not yet called from production code —
// dead-code scans can skip this helper.
[[nodiscard]] constexpr std::string_view rewardSourceLabel(RewardSource s) noexcept {
    switch (s) {
        case RewardSource::Labels:
            return "labels";
        case RewardSource::Proxy:
            return "proxy";
        case RewardSource::Mixed:
            return "mixed";
    }
    return "unknown";
}

/**
 * @brief Per-query proxy reward components consumed by `combineReward()`.
 *        All fields are unit-less and conventionally in [0, 1]; the combiner
 *        clamps out-of-range inputs and tolerates `std::nan` (treated as 0).
 */
struct ProxyRewardInputs {
    double kgUtility = 0.0;         // KG/graph rerank contribution (0 when KG off).
    double freshnessBoost = 0.0;    // Recency bias over returned docs.
    double agreementBoost = 0.0;    // Cross-component agreement (multiple sources agreed).
    double latencyOverBudget = 0.0; // >0 when over budget; subtracted from reward.
};

/**
 * @brief Combined reward used by R5+ contextual policies.
 *
 * Priority: labels ≫ proxy ≫ latency-penalty-only.
 *
 *  - If `labelReward` is present and finite, the combined reward is
 *    `clamp01(labelReward - max(0, latencyOverBudget))`. Proxy is reported
 *    via `source == Mixed` when proxy is non-zero, otherwise `Labels`.
 *  - Otherwise, combined reward is a weighted sum of the proxy inputs:
 *        0.6*kgUtility + 0.2*freshnessBoost + 0.2*agreementBoost
 *        − max(0, latencyOverBudget)
 *    clamped to [0, 1]. `source == Proxy`.
 *
 * The combiner is pure and deterministic — easy to unit-test and to replay.
 */
struct CombinedReward {
    double value = 0.0;
    RewardSource source = RewardSource::Proxy;
};

[[nodiscard]] CombinedReward combineReward(std::optional<double> labelReward,
                                           const ProxyRewardInputs& proxy) noexcept;

} // namespace yams::search
