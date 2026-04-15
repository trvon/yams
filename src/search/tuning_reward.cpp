#include <yams/search/tuning_reward.h>

#include <algorithm>
#include <cmath>

namespace yams::search {

namespace {

[[nodiscard]] inline double cleanOrZero(double v) noexcept {
    return std::isfinite(v) ? v : 0.0;
}

[[nodiscard]] inline double clamp01(double v) noexcept {
    if (!std::isfinite(v) || v < 0.0) {
        return 0.0;
    }
    if (v > 1.0) {
        return 1.0;
    }
    return v;
}

} // namespace

CombinedReward combineReward(std::optional<double> labelReward,
                             const ProxyRewardInputs& proxy) noexcept {
    const double kgU = cleanOrZero(proxy.kgUtility);
    const double freshness = cleanOrZero(proxy.freshnessBoost);
    const double agreement = cleanOrZero(proxy.agreementBoost);
    const double overBudget = std::max(0.0, cleanOrZero(proxy.latencyOverBudget));

    const double proxyBlended = (kgU * 0.6) + (freshness * 0.2) + (agreement * 0.2);
    const bool proxyNonTrivial = proxyBlended > 1e-6;

    if (labelReward.has_value() && std::isfinite(*labelReward)) {
        const double labelPenalized = *labelReward - overBudget;
        CombinedReward out;
        out.value = clamp01(labelPenalized);
        out.source = proxyNonTrivial ? RewardSource::Mixed : RewardSource::Labels;
        return out;
    }

    CombinedReward out;
    out.value = clamp01(proxyBlended - overBudget);
    out.source = RewardSource::Proxy;
    return out;
}

} // namespace yams::search
