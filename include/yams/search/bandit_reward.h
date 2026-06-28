#pragma once

#include <algorithm>
#include <cstddef>
#include <string_view>

namespace yams::search {

// Training-free proxy reward for the Simeon retrieval bandit (TunerMAB).
//
// Signal: result density per millisecond — more results returned, faster, is
// better — scaled x10 and clamped to [0, 0.95] (headroom kept below 1.0 so the
// proxy never saturates the UCB1 mean). At query time there are no relevance
// judgments, so this is the honest proxy available.
//
// `armName` is accepted for future per-recipe calibration but is deliberately
// NOT used: a prior implementation added a flat +0.05 bonus to any arm whose
// name contained "rm3", which biased selection toward the RM3 arms independent
// of outcome. The reward is now arm-name independent.
inline double computeSimeonBanditReward(std::size_t resultCount, double elapsedMs,
                                        std::string_view armName) noexcept {
    (void)armName;
    const double rate =
        static_cast<double>(resultCount) / std::max(1.0, elapsedMs) * 10.0;
    return std::clamp(rate, 0.0, 0.95);
}

} // namespace yams::search
