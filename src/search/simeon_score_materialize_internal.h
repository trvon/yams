// Turning a dense corpus-order score buffer into the caller's candidate order.
//
// Every Simeon scoring entry point (routed, strategy-routed, bandit-routed) ends the same way:
// it has one float per corpus document in the backend's dense index order and must hand back
// one float per requested candidate id. Non-finite scores (a NaN from a z-blend, an infinite
// from an empty posting) are replaced by the BM25 baseline when the caller computed one, and
// by 0 otherwise. This header is the single definition of that policy.
#pragma once

#include <cmath>
#include <cstdint>
#include <span>
#include <unordered_map>
#include <vector>

namespace yams::search::simeon_internal {

/// True when `candidates` is the backend's own dense corpus order: the same buffer, the same
/// length. QuerySession asks for that order so the score buffer can be retained as-is.
[[nodiscard]] inline bool isDenseCorpusOrder(std::span<const std::int64_t> candidates,
                                             std::span<const std::int64_t> corpusOrder) noexcept {
    return candidates.data() == corpusOrder.data() && candidates.size() == corpusOrder.size();
}

/// Score for dense index `di`: the blended value when finite, else the baseline when finite,
/// else 0.
[[nodiscard]] inline float finiteScoreAt(std::span<const float> full,
                                         std::span<const float> lexical, std::size_t di) noexcept {
    if (di < full.size() && std::isfinite(full[di]))
        return full[di];
    if (di < lexical.size() && std::isfinite(lexical[di]))
        return lexical[di];
    return 0.0f;
}

/// Replace every non-finite entry of `scores` in place using the policy above.
inline void replaceNonFinite(std::span<float> scores, std::span<const float> lexical) noexcept {
    for (std::size_t di = 0; di < scores.size(); ++di) {
        if (!std::isfinite(scores[di]))
            scores[di] = finiteScoreAt({}, lexical, di);
    }
}

/// One score per candidate id, in candidate order; unknown ids score 0.
[[nodiscard]] inline std::vector<float>
gatherCandidateScores(std::span<const float> full, std::span<const float> lexical,
                      const std::unordered_map<std::int64_t, std::uint32_t>& docIndex,
                      std::span<const std::int64_t> candidates) {
    std::vector<float> out;
    out.reserve(candidates.size());
    for (auto id : candidates) {
        auto it = docIndex.find(id);
        out.push_back(it == docIndex.end() ? 0.0f : finiteScoreAt(full, lexical, it->second));
    }
    return out;
}

/// Materialize scores for `candidates`. Takes ownership of `full` so the dense fast path can
/// return the buffer without copying.
[[nodiscard]] inline std::vector<float>
materializeScores(std::vector<float> full, std::span<const float> lexical,
                  const std::unordered_map<std::int64_t, std::uint32_t>& docIndex,
                  std::span<const std::int64_t> corpusOrder,
                  std::span<const std::int64_t> candidates) {
    if (isDenseCorpusOrder(candidates, corpusOrder)) {
        replaceNonFinite(full, lexical);
        return full;
    }
    return gatherCandidateScores(full, lexical, docIndex, candidates);
}

/// Same as above for a borrowed buffer (thread-local scratch): the dense fast path copies.
[[nodiscard]] inline std::vector<float>
materializeScores(std::span<const float> full, std::span<const float> lexical,
                  const std::unordered_map<std::int64_t, std::uint32_t>& docIndex,
                  std::span<const std::int64_t> corpusOrder,
                  std::span<const std::int64_t> candidates) {
    if (isDenseCorpusOrder(candidates, corpusOrder)) {
        std::vector<float> owned(full.begin(), full.end());
        replaceNonFinite(owned, lexical);
        return owned;
    }
    return gatherCandidateScores(full, lexical, docIndex, candidates);
}

} // namespace yams::search::simeon_internal
