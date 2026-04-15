#pragma once

#include <yams/search/search_tuner.h>

#include <array>
#include <cstddef>
#include <string>

namespace yams::search {

/// Fixed dimensionality of the normalized feature vector returned by
/// `featurize()`. The order is stable and documented in `featurize.cpp`; any
/// contextual-bandit state persisted across process restarts depends on it.
inline constexpr std::size_t kTuningFeatureDim = 11;

/**
 * @brief Normalize a `TuningContext` to a fixed-length feature vector in
 *        [-1, 1]. Used by the R4 contextual bandit's per-arm linear update.
 *
 * Feature order (index → source field):
 *   0  docCountLog10               scaled by 1/6  (corpus <1e6 docs stays in [0,1])
 *   1  codeRatio                   centered around 0.5
 *   2  proseRatio                  centered around 0.5
 *   3  embeddingCoverage           centered around 0.5
 *   4  nativeSymbolDensity         clamped/scaled (entities per doc, typical 0..20)
 *   5  pathRelativeDepthAvg        scaled by 1/10 (typical 0..10)
 *   6  binaryRatio                 centered around 0.5
 *   7  kgEdgeDensity               clamped/scaled (typical 0..20)
 *   8  queryTokenCountLog2         scaled by 1/6  (queries up to 64 tokens saturate)
 *   9  queryHasVectorPath          {-1, +1} from {0, 1}
 *  10  queryHasKgAnchors           {-1, +1} from {0, 1}
 *
 * Every feature is clamped to [-1, 1]. A zero-initialized `TuningContext`
 * yields the zero vector (modulo the {-1,+1}-remap of booleans, which map
 * to -1). Policies that want a true "no information" signal should detect
 * a cold-start context upstream (e.g., by checking `corpusEpoch == 0`).
 */
[[nodiscard]] std::array<double, kTuningFeatureDim> featurize(const TuningContext& ctx) noexcept;

/**
 * @brief Stable discretized bucket key for a `TuningContext`. Used as the
 *        handoff key between the rules policy and the contextual policy
 *        (R5): per-bucket pull counts gate the handoff.
 *
 * Discretization is coarse on purpose so the per-bucket state stays bounded
 * and the contextual policy can accumulate evidence before committing:
 *
 *   - `codeRatio`  : quartile (0..3)
 *   - `docCountLog10` decade-floor (0..6+; capped at 6)
 *   - `embeddingCoverage` tercile (0..2)
 *   - `binaryRatio` "media" flag (>= 0.5 → 1 else 0)
 *   - `corpusEpoch`   : embedded verbatim (a new epoch is a new bucket)
 *   - `topologyEpoch` : embedded verbatim (new topology is a new bucket)
 *
 * The returned string is stable across process restarts and safe to use as
 * a map key or JSON property. Format:
 *   "c{q}/d{decade}/e{t}/m{flag}/ce{epoch}/te{epoch}"
 */
[[nodiscard]] std::string bucketize(const TuningContext& ctx);

} // namespace yams::search
