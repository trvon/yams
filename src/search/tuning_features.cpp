#include <yams/search/tuning_features.h>

#include <algorithm>
#include <cmath>
#include <string>

namespace yams::search {

namespace {

[[nodiscard]] inline double clamp1(double v) noexcept {
    if (v < -1.0) {
        return -1.0;
    }
    if (v > 1.0) {
        return 1.0;
    }
    return v;
}

[[nodiscard]] inline double centeredHalf(double v) noexcept {
    // Map a [0,1] ratio to a [-1,+1] centered signal. Out-of-range inputs
    // (e.g. ratios > 1 from noisy stats) are clamped.
    const double clamped = std::clamp(v, 0.0, 1.0);
    return clamp1((clamped - 0.5) * 2.0);
}

[[nodiscard]] inline double boolPlusMinus(std::uint8_t flag) noexcept {
    return flag != 0 ? 1.0 : -1.0;
}

} // namespace

std::array<double, kTuningFeatureDim> featurize(const TuningContext& ctx) noexcept {
    std::array<double, kTuningFeatureDim> out{};
    // 0 — docCountLog10 / 6 (saturates at 1e6 docs).
    out[0] = clamp1(ctx.docCountLog10 / 6.0);
    // 1..3 — ratios centered on 0.5.
    out[1] = centeredHalf(ctx.codeRatio);
    out[2] = centeredHalf(ctx.proseRatio);
    out[3] = centeredHalf(ctx.embeddingCoverage);
    // 4 — nativeSymbolDensity: typical 0..20 entities/doc; scale /10 then clamp.
    out[4] = clamp1((ctx.nativeSymbolDensity / 10.0) - 1.0);
    // 5 — pathRelativeDepthAvg: typical 0..10; scale /10 then clamp.
    out[5] = clamp1(ctx.pathRelativeDepthAvg / 10.0);
    // 6 — binaryRatio centered on 0.5.
    out[6] = centeredHalf(ctx.binaryRatio);
    // 7 — kgEdgeDensity: scale /10 (same shape as symbol density in R2 approximation).
    out[7] = clamp1((ctx.kgEdgeDensity / 10.0) - 1.0);
    // 8 — queryTokenCountLog2 / 6 (saturates at 64-token queries).
    out[8] = clamp1(ctx.queryTokenCountLog2 / 6.0);
    // 9..10 — boolean flags as {-1,+1}.
    out[9] = boolPlusMinus(ctx.queryHasVectorPath);
    out[10] = boolPlusMinus(ctx.queryHasKgAnchors);
    return out;
}

namespace {

[[nodiscard]] inline unsigned quartile(double v) noexcept {
    // Quartile over [0,1]: {[0,0.25), [0.25,0.5), [0.5,0.75), [0.75,1]}.
    const double clamped = std::clamp(v, 0.0, 1.0);
    if (clamped < 0.25) {
        return 0U;
    }
    if (clamped < 0.5) {
        return 1U;
    }
    if (clamped < 0.75) {
        return 2U;
    }
    return 3U;
}

[[nodiscard]] inline unsigned tercile(double v) noexcept {
    const double clamped = std::clamp(v, 0.0, 1.0);
    if (clamped < (1.0 / 3.0)) {
        return 0U;
    }
    if (clamped < (2.0 / 3.0)) {
        return 1U;
    }
    return 2U;
}

[[nodiscard]] inline unsigned docDecade(double log10val) noexcept {
    if (!std::isfinite(log10val) || log10val <= 0.0) {
        return 0U;
    }
    const double floored = std::floor(log10val);
    if (floored >= 6.0) {
        return 6U;
    }
    return static_cast<unsigned>(floored);
}

} // namespace

std::string bucketize(const TuningContext& ctx) {
    std::string out;
    out.reserve(64);
    out.append("c").append(std::to_string(quartile(ctx.codeRatio)));
    out.append("/d").append(std::to_string(docDecade(ctx.docCountLog10)));
    out.append("/e").append(std::to_string(tercile(ctx.embeddingCoverage)));
    out.append("/m").append(ctx.binaryRatio >= 0.5 ? "1" : "0");
    out.append("/ce").append(std::to_string(ctx.corpusEpoch));
    out.append("/te").append(std::to_string(ctx.topologyEpoch));
    return out;
}

} // namespace yams::search
