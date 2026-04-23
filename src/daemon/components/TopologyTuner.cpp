// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

#include <yams/daemon/components/TopologyTuner.h>

#include <algorithm>
#include <cmath>
#include <sstream>

namespace yams::daemon {

namespace {

constexpr double kGiniTarget = 0.4;
constexpr double kIntraEdgeNormalizer = 1.0;

std::size_t scaleClusterSize(std::size_t corpusDocCount, double factor) {
    if (corpusDocCount == 0) {
        return 2;
    }
    auto v = static_cast<double>(corpusDocCount);
    auto computed = static_cast<std::size_t>(std::max(2.0, std::round(factor * v)));
    return std::min(computed, std::max<std::size_t>(2, corpusDocCount / 2));
}

std::size_t logBaseTwoCeil(std::size_t corpusDocCount) {
    if (corpusDocCount <= 2) {
        return 2;
    }
    return static_cast<std::size_t>(std::ceil(std::log2(static_cast<double>(corpusDocCount))));
}

std::size_t sqrtSize(std::size_t corpusDocCount) {
    if (corpusDocCount == 0) {
        return 2;
    }
    return std::max<std::size_t>(
        2, static_cast<std::size_t>(std::round(std::sqrt(static_cast<double>(corpusDocCount)))));
}

std::string makeHdbscanArmId(std::size_t minc, std::size_t minp, std::size_t hops) {
    std::ostringstream os;
    os << "hdbscan_minc" << minc << "_minp" << minp << "_hops" << hops;
    return os.str();
}

double clamp01(double v) noexcept {
    if (v < 0.0)
        return 0.0;
    if (v > 1.0)
        return 1.0;
    return v;
}

} // namespace

double computeIntrinsicReward(const TopologyManager::RebuildStats& stats,
                              const IntrinsicRewardWeights& weights) noexcept {
    // Skipped or failed rebuilds yield zero reward — the bandit shouldn't
    // be encouraged to pull arms that produce no usable topology.
    if (stats.skipped || stats.documentsProcessed == 0) {
        return 0.0;
    }

    const double singletonPenalty = weights.alphaSingleton * clamp01(stats.singletonRatio);
    const double giantPenalty = weights.betaGiantCluster * clamp01(stats.giantClusterRatio);
    const double giniDeviation = std::abs(stats.clusterSizeGini - kGiniTarget);
    const double giniPenalty = weights.gammaGiniDeviation * clamp01(giniDeviation);
    const double intraEdgeBonus =
        weights.deltaIntraEdge * clamp01(stats.avgIntraEdgeWeight / kIntraEdgeNormalizer);

    return clamp01(1.0 - singletonPenalty - giantPenalty - giniPenalty + intraEdgeBonus);
}

std::vector<TopologyArm> defaultArmGrid(std::size_t corpusDocCount) {
    std::vector<TopologyArm> arms;

    // ConnectedComponents has no per-engine knobs today; one arm only.
    arms.push_back(TopologyArm{/*id=*/"connected", /*engine=*/"connected", 0, 0, 0});

    const std::size_t logN = logBaseTwoCeil(corpusDocCount);
    const std::size_t sqrtN = sqrtSize(corpusDocCount);

    const std::size_t clusterSizes[] = {
        std::max<std::size_t>(2, logN), std::max<std::size_t>(2, logN + 2),
        std::max<std::size_t>(2, sqrtN),
        std::max<std::size_t>(2, scaleClusterSize(corpusDocCount, 0.05))};

    for (auto minc : clusterSizes) {
        const std::size_t minPointsCandidates[] = {2u, minc, std::max<std::size_t>(2, minc * 2)};
        for (auto minp : minPointsCandidates) {
            for (std::size_t hops = 0; hops <= 2; ++hops) {
                arms.push_back(TopologyArm{/*id=*/makeHdbscanArmId(minc, minp, hops),
                                           /*engine=*/"hdbscan", minc, minp, hops});
            }
        }
    }

    // Deduplicate arms with identical ids (small cluster sizes can collide
    // when log2(n) == sqrt(n) on small corpora). Keep first occurrence.
    std::vector<TopologyArm> deduped;
    deduped.reserve(arms.size());
    for (const auto& arm : arms) {
        bool exists = std::any_of(deduped.begin(), deduped.end(),
                                  [&](const TopologyArm& a) { return a.id == arm.id; });
        if (!exists) {
            deduped.push_back(arm);
        }
    }
    return deduped;
}

TopologyTuner::TopologyTuner(TopologyTunerConfig cfg) : cfg_(cfg) {}

void TopologyTuner::setArms(std::vector<TopologyArm> arms) {
    std::lock_guard<std::mutex> lock(mutex_);
    arms_ = std::move(arms);
    std::vector<yams::search::TunerMAB::Arm> mabArms;
    mabArms.reserve(arms_.size());
    for (const auto& arm : arms_) {
        mabArms.push_back(yams::search::TunerMAB::Arm{
            arm.id, static_cast<double>(arm.hdbscanMinClusterSize), {}});
    }
    mab_.setArms(std::move(mabArms));
}

std::optional<TopologyArm> TopologyTuner::selectArm() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!cfg_.enabled || arms_.empty()) {
        return std::nullopt;
    }
    auto idx = mab_.selectArm();
    if (!idx || *idx >= arms_.size()) {
        return std::nullopt;
    }
    return arms_[*idx];
}

void TopologyTuner::observeRebuildStats(std::string_view armId,
                                        const TopologyManager::RebuildStats& stats) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (arms_.empty()) {
        return;
    }
    auto it = std::find_if(arms_.begin(), arms_.end(),
                           [&](const TopologyArm& arm) { return arm.id == armId; });
    if (it == arms_.end()) {
        return;
    }
    const auto idx = static_cast<std::size_t>(std::distance(arms_.begin(), it));
    const double reward = computeIntrinsicReward(stats, cfg_.weights);
    mab_.recordReward(idx, reward, yams::search::TunerMAB::RewardSource::Proxy);
}

bool TopologyTuner::canPullArm(std::chrono::steady_clock::time_point now,
                               std::chrono::steady_clock::time_point lastPullTime,
                               std::chrono::milliseconds lastDuration, std::size_t corpusDocCount,
                               std::size_t lastPullDocCount) const noexcept {
    if (!cfg_.enabled) {
        return false;
    }
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - lastPullTime);
    const auto cooldownMs = std::chrono::duration_cast<std::chrono::milliseconds>(cfg_.cooldown);
    const auto scaledCooldown = std::max(cooldownMs, lastDuration * 10);
    if (elapsed < scaledCooldown) {
        return false;
    }
    if (corpusDocCount < lastPullDocCount + cfg_.docCountDelta) {
        return false;
    }
    return true;
}

std::optional<std::string> TopologyTuner::bestArmId() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return mab_.bestArmId();
}

std::vector<TopologyArm> TopologyTuner::arms() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return arms_;
}

nlohmann::json TopologyTuner::toJson() const {
    std::lock_guard<std::mutex> lock(mutex_);
    nlohmann::json j;
    j["mab"] = mab_.toJson();
    nlohmann::json armsJson = nlohmann::json::array();
    for (const auto& arm : arms_) {
        armsJson.push_back(nlohmann::json{
            {"id", arm.id},
            {"engine", arm.engine},
            {"hdbscan_min_cluster_size", arm.hdbscanMinClusterSize},
            {"hdbscan_min_points", arm.hdbscanMinPoints},
            {"feature_smoothing_hops", arm.featureSmoothingHops},
        });
    }
    j["arms"] = std::move(armsJson);
    return j;
}

Result<void> TopologyTuner::fromJson(const nlohmann::json& payload) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!payload.is_object() || !payload.contains("mab")) {
        return Error{ErrorCode::InvalidArgument, "TopologyTuner state missing 'mab' field"};
    }
    return mab_.fromJson(payload.at("mab"));
}

} // namespace yams::daemon
