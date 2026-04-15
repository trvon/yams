#include <yams/search/tuner_mab.h>

#include <spdlog/spdlog.h>

#include <algorithm>
#include <cmath>
#include <limits>

namespace yams::search {

namespace {

constexpr double kSqrt2 = 1.4142135623730951;

} // namespace

TunerMAB::TunerMAB() : explorationC_(kSqrt2) {}

void TunerMAB::setArms(std::vector<Arm> arms) {
    arms_ = std::move(arms);
    for (auto& a : arms_) {
        a.stats = ArmStats{};
    }
    totalPulls_ = 0;
    lastRewardSource_ = RewardSource::Unknown;
}

std::optional<std::size_t> TunerMAB::selectArm() {
    if (arms_.empty()) {
        return std::nullopt;
    }
    // Ensure each arm is sampled at least once before UCB1 dominates selection.
    for (std::size_t i = 0; i < arms_.size(); ++i) {
        if (arms_[i].stats.pulls == 0) {
            return i;
        }
    }

    double bestScore = -std::numeric_limits<double>::infinity();
    std::size_t bestIdx = 0;
    const double totalPulls = static_cast<double>(totalPulls_);
    for (std::size_t i = 0; i < arms_.size(); ++i) {
        const auto& a = arms_[i];
        const double n = static_cast<double>(a.stats.pulls);
        const double mean = a.stats.rewardSum / n;
        const double exploration =
            explorationC_ * std::sqrt(std::log(std::max(1.0, totalPulls)) / n);
        const double score = mean + exploration;
        // Deterministic tie-break: prefer the arm with the lexicographically
        // smaller id so replayed streams yield identical outcomes.
        if (score > bestScore || (score == bestScore && a.id < arms_[bestIdx].id)) {
            bestScore = score;
            bestIdx = i;
        }
    }
    return bestIdx;
}

void TunerMAB::recordReward(std::size_t armIndex, double reward, RewardSource source) {
    if (armIndex >= arms_.size()) {
        return;
    }
    const double clamped = std::clamp(reward, 0.0, 1.0);
    auto& a = arms_[armIndex];
    a.stats.pulls += 1;
    a.stats.rewardSum += clamped;
    totalPulls_ += 1;
    lastRewardSource_ = source;
}

std::optional<std::string> TunerMAB::bestArmId() const {
    if (arms_.empty()) {
        return std::nullopt;
    }
    std::optional<std::size_t> bestIdx;
    double bestMean = -std::numeric_limits<double>::infinity();
    for (std::size_t i = 0; i < arms_.size(); ++i) {
        const auto& a = arms_[i];
        if (a.stats.pulls == 0) {
            continue;
        }
        const double mean = a.stats.rewardSum / static_cast<double>(a.stats.pulls);
        if (mean > bestMean || (bestIdx && mean == bestMean && a.id < arms_[*bestIdx].id)) {
            bestMean = mean;
            bestIdx = i;
        }
    }
    if (!bestIdx) {
        return std::nullopt;
    }
    return arms_[*bestIdx].id;
}

std::string_view TunerMAB::rewardSourceToString(RewardSource src) noexcept {
    switch (src) {
        case RewardSource::Labels:
            return "labels";
        case RewardSource::Proxy:
            return "proxy";
        case RewardSource::Unknown:
            return "unknown";
    }
    return "unknown";
}

nlohmann::json TunerMAB::toJson() const {
    nlohmann::json j;
    j["schema_version"] = kSchemaVersion;
    j["exploration_c"] = explorationC_;
    j["total_pulls"] = totalPulls_;
    j["last_reward_source"] = std::string{rewardSourceToString(lastRewardSource_)};
    nlohmann::json armsJson = nlohmann::json::array();
    for (const auto& a : arms_) {
        armsJson.push_back({
            {"id", a.id},
            {"value", a.value},
            {"pulls", a.stats.pulls},
            {"reward_sum", a.stats.rewardSum},
        });
    }
    j["arms"] = std::move(armsJson);
    return j;
}

Result<void> TunerMAB::fromJson(const nlohmann::json& payload) {
    try {
        if (!payload.is_object()) {
            return Error{ErrorCode::InvalidData, "mab payload not an object"};
        }
        const auto schema = payload.value("schema_version", 0u);
        if (schema != kSchemaVersion) {
            // Schema drift is non-fatal — callers treat this as "start fresh".
            return Error{ErrorCode::InvalidData, "mab schema version mismatch"};
        }
        const auto it = payload.find("arms");
        if (it == payload.end() || !it->is_array()) {
            return Error{ErrorCode::InvalidData, "mab payload missing arms"};
        }

        std::vector<Arm> restored;
        restored.reserve(it->size());
        for (const auto& entry : *it) {
            if (!entry.is_object() || !entry.contains("id")) {
                return Error{ErrorCode::InvalidData, "mab arm entry malformed"};
            }
            Arm a;
            a.id = entry.value("id", std::string{});
            a.value = entry.value("value", 0.0);
            a.stats.pulls = entry.value("pulls", 0ull);
            a.stats.rewardSum = entry.value("reward_sum", 0.0);
            restored.emplace_back(std::move(a));
        }

        // Match restored arms against the currently-configured arm list by id.
        // Arms whose ids no longer exist are silently dropped; missing arms
        // stay at their zero-init defaults. This is the right behavior after
        // a config change that shrinks or reshapes the arm set.
        std::uint64_t restoredTotal = 0;
        for (auto& current : arms_) {
            auto match = std::find_if(restored.begin(), restored.end(),
                                      [&current](const Arm& a) { return a.id == current.id; });
            if (match != restored.end()) {
                current.stats = match->stats;
                restoredTotal += current.stats.pulls;
            } else {
                current.stats = ArmStats{};
            }
        }

        explorationC_ = payload.value("exploration_c", kSqrt2);
        totalPulls_ = restoredTotal;
        const auto srcStr = payload.value("last_reward_source", std::string{"unknown"});
        if (srcStr == "labels") {
            lastRewardSource_ = RewardSource::Labels;
        } else if (srcStr == "proxy") {
            lastRewardSource_ = RewardSource::Proxy;
        } else {
            lastRewardSource_ = RewardSource::Unknown;
        }
        return Result<void>{};
    } catch (const std::exception& e) {
        spdlog::warn("[search] TunerMAB::fromJson failed ({}); discarding stored bandit state",
                     e.what());
        for (auto& a : arms_) {
            a.stats = ArmStats{};
        }
        totalPulls_ = 0;
        lastRewardSource_ = RewardSource::Unknown;
        return Error{ErrorCode::InvalidData, std::string{"mab payload parse failure: "} + e.what()};
    }
}

} // namespace yams::search
