#include <yams/search/hotzone_manager.h>

#include <algorithm>
#include <cmath>

namespace yams::search {

namespace {
inline double clamp(double v, double lo, double hi) {
    return std::max(lo, std::min(hi, v));
}
} // namespace

double HotzoneManager::decayFactor(double hours, double half_life_hours) {
    if (half_life_hours <= 0.0)
        return 0.0; // immediate decay
    // Exponential decay: factor = 0.5^(hours / half_life)
    return std::pow(0.5, hours / half_life_hours);
}

void HotzoneManager::record(const std::string& key, double weight) {
    if (key.empty() || weight <= 0.0)
        return;
    const auto now = std::chrono::system_clock::now();
    std::lock_guard<std::mutex> lk(mu_);
    auto& node = map_[key];
    if (node.updated.time_since_epoch().count() == 0) {
        node.updated = now;
        node.score = weight;
        return;
    }
    const auto elapsed = std::chrono::duration_cast<std::chrono::minutes>(now - node.updated);
    const double hours = elapsed.count() / 60.0;
    const double f = decayFactor(hours, cfg_.half_life_hours);
    node.score = node.score * f + weight;
    node.updated = now;
}

double HotzoneManager::getBoost(const std::string& key,
                                std::chrono::system_clock::time_point now) const {
    if (key.empty())
        return 1.0; // no boost
    std::lock_guard<std::mutex> lk(mu_);
    auto it = map_.find(key);
    if (it == map_.end())
        return 1.0;
    const auto elapsed = std::chrono::duration_cast<std::chrono::minutes>(now - it->second.updated);
    const double hours = elapsed.count() / 60.0;
    const double f = decayFactor(hours, cfg_.half_life_hours);
    const double decayed = it->second.score * f;
    // Normalize with a soft saturation: boost = 1 + (max_boost-1) * (1 - e^{-decayed}) / (1 -
    // e^{-1}) So that score=1 maps near max influence without hard step.
    const double maxB = std::max(1.0, cfg_.max_boost_factor);
    const double sat = (1.0 - std::exp(-decayed)) / (1.0 - std::exp(-1.0));
    const double boost = 1.0 + (maxB - 1.0) * clamp(sat, 0.0, 1.0);
    return clamp(boost, 1.0, maxB);
}

void HotzoneManager::decaySweep(std::chrono::system_clock::time_point now) {
    std::lock_guard<std::mutex> lk(mu_);
    for (auto it = map_.begin(); it != map_.end();) {
        const auto elapsed =
            std::chrono::duration_cast<std::chrono::minutes>(now - it->second.updated);
        const double hours = elapsed.count() / 60.0;
        const double f = decayFactor(hours, cfg_.half_life_hours);
        it->second.score *= f;
        it->second.updated = now;
        if (it->second.score < 1e-6) {
            it = map_.erase(it);
        } else {
            ++it;
        }
    }
}

} // namespace yams::search
