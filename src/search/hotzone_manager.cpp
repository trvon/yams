#include <yams/search/hotzone_manager.h>

#include <algorithm>
#include <cmath>
#include <filesystem>
#include <fstream>

#include <nlohmann/json.hpp>

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

bool HotzoneManager::save(const std::filesystem::path& path) const {
    std::lock_guard<std::mutex> lk(mu_);

    if (map_.empty()) {
        return true;
    }

    nlohmann::json j;
    j["version"] = 1;
    j["half_life_hours"] = cfg_.half_life_hours;
    j["max_boost_factor"] = cfg_.max_boost_factor;

    nlohmann::json entries = nlohmann::json::array();
    for (const auto& [key, node] : map_) {
        auto epoch = std::chrono::duration_cast<std::chrono::milliseconds>(
                         node.updated.time_since_epoch())
                         .count();
        entries.push_back({{"key", key}, {"score", node.score}, {"updated_ms", epoch}});
    }
    j["entries"] = entries;

    auto tempPath = path;
    tempPath += ".tmp";

    std::ofstream ofs(tempPath);
    if (!ofs) {
        return false;
    }

    ofs << j.dump(2);
    ofs.close();

    if (!ofs) {
        return false;
    }

    std::error_code ec;
    std::filesystem::rename(tempPath, path, ec);
    return !ec;
}

bool HotzoneManager::load(const std::filesystem::path& path) {
    if (!std::filesystem::exists(path)) {
        return false;
    }

    std::ifstream ifs(path);
    if (!ifs) {
        return false;
    }

    nlohmann::json j;
    try {
        ifs >> j;
    } catch (...) {
        return false;
    }

    if (!j.contains("version") || !j.contains("entries")) {
        return false;
    }

    std::lock_guard<std::mutex> lk(mu_);
    map_.clear();

    for (const auto& entry : j["entries"]) {
        if (!entry.contains("key") || !entry.contains("score") || !entry.contains("updated_ms")) {
            continue;
        }

        Node node;
        node.score = entry["score"].get<double>();
        auto epoch_ms = entry["updated_ms"].get<int64_t>();
        node.updated = std::chrono::system_clock::time_point(std::chrono::milliseconds(epoch_ms));

        map_[entry["key"].get<std::string>()] = node;
    }

    return true;
}

} // namespace yams::search
