#pragma once

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>

namespace yams::search {

// Lightweight “hotzone” manager: tracks recent interest for docs and returns a boost factor.
// Phase A (MVP): in-memory with time-based decay; persistence is a later enhancement.
struct HotzoneConfig {
    // Exponential decay half-life (hours). Larger = slower decay.
    double half_life_hours{24.0};
    // Maximum multiplicative boost returned by getBoost (1.0 = no boost, 2.0 = up to 2x score).
    double max_boost_factor{2.0};
    // Whether to persist to a file (not implemented in MVP).
    bool enable_persistence{false};
    std::string data_file; // path to persistence DB/file when enabled
};

class HotzoneManager {
public:
    explicit HotzoneManager(HotzoneConfig cfg = {}) : cfg_(std::move(cfg)) {}

    // Record an interaction with a document id/hash/path.
    // weight is a unitless strength (e.g., 1=view, 3=click), default 1.0.
    void record(const std::string& key, double weight = 1.0);

    // Compute a multiplicative boost in [1.0, max_boost_factor]. 1.0 means no boost.
    double
    getBoost(const std::string& key,
             std::chrono::system_clock::time_point now = std::chrono::system_clock::now()) const;

    // Manually trigger decay sweep (optional; getBoost applies decay on the fly as well).
    void decaySweep(std::chrono::system_clock::time_point now = std::chrono::system_clock::now());

    const HotzoneConfig& config() const { return cfg_; }
    void setConfig(const HotzoneConfig& c) { cfg_ = c; }

    bool save(const std::filesystem::path& path) const;
    bool load(const std::filesystem::path& path);

    size_t size() const {
        std::lock_guard<std::mutex> lk(mu_);
        return map_.size();
    }

private:
    struct Node {
        double score{0.0};
        std::chrono::system_clock::time_point updated{};
    };

    static double decayFactor(double hours, double half_life_hours);

    mutable std::mutex mu_;
    HotzoneConfig cfg_{};
    std::unordered_map<std::string, Node> map_;
};

} // namespace yams::search
