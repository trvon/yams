#include <yams/daemon/components/PoolManager.h>

#include <algorithm>
#include <chrono>
#if __has_include(<yams/profiling.h>)
#include <yams/profiling.h>
#else
#define YAMS_PLOT(name, val) ((void)0)
#endif

namespace yams::daemon {

static inline std::uint64_t now_ns() {
    using namespace std::chrono;
    return duration_cast<nanoseconds>(steady_clock::now().time_since_epoch()).count();
}

PoolManager& PoolManager::instance() {
    static PoolManager inst;
    return inst;
}

void PoolManager::configure(const std::string& component, const Config& cfg) {
    auto& e = entry_for(component);
    e.cfg = cfg;
    e.size = std::clamp(e.size, cfg.min_size, cfg.max_size);
}

std::uint32_t PoolManager::apply_delta(const Delta& d) {
    auto& e = entry_for(d.component);
    const auto now = now_ns();
    const auto cooldown = (d.cooldown_ms ? d.cooldown_ms : e.cfg.cooldown_ms) * 1'000'000ull;
    if (cooldown && (now - e.last_resize_ns) < cooldown) {
        e.s.throttled_on_cooldown++;
        YAMS_PLOT(("pool_" + d.component + "_throttled").c_str(),
                  static_cast<int64_t>(e.s.throttled_on_cooldown));
        return e.size;
    }
    int target = static_cast<int>(e.size) + d.change;
    target = std::max<int>(target, static_cast<int>(e.cfg.min_size));
    target = std::min<int>(target, static_cast<int>(e.cfg.max_size));
    if (target == static_cast<int>(e.size)) {
        e.s.rejected_on_cap++;
        YAMS_PLOT(("pool_" + d.component + "_rejected").c_str(),
                  static_cast<int64_t>(e.s.rejected_on_cap));
        return e.size;
    }
    e.size = static_cast<std::uint32_t>(target);
    e.last_resize_ns = now;
    e.s.resize_events++;
    YAMS_PLOT(("pool_" + d.component + "_size").c_str(), static_cast<int64_t>(e.size));
    YAMS_PLOT(("pool_" + d.component + "_resizes").c_str(),
              static_cast<int64_t>(e.s.resize_events));
    return e.size;
}

PoolManager::Stats PoolManager::stats(const std::string& component) const {
    if (auto* e = entry_for_const(component)) {
        auto s = e->s;
        s.current_size = e->size;
        return s;
    }
    return {};
}

std::size_t PoolManager::shrinkAll() {
    std::lock_guard<std::mutex> lock(mutex_);
    std::size_t shrunkCount = 0;
    const auto now = now_ns();

    for (auto& [name, entry] : pools_) {
        if (entry.size > entry.cfg.min_size) {
            entry.size = entry.cfg.min_size;
            entry.last_resize_ns = now;
            entry.s.resize_events++;
            shrunkCount++;
            YAMS_PLOT(("pool_" + name + "_size").c_str(), static_cast<int64_t>(entry.size));
        }
    }

    return shrunkCount;
}

PoolManager::Entry& PoolManager::entry_for(const std::string& component) {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& kv : pools_) {
        if (kv.first == component)
            return kv.second;
    }
    pools_.push_back({component, Entry{}});
    // initialize defaults
    auto& e = pools_.back().second;
    e.size = e.cfg.min_size;
    e.last_resize_ns = 0;
    return e;
}

const PoolManager::Entry* PoolManager::entry_for_const(const std::string& component) const {
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& kv : pools_) {
        if (kv.first == component)
            return &kv.second;
    }
    return nullptr;
}

} // namespace yams::daemon
