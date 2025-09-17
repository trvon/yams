#pragma once

#include <atomic>
#include <memory>
#include <yams/daemon/components/DaemonMetrics.h>

namespace yams::daemon {

// Zero-copy publication of the latest MetricsSnapshot for multi-reader consumers.
class MetricsSnapshotRegistry {
public:
    static MetricsSnapshotRegistry& instance() {
        static MetricsSnapshotRegistry r;
        return r;
    }
    void set(std::shared_ptr<const MetricsSnapshot> s) {
        std::atomic_store_explicit(&snap_, std::move(s), std::memory_order_release);
    }
    std::shared_ptr<const MetricsSnapshot> get() const {
        return std::atomic_load_explicit(&snap_, std::memory_order_acquire);
    }

private:
    MetricsSnapshotRegistry() = default;
    std::shared_ptr<const MetricsSnapshot> snap_{nullptr};
};

} // namespace yams::daemon
