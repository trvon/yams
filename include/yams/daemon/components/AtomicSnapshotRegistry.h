#pragma once

#include <atomic>
#include <memory>
#include <utility>

namespace yams::daemon {

// Zero-copy publication of immutable snapshots for read-mostly daemon hot paths.
// Uses C++11 atomic shared_ptr free functions for libc++ compatibility.
template <typename Snapshot> class AtomicSnapshotRegistry {
public:
    using SnapshotType = Snapshot;

    static AtomicSnapshotRegistry& instance() {
        static AtomicSnapshotRegistry registry;
        return registry;
    }

    void set(std::shared_ptr<const Snapshot> snap) {
        std::atomic_store_explicit(&snap_, std::move(snap), std::memory_order_release);
    }

    std::shared_ptr<const Snapshot> get() const {
        return std::atomic_load_explicit(&snap_, std::memory_order_acquire);
    }

private:
    AtomicSnapshotRegistry() = default;

    std::shared_ptr<const Snapshot> snap_{nullptr};
};

} // namespace yams::daemon
