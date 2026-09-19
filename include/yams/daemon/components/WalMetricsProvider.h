#pragma once

#include <spdlog/spdlog.h>
#include <atomic>
#include <memory>
#include <yams/wal/wal_manager.h>

namespace yams::daemon {

class WalMetricsProvider {
public:
    struct Stats {
        std::size_t activeTransactions{0};
        std::size_t pendingEntries{0};
        std::size_t totalEntries{0};
        std::size_t totalBytes{0};
        std::size_t logFileCount{0};
    };

    void setManager(std::shared_ptr<yams::wal::WALManager> wal) {
        std::atomic_store_explicit(&wal_, std::move(wal), std::memory_order_release);
    }

    // Best-effort metrics: must never propagate an exception, because WALManager::getStats()
    // performs filesystem queries (existence and directory iteration over walDirectory) that can
    // throw. This function is noexcept, so an escaping exception would terminate the daemon.
    Stats getStats() const noexcept {
        // Return zeros unless WAL stats are explicitly available.
        // Avoid hard link dependency on yams_wal in daemon binary linking.
        Stats s;
        auto wal = std::atomic_load_explicit(&wal_, std::memory_order_acquire);
        if (!wal) {
            return s;
        }
        try {
            auto walStats = wal->getStats();
            s.activeTransactions = walStats.activeTransactions;
            s.pendingEntries = walStats.pendingEntriesCount;
            s.totalEntries = walStats.totalEntries;
            s.totalBytes = walStats.totalBytes;
            s.logFileCount = walStats.logFileCount;
        } catch (const std::exception& e) {
            spdlog::debug("[WalMetricsProvider] WAL stats unavailable: {}", e.what());
        } catch (...) {
            spdlog::debug("[WalMetricsProvider] WAL stats unavailable: unknown exception");
        }
        return s;
    }

private:
    std::shared_ptr<yams::wal::WALManager> wal_;
};

} // namespace yams::daemon
