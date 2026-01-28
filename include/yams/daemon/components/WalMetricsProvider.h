#pragma once

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

    void setManager(std::shared_ptr<yams::wal::WALManager> wal) { wal_ = std::move(wal); }

    Stats getStats() const noexcept {
        // Best-effort: return zeros unless WAL stats are explicitly available.
        // Avoid hard link dependency on yams_wal in daemon binary linking.
        Stats s;
        auto wal = wal_.lock();
        if (wal) {
            auto walStats = wal->getStats();
            s.activeTransactions = walStats.activeTransactions;
            s.pendingEntries = walStats.pendingEntriesCount;
            s.totalEntries = walStats.totalEntries;
            s.totalBytes = walStats.totalBytes;
            s.logFileCount = walStats.logFileCount;
        }
        return s;
    }

private:
    std::weak_ptr<yams::wal::WALManager> wal_;
};

} // namespace yams::daemon
