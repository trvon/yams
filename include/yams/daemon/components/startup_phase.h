// Copyright (c) 2026 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <string_view>

namespace yams::daemon {

/// Coarse startup progress for diagnostics. Deliberately separate from
/// ServiceManagerState: the FSM stays at VectorsReady while a corpus-sized recovery step runs,
/// so operators need to see *which* step is blocking without querying memory-sync (whose mutex
/// is held for the whole sweep).
enum class StartupPhase : std::uint8_t {
    Idle = 0,
    VectorsInitialized = 1,
    EmbeddingReconcile = 2,
    MemorySyncApply = 3,
    DirectP2p = 4,
    SearchBuild = 5,
    AsyncInitComplete = 6,
};

constexpr std::array<std::string_view, 7> kStartupPhaseNames{"idle",
                                                             "vectors_initialized",
                                                             "embedding_reconcile",
                                                             "memory_sync_apply",
                                                             "direct_p2p_recovery",
                                                             "search_build",
                                                             "async_init_complete"};

constexpr std::string_view startupPhaseName(StartupPhase phase) noexcept {
    const auto index = static_cast<std::size_t>(phase);
    return index < kStartupPhaseNames.size() ? kStartupPhaseNames[index] : "unknown";
}

/// Atomic-only phase record. Readers must never take a mutex here: startup recovery can hold the
/// memory-sync loop mutex for minutes, and a status or watchdog path that blocks on it would make
/// the stall invisible again.
class StartupPhaseTracker {
public:
    using Clock = std::chrono::steady_clock;

    void begin(StartupPhase phase) noexcept {
        phaseSinceMs_.store(nowMs(), std::memory_order_release);
        phase_.store(phase, std::memory_order_release);
    }

    [[nodiscard]] StartupPhase phase() const noexcept {
        return phase_.load(std::memory_order_acquire);
    }

    /// Elapsed time in the current phase; zero while idle.
    [[nodiscard]] std::int64_t elapsedMs() const noexcept {
        if (phase() == StartupPhase::Idle) {
            return 0;
        }
        const auto since = phaseSinceMs_.load(std::memory_order_acquire);
        return since > 0 ? nowMs() - since : 0;
    }

private:
    static std::int64_t nowMs() noexcept {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                   Clock::now().time_since_epoch())
            .count();
    }

    std::atomic<StartupPhase> phase_{StartupPhase::Idle};
    std::atomic<std::int64_t> phaseSinceMs_{0};
};

} // namespace yams::daemon
