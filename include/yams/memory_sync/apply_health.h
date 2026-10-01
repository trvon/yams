// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors
#pragma once

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace yams::memory_sync {

/// Inbound apply stages of a memory-sync cycle, in prerequisite order.
enum class ApplyStage : std::uint8_t { Content = 0, Metadata = 1, Vector = 2, Topology = 3 };

inline constexpr std::size_t kApplyStageCount = 4;

constexpr std::string_view applyStageName(ApplyStage stage) noexcept {
    switch (stage) {
        case ApplyStage::Content:
            return "content";
        case ApplyStage::Metadata:
            return "metadata";
        case ApplyStage::Vector:
            return "vector";
        case ApplyStage::Topology:
            return "topology";
    }
    return "unknown";
}

/// Point-in-time view of the apply path, for status reporting.
struct ApplyHealthSnapshot {
    /// Records each stage left unapplied in its latest pass because a replicated prerequisite
    /// (content bytes, an endpoint node) is not present locally yet.
    std::array<std::uint64_t, kApplyStageCount> deferred{};
    /// Cumulative hard failures per stage (I/O, corruption, invariant violations).
    std::array<std::uint64_t, kApplyStageCount> failures{};
    /// Age of the longest-standing deferral still pending; 0 when nothing is deferred.
    std::uint64_t oldestDeferralAgeMs{0};
    /// Apply cycles that ran every inbound stage, and how many of those had a stage fail.
    std::uint64_t applyCycles{0};
    std::uint64_t applyFailedCycles{0};
    /// Cycles whose outbound publish was preempted, and outbound publishes that failed.
    std::uint64_t publishSkippedCycles{0};
    std::uint64_t publishFailedCycles{0};
    std::string lastFailureStage;
    std::string lastFailure;
    std::uint64_t lastFailureAgeMs{0};

    std::uint64_t deferredIn(ApplyStage stage) const noexcept {
        return deferred[static_cast<std::size_t>(stage)];
    }
    std::uint64_t failuresIn(ApplyStage stage) const noexcept {
        return failures[static_cast<std::size_t>(stage)];
    }
    std::uint64_t deferredTotal() const noexcept {
        std::uint64_t total = 0;
        for (const auto count : deferred) {
            total += count;
        }
        return total;
    }
};

/// Outcome of one stage pass that scanned every record. A pass can defer some records and fail
/// on others: one corrupt record must not hide the prerequisites the same scan found missing.
struct ApplyStagePass {
    /// Records the pass left unapplied because a replicated prerequisite is not local yet.
    std::vector<std::string> deferredKeys;
    /// First hard failure of the pass, if any.
    std::optional<std::string> failure;
};

/// Tracks deferred records, stage failures, and outbound-publish outcomes of the apply path.
///
/// Deferred records themselves live in the replicated memory-sync index: an adapter skips a
/// winner whose prerequisite is missing without marking it applied, so every later cycle retries
/// it. This tracker only remembers when each deferral was first seen so a prerequisite that never
/// arrives stays visible (count and oldest age) instead of being retried silently. Memory is
/// bounded: at most kMaxTrackedDeferralsPerStage keys keep a first-seen time per stage (the
/// oldest are kept), while the deferred count stays exact. Thread-safe.
class ApplyHealth {
public:
    using Clock = std::chrono::steady_clock;

    static constexpr std::size_t kMaxTrackedDeferralsPerStage = 4096;
    /// A deferral older than this is reported once as stale: its prerequisite likely never came.
    static constexpr std::chrono::minutes kStaleDeferralAge{10};
    static constexpr std::size_t kMaxStaleReportsPerPass = 8;

    /// Replace a stage's deferred set with the keys its latest pass deferred. Returns keys whose
    /// deferral just crossed kStaleDeferralAge (each is returned once, at most
    /// kMaxStaleReportsPerPass per call) so the caller can log them.
    std::vector<std::string> recordDeferred(ApplyStage stage, std::span<const std::string> keys,
                                            Clock::time_point now) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto& state = stages_[index(stage)];
        std::unordered_map<std::string, Deferral> next;
        next.reserve(std::min(keys.size(), kMaxTrackedDeferralsPerStage));
        // Carry every tracked key that is still deferred, wherever it sits in `keys`, before any
        // new key takes a slot: the cap must never evict an older first-seen time for a newer
        // one. This pass cannot overflow the cap, since `tracked` itself never exceeds it.
        for (const auto& key : keys) {
            if (const auto it = state.tracked.find(key); it != state.tracked.end()) {
                next.emplace(key, it->second);
            }
        }
        for (const auto& key : keys) {
            if (next.size() >= kMaxTrackedDeferralsPerStage) {
                break;
            }
            next.try_emplace(key, Deferral{now, false});
        }
        state.tracked = std::move(next);
        state.deferred = keys.size();

        std::vector<std::string> stale;
        for (auto& [key, deferral] : state.tracked) {
            if (stale.size() >= kMaxStaleReportsPerPass) {
                break;
            }
            if (!deferral.reportedStale && now - deferral.firstDeferred >= kStaleDeferralAge) {
                deferral.reportedStale = true;
                stale.push_back(key);
            }
        }
        return stale;
    }

    /// Record a stage pass that scanned every record: its deferred set replaces the stage's, and
    /// its failure, if any, is counted. Returns the newly stale deferrals, as recordDeferred.
    std::vector<std::string> recordPass(ApplyStage stage, const ApplyStagePass& pass,
                                        Clock::time_point now) {
        if (pass.failure) {
            recordFailure(stage, *pass.failure, now);
        }
        return recordDeferred(stage, pass.deferredKeys, now);
    }

    /// Record a hard stage failure from a pass that did not scan every record. The stage's
    /// deferred set is left as last observed, since an incomplete pass says nothing about which
    /// prerequisites are still missing.
    void recordFailure(ApplyStage stage, std::string message, Clock::time_point now) {
        std::lock_guard<std::mutex> lock(mutex_);
        ++stages_[index(stage)].failures;
        lastFailureStage_ = stage;
        lastFailure_ = std::move(message);
        lastFailureAt_ = now;
        hasFailure_ = true;
    }

    /// Record an apply cycle that ran every inbound stage; `stageFailed` if any stage failed.
    void recordCycle(bool stageFailed) {
        std::lock_guard<std::mutex> lock(mutex_);
        ++applyCycles_;
        if (stageFailed) {
            ++applyFailedCycles_;
        }
    }

    void recordPublishSkipped() {
        std::lock_guard<std::mutex> lock(mutex_);
        ++publishSkippedCycles_;
    }

    void recordPublishFailed() {
        std::lock_guard<std::mutex> lock(mutex_);
        ++publishFailedCycles_;
    }

    ApplyHealthSnapshot snapshot(Clock::time_point now) const {
        std::lock_guard<std::mutex> lock(mutex_);
        ApplyHealthSnapshot out;
        bool anyDeferral = false;
        Clock::time_point oldest = now;
        for (std::size_t stage = 0; stage < kApplyStageCount; ++stage) {
            const auto& state = stages_[stage];
            out.deferred[stage] = state.deferred;
            out.failures[stage] = state.failures;
            for (const auto& [key, deferral] : state.tracked) {
                (void)key;
                if (!anyDeferral || deferral.firstDeferred < oldest) {
                    oldest = deferral.firstDeferred;
                    anyDeferral = true;
                }
            }
        }
        out.oldestDeferralAgeMs = anyDeferral ? ageMs(oldest, now) : 0;
        out.applyCycles = applyCycles_;
        out.applyFailedCycles = applyFailedCycles_;
        out.publishSkippedCycles = publishSkippedCycles_;
        out.publishFailedCycles = publishFailedCycles_;
        if (hasFailure_) {
            out.lastFailureStage = std::string(applyStageName(lastFailureStage_));
            out.lastFailure = lastFailure_;
            out.lastFailureAgeMs = ageMs(lastFailureAt_, now);
        }
        return out;
    }

private:
    struct Deferral {
        Clock::time_point firstDeferred;
        bool reportedStale{false};
    };
    struct StageState {
        std::unordered_map<std::string, Deferral> tracked;
        std::uint64_t deferred{0};
        std::uint64_t failures{0};
    };

    static constexpr std::size_t index(ApplyStage stage) noexcept {
        return static_cast<std::size_t>(stage);
    }

    static std::uint64_t ageMs(Clock::time_point since, Clock::time_point now) noexcept {
        if (now <= since) {
            return 0;
        }
        return static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(now - since).count());
    }

    mutable std::mutex mutex_;
    std::array<StageState, kApplyStageCount> stages_{};
    std::uint64_t applyCycles_{0};
    std::uint64_t applyFailedCycles_{0};
    std::uint64_t publishSkippedCycles_{0};
    std::uint64_t publishFailedCycles_{0};
    bool hasFailure_{false};
    ApplyStage lastFailureStage_{ApplyStage::Content};
    std::string lastFailure_;
    Clock::time_point lastFailureAt_{};
};

} // namespace yams::memory_sync
