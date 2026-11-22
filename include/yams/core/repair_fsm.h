#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <utility>

namespace yams::core {

// Header-only state machine for one repair cycle.
//
//   Idle -start-> Scan -> Detect -> Fix -> Reindex -> Complete
//
// A cycle only moves forward. advance() may skip stages that have no work this cycle but never
// re-enters or revisits one. fail() parks an active cycle in Error; retry() restarts it at Scan
// while the retry budget (Config::max_retries) lasts; reset() returns to Idle for the next
// cycle. Every rejected transition returns false and leaves the state untouched.
//
// Not thread-safe: one owner drives it. Callbacks run inline and must not throw.
class RepairFsm {
public:
    enum class State { Idle, Scan, Detect, Fix, Reindex, Complete, Error };

    struct Config {
        bool enable_online_repair{false};
        std::uint32_t max_repair_concurrency{1};
        std::uint32_t repair_backoff_ms{250};
        std::uint32_t max_retries{3};
    };

    using ClockMs = std::uint64_t; // avoid <chrono> in the public header
    using Callback = std::function<void(State)>;

    explicit RepairFsm(Config cfg) : cfg_(cfg) {}
    explicit RepairFsm() : cfg_{} {}

    State state() const noexcept { return state_; }
    const Config& config() const noexcept { return cfg_; }

    // Optional: surface scheduling hints (fed by a thin adapter that reads ConnectionFsm)
    struct SchedulingHints {
        bool streaming_high_load{false};
        bool maintenance_allowed{false};
        bool closing{false};
    };
    void set_scheduling_hints(SchedulingHints h) noexcept { hints_ = h; }
    SchedulingHints hints() const noexcept { return hints_; }

    // Begins a cycle: Idle -> Scan.
    bool start() noexcept {
        if (state_ != State::Idle)
            return false;
        return enter(State::Scan);
    }

    // Moves an active cycle forward to a later stage, or to Complete.
    bool advance(State next) noexcept {
        if (!is_active(state_) || !(is_active(next) || next == State::Complete))
            return false;
        if (rank(next) <= rank(state_))
            return false;
        return enter(next);
    }

    // Marks the active cycle failed: any stage before Complete -> Error.
    bool fail() noexcept {
        if (!is_active(state_))
            return false;
        return enter(State::Error);
    }

    // Restarts a failed cycle: Error -> Scan, while fewer than max_retries retries were spent.
    // Reports the attempt number and its backoff first; the caller does the waiting.
    bool retry() noexcept {
        if (state_ != State::Error || retries_ >= cfg_.max_retries)
            return false;
        ++retries_;
        if (on_backoff_)
            on_backoff_(retries_, backoff_ms(retries_));
        return enter(State::Scan);
    }

    // Ends the cycle, finished, failed or abandoned: any state but Idle -> Idle. Restores the
    // retry budget.
    bool reset() noexcept {
        if (state_ == State::Idle)
            return false;
        retries_ = 0;
        return enter(State::Idle);
    }

    // Retries spent in the current cycle.
    std::uint32_t retries() const noexcept { return retries_; }

    // Backoff for the given retry attempt (1-based): repair_backoff_ms doubled per earlier
    // attempt, saturating.
    std::uint32_t backoff_ms(std::uint32_t attempt) const noexcept {
        std::uint64_t ms = cfg_.repair_backoff_ms;
        for (std::uint32_t i = 1; i < attempt && ms < kMaxBackoffMs; ++i)
            ms *= 2;
        return ms > kMaxBackoffMs ? kMaxBackoffMs : static_cast<std::uint32_t>(ms);
    }

    using BackoffCallback = std::function<void(std::uint32_t attempt, std::uint32_t backoff_ms)>;
    void set_on_backoff(BackoffCallback cb) { on_backoff_ = std::move(cb); }
    void set_on_state_change(Callback cb) { on_state_change_ = std::move(cb); }

    static const char* to_string(State s) noexcept {
        switch (s) {
            case State::Idle:
                return "Idle";
            case State::Scan:
                return "Scan";
            case State::Detect:
                return "Detect";
            case State::Fix:
                return "Fix";
            case State::Reindex:
                return "Reindex";
            case State::Complete:
                return "Complete";
            case State::Error:
                return "Error";
        }
        return "Unknown";
    }

private:
    static constexpr std::uint32_t kMaxBackoffMs = 0xFFFFFFFFU;

    // Position in the cycle; the enum order is the stage order.
    static constexpr std::uint32_t rank(State s) noexcept { return static_cast<std::uint32_t>(s); }
    // A stage in which work happens: Scan through Reindex.
    static constexpr bool is_active(State s) noexcept {
        return rank(s) >= rank(State::Scan) && rank(s) <= rank(State::Reindex);
    }

    bool enter(State next) noexcept {
        state_ = next;
        if (on_state_change_)
            on_state_change_(state_);
        return true;
    }

    State state_{State::Idle};
    Config cfg_{};
    SchedulingHints hints_{};
    std::uint32_t retries_{0};
    BackoffCallback on_backoff_{};
    Callback on_state_change_{};
};

} // namespace yams::core
