#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <utility>

namespace yams::core {

// Minimal, header-only RepairFsm skeleton to enable incremental, non-breaking integration.
// Coordinates data integrity workflows; implementations can be wired later.
class RepairFsm {
public:
    enum class State {
        Idle,
        Scan,
        Detect,
        Classify,
        Isolate,
        Fix,
        Verify,
        Reindex,
        Complete,
        Error
    };

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

    // Stage transitions (no-ops for now; enforce basic order with minimal checks)
    bool start() noexcept { return transition(State::Idle, State::Scan); }
    bool on_scan_done() noexcept { return transition(State::Scan, State::Detect); }
    bool on_detect_done() noexcept { return transition(State::Detect, State::Classify); }
    bool on_classify_done() noexcept { return transition(State::Classify, State::Isolate); }
    bool on_isolate_done() noexcept { return transition(State::Isolate, State::Fix); }
    bool on_fix_done(bool ok) noexcept {
        return ok ? transition(State::Fix, State::Verify) : fail();
    }
    bool on_verify_done(bool ok) noexcept {
        return ok ? transition(State::Verify, State::Reindex) : fail();
    }
    bool on_reindex_done(bool ok) noexcept {
        return ok ? transition(State::Reindex, State::Complete) : fail();
    }

    // Retry/backoff hooks (placeholders; actual timing handled by caller)
    void set_on_backoff(std::function<void(std::uint32_t /*ms*/, std::uint32_t /*attempt*/)> cb) {
        on_backoff_ = std::move(cb);
    }
    void set_on_state_change(Callback cb) { on_state_change_ = std::move(cb); }

    static const char* to_string(State s) noexcept {
        switch (s) {
            case State::Idle:
                return "Idle";
            case State::Scan:
                return "Scan";
            case State::Detect:
                return "Detect";
            case State::Classify:
                return "Classify";
            case State::Isolate:
                return "Isolate";
            case State::Fix:
                return "Fix";
            case State::Verify:
                return "Verify";
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
    bool transition(State expect, State next) noexcept {
        if (state_ != expect)
            return false;
        state_ = next;
        if (on_state_change_)
            on_state_change_(state_);
        return true;
    }
    bool fail() noexcept {
        state_ = State::Error;
        if (on_state_change_)
            on_state_change_(state_);
        return false;
    }

    State state_{State::Idle};
    Config cfg_{};
    SchedulingHints hints_{};
    std::function<void(std::uint32_t, std::uint32_t)> on_backoff_{};
    Callback on_state_change_{};
};

} // namespace yams::core
