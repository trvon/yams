// Adapter implementation kept private; headers remain minimal and 3P-free.
#include <yams/daemon/ipc/connection_fsm.h>

#include <yams/daemon/ipc/fsm_metrics_registry.h>
#include <yams/daemon/ipc/socket_utils.h>

#include <spdlog/spdlog.h>
#include <algorithm>
#include <array>
#include <chrono>
#include <memory>
#if __has_include(<tinyfsm.hpp>)
#include <tinyfsm.hpp>
#endif
#if __has_include(<yams/profiling.h>)
#include <yams/profiling.h>
#else
#define YAMS_PLOT(name, val) ((void)0)
#endif

namespace yams {
namespace daemon {

// ---------------------------------------------------------------------------
// Adapter-private state classes (no header changes)
// ---------------------------------------------------------------------------
enum class ImplState {
    Disconnected,
    Accepting,
    Connected,
    ReadingHeader,
    ReadingPayload,
    WritingHeader,
    StreamingChunks,
    Closing,
    Closed,
    Error
};

struct Next {
    bool has{false};
    ImplState state{ImplState::Disconnected};
};

static inline ConnectionFsm::State to_public(ImplState s) {
    using S = ConnectionFsm::State;
    switch (s) {
        case ImplState::Disconnected:
            return S::Disconnected;
        case ImplState::Accepting:
            return S::Accepting;
        case ImplState::Connected:
            return S::Connected;
        case ImplState::ReadingHeader:
            return S::ReadingHeader;
        case ImplState::ReadingPayload:
            return S::ReadingPayload;
        case ImplState::WritingHeader:
            return S::WritingHeader;
        case ImplState::StreamingChunks:
            return S::StreamingChunks;
        case ImplState::Closing:
            return S::Closing;
        case ImplState::Closed:
            return S::Closed;
        case ImplState::Error:
            return S::Error;
    }
    return S::Error;
}

struct StateBase {
    virtual ~StateBase() = default;
    // Phase 4: entry/exit hooks (no-op by default)
    virtual void entry() {}
    virtual void exit() {}
    virtual Next on_accept(uint64_t) { return {}; }
    virtual Next on_connect(uint64_t) { return {}; }
    virtual Next on_header_parsed(uint64_t /*payload_size*/) { return {}; }
    virtual Next on_body_parsed() { return {}; }
    virtual Next on_stream_next(bool /*done*/) { return {}; }
    virtual Next on_timeout() { return {}; }
    virtual Next on_error(int /*err*/) { return {}; }
    virtual Next on_close_request() { return {}; }
    virtual const char* name() const noexcept = 0;
};

struct DisconnectedState final : StateBase {
    Next on_accept(uint64_t fd) override {
        if (fd == static_cast<uint64_t>(-1))
            return {true, ImplState::Error};
        return {true, ImplState::Accepting};
    }
    Next on_connect(uint64_t fd) override {
        if (fd == static_cast<uint64_t>(-1))
            return {true, ImplState::Error};
        return {true, ImplState::Connected};
    }
    const char* name() const noexcept override { return "DisconnectedState"; }
};

struct AcceptingState final : StateBase {
    Next on_connect(uint64_t fd) override {
        if (fd == static_cast<uint64_t>(-1))
            return {true, ImplState::Error};
        return {true, ImplState::Connected};
    }
    const char* name() const noexcept override { return "AcceptingState"; }
};

struct ConnectedState final : StateBase {
    Next on_header_parsed(uint64_t payload_size) override {
        if (payload_size > 0)
            return {true, ImplState::ReadingPayload};
        return {true, ImplState::WritingHeader};
    }
    Next on_close_request() override { return {true, ImplState::Closing}; }
    const char* name() const noexcept override { return "ConnectedState"; }
};

struct ReadingHeaderState final : StateBase {
    Next on_header_parsed(uint64_t payload_size) override {
        if (payload_size > 0)
            return {true, ImplState::ReadingPayload};
        return {true, ImplState::WritingHeader};
    }
    Next on_error(int) override { return {true, ImplState::Error}; }
    const char* name() const noexcept override { return "ReadingHeaderState"; }
};

struct ReadingPayloadState final : StateBase {
    Next on_body_parsed() override { return {true, ImplState::WritingHeader}; }
    Next on_error(int) override { return {true, ImplState::Error}; }
    const char* name() const noexcept override { return "ReadingPayloadState"; }
};

struct WritingHeaderState final : StateBase {
    Next on_stream_next(bool done) override {
        if (done)
            return {true, ImplState::Closing};
        return {true, ImplState::StreamingChunks};
    }
    Next on_error(int) override { return {true, ImplState::Error}; }
    const char* name() const noexcept override { return "WritingHeaderState"; }
};

struct StreamingChunksState final : StateBase {
    Next on_stream_next(bool done) override {
        if (done)
            return {true, ImplState::Closing};
        return {true, ImplState::StreamingChunks};
    }
    Next on_error(int) override { return {true, ImplState::Error}; }
    const char* name() const noexcept override { return "StreamingChunksState"; }
};

struct ClosingState final : StateBase {
    Next on_close_request() override { return {true, ImplState::Closed}; }
    Next on_error(int) override { return {true, ImplState::Closed}; }
    const char* name() const noexcept override { return "ClosingState"; }
};

struct ClosedState final : StateBase {
    const char* name() const noexcept override { return "ClosedState"; }
};

struct ErrorState final : StateBase {
    Next on_close_request() override { return {true, ImplState::Closing}; }
    const char* name() const noexcept override { return "ErrorState"; }
};

static StateBase* state_for(ConnectionFsm::State s) {
    static DisconnectedState s_disconnected;
    static AcceptingState s_accepting;
    static ConnectedState s_connected;
    static ReadingHeaderState s_reading_header;
    static ReadingPayloadState s_reading_payload;
    static WritingHeaderState s_writing_header;
    static StreamingChunksState s_streaming_chunks;
    static ClosingState s_closing;
    static ClosedState s_closed;
    static ErrorState s_error;
    switch (s) {
        case ConnectionFsm::State::Disconnected:
            return &s_disconnected;
        case ConnectionFsm::State::Accepting:
            return &s_accepting;
        case ConnectionFsm::State::Connected:
            return &s_connected;
        case ConnectionFsm::State::ReadingHeader:
            return &s_reading_header;
        case ConnectionFsm::State::ReadingPayload:
            return &s_reading_payload;
        case ConnectionFsm::State::WritingHeader:
            return &s_writing_header;
        case ConnectionFsm::State::StreamingChunks:
            return &s_streaming_chunks;
        case ConnectionFsm::State::Closing:
            return &s_closing;
        case ConnectionFsm::State::Closed:
            return &s_closed;
        case ConnectionFsm::State::Error:
            return &s_error;
        default:
            return &s_connected; // fallback
    }
}

struct SnapshotEntry {
    ConnectionFsm::State state;
    uint64_t ns_since_epoch;
    const char* last_event;
    uint64_t bytes_transferred;
};

struct MetricsCounters {
    uint64_t header_reads_started{0};
    uint64_t payload_reads_completed{0};
    uint64_t payload_writes_completed{0};
    uint64_t bytes_sent{0};
    uint64_t bytes_received{0};
    uint64_t transitions{0};
    uint64_t timeouts_total{0};
    uint64_t retries_total{0};
    uint64_t errors_total{0};
};

struct ConfigValues {
    uint32_t header_timeout_ms{3000};
    uint32_t payload_timeout_ms{27000};
    uint32_t idle_timeout_ms{30000};
    std::size_t max_retries{3};
    bool enable_metrics{true};
    bool enable_snapshots{false};
};

namespace {
// Fixed-size ring buffer to bound snapshot memory
template <std::size_t N> struct SnapshotRing {
    std::array<SnapshotEntry, N> buf{};
    std::uint32_t head{0};
    std::uint32_t size{0};
    std::uint64_t dropped{0};
    void push(const SnapshotEntry& s) {
        buf[head] = s;
        head = (head + 1) % N;
        if (size < N)
            ++size;
        else
            ++dropped;
    }
};
} // unnamed namespace

// Define the PIMPL that was forward-declared in the header
struct ConnectionFsm::Impl {
    ConfigValues cfg{};
    MetricsCounters metrics{};
    SnapshotRing<256> snapshots{};
    const char* last_event{"init"};
    uint64_t total_bytes{0};
    StateBase* current{nullptr};
    // Phase 2: timer deadlines (consumed by on_timeout externally)
    std::chrono::steady_clock::time_point idle_deadline{};
    std::chrono::steady_clock::time_point op_deadline{};
    bool idle_armed{false};
    bool op_armed{false};
    int retries{0};
    // Phase 2: write backpressure queue (bytes only; payload owned by higher layers)
    std::size_t write_bytes{0};
    std::size_t write_cap_bytes{2 * 1024 * 1024};
    uint32_t low_wm_percent{50};
    uint32_t high_wm_percent{85};
    bool backpressured{false};
    int last_errno{0};
};

ConnectionFsm::ConnectionFsm() : impl_(std::make_unique<Impl>()) {
    auto* impl = impl_.get();
    if (impl)
        impl->current = state_for(state_);
}

ConnectionFsm::~ConnectionFsm() = default;
ConnectionFsm::ConnectionFsm(ConnectionFsm&&) noexcept = default;
ConnectionFsm& ConnectionFsm::operator=(ConnectionFsm&&) noexcept = default;

// Public configuration API (kept light-weight)
void ConnectionFsm::set_header_timeout_ms(uint32_t ms) noexcept {
    if (auto* impl = impl_.get())
        impl->cfg.header_timeout_ms = ms;
}
void ConnectionFsm::set_payload_timeout_ms(uint32_t ms) noexcept {
    if (auto* impl = impl_.get())
        impl->cfg.payload_timeout_ms = ms;
}
void ConnectionFsm::set_idle_timeout_ms(uint32_t ms) noexcept {
    if (auto* impl = impl_.get())
        impl->cfg.idle_timeout_ms = ms;
}
void ConnectionFsm::set_max_retries(std::size_t n) noexcept {
    if (auto* impl = impl_.get())
        impl->cfg.max_retries = n;
}
void ConnectionFsm::enable_metrics(bool on) noexcept {
    if (auto* impl = impl_.get())
        impl->cfg.enable_metrics = on;
}
void ConnectionFsm::enable_snapshots(bool on) noexcept {
    if (auto* impl = impl_.get())
        impl->cfg.enable_snapshots = on;
}
void ConnectionFsm::set_write_cap_bytes(std::size_t bytes) noexcept {
    if (auto* impl = impl_.get())
        impl->write_cap_bytes = bytes;
}
void ConnectionFsm::set_backpressure_watermarks(uint32_t low_percent,
                                                uint32_t high_percent) noexcept {
    if (auto* impl = impl_.get()) {
        impl->low_wm_percent = low_percent;
        impl->high_wm_percent = high_percent;
    }
}

void ConnectionFsm::debug_dump_snapshots(std::size_t max_entries) const noexcept {
    auto* impl = impl_.get();
    if (!impl || impl->snapshots.size == 0)
        return;
    const std::uint32_t cap = 256;
    std::uint32_t count =
        static_cast<std::uint32_t>(std::min<std::size_t>(max_entries, impl->snapshots.size));
    std::uint32_t start = (impl->snapshots.head + (impl->snapshots.size + cap - count) % cap) % cap;
    for (std::uint32_t i = 0; i < count; ++i) {
        const auto& s = impl->snapshots.buf[(start + i) % cap];
    }
}

bool ConnectionFsm::transition(State next, const char* reason) noexcept {
    if (state_ == next)
        return true; // no-op but not an error
    auto from = state_;
    // Compile-time legality matrix (bitmask per state)
    constexpr auto b = [](int x) constexpr { return 1u << x; };
    static constexpr std::array<std::uint32_t, 10> kLegal = {
        /* Disconnected */ b((int)State::Accepting) | b((int)State::Connected) |
            b((int)State::Error),
        /* Accepting */ b((int)State::Connected) | b((int)State::Closing) | b((int)State::Error),
        /* Connected */ b((int)State::ReadingHeader) | b((int)State::Closing) |
            b((int)State::Error),
        /* ReadingHeader */ b((int)State::ReadingPayload) | b((int)State::WritingHeader) |
            b((int)State::Closing) | b((int)State::Error),
        /* ReadingPayload */ b((int)State::WritingHeader) | b((int)State::Closing) |
            b((int)State::Error),
        /* WritingHeader */ b((int)State::StreamingChunks) | b((int)State::Closing) |
            b((int)State::Connected) | b((int)State::Error),
        /* StreamingChunks */ b((int)State::StreamingChunks) | b((int)State::Closing) |
            b((int)State::Connected) | b((int)State::Error),
        /* Closing */ b((int)State::Closed) | b((int)State::Error),
        /* Closed */ b((int)State::Closed),
        /* Error */ b((int)State::Closing) | b((int)State::Closed) | b((int)State::Error)};
    auto can_transition = [](State f, State t) noexcept { return (kLegal[(int)f] >> (int)t) & 1u; };
    bool legal = can_transition(from, next);

    if (!legal) {
        return false;
    }
    // Leaving from-state: call exit(), cancel timers by default; re-arm as needed on enter
    if (auto* impl = impl_.get()) {
        if (impl->current)
            impl->current->exit();
        impl->idle_armed = false;
        impl->op_armed = false;
    }
    state_ = next;
    if (auto* impl = impl_.get()) {
        impl->metrics.transitions++;
        // Emit to FsmMetricsRegistry
        FsmMetricsRegistry::instance().incrementTransitions(1);
        if (impl->cfg.enable_metrics) {
            YAMS_PLOT("daemon_fsm_transitions", static_cast<int64_t>(impl->metrics.transitions));
            // Also expose a simple gauge for current state id for observability
#if __has_include(<yams/profiling.h>)
            YAMS_PLOT("daemon_connection_state",
                      static_cast<int64_t>([s = state_]() constexpr -> int {
                          switch (s) {
                              case State::Disconnected:
                                  return 0;
                              case State::Accepting:
                                  return 1;
                              case State::Connected:
                                  return 2;
                              case State::ReadingHeader:
                                  return 3;
                              case State::ReadingPayload:
                                  return 4;
                              case State::WritingHeader:
                                  return 5;
                              case State::StreamingChunks:
                                  return 6;
                              case State::Closing:
                                  return 7;
                              case State::Closed:
                                  return 8;
                              case State::Error:
                                  return 9;
                          }
                          return -1;
                      }()));
#endif
        }
        if (impl->cfg.enable_snapshots) {
            SnapshotEntry e{
                state_,
                static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                          std::chrono::steady_clock::now().time_since_epoch())
                                          .count()),
                impl->last_event, impl->total_bytes};
            impl->snapshots.push(e);
        }
        // Switch active state implementation and call entry()
        impl->current = state_for(state_);
        if (impl->current)
            impl->current->entry();
        // Enter actions: arm timers based on state role
        auto now = std::chrono::steady_clock::now();
        if (state_ == State::Connected) {
            if (impl->cfg.idle_timeout_ms > 0) {
                impl->idle_deadline = now + std::chrono::milliseconds(impl->cfg.idle_timeout_ms);
                impl->idle_armed = true;
            }
            impl->retries = 0;
        } else if (state_ == State::ReadingHeader) {
            if (impl->cfg.header_timeout_ms > 0) {
                impl->op_deadline = now + std::chrono::milliseconds(impl->cfg.header_timeout_ms);
                impl->op_armed = true;
            }
            impl->retries = 0;
        } else if (state_ == State::ReadingPayload || state_ == State::WritingHeader ||
                   state_ == State::StreamingChunks) {
            if (impl->cfg.payload_timeout_ms > 0) {
                impl->op_deadline = now + std::chrono::milliseconds(impl->cfg.payload_timeout_ms);
                impl->op_armed = true;
            }
            impl->retries = 0;
        }
    }
    return true;
}

void ConnectionFsm::on_accept(uint64_t fd) {
    if (auto* impl = impl_.get())
        impl->last_event = "accept";
    fd_ = fd;
    if (auto* impl = impl_.get(); impl && impl->current) {
        auto next = impl->current->on_accept(fd);
        if (next.has)
            transition(to_public(next.state), "on_accept:state_impl");
    }
}

void ConnectionFsm::on_connect(uint64_t fd) {
    if (auto* impl = impl_.get())
        impl->last_event = "connect";
    fd_ = fd;
    if (auto* impl = impl_.get(); impl && impl->current) {
        auto next = impl->current->on_connect(fd);
        if (next.has)
            transition(to_public(next.state), "on_connect:state_impl");
    }
}

void ConnectionFsm::on_readable(std::size_t) {
    if (auto* impl = impl_.get()) {
        impl->last_event = "readable";
        impl->metrics.header_reads_started +=
            (state_ == State::Connected || state_ == State::ReadingHeader) ? 1 : 0;
        if (impl->cfg.enable_metrics) {
            YAMS_PLOT("daemon_fsm_header_reads",
                      static_cast<int64_t>(impl->metrics.header_reads_started));
        }
    }
    // Drive reads based on current state
    switch (state_) {
        case State::Connected:
            transition(State::ReadingHeader, "on_readable:connected->reading_header");
            break;
        case State::ReadingHeader:
            // Header bytes available, remain until parsed
            break;
        case State::ReadingPayload:
            // Payload bytes available, remain until parsed
            break;
        default:
            break;
    }
}

void ConnectionFsm::on_writable(std::size_t) {
    if (auto* impl = impl_.get())
        impl->last_event = "writable";
    // Writes are driven by higher-level events; ignore spurious signals in other states
    switch (state_) {
        case State::WritingHeader:
        case State::StreamingChunks:
            break; // ok
        default:
            break; // ignore
    }
}

void ConnectionFsm::on_header_parsed(const FrameInfo& info) {
    if (auto* impl = impl_.get())
        impl->last_event = "header_parsed";
    if (auto* impl = impl_.get(); impl && impl->current) {
        auto next = impl->current->on_header_parsed(info.payload_size);
        if (next.has)
            transition(to_public(next.state), "on_header_parsed:state_impl");
    }
}

void ConnectionFsm::on_body_parsed() {
    if (auto* impl = impl_.get()) {
        impl->last_event = "body_parsed";
        impl->metrics.payload_reads_completed++;
        // Emit to FsmMetricsRegistry
        FsmMetricsRegistry::instance().incrementPayloadReads(1);
        if (impl->cfg.enable_metrics) {
            YAMS_PLOT("daemon_fsm_payload_reads",
                      static_cast<int64_t>(impl->metrics.payload_reads_completed));
        }
    }
    if (auto* impl = impl_.get(); impl && impl->current) {
        auto next = impl->current->on_body_parsed();
        if (next.has) {
            transition(to_public(next.state), "on_body_parsed:state_impl");
            return;
        }
    }
    // Fallback: previous behavior
    if (state_ == State::ReadingPayload || state_ == State::ReadingHeader) {
        transition(State::WritingHeader, "on_body_parsed:fallback");
    }
}

void ConnectionFsm::on_stream_next(bool done) {
    if (auto* impl = impl_.get())
        impl->last_event = done ? "stream_done" : "stream_next";
    if (auto* impl = impl_.get(); impl && impl->current) {
        auto next = impl->current->on_stream_next(done);
        if (next.has) {
            transition(to_public(next.state), "on_stream_next:state_impl");
            return;
        }
    }
    // Fallback
    if (state_ == State::WritingHeader || state_ == State::StreamingChunks) {
        transition(done ? State::Closing : State::StreamingChunks,
                   done ? "on_stream_next:done" : "on_stream_next:more");
    }
}

void ConnectionFsm::on_timeout(Operation op) {
    if (auto* impl = impl_.get())
        impl->last_event = "timeout";
    if (auto* impl = impl_.get()) {
        impl->metrics.timeouts_total++;
        if (impl->cfg.enable_metrics) {
            YAMS_PLOT("daemon_fsm_timeouts_total",
                      static_cast<int64_t>(impl->metrics.timeouts_total));
        }
    }
    if (auto* impl = impl_.get(); impl && impl->current) {
        auto next = impl->current->on_timeout();
        if (next.has) {
            transition(to_public(next.state), "on_timeout:state_impl");
            return;
        }
    }
    // Retry policy: if operation timer armed and retries left, re-arm and continue in-place
    if (auto* impl = impl_.get()) {
        if (impl->op_armed && impl->retries < static_cast<int>(impl->cfg.max_retries)) {
            impl->retries++;
            impl->metrics.retries_total++;
            if (impl->cfg.enable_metrics) {
                YAMS_PLOT("daemon_fsm_retries_total",
                          static_cast<int64_t>(impl->metrics.retries_total));
            }
            auto now = std::chrono::steady_clock::now();
            uint32_t dur_ms = (state_ == State::ReadingHeader) ? impl->cfg.header_timeout_ms
                                                               : impl->cfg.payload_timeout_ms;
            if (dur_ms > 0) {
                impl->op_deadline = now + std::chrono::milliseconds(dur_ms);
                impl->op_armed = true;
            }
            return; // stay in current state
        }
    }
    transition(State::Error, "on_timeout:fallback_error");
}

void ConnectionFsm::on_error(int err) {
    on_error(err, nullptr);
}

void ConnectionFsm::on_error(int err, const char* where) {
    if (auto* impl = impl_.get())
        impl->last_event = "error";
    if (auto* impl = impl_.get()) {
        impl->metrics.errors_total++;
        impl->last_errno = err;
        if (impl->cfg.enable_metrics) {
            YAMS_PLOT("daemon_fsm_errors_total", static_cast<int64_t>(impl->metrics.errors_total));
        }
    }
    if (auto* impl = impl_.get(); impl && impl->current) {
        auto next = impl->current->on_error(0);
        if (next.has) {
            transition(to_public(next.state), where ? where : "on_error:state_impl");
            return;
        }
    }
    transition(State::Error, where ? where : "on_error:fallback_error");
}

void ConnectionFsm::on_close_request() {
    if (auto* impl = impl_.get())
        impl->last_event = "close_request";
    if (auto* impl = impl_.get(); impl && impl->current) {
        auto next = impl->current->on_close_request();
        if (next.has) {
            transition(to_public(next.state), "on_close_request:state_impl");
            return;
        }
    }
    // Fallback
    switch (state_) {
        case State::Closed:
        case State::Closing:
            break;
        default:
            transition(State::Closing, "on_close_request:fallback_closing");
            break;
    }
}

void ConnectionFsm::on_response_complete(bool close_after) {
    if (auto* impl = impl_.get())
        impl->last_event = close_after ? "response_complete_close" : "response_complete_keep";
    // If we are streaming or just wrote a header/payload and persistent connection is desired,
    // transition back to Connected to allow the next request. Otherwise go to Closing.
    switch (state_) {
        case State::WritingHeader:
        case State::StreamingChunks:
            transition(close_after ? State::Closing : State::Connected,
                       close_after ? "on_response_complete:close"
                                   : "on_response_complete:keepalive");
            break;
        default:
            transition(close_after ? State::Closing : State::Connected,
                       close_after ? "on_response_complete:close"
                                   : "on_response_complete:keepalive");
            break;
    }
}

const char* ConnectionFsm::to_string(State s) noexcept {
    switch (s) {
        case State::Disconnected:
            return "Disconnected";
        case State::Accepting:
            return "Accepting";
        case State::Connected:
            return "Connected";
        case State::ReadingHeader:
            return "ReadingHeader";
        case State::ReadingPayload:
            return "ReadingPayload";
        case State::WritingHeader:
            return "WritingHeader";
        case State::StreamingChunks:
            return "StreamingChunks";
        case State::Closing:
            return "Closing";
        case State::Closed:
            return "Closed";
        case State::Error:
            return "Error";
    }
    return "Unknown";
}

} // namespace daemon
} // namespace yams

namespace yams::daemon {

std::filesystem::path ConnectionFsm::resolve_socket_path() {
    return socket_utils::resolve_socket_path();
}

std::filesystem::path ConnectionFsm::resolve_socket_path_config_first() {
    return socket_utils::resolve_socket_path_config_first();
}

// ======================
// Phase 2b: Backpressure
// ======================
void ConnectionFsm::on_write_queued(std::size_t bytes) noexcept {
    if (bytes == 0)
        return;
    if (auto* impl = impl_.get()) {
        const std::size_t cap = impl->write_cap_bytes ? impl->write_cap_bytes : (2 * 1024 * 1024);
        const std::size_t before = impl->write_bytes;
        impl->write_bytes = std::min(before + bytes, cap);
        const std::size_t percent = cap ? (impl->write_bytes * 100) / cap : 0;
        const bool was = impl->backpressured;
        if (!impl->backpressured && percent >= impl->high_wm_percent) {
            impl->backpressured = true;
        } else if (impl->backpressured && percent <= impl->low_wm_percent) {
            impl->backpressured = false;
        }
        if (was != impl->backpressured) {
            YAMS_PLOT("daemon_fsm_backpressure_flip",
                      static_cast<int64_t>(impl->backpressured ? 1 : 0));
        }
    }
}

void ConnectionFsm::on_write_flushed(std::size_t bytes) noexcept {
    if (auto* impl = impl_.get()) {
        const std::size_t cap = impl->write_cap_bytes ? impl->write_cap_bytes : (2 * 1024 * 1024);
        if (bytes >= impl->write_bytes)
            impl->write_bytes = 0;
        else
            impl->write_bytes -= bytes;
        const std::size_t percent = cap ? (impl->write_bytes * 100) / cap : 0;
        const bool was = impl->backpressured;
        if (impl->backpressured && percent <= impl->low_wm_percent) {
            // Hysteresis: when flipping OFF, clamp to low watermark to stabilize capacity
            impl->backpressured = false;
            impl->write_bytes = (cap * impl->low_wm_percent) / 100;
        }
        if (was != impl->backpressured) {
            YAMS_PLOT("daemon_fsm_backpressure_flip",
                      static_cast<int64_t>(static_cast<int>(impl->backpressured)));
        }
    }
}

bool ConnectionFsm::backpressured() const noexcept {
    if (auto* impl = impl_.get())
        return impl->backpressured;
    return false;
}

std::size_t ConnectionFsm::write_capacity_remaining() const noexcept {
    if (auto* impl = impl_.get()) {
        const std::size_t cap = impl->write_cap_bytes ? impl->write_cap_bytes : (2 * 1024 * 1024);
        if (impl->write_bytes >= cap)
            return 0;
        return cap - impl->write_bytes;
    }
    return 0;
}

} // namespace yams::daemon
