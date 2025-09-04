// Adapter implementation kept private; headers remain minimal and 3P-free.
#include <yams/daemon/ipc/connection_fsm.h>
#include <yams/daemon/ipc/socket_utils.h>

#include <spdlog/spdlog.h>
#include <algorithm>
#include <chrono>
#include <memory>
#include <vector>
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
    virtual Next on_accept(int) { return {}; }
    virtual Next on_connect(int) { return {}; }
    virtual Next on_header_parsed(uint64_t /*payload_size*/) { return {}; }
    virtual Next on_body_parsed() { return {}; }
    virtual Next on_stream_next(bool /*done*/) { return {}; }
    virtual Next on_timeout() { return {}; }
    virtual Next on_error(int /*err*/) { return {}; }
    virtual Next on_close_request() { return {}; }
    virtual const char* name() const noexcept = 0;
};

struct DisconnectedState final : StateBase {
    Next on_accept(int fd) override {
        if (fd < 0)
            return {true, ImplState::Error};
        return {true, ImplState::Accepting};
    }
    Next on_connect(int fd) override {
        if (fd < 0)
            return {true, ImplState::Error};
        return {true, ImplState::Connected};
    }
    const char* name() const noexcept override { return "DisconnectedState"; }
};

struct AcceptingState final : StateBase {
    Next on_connect(int fd) override {
        if (fd < 0)
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
};

struct ConfigValues {
    uint32_t header_timeout_ms{3000};
    uint32_t payload_timeout_ms{27000};
    std::size_t max_retries{3};
    bool enable_metrics{true};
    bool enable_snapshots{false};
};

namespace {
struct TinyAccessImpl {
    ConfigValues cfg;
    MetricsCounters metrics;
    std::vector<SnapshotEntry> snapshots;
    const char* last_event{"init"};
    uint64_t total_bytes{0};
    StateBase* current{nullptr};
};

} // namespace

void ConnectionFsm::TinyDeleter::operator()(void* p) const noexcept {
    delete static_cast<TinyAccessImpl*>(p);
}

ConnectionFsm::ConnectionFsm() : impl_(static_cast<void*>(new TinyAccessImpl{})) {
    auto* impl = static_cast<TinyAccessImpl*>(impl_.get());
    if (impl)
        impl->current = state_for(state_);
}

ConnectionFsm::~ConnectionFsm() = default;
ConnectionFsm::ConnectionFsm(ConnectionFsm&&) noexcept = default;
ConnectionFsm& ConnectionFsm::operator=(ConnectionFsm&&) noexcept = default;

// Public configuration API (kept light-weight)
void ConnectionFsm::set_header_timeout_ms(uint32_t ms) noexcept {
    if (auto* impl = static_cast<TinyAccessImpl*>(impl_.get()))
        impl->cfg.header_timeout_ms = ms;
}
void ConnectionFsm::set_payload_timeout_ms(uint32_t ms) noexcept {
    if (auto* impl = static_cast<TinyAccessImpl*>(impl_.get()))
        impl->cfg.payload_timeout_ms = ms;
}
void ConnectionFsm::set_max_retries(std::size_t n) noexcept {
    if (auto* impl = static_cast<TinyAccessImpl*>(impl_.get()))
        impl->cfg.max_retries = n;
}
void ConnectionFsm::enable_metrics(bool on) noexcept {
    if (auto* impl = static_cast<TinyAccessImpl*>(impl_.get()))
        impl->cfg.enable_metrics = on;
}
void ConnectionFsm::enable_snapshots(bool on) noexcept {
    if (auto* impl = static_cast<TinyAccessImpl*>(impl_.get()))
        impl->cfg.enable_snapshots = on;
}

void ConnectionFsm::debug_dump_snapshots(std::size_t max_entries) const noexcept {
    auto* impl = static_cast<TinyAccessImpl*>(impl_.get());
    if (!impl || impl->snapshots.empty())
        return;
    auto count = std::min(max_entries, impl->snapshots.size());
    for (std::size_t i = impl->snapshots.size() - count; i < impl->snapshots.size(); ++i) {
        const auto& s = impl->snapshots[i];
        spdlog::debug("FSM snapshot: state={}, t(ns)={}, last_event={}, bytes={}",
                      to_string(s.state), s.ns_since_epoch, s.last_event, s.bytes_transferred);
    }
}

void ConnectionFsm::transition(State next) noexcept {
    if (state_ == next)
        return;
    auto from = state_;
    // Basic legality table to avoid nonsensical jumps
    auto legal = [from, next]() noexcept -> bool {
        using S = ConnectionFsm::State;
        switch (from) {
            case S::Disconnected:
                return (next == S::Accepting || next == S::Connected || next == S::Error);
            case S::Accepting:
                return (next == S::Connected || next == S::Closing || next == S::Error);
            case S::Connected:
                return (next == S::ReadingHeader || next == S::Closing || next == S::Error);
            case S::ReadingHeader:
                return (next == S::ReadingPayload || next == S::WritingHeader ||
                        next == S::Closing || next == S::Error);
            case S::ReadingPayload:
                return (next == S::WritingHeader || next == S::Closing || next == S::Error);
            case S::WritingHeader:
                return (next == S::StreamingChunks || next == S::Closing || next == S::Connected ||
                        next == S::Error);
            case S::StreamingChunks:
                return (next == S::StreamingChunks || next == S::Closing || next == S::Connected ||
                        next == S::Error);
            case S::Closing:
                return (next == S::Closed || next == S::Error);
            case S::Closed:
                return (next == S::Closed);
            case S::Error:
                return (next == S::Closing || next == S::Closed || next == S::Error);
        }
        return false;
    }();

    if (!legal) {
        spdlog::debug("ConnectionFsm: illegal transition {} -> {} (fd={})", to_string(from),
                      to_string(next), fd_);
        return;
    }
    spdlog::debug("ConnectionFsm: {} -> {} (fd={})", to_string(from), to_string(next), fd_);
    state_ = next;
    if (auto* impl = static_cast<TinyAccessImpl*>(impl_.get())) {
        impl->metrics.transitions++;
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
            impl->snapshots.emplace_back(e);
            if (impl->snapshots.size() > 256) {
                impl->snapshots.erase(impl->snapshots.begin(), impl->snapshots.begin() + 128);
            }
        }
        // Switch active state implementation
        impl->current = state_for(state_);
    }
}

void ConnectionFsm::on_accept(int fd) {
    if (auto* impl = static_cast<TinyAccessImpl*>(impl_.get()))
        impl->last_event = "accept";
    fd_ = fd;
    if (auto* impl = static_cast<TinyAccessImpl*>(impl_.get()); impl && impl->current) {
        auto next = impl->current->on_accept(fd);
        if (next.has)
            transition(to_public(next.state));
    }
}

void ConnectionFsm::on_connect(int fd) {
    if (auto* impl = static_cast<TinyAccessImpl*>(impl_.get()))
        impl->last_event = "connect";
    fd_ = fd;
    if (auto* impl = static_cast<TinyAccessImpl*>(impl_.get()); impl && impl->current) {
        auto next = impl->current->on_connect(fd);
        if (next.has)
            transition(to_public(next.state));
    }
}

void ConnectionFsm::on_readable(std::size_t) {
    if (auto* impl = static_cast<TinyAccessImpl*>(impl_.get())) {
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
            transition(State::ReadingHeader);
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
    if (auto* impl = static_cast<TinyAccessImpl*>(impl_.get()))
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
    if (auto* impl = static_cast<TinyAccessImpl*>(impl_.get()))
        impl->last_event = "header_parsed";
    if (auto* impl = static_cast<TinyAccessImpl*>(impl_.get()); impl && impl->current) {
        auto next = impl->current->on_header_parsed(info.payload_size);
        if (next.has)
            transition(to_public(next.state));
    }
}

void ConnectionFsm::on_body_parsed() {
    if (auto* impl = static_cast<TinyAccessImpl*>(impl_.get())) {
        impl->last_event = "body_parsed";
        impl->metrics.payload_reads_completed++;
        if (impl->cfg.enable_metrics) {
            YAMS_PLOT("daemon_fsm_payload_reads",
                      static_cast<int64_t>(impl->metrics.payload_reads_completed));
        }
    }
    if (auto* impl = static_cast<TinyAccessImpl*>(impl_.get()); impl && impl->current) {
        auto next = impl->current->on_body_parsed();
        if (next.has) {
            transition(to_public(next.state));
            return;
        }
    }
    // Fallback: previous behavior
    if (state_ == State::ReadingPayload || state_ == State::ReadingHeader) {
        transition(State::WritingHeader);
    } else {
        spdlog::debug("ConnectionFsm::on_body_parsed ignored in state {} (fd={})",
                      to_string(state_), fd_);
    }
}

void ConnectionFsm::on_stream_next(bool done) {
    if (auto* impl = static_cast<TinyAccessImpl*>(impl_.get()))
        impl->last_event = done ? "stream_done" : "stream_next";
    if (auto* impl = static_cast<TinyAccessImpl*>(impl_.get()); impl && impl->current) {
        auto next = impl->current->on_stream_next(done);
        if (next.has) {
            transition(to_public(next.state));
            return;
        }
    }
    // Fallback
    if (state_ == State::WritingHeader || state_ == State::StreamingChunks) {
        transition(done ? State::Closing : State::StreamingChunks);
    } else {
        spdlog::debug("ConnectionFsm::on_stream_next ignored in state {} (fd={})",
                      to_string(state_), fd_);
    }
}

void ConnectionFsm::on_timeout(Operation) {
    if (auto* impl = static_cast<TinyAccessImpl*>(impl_.get()))
        impl->last_event = "timeout";
    if (auto* impl = static_cast<TinyAccessImpl*>(impl_.get()); impl && impl->current) {
        auto next = impl->current->on_timeout();
        if (next.has) {
            transition(to_public(next.state));
            return;
        }
    }
    transition(State::Error);
}

void ConnectionFsm::on_error(int) {
    if (auto* impl = static_cast<TinyAccessImpl*>(impl_.get()))
        impl->last_event = "error";
    if (auto* impl = static_cast<TinyAccessImpl*>(impl_.get()); impl && impl->current) {
        auto next = impl->current->on_error(0);
        if (next.has) {
            transition(to_public(next.state));
            return;
        }
    }
    transition(State::Error);
}

void ConnectionFsm::on_close_request() {
    if (auto* impl = static_cast<TinyAccessImpl*>(impl_.get()))
        impl->last_event = "close_request";
    if (auto* impl = static_cast<TinyAccessImpl*>(impl_.get()); impl && impl->current) {
        auto next = impl->current->on_close_request();
        if (next.has) {
            transition(to_public(next.state));
            return;
        }
    }
    // Fallback
    switch (state_) {
        case State::Closed:
        case State::Closing:
            break;
        default:
            transition(State::Closing);
            break;
    }
}

void ConnectionFsm::on_response_complete(bool close_after) {
    if (auto* impl = static_cast<TinyAccessImpl*>(impl_.get()))
        impl->last_event = close_after ? "response_complete_close" : "response_complete_keep";
    // If we are streaming or just wrote a header/payload and persistent connection is desired,
    // transition back to Connected to allow the next request. Otherwise go to Closing.
    switch (state_) {
        case State::WritingHeader:
        case State::StreamingChunks:
            transition(close_after ? State::Closing : State::Connected);
            break;
        default:
            transition(close_after ? State::Closing : State::Connected);
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

} // namespace yams::daemon
