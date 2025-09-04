#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <filesystem>
// Intentionally avoid heavy project headers here to keep adapter minimal.

namespace yams {
namespace daemon {

// Lightweight adapter for a per-connection state machine.
// Implementation is intentionally minimal for now and does not expose any 3P types.
// We will switch internals to tinyfsm after vendoring without changing this interface.
class ConnectionFsm {
public:
    enum class State {
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

    struct FrameInfo {
        uint32_t message_type{0};
        uint32_t flags{0};
        uint64_t payload_size{0};
    };

    enum class Operation { Read, Write, Header, Body, Stream };

    ConnectionFsm();
    ~ConnectionFsm();
    ConnectionFsm(const ConnectionFsm&) = delete;
    ConnectionFsm& operator=(const ConnectionFsm&) = delete;
    ConnectionFsm(ConnectionFsm&&) noexcept;
    ConnectionFsm& operator=(ConnectionFsm&&) noexcept;

    State state() const noexcept { return state_; }

    // Liveness helpers: treat any state prior to Closed as alive; Closing is still alive until
    // socket teardown completes. Error is considered not-alive for new operations.
    bool alive() const noexcept {
        switch (state_) {
            case State::Closed:
                return false;
            case State::Error:
                return false;
            default:
                return true;
        }
    }
    bool is_closing() const noexcept { return state_ == State::Closing; }

    // Optional runtime configuration (kept simple to avoid heavy headers)
    // Milliseconds-based timeouts to avoid std::chrono in the public header.
    void set_header_timeout_ms(uint32_t ms) noexcept;
    void set_payload_timeout_ms(uint32_t ms) noexcept;
    void set_max_retries(std::size_t n) noexcept;
    void enable_metrics(bool on) noexcept;
    void enable_snapshots(bool on) noexcept;
    // Dump recent state snapshots to the log (no output parameters for header minimalism)
    void debug_dump_snapshots(std::size_t max_entries = 10) const noexcept;

    // Event ingress (call on IO thread)
    void on_accept(int fd);
    void on_connect(int fd);
    void on_readable(std::size_t n);
    void on_writable(std::size_t n);
    void on_header_parsed(const FrameInfo& info);
    void on_body_parsed();
    void on_stream_next(bool done);
    void on_timeout(Operation op);
    void on_error(int err);
    void on_close_request();
    // Called by server after a full response (non-streaming or end of streaming) has been sent.
    // If close_after is true, transitions toward Closing; otherwise returns to Connected so
    // another request can be read on the same persistent connection.
    void on_response_complete(bool close_after);

    // State validation helpers
    bool can_read() const noexcept {
        return state_ == State::Connected || state_ == State::ReadingHeader ||
               state_ == State::ReadingPayload;
    }
    bool can_write() const noexcept {
        return state_ == State::Connected || state_ == State::WritingHeader ||
               state_ == State::StreamingChunks;
    }
    void validate_operation(Operation op) const {
        switch (op) {
            case Operation::Read:
                if (!can_read())
                    throw std::runtime_error("Invalid read in state");
                break;
            case Operation::Write:
                if (!can_write())
                    throw std::runtime_error("Invalid write in state");
                break;
            default:
                break;
        }
    }

    // Debugging helper
    static const char* to_string(State s) noexcept;

    // Centralized socket path resolution (unified for client/server/CLI)
    static std::filesystem::path resolve_socket_path();
    static std::filesystem::path resolve_socket_path_config_first();

private:
    // Internal access for the tinyfsm-backed implementation (kept private API stable)
    struct TinyDeleter {
        void operator()(void*) const noexcept;
    };
    void transition(State next) noexcept;
    State state_{State::Disconnected};
    int fd_{-1};
    // Pimpl to hide tinyfsm, metrics, and snapshot details from the header
    // Use void* to avoid incomplete-type deletion at compile sites that only see this header
    std::unique_ptr<void, TinyDeleter> impl_;
};

} // namespace daemon
} // namespace yams
