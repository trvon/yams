#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <thread>
#include <vector>

#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>

#include <yams/daemon/ipc/async_socket.h>

namespace yams::daemon::test {

using namespace std::chrono_literals;

// Helper: RAII thread that runs AsyncIOContext::run() until stop() is called
class IoThread {
public:
    explicit IoThread(AsyncIOContext& ctx) : ctx_(ctx) {
        th_ = std::jthread([this](std::stop_token st) {
            // Run in a soft loop; AsyncIOContext::run() internally uses a timed kevent wait.
            while (!st.stop_requested()) {
                ctx_.run();
                // Small breather; run() returns periodically
                std::this_thread::sleep_for(1ms);
            }
        });
    }

    ~IoThread() { stop(); }

    void stop() {
        if (stopped_.exchange(true))
            return;
        ctx_.stop();
        if (th_.joinable()) {
            th_.request_stop();
            th_.join();
        }
    }

private:
    AsyncIOContext& ctx_;
    std::jthread th_;
    std::atomic<bool> stopped_{false};
};

// Minimal coroutine helpers using the project's Task type

// Attempt to write 'bytes' of data; intended to create backpressure and leave pending ops.
Task<Result<void>> pump_write(AsyncSocket& sock, size_t bytes) {
    std::vector<uint8_t> payload(bytes, 0xAB);
    co_return co_await sock.async_write_all(payload);
}

// Attempt to read exactly 'bytes' from socket; will pend until peer writes or closes.
Task<Result<std::vector<uint8_t>>> read_exact(AsyncSocket& sock, size_t bytes) {
    co_return co_await sock.async_read_exact(bytes);
}

// Create a UNIX socketpair for controlled in-process testing.
static inline void make_socketpair(int fds[2]) {
    int rc = ::socketpair(AF_UNIX, SOCK_STREAM, 0, fds);
    ASSERT_EQ(rc, 0) << "socketpair failed: " << std::strerror(errno);
}

// This test exercises a race where a pending write is cancelled due to socket.close()
// while the IO loop may still deliver a WRITE event. The expected behavior is that no
// crash occurs (no use-after-free), and the IO loop safely cancels the awaiter.
TEST(AsyncSocketRaceTest, CancelPendingWriteOnClose) {
    AsyncIOContext io;
    IoThread runner(io);

    int fds[2]{-1, -1};
    make_socketpair(fds);

    {
        // Wrap one end with AsyncSocket, leave the peer unread to create backpressure.
        AsyncSocket sock(fds[0], io);
        // Peer remains open but not reading to force EAGAIN during write
        int peer_fd = fds[1];

        // Schedule a large write to ensure it doesn't complete eagerly.
        auto task = pump_write(sock, 8 * 1024 * 1024); // 8MB

        // Give the IO loop a moment to register the write
        std::this_thread::sleep_for(5ms);

        // Immediately close the socket to trigger cancellation of pending ops.
        sock.close();

        // Keep peer around briefly, then close it
        std::this_thread::sleep_for(10ms);
        ::close(peer_fd);

        // Let IO loop drain any pending events safely
        std::this_thread::sleep_for(20ms);

        // Test passes if no crash; result is not asserted here to avoid dependency on Task join
        // helpers.
    }

    runner.stop();
}

// This test covers read-side cancellation: start an exact read and then close the socket
// so that a stale READ event does not cause a crash when processed by the IO loop.
TEST(AsyncSocketRaceTest, CancelPendingReadOnClose) {
    AsyncIOContext io;
    IoThread runner(io);

    int fds[2]{-1, -1};
    make_socketpair(fds);

    {
        AsyncSocket sock(fds[0], io);
        int peer_fd = fds[1];

        // Start a read that cannot complete immediately (peer not writing yet)
        auto task = read_exact(sock, 1024 * 1024); // 1MB

        // Allow registration
        std::this_thread::sleep_for(5ms);

        // Close the reading side early to cancel the pending op
        sock.close();

        // Optionally, write some data into peer to generate events after close
        // Then close peer.
        std::vector<uint8_t> dummy(4096, 0);
        (void)::send(peer_fd, dummy.data(), dummy.size(), 0);
        std::this_thread::sleep_for(5ms);
        ::close(peer_fd);

        // Let IO loop process any late events safely
        std::this_thread::sleep_for(20ms);
    }

    runner.stop();
}

// This test intentionally creates a "stale event" scenario: submit I/O, unregister the socket,
// and then ensure that the IO loop ignores/handles kqueue returns without attempting unsafe
// resumes.
TEST(AsyncSocketRaceTest, StaleEventAfterUnregister) {
    AsyncIOContext io;
    IoThread runner(io);

    int fds[2]{-1, -1};
    make_socketpair(fds);

    {
        AsyncSocket sock(fds[0], io);
        int peer_fd = fds[1];

        // Kick off a small write (likely to succeed immediately), but the loop may still
        // add a WRITE oneshot. We close immediately to force unregister.
        auto task = pump_write(sock, 256 * 1024); // 256KB

        // Small delay to let submit_write run
        std::this_thread::sleep_for(2ms);

        // Force close/unregister; subsequent kevent notifications must be handled safely
        sock.close();

        // Nudge peer (optional)
        ::close(peer_fd);

        // Give IO loop a little time to process any stale notifications
        std::this_thread::sleep_for(15ms);
    }

    runner.stop();
}

} // namespace yams::daemon::test