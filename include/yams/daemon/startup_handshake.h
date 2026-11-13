// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#ifndef _WIN32

#include <cerrno>
#include <csignal>
#include <optional>

#include <fcntl.h>
#include <unistd.h>

namespace yams::daemon {

/// Pipe between the process that launches a detaching daemon and the detached daemon, so the
/// launcher's exit status is the daemon's start result rather than "fork succeeded".
///
/// Open it before fork(). The launcher calls await(); the daemon calls report() once start-up
/// has succeeded or failed. A daemon that dies first closes its end, which reads as failure.
class StartupHandshake {
public:
    static std::optional<StartupHandshake> open() {
        int fds[2] = {-1, -1};
        if (::pipe(fds) != 0) {
            return std::nullopt;
        }
        StartupHandshake handshake;
        handshake.readFd_ = fds[0];
        handshake.writeFd_ = fds[1];
        // The daemon closes and reopens descriptors 0-2 while detaching.
        if (handshake.writeFd_ <= STDERR_FILENO) {
            const int moved = ::fcntl(handshake.writeFd_, F_DUPFD, STDERR_FILENO + 1);
            if (moved < 0) {
                return std::nullopt; // destructor closes both ends
            }
            ::close(handshake.writeFd_);
            handshake.writeFd_ = moved;
        }
        // Plugin and helper processes the daemon spawns must not hold the launcher open.
        (void)::fcntl(handshake.readFd_, F_SETFD, FD_CLOEXEC);
        (void)::fcntl(handshake.writeFd_, F_SETFD, FD_CLOEXEC);
        return handshake;
    }

    StartupHandshake(StartupHandshake&& other) noexcept
        : readFd_(other.readFd_), writeFd_(other.writeFd_) {
        other.readFd_ = -1;
        other.writeFd_ = -1;
    }
    StartupHandshake& operator=(StartupHandshake&& other) noexcept {
        if (this != &other) {
            closeReader();
            closeWriter();
            readFd_ = other.readFd_;
            writeFd_ = other.writeFd_;
            other.readFd_ = -1;
            other.writeFd_ = -1;
        }
        return *this;
    }
    StartupHandshake(const StartupHandshake&) = delete;
    StartupHandshake& operator=(const StartupHandshake&) = delete;

    ~StartupHandshake() {
        closeReader();
        closeWriter();
    }

    /// Daemon side: publish the start result and release the launcher. The daemon should
    /// have closed its copy of the reading end (closeReader) right after fork.
    void report(bool started) {
        if (writeFd_ < 0) {
            return;
        }
        // A launcher that is already gone must not take the daemon down with SIGPIPE.
        struct sigaction ignore{};
        struct sigaction previous{};
        ignore.sa_handler = SIG_IGN;
        sigemptyset(&ignore.sa_mask);
        const bool guarded = ::sigaction(SIGPIPE, &ignore, &previous) == 0;
        const char status = started ? '0' : '1';
        ssize_t written = 0;
        do {
            written = ::write(writeFd_, &status, 1);
        } while (written < 0 && errno == EINTR);
        if (guarded) {
            (void)::sigaction(SIGPIPE, &previous, nullptr);
        }
        closeWriter();
    }

    /// Launcher side: block until the daemon reports or exits. Returns the exit status the
    /// launcher should use: 0 when the daemon started, 1 otherwise.
    [[nodiscard]] int await() {
        closeWriter();
        if (readFd_ < 0) {
            return 1;
        }
        char status = '1';
        ssize_t got = 0;
        do {
            got = ::read(readFd_, &status, 1);
        } while (got < 0 && errno == EINTR);
        closeReader();
        return (got == 1 && status == '0') ? 0 : 1;
    }

    void closeReader() {
        if (readFd_ >= 0) {
            ::close(readFd_);
            readFd_ = -1;
        }
    }
    void closeWriter() {
        if (writeFd_ >= 0) {
            ::close(writeFd_);
            writeFd_ = -1;
        }
    }
    [[nodiscard]] int writerFd() const { return writeFd_; }

private:
    StartupHandshake() = default;

    int readFd_{-1};
    int writeFd_{-1};
};

} // namespace yams::daemon

#endif // !_WIN32
