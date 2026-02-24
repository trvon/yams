#include <yams/mcp/mcp_server.h>

#include <spdlog/spdlog.h>

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdlib>
#include <deque>
#include <exception>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>

// Platform-specific includes for non-blocking I/O
#if defined(_WIN32)
#include <fcntl.h>
#include <io.h>
#include <windows.h>
#elif !defined(YAMS_WASI)
#include <poll.h>
#include <unistd.h>
#endif

namespace yams::mcp {

// Non-blocking send: enqueue message for writer thread
void StdioTransport::sendAsync(json message) {
    if (state_.load() != TransportState::Connected) {
        return;
    }
    // Writer thread retired; fall back to immediate framed send
    send(message);
}

// Dedicated writer thread: drains queue and writes framed messages to stdout
void StdioTransport::writerLoop() { /* retired */ }

StdioTransport::StdioTransport() {
    // Ensure predictable stdio behavior. In tests, avoid changing global iostream
    // configuration so that rdbuf redirection in unit tests works as expected.
#ifndef YAMS_TESTING
#ifdef _WIN32
    // On Windows, keep sync_with_stdio(true) to ensure std::cin/cout stay synchronized
    // with the underlying C stdio buffers and Windows handles. Disabling sync causes
    // std::getline to block even when PeekNamedPipe shows data available, because
    // the C++ stream buffer becomes disconnected from the Windows pipe state.
    std::ios::sync_with_stdio(true);
#else
    std::ios::sync_with_stdio(false);
#endif
    std::cin.tie(nullptr);
#endif

    // Set stdin/stdout to binary mode on Windows to prevent CRLF translation
#ifdef _WIN32
    _setmode(_fileno(stdin), _O_BINARY);
    _setmode(_fileno(stdout), _O_BINARY);
    // Disable C-level buffering on stdout for Windows pipes - critical for MCP reliability.
    // Without this, responses may be delayed in the CRT buffer causing client timeouts.
    setvbuf(stdout, nullptr, _IONBF, 0);
    // Also disable input buffering to ensure PeekNamedPipe state matches std::cin state
    setvbuf(stdin, nullptr, _IONBF, 0);
#endif

    // MCP stdio transport: always use unbuffered output for immediate response delivery.
    // On Windows pipes, buffered output can cause MCP client timeouts because responses
    // may sit in the buffer even with explicit flush() calls. Always enable unitbuf for
    // reliable real-time communication. stderr is also unbuffered for log visibility.
    std::cout << std::unitbuf;
    std::cerr << std::unitbuf;

    // Configure receive timeout from environment, enforce a sane minimum
    if (const char* env = std::getenv("YAMS_MCP_RECV_TIMEOUT_MS"); env && *env) {
        try {
            recvTimeoutMs_ = std::stoi(env);
            if (recvTimeoutMs_ < 50)
                recvTimeoutMs_ = 50;
        } catch (...) {
            // ignore and keep default
        }
    }
    state_.store(TransportState::Connected);
    // Writer thread retired; unified outbound path in MCPServer handles ordering
    writerRunning_.store(false);
}

StdioTransport::~StdioTransport() {
    state_.store(TransportState::Closing);
    queueCv_.notify_all();
    if (writerThread_.joinable()) {
        try {
            writerThread_.join();
        } catch (...) {
            // swallow any join exceptions
        }
    }
}

void StdioTransport::send(const json& message) {
    auto currentState = state_.load();
    if (currentState == TransportState::Connected) {
        try {
            std::lock_guard<std::mutex> lock(outMutex_);
            sendSerialized(message.dump());
        } catch (const std::exception& e) {
            spdlog::error("StdioTransport::send exception: {}", e.what());
        } catch (...) {
            spdlog::error("StdioTransport::send unknown exception");
        }
    }
}

void StdioTransport::sendNdjson(const json& message) {
    // For stdio transport, sendNdjson() is identical to send()
    // Both output MCP spec-compliant NDJSON
    send(message);
}

void StdioTransport::sendFramedSerialized(const std::string& payload) {
    if (state_.load() != TransportState::Connected) {
        return;
    }
    try {
        std::lock_guard<std::mutex> lock(outMutex_);
        sendSerialized(payload);
    } catch (const std::exception& e) {
        spdlog::error("StdioTransport::sendFramedSerialized exception: {}", e.what());
    } catch (...) {
        spdlog::error("StdioTransport::sendFramedSerialized unknown exception");
    }
}

void StdioTransport::sendSerialized(const std::string& payload) {
    // Per MCP spec: always output NDJSON (newline-delimited JSON)
    // regardless of input framing mode. The lastFraming_ variable
    // is only used for input parsing, not output formatting.
    auto& out = std::cout;
    out << payload << "\n";
    out.flush();
}

bool StdioTransport::isInputAvailable(int timeoutMs) const {
#if defined(_WIN32)
    // Windows: Prefer PeekNamedPipe for pipes, fallback to console wait
    HANDLE stdinHandle = GetStdHandle(STD_INPUT_HANDLE);
    if (stdinHandle == INVALID_HANDLE_VALUE || stdinHandle == nullptr) {
        return false;
    }
    DWORD fileType = GetFileType(stdinHandle);
    if (fileType == FILE_TYPE_PIPE) {
        DWORD bytesAvail = 0;
        if (PeekNamedPipe(stdinHandle, nullptr, 0, nullptr, &bytesAvail, nullptr)) {
            if (bytesAvail > 0)
                return true;
            // Simple timed wait: sleep for the timeout and check again once
            if (timeoutMs > 0) {
                Sleep(static_cast<DWORD>(timeoutMs));
                bytesAvail = 0;
                if (PeekNamedPipe(stdinHandle, nullptr, 0, nullptr, &bytesAvail, nullptr)) {
                    return bytesAvail > 0;
                }
            }
            return false;
        }
        // If PeekNamedPipe fails, fall back to a short sleep
        if (timeoutMs > 0)
            Sleep(static_cast<DWORD>(timeoutMs));
        return false;
    }
    // Console input or unknown: WaitForSingleObject is acceptable
    DWORD waitResult = WaitForSingleObject(stdinHandle, static_cast<DWORD>(timeoutMs));
    return waitResult == WAIT_OBJECT_0;
#elif defined(YAMS_WASI)
    // WASI: no poll/PeekNamedPipe. Treat input availability as unknown; allow receive() to try.
    (void)timeoutMs;
    return true;
#else
    // Unix/Linux/macOS implementation using poll
    struct pollfd fds;
    fds.fd = STDIN_FILENO;
    fds.events = POLLIN | POLLHUP;
    fds.revents = 0;

    int result = poll(&fds, 1, timeoutMs);

    if (result == -1) {
        if (errno == EINTR) {
            // Signal interrupted, check shutdown
            if (externalShutdown_ && !*externalShutdown_) {
                return false;
            }
        }
        return false;
    } else if (result == 0) {
        // Timeout - check shutdown
        if (externalShutdown_ && !*externalShutdown_) {
            return false;
        }
    }

    // Treat POLLHUP as readable too.
    // Some stdio clients can close stdin after writing, and C++ iostreams may still have
    // buffered bytes to consume even if the fd no longer reports POLLIN.
    return result > 0 && ((fds.revents & POLLIN) || (fds.revents & POLLHUP));
#endif
}

MessageResult StdioTransport::receive() {
    if (state_.load() != TransportState::Connected) {
        return Error{ErrorCode::NetworkError, "Transport not connected"};
    }
    std::streambuf* inputBuffer = std::cin.rdbuf();
    auto* stringBuffer = dynamic_cast<std::stringbuf*>(inputBuffer);
    constexpr std::size_t kTestingIdleSpinLimit = 200;
    std::size_t idleIterations = 0;

    auto is_ws = [](unsigned char c) {
        return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v';
    };

    auto sanitizeLine = [&](std::string& line) {
        if (!line.empty() && line.back() == '\r')
            line.pop_back();

        // Some clients may send leading whitespace (or whitespace-only keepalive lines).
        auto firstNonWs =
            std::find_if_not(line.begin(), line.end(), [&](unsigned char c) { return is_ws(c); });
        if (firstNonWs == line.end()) {
            line.clear();
        } else if (firstNonWs != line.begin()) {
            line.erase(line.begin(), firstNonWs);
        }

        // Tolerate occasional record separators / BOMs that may prefix JSON.
        if (line.size() >= 3 && static_cast<unsigned char>(line[0]) == 0xEF &&
            static_cast<unsigned char>(line[1]) == 0xBB &&
            static_cast<unsigned char>(line[2]) == 0xBF) {
            line.erase(0, 3);
        }
        while (!line.empty()) {
            unsigned char c = static_cast<unsigned char>(line.front());
            if (c == '{' || c == '[') {
                break;
            }
            if (is_ws(c) || c == 0x1e || c < 0x20) {
                line.erase(0, 1);
                continue;
            }
            break;
        }
    };

    while (state_.load() != TransportState::Closing) {
        // MCP stdio spec: Read NDJSON (newline-delimited JSON)
        std::string line;

        if (stringBuffer) {
            // Tests: avoid blocking forever.
            if (stringBuffer->in_avail() <= 0) {
                if (externalShutdown_ && !*externalShutdown_) {
                    state_.store(TransportState::Closing);
                    return Error{ErrorCode::NetworkError, "External shutdown requested"};
                }
                if (idleIterations++ >= kTestingIdleSpinLimit) {
                    state_.store(TransportState::Disconnected);
                    return Error{ErrorCode::NetworkError, "No stdin data available"};
                }
                std::this_thread::yield();
                continue;
            }
            idleIterations = 0;

            std::istream in(inputBuffer);
            if (!std::getline(in, line)) {
                recordError();
                if (!shouldRetryAfterError()) {
                    state_.store(TransportState::Disconnected);
                    return Error{ErrorCode::NetworkError, "No stdin data available"};
                }
                std::this_thread::yield();
                continue;
            }
        } else {
            // Real stdio: keep it simple and block on getline.
            // Using poll() here is fragile due to iostream buffering differences across platforms.
            if (!std::getline(std::cin, line)) {
                if (externalShutdown_ && !*externalShutdown_) {
                    state_.store(TransportState::Closing);
                    return Error{ErrorCode::NetworkError, "External shutdown requested"};
                }

                if (std::cin.eof()) {
                    spdlog::info("StdioTransport: EOF on stdin; treating as client disconnect");
                    state_.store(TransportState::Disconnected);
                    return Error{ErrorCode::NetworkError, "EOF on stdin"};
                }

                // Transient stream failure; clear and retry.
                std::cin.clear();
                continue;
            }
        }

        sanitizeLine(line);
        if (line.empty()) {
            continue;
        }

        spdlog::debug("StdioTransport: Read line: '{}'", line);

        // NDJSON (newline-delimited JSON) - MCP stdio standard format
        // Per MCP spec 2025-06-18: "Messages are delimited by newlines and MUST NOT
        // contain embedded newlines. Implementations SHOULD reject messages that
        // contain embedded newlines."
        if (!line.empty() && (line.front() == '{' || line.front() == '[')) {
            spdlog::debug("StdioTransport: Received NDJSON message (MCP stdio standard)");
            auto parsed = json_utils::parse_json(line);
            if (!parsed) {
                spdlog::error("StdioTransport: Failed to parse JSON: {}", line);
                recordError();
                if (!shouldRetryAfterError())
                    state_.store(TransportState::Error);
                return parsed.error();
            }
            resetErrorCount();
            return parsed.value();
        }

        // If the line doesn't start with '{' or '[', it's not valid NDJSON
        spdlog::error("StdioTransport: Invalid message format (expected NDJSON): {}", line);
        recordError();
        if (!shouldRetryAfterError())
            state_.store(TransportState::Error);
        return Error{ErrorCode::InvalidData, "Invalid message format - expected NDJSON"};
    }

    return Error{ErrorCode::NetworkError, "Transport closed during receive"};
}

bool StdioTransport::shouldRetryAfterError() const noexcept {
    constexpr size_t MAX_CONSECUTIVE_ERRORS = 5;
    return errorCount_.load() < MAX_CONSECUTIVE_ERRORS;
}

void StdioTransport::recordError() noexcept {
    errorCount_.fetch_add(1);
}

void StdioTransport::resetErrorCount() noexcept {
    errorCount_.store(0);
}

} // namespace yams::mcp
