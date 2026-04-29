#include <yams/daemon/client/sandbox_probe.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <mutex>
#include <random>
#include <string>
#include <utility>

#include <spdlog/spdlog.h>

#if defined(_WIN32)
#include <winsock2.h>
#else
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#endif

namespace yams::daemon {
namespace {

std::mutex& cache_mutex() {
    static std::mutex m;
    return m;
}

std::atomic<bool>& cache_valid() {
    static std::atomic<bool> v{false};
    return v;
}

std::atomic<bool>& cached_result() {
    static std::atomic<bool> r{true};
    return r;
}

std::atomic<long long>& cached_duration_ns() {
    static std::atomic<long long> d{0};
    return d;
}

UnixSocketProbeFn& test_probe_slot() {
    static UnixSocketProbeFn slot;
    return slot;
}

std::filesystem::path probe_temp_dir() {
    if (const char* xdg = std::getenv("XDG_RUNTIME_DIR"); xdg && *xdg) {
        std::error_code ec;
        if (std::filesystem::is_directory(xdg, ec)) {
            return std::filesystem::path(xdg);
        }
    }
    if (const char* tmp = std::getenv("TMPDIR"); tmp && *tmp) {
        std::error_code ec;
        if (std::filesystem::is_directory(tmp, ec)) {
            return std::filesystem::path(tmp);
        }
    }
    return std::filesystem::temp_directory_path();
}

std::string probe_socket_path() {
    std::random_device rd;
    std::mt19937_64 rng(rd());
    auto suffix = rng();
    auto dir = probe_temp_dir();
    return (dir /
            ("yams-probe-" + std::to_string(::getpid()) + "-" + std::to_string(suffix) + ".sock"))
        .string();
}

bool real_unix_socket_probe() {
#if defined(_WIN32)
    return true;
#else
    int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        return false;
    }

    const std::string path = probe_socket_path();
    sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    if (path.size() + 1 > sizeof(addr.sun_path)) {
        ::close(fd);
        return false;
    }
    std::memcpy(addr.sun_path, path.c_str(), path.size() + 1);

    bool permitted = true;
    if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        if (errno == EPERM || errno == EACCES) {
            permitted = false;
        }
    }

    ::close(fd);
    std::error_code ec;
    std::filesystem::remove(path, ec);
    return permitted;
#endif
}

// Caller must already hold cache_mutex().
bool run_probe_locked() {
    UnixSocketProbeFn override_fn = test_probe_slot();
    auto start = std::chrono::steady_clock::now();
    bool result = override_fn ? override_fn() : real_unix_socket_probe();
    auto elapsed = std::chrono::steady_clock::now() - start;
    cached_duration_ns().store(
        std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count(),
        std::memory_order_release);
    return result;
}

} // namespace

bool unix_socket_io_permitted() {
    if (cache_valid().load(std::memory_order_acquire)) {
        return cached_result().load(std::memory_order_acquire);
    }
    std::lock_guard<std::mutex> lk(cache_mutex());
    if (cache_valid().load(std::memory_order_acquire)) {
        return cached_result().load(std::memory_order_acquire);
    }
    bool result = run_probe_locked();
    cached_result().store(result, std::memory_order_release);
    cache_valid().store(true, std::memory_order_release);
    spdlog::debug("[SandboxProbe] AF_UNIX permitted={} ({} ns)", result,
                  cached_duration_ns().load(std::memory_order_acquire));
    return result;
}

void set_unix_socket_probe_for_tests(UnixSocketProbeFn probe) {
    std::lock_guard<std::mutex> lk(cache_mutex());
    test_probe_slot() = std::move(probe);
    cache_valid().store(false, std::memory_order_release);
    cached_duration_ns().store(0, std::memory_order_release);
}

std::chrono::nanoseconds last_unix_socket_probe_duration() {
    return std::chrono::nanoseconds{cached_duration_ns().load(std::memory_order_acquire)};
}

} // namespace yams::daemon
