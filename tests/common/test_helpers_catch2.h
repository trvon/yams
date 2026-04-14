// Test helpers for Catch2 tests (no GMock dependency)
// Based on tests/common/test_helpers.h but without GTest/GMock includes

#pragma once

#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <optional>
#include <random>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

namespace yams::test {

/**
 * @brief Creates a unique temporary directory with the given prefix.
 */
inline std::filesystem::path make_temp_dir(std::string_view prefix = "yams_test_") {
    namespace fs = std::filesystem;
    const auto base = fs::temp_directory_path();
    std::uniform_int_distribution<int> dist(0, 9999);
    thread_local std::mt19937_64 rng{std::random_device{}()};
    for (int attempt = 0; attempt < 512; ++attempt) {
        const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
        auto candidate =
            base / (std::string(prefix) + std::to_string(stamp) + "_" + std::to_string(dist(rng)));
        std::error_code ec;
        if (fs::create_directories(candidate, ec)) {
            return candidate;
        }
    }
    return base;
}

/**
 * @brief Write data to a file, creating parent directories as needed.
 */
inline std::filesystem::path write_file(const std::filesystem::path& path, std::string_view data) {
    std::filesystem::create_directories(path.parent_path());
    std::ofstream stream(path, std::ios::binary);
    stream.write(data.data(), static_cast<std::streamsize>(data.size()));
    stream.close();
    return path;
}

/**
 * @brief RAII helper to set an environment variable and restore it on scope exit.
 */
class ScopedEnvVar {
public:
    // Capture-only constructor: saves the current value of `key` for restore on destruction.
    // Does not modify the environment variable.
    explicit ScopedEnvVar(std::string key) : key_(std::move(key)), previous_(get_env(key_)) {}

    // Primary constructor taking string key and optional string value.
    // Saves the current value and sets (or unsets if nullopt) the variable.
    ScopedEnvVar(std::string key, std::optional<std::string> value)
        : key_(std::move(key)), previous_(get_env(key_)) {
        set_env(key_, std::move(value));
    }

    ScopedEnvVar(const ScopedEnvVar&) = delete;
    ScopedEnvVar& operator=(const ScopedEnvVar&) = delete;

    ScopedEnvVar(ScopedEnvVar&& other) noexcept
        : key_(std::move(other.key_)), previous_(std::move(other.previous_)),
          active_(other.active_) {
        other.active_ = false;
    }

    ScopedEnvVar& operator=(ScopedEnvVar&& other) noexcept {
        if (this != &other) {
            restore();
            key_ = std::move(other.key_);
            previous_ = std::move(other.previous_);
            active_ = other.active_;
            other.active_ = false;
        }
        return *this;
    }

    ~ScopedEnvVar() { restore(); }

private:
    static std::optional<std::string> get_env(const std::string& key) {
        if (const auto* value = std::getenv(key.c_str()); value != nullptr) {
            return std::string(value);
        }
        return std::nullopt;
    }

    static void set_env(const std::string& key, std::optional<std::string> value) {
#ifdef _WIN32
        if (value) {
            _putenv_s(key.c_str(), value->c_str());
        } else {
            // MSVC does not provide setenv/unsetenv; `_putenv("KEY=")` removes the variable.
            std::string entry = key + "=";
            _putenv(entry.c_str());
        }
#else
        if (value) {
            ::setenv(key.c_str(), value->c_str(), 1);
        } else {
            ::unsetenv(key.c_str());
        }
#endif
    }

    void restore() {
        if (active_) {
            set_env(key_, previous_);
            active_ = false;
        }
    }

    std::string key_;
    std::optional<std::string> previous_;
    bool active_ = true;
};

/**
 * @brief RAII helper that creates a unique temporary directory and removes it on destruction.
 */
class TempDirGuard {
public:
    explicit TempDirGuard(std::string_view prefix = "yams_test_") : dir_(make_temp_dir(prefix)) {}

    ~TempDirGuard() {
        std::error_code ec;
        std::filesystem::remove_all(dir_, ec);
    }

    TempDirGuard(const TempDirGuard&) = delete;
    TempDirGuard& operator=(const TempDirGuard&) = delete;

    const std::filesystem::path& path() const { return dir_; }

private:
    std::filesystem::path dir_;
};

/**
 * @brief Canonical SHA-256 test vectors and input byte buffers.
 */
struct TestVectors {
    static constexpr std::string_view EMPTY_SHA256 =
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    static constexpr std::string_view ABC_SHA256 =
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";

    static constexpr std::string_view HELLO_WORLD_SHA256 =
        "a591a6d40bf420404a011733cfb7b190d62c65bf0bcda32b57b277d9ad9f146e";

    static std::vector<std::byte> getEmptyData() { return {}; }

    static std::vector<std::byte> getABCData() {
        return {std::byte{'a'}, std::byte{'b'}, std::byte{'c'}};
    }

    static std::vector<std::byte> getHelloWorldData() {
        const char* str = "Hello World";
        return std::vector<std::byte>(reinterpret_cast<const std::byte*>(str),
                                      reinterpret_cast<const std::byte*>(str + 11));
    }
};

/**
 * @brief Poll @p predicate until it returns true or @p timeout elapses.
 * @return true if the predicate became true, false on timeout.
 */
inline bool wait_for_condition(std::chrono::milliseconds timeout,
                               std::chrono::milliseconds interval,
                               const std::function<bool()>& predicate) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) {
            return true;
        }
        std::this_thread::sleep_for(interval);
    }
    return predicate();
}

#if __has_include(<spdlog/spdlog.h>)
} // namespace yams::test
#include <spdlog/spdlog.h>
namespace yams::test {

/**
 * @brief RAII helper to save and restore the global spdlog log level.
 */
class SpdlogLevelGuard {
public:
    SpdlogLevelGuard() : previous_(spdlog::get_level()) {}
    ~SpdlogLevelGuard() { spdlog::set_level(previous_); }

    SpdlogLevelGuard(const SpdlogLevelGuard&) = delete;
    SpdlogLevelGuard& operator=(const SpdlogLevelGuard&) = delete;

private:
    spdlog::level::level_enum previous_;
};
#endif

} // namespace yams::test
