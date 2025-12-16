// Test helpers for Catch2 tests (no GMock dependency)
// Based on tests/common/test_helpers.h but without GTest/GMock includes

#pragma once

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <optional>
#include <random>
#include <string>
#include <string_view>

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
    // Primary constructor taking string key and optional string value
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
        _putenv_s(key.c_str(), value ? value->c_str() : "");
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

} // namespace yams::test
