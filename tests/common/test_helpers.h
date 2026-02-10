#pragma once

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <ios>
#include <optional>
#include <random>
#include <ranges>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <yams/core/format.h>
#include <yams/core/types.h>

namespace yams::test {

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

inline std::filesystem::path write_file(const std::filesystem::path& path, std::string_view data) {
    std::filesystem::create_directories(path.parent_path());
    std::ofstream stream(path, std::ios::binary);
    stream.write(data.data(), static_cast<std::streamsize>(data.size()));
    stream.close();
    return path;
}

class ScopedEnvVar {
public:
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
        if (!active_) {
            return;
        }
        set_env(key_, previous_);
        active_ = false;
    }

    std::string key_;
    std::optional<std::string> previous_;
    bool active_{true};
};

class YamsTest : public ::testing::Test {
protected:
    void SetUp() override { testDir = make_temp_dir("yams_test_"); }

    void TearDown() override {
        std::error_code ec;
        std::filesystem::remove_all(testDir, ec);
    }

    static std::vector<std::byte> generateRandomBytes(std::size_t size) {
        std::vector<std::byte> data(size);
        std::uniform_int_distribution<int> dist(0, 255);
        std::generate(data.begin(), data.end(), [&] { return std::byte(dist(rng())); });
        return data;
    }

    static std::vector<std::byte> generatePattern(std::size_t size, std::size_t pattern = 256) {
        std::vector<std::byte> base = generateRandomBytes(pattern);
        std::vector<std::byte> out;
        out.reserve(size);
        while (out.size() < size) {
            const auto remaining = size - out.size();
            const auto to_copy = std::min(pattern, remaining);
            out.insert(out.end(), base.begin(),
                       base.begin() + static_cast<std::ptrdiff_t>(to_copy));
        }
        return out;
    }

    std::filesystem::path createTestFile(std::string_view name, std::string_view content) {
        const auto path = testDir / std::filesystem::path(name);
        write_file(path, content);
        return path;
    }

    std::filesystem::path createBinaryFile(std::string_view name,
                                           const std::vector<std::byte>& data) {
        const auto path = testDir / std::filesystem::path(name);
        std::filesystem::create_directories(path.parent_path());
        std::ofstream stream(path, std::ios::binary);
        stream.write(reinterpret_cast<const char*>(data.data()),
                     static_cast<std::streamsize>(data.size()));
        stream.close();
        return path;
    }

protected:
    std::filesystem::path testDir;

private:
    static std::mt19937_64& rng() {
        static thread_local std::mt19937_64 engine{std::random_device{}()};
        return engine;
    }
};

inline std::filesystem::path getTempDir() {
    static std::filesystem::path root = make_temp_dir("yams_shared_");
    return root;
}

MATCHER_P(BytesEqual, expected, "Byte vectors are equal") {
    return std::ranges::equal(arg, expected);
}

MATCHER_P(HasErrorCode, code, "Result has expected error code") {
    return !arg.has_value() && arg.error() == code;
}

MATCHER_P(HasValue, value, "Result has expected value") {
    return arg.has_value() && *arg == value;
}

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

} // namespace yams::test
