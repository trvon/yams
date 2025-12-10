#pragma once

#include <atomic>
#include <chrono>
#include <filesystem>
#include <random>
#include <string>
#include <thread>

namespace yams::test_support {

// RAII guard that owns a unique temporary directory and cleans it up on destruction.
class TempDirScope {
public:
    explicit TempDirScope(std::filesystem::path root) : root_(std::move(root)) {}
    TempDirScope(const TempDirScope&) = delete;
    TempDirScope& operator=(const TempDirScope&) = delete;
    TempDirScope(TempDirScope&& other) noexcept = default;
    TempDirScope& operator=(TempDirScope&& other) noexcept = default;

    ~TempDirScope() {
        std::error_code ec;
        std::filesystem::remove_all(root_, ec);
    }

    const std::filesystem::path& path() const { return root_; }

    static TempDirScope unique_under(const std::string& base_name) {
        auto root = std::filesystem::temp_directory_path();
        auto unique = make_unique_component(base_name);
        root /= unique;
        std::error_code ec;
        std::filesystem::create_directories(root, ec);
        return TempDirScope(root);
    }

private:
    static std::string make_unique_component(const std::string& base_name) {
        // Compose base + timestamp + thread id + counter for uniqueness across shards.
        auto now = std::chrono::steady_clock::now().time_since_epoch().count();
        auto tid = std::hash<std::thread::id>{}(std::this_thread::get_id());
        auto counter = counter_.fetch_add(1, std::memory_order_relaxed);
        return base_name + "-" + std::to_string(now) + "-" + std::to_string(tid) + "-" + std::to_string(counter);
    }

    std::filesystem::path root_;
    static inline std::atomic<uint64_t> counter_{0};
};

} // namespace yams::test_support
