#pragma once
// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

// Shared helpers for direct-P2P unit tests. bytes/text/TempDir were previously
// copy-pasted across daemon/p2p test executables; keep them here so behavior
// stays identical across suites.

#include <chrono>
#include <cstring>
#include <filesystem>
#include <string>
#include <string_view>
#include <vector>

namespace yams::p2p_test {

inline std::vector<std::byte> bytes(std::string_view text) {
    std::vector<std::byte> result(text.size());
    std::memcpy(result.data(), text.data(), text.size());
    return result;
}

inline std::string text(const std::vector<std::byte>& data) {
    return std::string(reinterpret_cast<const char*>(data.data()), data.size());
}

/// Scoped directory under the system temp dir, removed on destruction. The
/// per-run timestamp suffix keeps concurrent test processes from colliding.
class TempDir {
public:
    explicit TempDir(std::string_view label) {
        path = std::filesystem::temp_directory_path() /
               ("yams-p2p-test-" + std::string(label) + "-" +
                std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        std::filesystem::create_directories(path);
    }
    TempDir(const TempDir&) = delete;
    TempDir& operator=(const TempDir&) = delete;
    ~TempDir() {
        std::error_code error;
        std::filesystem::remove_all(path, error);
    }

    std::filesystem::path path;
};

} // namespace yams::p2p_test
