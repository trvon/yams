// Minimal fixture manager placeholder to satisfy includes in integration tests.
#pragma once
#include <filesystem>
#include <string>

namespace yams::tests {
struct FixtureManager {
    std::filesystem::path root;
    explicit FixtureManager(std::filesystem::path r = std::filesystem::temp_directory_path())
        : root(std::move(r)) {}
    std::filesystem::path path(const std::string& name) const { return root / name; }
};
} // namespace yams::tests
