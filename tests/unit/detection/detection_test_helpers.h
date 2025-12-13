#pragma once

#include <chrono>
#include <cstddef>
#include <filesystem>
#include <fstream>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>
#include <yams/detection/file_type_detector.h>

namespace yams::detection::test_utils {
namespace fs = std::filesystem;

inline std::string uniqueSuffix() {
    auto now = std::chrono::steady_clock::now().time_since_epoch().count();
    return std::to_string(now);
}

class ScopedCurrentPath {
public:
    explicit ScopedCurrentPath(const fs::path& newPath) : original_(fs::current_path()) {
        fs::current_path(newPath);
    }

    ~ScopedCurrentPath() {
        std::error_code ec;
        fs::current_path(original_, ec);
    }

    ScopedCurrentPath(const ScopedCurrentPath&) = delete;
    ScopedCurrentPath& operator=(const ScopedCurrentPath&) = delete;
    ScopedCurrentPath(ScopedCurrentPath&&) = delete;
    ScopedCurrentPath& operator=(ScopedCurrentPath&&) = delete;

private:
    fs::path original_;
};

class TestDirectory {
public:
    explicit TestDirectory(std::string_view name = "detection")
        : root_(fs::temp_directory_path() / ("yams_" + std::string(name) + "_" + uniqueSuffix())),
          sourceDataDir_(fs::path(__FILE__).parent_path() / "test_data"),
          testDataDir_(root_ / "test_data") {
        fs::create_directories(testDataDir_);
        copyTestDataFile("valid_magic_numbers.json");
        copyTestDataFile("invalid_magic_numbers.json");
    }

    ~TestDirectory() {
        std::error_code ec;
        fs::remove_all(root_, ec);
        FileTypeDetector::instance().clearCache();
    }

    fs::path testDataDir() const { return testDataDir_; }
    fs::path root() const { return root_; }
    fs::path validJson() const { return testDataDir_ / "valid_magic_numbers.json"; }
    fs::path invalidJson() const { return testDataDir_ / "invalid_magic_numbers.json"; }
    fs::path nonExistentJson() const { return testDataDir_ / "non_existent.json"; }

    fs::path repoMagicNumbers() const {
        auto path = fs::path(__FILE__).parent_path().parent_path().parent_path().parent_path() /
                    "data" / "magic_numbers.json";
        if (fs::exists(path)) {
            return path;
        }
        return {};
    }

    fs::path createDataFile(const fs::path& relativePath, std::string_view content) const {
        auto path = root_ / relativePath;
        fs::create_directories(path.parent_path());
        std::ofstream file(path);
        file << content;
        return path;
    }

    fs::path createBinaryFile(const std::string& name, std::span<const std::byte> data) const {
        auto path = testDataDir_ / name;
        std::ofstream file(path, std::ios::binary);
        file.write(reinterpret_cast<const char*>(data.data()),
                   static_cast<std::streamsize>(data.size()));
        return path;
    }

    fs::path writeTextFile(const std::string& name, std::string_view content) const {
        auto path = testDataDir_ / name;
        std::ofstream file(path);
        file << content;
        return path;
    }

private:
    void copyTestDataFile(const std::string& filename) const {
        auto src = sourceDataDir_ / filename;
        auto dst = testDataDir_ / filename;
        std::error_code ec;
        fs::copy_file(src, dst, fs::copy_options::overwrite_existing, ec);
        if (ec) {
            std::ofstream(dst).close();
        }
    }

    fs::path root_;
    fs::path sourceDataDir_;
    fs::path testDataDir_;
};

} // namespace yams::detection::test_utils
