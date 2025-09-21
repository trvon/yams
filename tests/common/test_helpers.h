// Minimal test helpers scaffold for legacy includes
#pragma once
#include <filesystem>
#include <string>
#include <vector>

namespace yams::tests {
inline std::filesystem::path make_temp_dir(const std::string& prefix = "yams_test_") {
    auto base = std::filesystem::temp_directory_path();
    for (int i = 0; i < 1000; ++i) {
        auto p = base / (prefix + std::to_string(::getpid()) + "_" + std::to_string(i));
        if (std::filesystem::create_directories(p))
            return p;
    }
    return base;
}

inline std::filesystem::path write_file(const std::filesystem::path& p, const std::string& data) {
    std::filesystem::create_directories(p.parent_path());
    std::ofstream ofs(p, std::ios::binary);
    ofs << data;
    return p;
}
} // namespace yams::tests
