#include <gtest/gtest.h>
#include <yams/app/services/services.hpp>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>

namespace {
struct ScopedWorkingDirectory {
    std::filesystem::path original;
    explicit ScopedWorkingDirectory(const std::filesystem::path& target) {
        original = std::filesystem::current_path();
        std::filesystem::current_path(target);
    }
    ~ScopedWorkingDirectory() {
        try {
            std::filesystem::current_path(original);
        } catch (...) {
        }
    }
};
} // namespace

using yams::app::services::utils::NormalizedLookupPath;
using yams::app::services::utils::normalizeLookupPath;

TEST(NormalizeLookupPathTest, ConvertsRelativePathToCanonical) {
    namespace fs = std::filesystem;
    const auto uniqueSuffix =
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    fs::path tempRoot = fs::temp_directory_path() / ("yams_utils_test_" + uniqueSuffix);
    fs::create_directories(tempRoot);
    fs::path nestedDir = tempRoot / "nested";
    fs::create_directories(nestedDir);
    fs::path targetFile = nestedDir / "file.txt";
    {
        std::ofstream out(targetFile);
        out << "sample";
    }

    ScopedWorkingDirectory cwdScope(tempRoot);
    NormalizedLookupPath result = normalizeLookupPath("./nested/../nested/file.txt");

    EXPECT_TRUE(result.changed);
    EXPECT_FALSE(result.hasWildcards);
    EXPECT_EQ(fs::weakly_canonical(targetFile).string(), result.normalized);
    EXPECT_EQ("./nested/../nested/file.txt", result.original);

    fs::remove_all(tempRoot);
}

TEST(NormalizeLookupPathTest, LeavesGlobInputsUntouched) {
    NormalizedLookupPath result = normalizeLookupPath("docs/**/*.md");
    EXPECT_FALSE(result.changed);
    EXPECT_TRUE(result.hasWildcards);
    EXPECT_EQ("docs/**/*.md", result.normalized);
}

TEST(NormalizeLookupPathTest, LeavesDashLiteralUntouched) {
    NormalizedLookupPath result = normalizeLookupPath("-");
    EXPECT_FALSE(result.changed);
    EXPECT_FALSE(result.hasWildcards);
    EXPECT_EQ("-", result.normalized);
}
