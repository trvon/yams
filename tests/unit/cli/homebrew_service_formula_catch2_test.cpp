#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

namespace fs = std::filesystem;
using Catch::Matchers::ContainsSubstring;

namespace {

fs::path sourceRoot() {
    if (const char* root = std::getenv("YAMS_SOURCE_DIR")) {
        return fs::path(root);
    }
    if (const char* root = std::getenv("MESON_SOURCE_ROOT")) {
        return fs::path(root);
    }
    return fs::current_path();
}

std::string readText(const fs::path& path) {
    std::ifstream input(path);
    REQUIRE(input.good());
    std::ostringstream buffer;
    buffer << input.rdbuf();
    return buffer.str();
}

} // namespace

TEST_CASE("Homebrew service formulas run yams-daemon in foreground",
          "[cli][homebrew][service][formula][catch2]") {
    const fs::path root = sourceRoot();
    const std::vector<fs::path> formulaPaths = {
        root / "packaging/homebrew/yams.rb.template",
        root / "packaging/homebrew/yams-nightly.rb.template",
        root / "homebrew-yams/Formula/yams.rb",
        root / "homebrew-yams/Formula/yams-nightly.rb",
    };

    for (const auto& path : formulaPaths) {
        INFO(path.string());
        REQUIRE(fs::exists(path));
        const std::string text = readText(path);
        CHECK_THAT(text, ContainsSubstring("run [opt_bin/\"yams-daemon\", \"--foreground\"]"));
    }
}
