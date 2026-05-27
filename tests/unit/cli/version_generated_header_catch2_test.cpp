#include <catch2/catch_test_macros.hpp>

#include <array>
#include <cstdio>
#include <regex>
#include <string>

#include <yams/version.hpp>

namespace {

std::string runCommand(const char* command) {
    std::array<char, 256> buffer{};
    std::string output;
    FILE* pipe = popen(command, "r");
    if (!pipe) {
        return {};
    }
    while (fgets(buffer.data(), static_cast<int>(buffer.size()), pipe) != nullptr) {
        output += buffer.data();
    }
    pclose(pipe);
    while (!output.empty() && (output.back() == '\n' || output.back() == '\r')) {
        output.pop_back();
    }
    return output;
}

} // namespace

TEST_CASE("Version header tracks current git commit", "[cli][version]") {
    const auto gitCommit = runCommand("git rev-parse --short=8 HEAD 2>/dev/null");
    REQUIRE_FALSE(gitCommit.empty());
    CHECK(std::string(yams::version::commit_v) == gitCommit);
}

TEST_CASE("Version header uses ISO-8601 build timestamp", "[cli][version]") {
    const std::string buildTimestamp = yams::version::build_date_v;
    CHECK(
        std::regex_match(buildTimestamp, std::regex(R"(^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$)")));
}
