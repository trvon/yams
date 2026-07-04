#include <catch2/catch_test_macros.hpp>

#include <array>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <regex>
#include <string>

namespace {

FILE* openPipe(const char* command, const char* mode) {
#ifdef _WIN32
    return _popen(command, mode);
#else
    return popen(command, mode);
#endif
}

int closePipe(FILE* pipe) {
#ifdef _WIN32
    return _pclose(pipe);
#else
    return pclose(pipe);
#endif
}

std::string runCommand(const std::string& command) {
    std::array<char, 256> buffer{};
    std::string output;
    FILE* pipe = openPipe(command.c_str(), "r");
    if (!pipe) {
        return {};
    }
    while (fgets(buffer.data(), static_cast<int>(buffer.size()), pipe) != nullptr) {
        output += buffer.data();
    }
    closePipe(pipe);
    while (!output.empty() && (output.back() == '\n' || output.back() == '\r')) {
        output.pop_back();
    }
    return output;
}

std::string shellQuote(const std::string& value) {
    std::string quoted{"'"};
    for (char ch : value) {
        if (ch == '\'') {
            quoted += "'\\''";
        } else {
            quoted += ch;
        }
    }
    quoted += "'";
    return quoted;
}

std::filesystem::path findSourceRoot() {
    if (const char* sourceDir = std::getenv("YAMS_SOURCE_DIR")) {
        return sourceDir;
    }

    std::error_code ec;
    auto cwd = std::filesystem::current_path(ec);
    if (ec) {
        return {};
    }
    while (!cwd.empty()) {
        if (std::filesystem::exists(cwd / ".git") || std::filesystem::exists(cwd / "meson.build")) {
            return cwd;
        }
        auto parent = cwd.parent_path();
        if (parent == cwd) {
            break;
        }
        cwd = parent;
    }
    return {};
}

std::filesystem::path findGeneratedVersionHeader() {
    if (const char* buildRoot = std::getenv("MESON_BUILD_ROOT")) {
        auto candidate = std::filesystem::path(buildRoot) / "version_generated.h";
        if (std::filesystem::exists(candidate)) {
            return candidate;
        }
    }

    std::error_code ec;
    auto cwd = std::filesystem::current_path(ec);
    if (!ec) {
        auto candidate = cwd / "version_generated.h";
        if (std::filesystem::exists(candidate)) {
            return candidate;
        }
    }

    return {};
}

std::string readFile(const std::filesystem::path& path) {
    std::ifstream input(path);
    if (!input) {
        return {};
    }
    return std::string((std::istreambuf_iterator<char>(input)), std::istreambuf_iterator<char>());
}

std::string extractDefine(const std::string& text, const char* name) {
    const std::regex pattern(std::string("#define\\s+") + name + "\\s+\"([^\"]*)\"");
    std::smatch match;
    if (!std::regex_search(text, match, pattern)) {
        return {};
    }
    return match[1].str();
}

} // namespace

TEST_CASE("Version header tracks current git commit", "[cli][version]") {
    const auto sourceRoot = findSourceRoot();
    REQUIRE_FALSE(sourceRoot.empty());

    const auto quotedRoot = shellQuote(sourceRoot.string());
    const auto gitCommit = runCommand("git -c safe.directory=" + quotedRoot + " -C " + quotedRoot +
                                      " rev-parse --short=8 HEAD");
    REQUIRE_FALSE(gitCommit.empty());

    const auto headerPath = findGeneratedVersionHeader();
    REQUIRE_FALSE(headerPath.empty());
    const auto headerText = readFile(headerPath);
    REQUIRE_FALSE(headerText.empty());

    CHECK((extractDefine(headerText, "YAMS_GIT_COMMIT") == gitCommit));
}

TEST_CASE("Version header uses ISO-8601 build timestamp", "[cli][version]") {
    const auto headerPath = findGeneratedVersionHeader();
    REQUIRE_FALSE(headerPath.empty());
    const auto headerText = readFile(headerPath);
    REQUIRE_FALSE(headerText.empty());

    const std::string buildTimestamp = extractDefine(headerText, "YAMS_BUILD_TIMESTAMP");
    CHECK(
        std::regex_match(buildTimestamp, std::regex(R"(^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$)")));
}
