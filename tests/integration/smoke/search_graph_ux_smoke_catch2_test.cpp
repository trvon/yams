#include <catch2/catch_test_macros.hpp>

#include <nlohmann/json.hpp>

#include <algorithm>
#include <exception>
#include <filesystem>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

#include "common/test_helpers_catch2.h"

#include <yams/cli/yams_cli.h>
#include <yams/daemon/client/global_io_context.h>

namespace fs = std::filesystem;

namespace {

class CaptureStdout {
public:
    CaptureStdout() : old_(std::cout.rdbuf(buffer_.rdbuf())) {}
    ~CaptureStdout() { std::cout.rdbuf(old_); }

    std::string str() const { return buffer_.str(); }

private:
    std::ostringstream buffer_;
    std::streambuf* old_{nullptr};
};

class ScopedCurrentPath {
public:
    explicit ScopedCurrentPath(const fs::path& target) : original_(fs::current_path()) {
        fs::current_path(target);
    }

    ~ScopedCurrentPath() {
        std::error_code ec;
        fs::current_path(original_, ec);
    }

    ScopedCurrentPath(const ScopedCurrentPath&) = delete;
    ScopedCurrentPath& operator=(const ScopedCurrentPath&) = delete;

private:
    fs::path original_;
};

int run_cli(const std::vector<std::string>& args, std::string* output = nullptr,
            std::optional<std::string> stdinData = std::nullopt) {
    std::vector<std::string> effectiveArgs = args;
    const bool hasDataDirFlag =
        std::find(effectiveArgs.begin(), effectiveArgs.end(), "--data-dir") !=
            effectiveArgs.end() ||
        std::find(effectiveArgs.begin(), effectiveArgs.end(), "--storage") != effectiveArgs.end();
    if (!hasDataDirFlag) {
        if (const char* dataDir = std::getenv("YAMS_DATA_DIR"); dataDir && *dataDir) {
            effectiveArgs.insert(effectiveArgs.begin() + 1, std::string(dataDir));
            effectiveArgs.insert(effectiveArgs.begin() + 1, "--data-dir");
        }
    }

    int rc = 0;
    std::string captured;
    try {
        yams::cli::YamsCLI cli;
        std::vector<char*> argv;
        argv.reserve(effectiveArgs.size());
        for (const auto& arg : effectiveArgs) {
            argv.push_back(const_cast<char*>(arg.c_str()));
        }

        CaptureStdout capture;

        std::istringstream in;
        std::streambuf* oldIn = nullptr;
        if (stdinData.has_value()) {
            in.str(*stdinData);
            oldIn = std::cin.rdbuf(in.rdbuf());
        }

        rc = cli.run(static_cast<int>(argv.size()), argv.data());

        if (oldIn) {
            std::cin.rdbuf(oldIn);
        }
        captured = capture.str();
    } catch (const std::exception& e) {
        rc = -1;
        captured = std::string("EXCEPTION: ") + e.what();
    } catch (...) {
        rc = -1;
        captured = "EXCEPTION: unknown";
    }

    if (output) {
        *output = std::move(captured);
    }
    return rc;
}

} // namespace

namespace {

struct SearchGraphUxFixture {
    yams::test::TempDirGuard rootGuard{"yams_search_graph_ux_"};
    fs::path root{rootGuard.path()};
    fs::path dataDir{root / "data"};
    fs::path stateDir{root / "state"};
    fs::path worktree{root / "repo"};
    fs::path sourceFile{worktree / "src" / "example.cpp"};
    fs::path externalFile{root / "corpus" / "external.txt"};

    SearchGraphUxFixture() {
        fs::create_directories(dataDir);
        fs::create_directories(stateDir);
        fs::create_directories(worktree / "src");
        fs::create_directories(externalFile.parent_path());
        yams::test::write_file(sourceFile, "#include <iostream>\n"
                                           "int main() {\n"
                                           "    std::cout << \"agent-ux-token\" << std::endl;\n"
                                           "    return 0;\n"
                                           "}\n");
        yams::test::write_file(externalFile, "global-corpus-token\n");
    }
};

void writeSessionSelectors(const fs::path& stateDir, const std::string& sessionName,
                           const std::vector<std::string>& selectors) {
    const fs::path sessionsDir = stateDir / "yams" / "sessions";
    fs::create_directories(sessionsDir);

    nlohmann::json index;
    index["current"] = sessionName;
    yams::test::write_file(sessionsDir / "index.json", index.dump(2));

    nlohmann::json session;
    session["selectors"] = nlohmann::json::array();
    for (const auto& selector : selectors) {
        session["selectors"].push_back({{"path", selector}});
    }
    yams::test::write_file(sessionsDir / (sessionName + ".json"), session.dump(2));
}

} // namespace

TEST_CASE("IntegrationSmoke.SearchAndGraphHumanOutputIsAgentFriendly",
          "[smoke][integrationsmoke]") {
    SearchGraphUxFixture fixture;

    yams::test::ScopedEnvVar embedded("YAMS_EMBEDDED", std::nullopt);
    yams::test::ScopedEnvVar inDaemon("YAMS_IN_DAEMON", std::nullopt);
    yams::test::ScopedEnvVar dataEnv("YAMS_DATA_DIR", fixture.dataDir.string());
    yams::test::ScopedEnvVar storageEnv("YAMS_STORAGE", fixture.dataDir.string());
    yams::test::ScopedEnvVar disableVectors("YAMS_DISABLE_VECTORS", std::string("1"));
    yams::test::ScopedEnvVar skipModelLoading("YAMS_SKIP_MODEL_LOADING", std::string("1"));
    yams::test::ScopedEnvVar disableWatcher("YAMS_DISABLE_SESSION_WATCHER", std::string("1"));

    ScopedCurrentPath cwdGuard(fixture.worktree);

    std::string out;
    int rc = run_cli({"yams", "add", "src/example.cpp", "--sync"}, &out);
    INFO(out);
    REQUIRE(rc == 0);

    out.clear();
    rc = run_cli({"yams", "search", "agent-ux-token", "--type", "keyword", "--limit", "5"}, &out);
    INFO(out);
    REQUIRE(rc == 0);
    CHECK(out.find("src/example.cpp") != std::string::npos);
    CHECK(out.find(fixture.worktree.string()) == std::string::npos);
    CHECK(out.find("-r blob_at_path") == std::string::npos);
    CHECK(out.find("yams graph --name \"src/example.cpp\" --depth 2") != std::string::npos);
    CHECK(out.find("Next: yams graph --name \"src/example.cpp\" --depth 2") != std::string::npos);
    CHECK(out.find("Alt: yams graph --search \"*example*\"") != std::string::npos);

    out.clear();
    rc = run_cli({"yams", "graph", "--name", "src/example.cpp", "--depth", "2"}, &out);
    INFO(out);
    REQUIRE(rc == 0);
    CHECK(out.find("Path: src/example.cpp") != std::string::npos);
    CHECK(out.find(fixture.worktree.string()) == std::string::npos);
    CHECK(out.find("Graph data unavailable") != std::string::npos);
    CHECK(out.find("not indexed yet") != std::string::npos);
    CHECK(out.find("yams add \"src/example.cpp\" --sync") != std::string::npos);
    CHECK(out.find("yams graph --name \"src/example.cpp\" --depth 2") != std::string::npos);
    CHECK(out.find("yams graph --search \"*example*\"") != std::string::npos);
}

TEST_CASE("IntegrationSmoke.SearchNextHintMatchesFirstRenderedResult",
          "[smoke][integrationsmoke]") {
    SearchGraphUxFixture fixture;

    yams::test::ScopedEnvVar embedded("YAMS_EMBEDDED", std::nullopt);
    yams::test::ScopedEnvVar inDaemon("YAMS_IN_DAEMON", std::nullopt);
    yams::test::ScopedEnvVar dataEnv("YAMS_DATA_DIR", fixture.dataDir.string());
    yams::test::ScopedEnvVar storageEnv("YAMS_STORAGE", fixture.dataDir.string());
    yams::test::ScopedEnvVar disableVectors("YAMS_DISABLE_VECTORS", std::string("1"));
    yams::test::ScopedEnvVar skipModelLoading("YAMS_SKIP_MODEL_LOADING", std::string("1"));
    yams::test::ScopedEnvVar disableWatcher("YAMS_DISABLE_SESSION_WATCHER", std::string("1"));

    ScopedCurrentPath cwdGuard(fixture.worktree);
    yams::test::write_file(fixture.worktree / "src" / "alpha.cpp", "shared-token alpha\n");
    yams::test::write_file(fixture.worktree / "src" / "beta.cpp", "shared-token beta\n");

    std::string out;
    int rc = run_cli({"yams", "add", "src/alpha.cpp", "--sync"}, &out);
    INFO(out);
    REQUIRE(rc == 0);

    out.clear();
    rc = run_cli({"yams", "add", "src/beta.cpp", "--sync"}, &out);
    INFO(out);
    REQUIRE(rc == 0);

    out.clear();
    rc = run_cli({"yams", "search", "shared-token", "--type", "keyword", "--limit", "5"}, &out);
    INFO(out);
    REQUIRE(rc == 0);

    const auto nextPos = out.find("\nNext: yams graph --name ");
    REQUIRE(nextPos != std::string::npos);
    const auto rendered = out.substr(0, nextPos);
    const auto firstLineEnd = rendered.find('\n');
    REQUIRE(firstLineEnd != std::string::npos);
    const std::string firstPath = rendered.substr(0, firstLineEnd);
    CHECK(out.find("Next: yams graph --name \"" + firstPath + "\" --depth 2") != std::string::npos);
}

TEST_CASE("IntegrationSmoke.SearchNoResultsSuggestsScopeGrepAndIndexing",
          "[smoke][integrationsmoke]") {
    SearchGraphUxFixture fixture;

    yams::test::ScopedEnvVar embedded("YAMS_EMBEDDED", std::nullopt);
    yams::test::ScopedEnvVar inDaemon("YAMS_IN_DAEMON", std::nullopt);
    yams::test::ScopedEnvVar dataEnv("YAMS_DATA_DIR", fixture.dataDir.string());
    yams::test::ScopedEnvVar storageEnv("YAMS_STORAGE", fixture.dataDir.string());
    yams::test::ScopedEnvVar disableVectors("YAMS_DISABLE_VECTORS", std::string("1"));
    yams::test::ScopedEnvVar skipModelLoading("YAMS_SKIP_MODEL_LOADING", std::string("1"));
    yams::test::ScopedEnvVar disableWatcher("YAMS_DISABLE_SESSION_WATCHER", std::string("1"));

    ScopedCurrentPath cwdGuard(fixture.worktree);

    std::string out;
    int rc = run_cli({"yams", "add", "src/example.cpp", "--sync"}, &out);
    INFO(out);
    REQUIRE(rc == 0);

    out.clear();
    rc = run_cli(
        {"yams", "search", "missing-agent-ux-token", "--type", "keyword", "--limit", "5", "--cwd"},
        &out);
    INFO(out);
    REQUIRE(rc == 0);
    CHECK(out.find("(no results)") != std::string::npos);
    CHECK(out.find("Try: yams grep -F \"missing-agent-ux-token\" --cwd .") != std::string::npos);
    CHECK(out.find("yams add . -r --include \"*.cpp,*.h,*.hpp\"") != std::string::npos);
    CHECK(out.find("yams graph --search \"*missing-agent-ux-token*\"") != std::string::npos);
}

TEST_CASE("IntegrationSmoke.GraphNotFoundSuggestsIndexAndRetry", "[smoke][integrationsmoke]") {
    SearchGraphUxFixture fixture;

    yams::test::ScopedEnvVar embedded("YAMS_EMBEDDED", std::nullopt);
    yams::test::ScopedEnvVar inDaemon("YAMS_IN_DAEMON", std::nullopt);
    yams::test::ScopedEnvVar dataEnv("YAMS_DATA_DIR", fixture.dataDir.string());
    yams::test::ScopedEnvVar storageEnv("YAMS_STORAGE", fixture.dataDir.string());
    yams::test::ScopedEnvVar disableVectors("YAMS_DISABLE_VECTORS", std::string("1"));
    yams::test::ScopedEnvVar skipModelLoading("YAMS_SKIP_MODEL_LOADING", std::string("1"));
    yams::test::ScopedEnvVar disableWatcher("YAMS_DISABLE_SESSION_WATCHER", std::string("1"));

    ScopedCurrentPath cwdGuard(fixture.worktree);
    yams::test::write_file(fixture.worktree / "src" / "new_file.cpp", "int value = 1;\n");

    std::string out;
    int rc = run_cli({"yams", "graph", "--name", "src/new_file.cpp", "--depth", "2"}, &out);
    INFO(out);
    CHECK(rc != 0);
    CHECK(out.find("If this file is new, run: yams add \"src/new_file.cpp\" --sync") !=
          std::string::npos);
    CHECK(out.find("Then retry: yams graph --name \"src/new_file.cpp\" --depth 2") !=
          std::string::npos);
    CHECK(out.find("yams graph --search \"*new_file*\"") != std::string::npos);
}

TEST_CASE("IntegrationSmoke.GrepHumanOutputUsesRelativePathsAndGraphHints",
          "[smoke][integrationsmoke]") {
    SearchGraphUxFixture fixture;

    yams::test::ScopedEnvVar embedded("YAMS_EMBEDDED", std::nullopt);
    yams::test::ScopedEnvVar inDaemon("YAMS_IN_DAEMON", std::nullopt);
    yams::test::ScopedEnvVar dataEnv("YAMS_DATA_DIR", fixture.dataDir.string());
    yams::test::ScopedEnvVar storageEnv("YAMS_STORAGE", fixture.dataDir.string());
    yams::test::ScopedEnvVar disableVectors("YAMS_DISABLE_VECTORS", std::string("1"));
    yams::test::ScopedEnvVar skipModelLoading("YAMS_SKIP_MODEL_LOADING", std::string("1"));
    yams::test::ScopedEnvVar disableWatcher("YAMS_DISABLE_SESSION_WATCHER", std::string("1"));

    ScopedCurrentPath cwdGuard(fixture.worktree);

    std::string out;
    int rc = run_cli({"yams", "add", "src/example.cpp", "--sync"}, &out);
    INFO(out);
    REQUIRE(rc == 0);

    out.clear();
    rc = run_cli({"yams", "grep", "agent-ux-token", "--regex-only", "--line-numbers"}, &out);
    INFO(out);
    REQUIRE(rc == 0);
    CHECK(out.find("src/example.cpp") != std::string::npos);
    CHECK(out.find(fixture.worktree.string()) == std::string::npos);
    CHECK(out.find("[hint: yams graph --name \"src/example.cpp\" --depth 2]") != std::string::npos);
    CHECK(out.find("Tip: Explore relationships with `yams graph --name <file> --depth 2`") !=
          std::string::npos);
}

TEST_CASE("IntegrationSmoke.GrepIgnoresUnrelatedActiveSessionSelectors",
          "[smoke][integrationsmoke]") {
    SearchGraphUxFixture fixture;

    yams::test::ScopedEnvVar embedded("YAMS_EMBEDDED", std::nullopt);
    yams::test::ScopedEnvVar inDaemon("YAMS_IN_DAEMON", std::nullopt);
    yams::test::ScopedEnvVar dataEnv("YAMS_DATA_DIR", fixture.dataDir.string());
    yams::test::ScopedEnvVar storageEnv("YAMS_STORAGE", fixture.dataDir.string());
    yams::test::ScopedEnvVar stateEnv("XDG_STATE_HOME", fixture.stateDir.string());
    yams::test::ScopedEnvVar disableVectors("YAMS_DISABLE_VECTORS", std::string("1"));
    yams::test::ScopedEnvVar skipModelLoading("YAMS_SKIP_MODEL_LOADING", std::string("1"));
    yams::test::ScopedEnvVar disableWatcher("YAMS_DISABLE_SESSION_WATCHER", std::string("1"));

    ScopedCurrentPath cwdGuard(fixture.worktree);

    std::string out;
    int rc = run_cli({"yams", "add", "src/example.cpp", "--sync"}, &out);
    INFO(out);
    REQUIRE(rc == 0);

    writeSessionSelectors(fixture.stateDir, "other-project",
                          {(fixture.root / "somewhere-else" / "**/*").string()});

    out.clear();
    rc = run_cli({"yams", "grep", "agent-ux-token", "--regex-only"}, &out);
    INFO(out);
    REQUIRE(rc == 0);
    CHECK(out.find("src/example.cpp") != std::string::npos);
    CHECK(out.find("(no results)") == std::string::npos);
}

TEST_CASE("IntegrationSmoke.SearchAndGrepAreGlobalByDefaultAndCwdScopedOnDemand",
          "[smoke][integrationsmoke]") {
    SearchGraphUxFixture fixture;

    yams::test::ScopedEnvVar embedded("YAMS_EMBEDDED", std::nullopt);
    yams::test::ScopedEnvVar inDaemon("YAMS_IN_DAEMON", std::nullopt);
    yams::test::ScopedEnvVar dataEnv("YAMS_DATA_DIR", fixture.dataDir.string());
    yams::test::ScopedEnvVar storageEnv("YAMS_STORAGE", fixture.dataDir.string());
    yams::test::ScopedEnvVar stateEnv("XDG_STATE_HOME", fixture.stateDir.string());
    yams::test::ScopedEnvVar disableVectors("YAMS_DISABLE_VECTORS", std::string("1"));
    yams::test::ScopedEnvVar skipModelLoading("YAMS_SKIP_MODEL_LOADING", std::string("1"));
    yams::test::ScopedEnvVar disableWatcher("YAMS_DISABLE_SESSION_WATCHER", std::string("1"));

    ScopedCurrentPath cwdGuard(fixture.worktree);

    std::string out;
    int rc = run_cli({"yams", "add", "src/example.cpp", "--sync"}, &out);
    INFO(out);
    REQUIRE(rc == 0);

    out.clear();
    rc = run_cli({"yams", "add", fixture.externalFile.string(), "--sync"}, &out);
    INFO(out);
    REQUIRE(rc == 0);

    writeSessionSelectors(fixture.stateDir, "repo-session", {(fixture.worktree / "**/*").string()});

    out.clear();
    rc = run_cli({"yams", "search", "global-corpus-token", "--type", "keyword", "--limit", "5"},
                 &out);
    INFO(out);
    REQUIRE(rc == 0);
    CHECK(out.find("external.txt") != std::string::npos);

    out.clear();
    rc = run_cli({"yams", "grep", "global-corpus-token", "--regex-only"}, &out);
    INFO(out);
    REQUIRE(rc == 0);
    CHECK(out.find("external.txt") != std::string::npos);

    out.clear();
    rc = run_cli(
        {"yams", "search", "global-corpus-token", "--type", "keyword", "--limit", "5", "--cwd"},
        &out);
    INFO(out);
    REQUIRE(rc == 0);
    CHECK(out.find("external.txt") == std::string::npos);
    CHECK(out.find("(no results)") != std::string::npos);

    out.clear();
    rc = run_cli({"yams", "grep", "global-corpus-token", "--regex-only", "--cwd"}, &out);
    INFO(out);
    REQUIRE(rc == 0);
    CHECK(out.find("external.txt") == std::string::npos);
    CHECK(out.find("(no results)") != std::string::npos);
}
