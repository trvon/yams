#include <chrono>
#include <filesystem>
#include <fstream>
#include <catch2/catch_test_macros.hpp>
#include <yams/cli/live_grep.h>
#include <yams/cli/repo_files.h>

using namespace yams::cli;

namespace {
struct TempRepo {
    RepoPath root = std::filesystem::temp_directory_path() /
                    ("yams-cli-repo-" +
                     std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    TempRepo() {
        std::filesystem::create_directories(root);
        REQUIRE(runCliProcess({"git", "init", "-q", root.string()}).exitCode == 0);
    }
    ~TempRepo() {
        std::error_code error;
        std::filesystem::remove_all(root, error);
    }
    void write(std::string_view name, std::string_view content) const {
        std::ofstream(root / name) << content;
    }
};
std::string contents(const RepoPath& path) {
    std::ifstream stream(path);
    return std::string(std::istreambuf_iterator<char>(stream), {});
}
} // namespace

TEST_CASE("CLI repo file enumeration respects gitignore and argv boundaries",
          "[cli][repo][freshness]") {
    TempRepo repo;
    repo.write(".gitignore", "ignored.txt\n");
    repo.write("tracked '; echo nope.txt", "tracked");
    repo.write("ignored.txt", "ignored");
    repo.write("untracked.txt", "new");
    REQUIRE(runCliProcess({"git", "-C", repo.root.string(), "add", "--", ".gitignore",
                           "tracked '; echo nope.txt"})
                .exitCode == 0);
    auto tracked = readRepoFiles(repo.root);
    REQUIRE(tracked.tracked.size() == 2);
    auto all = readRepoFiles(repo.root, true);
    REQUIRE(all.tracked.size() == 3);
    CHECK(all.root == std::filesystem::canonical(repo.root));
    CHECK_FALSE(all.worktree.empty());
}

TEST_CASE("CLI freshness detects edits and missing indexed files", "[cli][repo][freshness]") {
    TempRepo repo;
    repo.write("a.txt", "old");
    REQUIRE(runCliProcess({"git", "-C", repo.root.string(), "add", "--", "a.txt"}).exitCode == 0);
    const auto files = readRepoFiles(repo.root);
    RepoIndex index{{repo.root / "a.txt", {"old", 1}}};
    CHECK_FALSE(compareRepoIndex(files, index, contents).stale());
    repo.write("a.txt", "new");
    CHECK(compareRepoIndex(files, index, contents).changed.size() == 1);
    index[repo.root / "a.txt"].hash = "new";
    CHECK_FALSE(compareRepoIndex(files, index, contents).stale());
    std::filesystem::remove(repo.root / "a.txt");
    const auto diff = compareRepoIndex(files, index, contents);
    CHECK(diff.missing.size() == 1);
    CHECK(diff.changed.empty());
}

TEST_CASE("CLI repo comparison ignores sibling worktree index rows", "[cli][repo][freshness]") {
    TempRepo repo;
    repo.write("a.txt", "content");
    REQUIRE(runCliProcess({"git", "-C", repo.root.string(), "add", "--", "a.txt"}).exitCode == 0);
    RepoIndex index{{repo.root / "a.txt", {"content", 1}},
                    {RepoPath(repo.root.string() + "-other") / "a.txt", {"different", 1}}};
    CHECK_FALSE(compareRepoIndex(readRepoFiles(repo.root), index, contents).stale());
}

TEST_CASE("CLI worktree names resolve to distinct canonical roots", "[cli][repo][worktree]") {
    TempRepo repo;
    repo.write("a.txt", "seed");
    REQUIRE(runCliProcess({"git", "-C", repo.root.string(), "add", "--", "a.txt"}).exitCode == 0);
    REQUIRE(runCliProcess({"git", "-C", repo.root.string(), "-c", "user.name=Test", "-c",
                           "user.email=test@example.invalid", "-c", "commit.gpgsign=false",
                           "commit", "-qm", "seed"})
                .exitCode == 0);
    const RepoPath linked(repo.root.string() + "-lane");
    struct Cleanup {
        RepoPath path;
        ~Cleanup() {
            std::error_code error;
            std::filesystem::remove_all(path, error);
        }
    } cleanup{linked};
    REQUIRE(runCliProcess(
                {"git", "-C", repo.root.string(), "worktree", "add", "-b", "lane", linked.string()})
                .exitCode == 0);
    CHECK(resolveWorktree(repo.root, linked.filename().string()) ==
          std::filesystem::canonical(linked));
    CHECK(readRepoFiles(linked).branch == "lane");
    CHECK(readRepoFiles(linked).worktree == linked.filename().string());
    CHECK_THROWS_AS(resolveWorktree(repo.root, "missing-worktree"), std::runtime_error);
}

TEST_CASE("CLI process capture is bounded", "[cli][repo][process]") {
    CHECK_THROWS_AS(runCliProcess({"git", "--version"}, 1), std::runtime_error);
    CHECK_THROWS_AS(runCliProcess({}), std::invalid_argument);
    std::size_t lines = 0;
    const auto process = runCliProcess({"git", "--version"}, 1024, [&](auto) {
        ++lines;
        return false;
    });
    CHECK(process.exitCode == 0);
    CHECK(lines == 1);
    CHECK(process.output.empty());
}

TEST_CASE("CLI live grep honors global, per-file and unique-path caps", "[cli][live]") {
    TempRepo repo;
    repo.write("a.cpp", "needle\nneedle\n");
    repo.write("b.cpp", "needle\n");
    repo.write("skip.txt", "needle\n");
    LiveGrepOptions options;
    options.root = repo.root;
    options.pattern = "needle";
    options.literal = true;
    options.globs = {"*.cpp"};
    options.perFileLimit = 1;
    options.resultLimit = 1;
    auto result = liveGrep(options);
    if (!result.available)
        SKIP("rg not installed on this host");
    REQUIRE(result.hits.size() == 1);
    CHECK(result.hits.front().line == "needle");
    options.pathsOnly = true;
    options.resultLimit = 2;
    result = liveGrep(options);
    REQUIRE(result.hits.size() == 2);
    CHECK(result.hits[0].path != result.hits[1].path);
}

TEST_CASE("CLI live grep respects gitignore, no hits and literal punctuation", "[cli][live]") {
    TempRepo repo;
    repo.write(".gitignore", "ignored.cpp\n");
    repo.write("main.cpp", "[needle]\nNEEDLE\nneedlework\n");
    repo.write("ignored.cpp", "[needle]\n");
    LiveGrepOptions options;
    options.root = repo.root;
    options.pattern = "[needle]";
    options.literal = true;
    auto result = liveGrep(options);
    if (!result.available)
        SKIP("rg not installed on this host");
    REQUIRE(result.hits.size() == 1);
    CHECK(result.hits.front().lineNumber == 1);
    options.pattern = "needle";
    options.wholeWord = true;
    options.ignoreCase = true;
    CHECK(liveGrep(options).hits.size() == 2);
    options.pattern = "not-present";
    CHECK(liveGrep(options).hits.empty());
    options.filesWithoutMatch = true;
    REQUIRE(liveGrep(options).hits.size() >= 1);
}

TEST_CASE("CLI live grep keeps requested context at a global cap", "[cli][live]") {
    TempRepo repo;
    repo.write("a.txt", "before\nneedle\nafter\ntail\nneedle\n");
    LiveGrepOptions options;
    options.root = repo.root;
    options.pattern = "needle";
    options.beforeContext = 1;
    options.afterContext = 1;
    options.resultLimit = 1;
    auto result = liveGrep(options);
    if (!result.available)
        SKIP("rg not installed on this host");
    REQUIRE(result.hits.size() == 1);
    CHECK(result.hits[0].contextBefore == std::vector<std::string>{"before"});
    CHECK(result.hits[0].contextAfter == std::vector<std::string>{"after"});
}
