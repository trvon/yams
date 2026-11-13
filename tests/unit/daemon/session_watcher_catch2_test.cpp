// SPDX-License-Identifier: GPL-3.0-or-later
// What the session watcher indexes, removes and remembers, and when.

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <set>
#include <string>
#include <string_view>

#include <yams/app/services/factory.hpp>
#include <yams/app/services/session_service.hpp>
#include <yams/daemon/components/SessionWatcher.h>

#include "../../common/test_helpers_catch2.h"
#include "session_watch_fakes.h"

namespace {

namespace fs = std::filesystem;
using namespace std::chrono_literals;
using yams::daemon::SessionWatcher;
using yams::daemon::test::SessionWatchDocumentService;
using yams::daemon::test::SessionWatchIndexingService;
using Outcome = SessionWatcher::ScanOutcome;

void writeFile(const fs::path& path, std::string_view text) {
    fs::create_directories(path.parent_path());
    std::ofstream output(path);
    REQUIRE(output.good());
    output << text;
}

std::set<std::string> lastSubmitted(const SessionWatchIndexingService& indexing) {
    if (indexing.requests.empty()) {
        return {};
    }
    const auto& patterns = indexing.requests.back().explicitFiles;
    return {patterns.begin(), patterns.end()};
}

struct Tree {
    yams::test::TempDirGuard temp{"yams_session_watcher_"};
    fs::path root{temp.path() / "project"};
    fs::path state{temp.path() / "state"};
    SessionWatcher::WallClock::time_point now{SessionWatcher::WallClock::now()};

    Tree() { fs::create_directories(root); }
    [[nodiscard]] SessionWatcher::Target target(std::string session = "coding",
                                                std::uint32_t intervalMs = 1000) const {
        SessionWatcher::Target target;
        target.session = std::move(session);
        target.directory = root;
        target.intervalMs = intervalMs;
        return target;
    }
    [[nodiscard]] SessionWatcher::Options options() const {
        SessionWatcher::Options options;
        options.stateDir = state;
        return options;
    }
};

} // namespace

TEST_CASE("Session watcher does not resubmit a tree after a restart", "[daemon][session_watch]") {
    // The snapshot lived in memory, so every daemon start submitted every watched file.
    Tree tree;
    writeFile(tree.root / "a.cpp", "int a;\n");
    writeFile(tree.root / "src" / "b.cpp", "int b;\n");
    SessionWatchDocumentService documents;

    SessionWatchIndexingService first;
    {
        SessionWatcher watcher(tree.options());
        CHECK(watcher.scan(first, &documents, tree.target(), tree.now) == Outcome::Clean);
        CHECK(lastSubmitted(first) == std::set<std::string>{"a.cpp", "src/b.cpp"});
    }

    SessionWatchIndexingService afterRestart;
    SessionWatcher restarted(tree.options());
    CHECK(restarted.scan(afterRestart, &documents, tree.target(), tree.now) == Outcome::Clean);
    CHECK(afterRestart.requests.empty());

    SECTION("a file changed while the daemon was down is still picked up") {
        writeFile(tree.root / "a.cpp", "int a = 2; // longer\n");
        SessionWatcher again(tree.options());
        SessionWatchIndexingService indexing;
        CHECK(again.scan(indexing, &documents, tree.target(), tree.now) == Outcome::Clean);
        CHECK(lastSubmitted(indexing) == std::set<std::string>{"a.cpp"});
    }
    SECTION("a session created again under the same name gets its own first index") {
        // Removing a session leaves its snapshot behind; the new one has a new uuid.
        auto recreated = tree.target();
        recreated.sessionUuid = "second-incarnation";
        SessionWatchIndexingService indexing;
        CHECK(restarted.scan(indexing, &documents, recreated, tree.now) == Outcome::Clean);
        CHECK(lastSubmitted(indexing) == std::set<std::string>{"a.cpp", "src/b.cpp"});
    }
    SECTION("another session pinning the same directory gets its own first index") {
        // One snapshot per directory would let the first session's scan hide the tree
        // from the second, which indexes under its own session id.
        SessionWatchIndexingService indexing;
        CHECK(restarted.scan(indexing, &documents, tree.target("review"), tree.now) ==
              Outcome::Clean);
        REQUIRE(indexing.requests.size() == 1);
        CHECK(indexing.requests.front().sessionId == "review");
        CHECK(lastSubmitted(indexing) == std::set<std::string>{"a.cpp", "src/b.cpp"});
    }
}

TEST_CASE("Session watcher waits before removing a file that disappeared",
          "[daemon][session_watch]") {
    // A checkout or rebase makes files vanish and return; removing at once dropped their
    // documents and re-indexed them moments later.
    Tree tree;
    writeFile(tree.root / "kept.cpp", "int kept;\n");
    writeFile(tree.root / "moved.cpp", "int moved;\n");
    auto options = tree.options();
    options.removalGrace = 600s;
    SessionWatcher watcher(options);
    SessionWatchIndexingService indexing;
    SessionWatchDocumentService documents;
    REQUIRE(watcher.scan(indexing, &documents, tree.target(), tree.now) == Outcome::Clean);
    REQUIRE(indexing.requests.size() == 1);

    const auto movedTime = fs::last_write_time(tree.root / "moved.cpp");
    REQUIRE(fs::remove(tree.root / "moved.cpp"));
    CHECK(watcher.scan(indexing, &documents, tree.target(), tree.now + 10s) == Outcome::Clean);
    CHECK(documents.deleteRequests.empty());

    SECTION("it comes back unchanged within the grace period") {
        writeFile(tree.root / "moved.cpp", "int moved;\n");
        fs::last_write_time(tree.root / "moved.cpp", movedTime);
        CHECK(watcher.scan(indexing, &documents, tree.target(), tree.now + 20s) == Outcome::Clean);
        CHECK(watcher.scan(indexing, &documents, tree.target(), tree.now + 2000s) ==
              Outcome::Clean);
        CHECK(documents.deleteRequests.empty());
        CHECK(indexing.requests.size() == 1); // same content: not indexed again
    }
    SECTION("it stays gone past the grace period") {
        CHECK(watcher.scan(indexing, &documents, tree.target(), tree.now + 300s) == Outcome::Clean);
        CHECK(documents.deleteRequests.empty());
        CHECK(watcher.scan(indexing, &documents, tree.target(), tree.now + 700s) == Outcome::Clean);
        REQUIRE(documents.deleteRequests.size() == 1);
        CHECK(documents.deleteRequests.front().name == (tree.root / "moved.cpp").string());
        CHECK(documents.deleteRequests.front().force);
        // Removed once, not on every later pass.
        CHECK(watcher.scan(indexing, &documents, tree.target(), tree.now + 800s) == Outcome::Clean);
        CHECK(documents.deleteRequests.size() == 1);
    }
    SECTION("the wait survives a restart") {
        SessionWatcher restarted(options);
        CHECK(restarted.scan(indexing, &documents, tree.target(), tree.now + 700s) ==
              Outcome::Clean);
        CHECK(documents.deleteRequests.size() == 1);
    }
}

TEST_CASE("Session watcher applies every .gitignore in the tree", "[daemon][session_watch]") {
    // Only the root file was read, and `!pattern` lines were dropped.
    Tree tree;
    writeFile(tree.root / ".gitignore", "*.log\n!keep.log\nbuild/\n/top.txt\n");
    writeFile(tree.root / "a.cpp", "a");
    writeFile(tree.root / "noise.log", "n");
    writeFile(tree.root / "keep.log", "k");
    writeFile(tree.root / "top.txt", "t");
    writeFile(tree.root / "build" / "out.o", "o");
    writeFile(tree.root / "sub" / "top.txt", "not anchored here");
    writeFile(tree.root / "sub" / ".gitignore", "secret.txt\n*.gen.cpp\n!wanted.gen.cpp\n");
    writeFile(tree.root / "sub" / "secret.txt", "s");
    writeFile(tree.root / "sub" / "x.gen.cpp", "g");
    writeFile(tree.root / "sub" / "wanted.gen.cpp", "w");
    writeFile(tree.root / "sub" / "deep" / "secret.txt", "s");
    writeFile(tree.root / "other" / "secret.txt", "outside sub's rules");
    writeFile(tree.root / ".git" / "config", "never indexed");

    SessionWatcher watcher(tree.options());
    SessionWatchIndexingService indexing;
    SessionWatchDocumentService documents;
    REQUIRE(watcher.scan(indexing, &documents, tree.target(), tree.now) == Outcome::Clean);

    CHECK(lastSubmitted(indexing) ==
          std::set<std::string>{".gitignore", "a.cpp", "keep.log", "sub/top.txt", "sub/.gitignore",
                                "sub/wanted.gen.cpp", "other/secret.txt"});
    // The walk already filtered; the indexer's own root-only filter must not undo a rule.
    CHECK(indexing.requests.front().noGitignore);
    CHECK(indexing.requests.front().includePatterns.empty()); // exact files, not patterns

    SECTION("an edited .gitignore takes effect on the next pass") {
        writeFile(tree.root / "sub" / ".gitignore", "nothing-matches-this\n");
        CHECK(watcher.scan(indexing, &documents, tree.target(), tree.now + 5s) == Outcome::Clean);
        const auto submitted = lastSubmitted(indexing);
        CHECK(submitted.contains("sub/secret.txt"));
        CHECK(submitted.contains("sub/x.gen.cpp"));
    }
}

TEST_CASE("Session watcher slows down while nothing changes", "[daemon][session_watch]") {
    Tree tree;
    writeFile(tree.root / "a.cpp", "a");
    auto options = tree.options();
    options.maxBackoffDoublings = 3;
    SessionWatcher watcher(options);
    SessionWatchIndexingService indexing;
    SessionWatchDocumentService documents;
    const auto target = tree.target("coding", 1000);

    REQUIRE(watcher.scan(indexing, &documents, target, tree.now) == Outcome::Clean);
    CHECK(watcher.currentInterval(target) == 1000ms); // it just indexed something

    auto at = tree.now;
    for (const auto expected : {2000ms, 4000ms, 8000ms, 8000ms}) {
        at += 60s;
        REQUIRE(watcher.scan(indexing, &documents, target, at) == Outcome::Clean);
        CHECK(watcher.currentInterval(target) == expected);
    }

    writeFile(tree.root / "a.cpp", "a changed, and longer");
    REQUIRE(watcher.scan(indexing, &documents, target, at + 60s) == Outcome::Clean);
    CHECK(watcher.currentInterval(target) == 1000ms);
}

TEST_CASE("Session watcher gives up a pass when stopped or refused", "[daemon][session_watch]") {
    Tree tree;
    writeFile(tree.root / "a.cpp", "a"); // far fewer than a batch of entries
    SessionWatchIndexingService indexing;
    SessionWatchDocumentService documents;

    SECTION("a stop request is seen at the first entry") {
        auto options = tree.options();
        options.stopRequested = [] { return true; };
        SessionWatcher watcher(options);
        CHECK(watcher.scan(indexing, &documents, tree.target(), tree.now) == Outcome::Incomplete);
        CHECK(indexing.requests.empty());

        // Nothing was recorded as seen: the next watcher still indexes the file.
        SessionWatcher next(tree.options());
        CHECK(next.scan(indexing, &documents, tree.target(), tree.now) == Outcome::Clean);
        CHECK(lastSubmitted(indexing) == std::set<std::string>{"a.cpp"});
    }
    SECTION("admission is asked again during a long walk") {
        for (int i = 0; i < 600; ++i) {
            writeFile(tree.root / ("f" + std::to_string(i) + ".txt"), "x");
        }
        int asked = 0;
        auto options = tree.options();
        options.admit = [&] { return ++asked < 2; };
        SessionWatcher watcher(options);
        CHECK(watcher.scan(indexing, &documents, tree.target(), tree.now) == Outcome::Incomplete);
        CHECK(indexing.requests.empty());
        CHECK(asked == 2);
    }
    SECTION("a tree with too many files is not fed into the index") {
        // A session pinned a 137,000-file temporary directory and the watcher began
        // ingesting all of it.
        for (int i = 0; i < 12; ++i) {
            writeFile(tree.root / "bulk" / ("f" + std::to_string(i) + ".txt"), "x");
        }
        auto options = tree.options();
        options.maxFilesPerTarget = 10;
        SessionWatcher watcher(options);
        CHECK(watcher.scan(indexing, &documents, tree.target(), tree.now) == Outcome::Incomplete);
        CHECK(indexing.requests.empty());
        CHECK(documents.deleteRequests.empty());

        // Under the limit again (a .gitignore for the bulk), it is watched normally.
        writeFile(tree.root / ".gitignore", "bulk/\n");
        CHECK(watcher.scan(indexing, &documents, tree.target(), tree.now + 1s) == Outcome::Clean);
        CHECK(lastSubmitted(indexing) == std::set<std::string>{".gitignore", "a.cpp"});
    }
    SECTION("a selector that is not a directory is reported, not scanned") {
        SessionWatcher watcher(tree.options());
        auto glob = tree.target();
        glob.directory = tree.root / "src" / "**" / "*.cpp";
        CHECK(watcher.scan(indexing, &documents, glob, tree.now) == Outcome::Incomplete);
        CHECK(indexing.requests.empty());
    }
}

TEST_CASE("Session watcher pass follows each session's own schedule", "[daemon][session_watch]") {
    // End to end over real session files: enable watching, run passes, change files.
    Tree tree;
    yams::test::ScopedEnvVar stateHome{"XDG_STATE_HOME", (tree.temp.path() / "xdg").string()};
    yams::test::ScopedEnvVar noCurrent{"YAMS_SESSION_CURRENT", std::nullopt};
    const auto fast = tree.temp.path() / "fast";
    const auto slow = tree.temp.path() / "slow";
    writeFile(fast / "f.cpp", "f");
    writeFile(slow / "s.cpp", "s");

    auto sessions = yams::app::services::makeSessionService(nullptr);
    REQUIRE(sessions);
    sessions->init("fast", "");
    sessions->addPathSelector(fast.string(), {}, {});
    sessions->enableWatch(true, "fast");
    sessions->setWatchIntervalMs(1000, "fast");
    sessions->init("slow", "");
    sessions->addPathSelector(slow.string(), {}, {});
    sessions->enableWatch(true, "slow");
    sessions->setWatchIntervalMs(60000, "slow");
    sessions->init("unwatched", "");
    sessions->addPathSelector(tree.root.string(), {}, {});
    sessions->clearCurrent(); // watching does not depend on a current session

    auto options = tree.options();
    options.maxBackoffDoublings = 0;
    SessionWatcher watcher(options);
    SessionWatchIndexingService indexing;
    SessionWatchDocumentService documents;

    auto wait = watcher.runPass(*sessions, indexing, &documents, tree.now);
    REQUIRE(indexing.requests.size() == 2);
    std::set<std::string> indexedSessions;
    for (const auto& request : indexing.requests) {
        indexedSessions.insert(request.sessionId + ":" + request.directoryPath);
    }
    CHECK(indexedSessions ==
          std::set<std::string>{"fast:" + fast.string(), "slow:" + slow.string()});
    CHECK(wait == 1000ms); // the fast session is due first

    // A second later only the fast session is walked: the slow one keeps its interval.
    writeFile(fast / "f.cpp", "f changed, longer");
    writeFile(slow / "s.cpp", "s changed, longer");
    wait = watcher.runPass(*sessions, indexing, &documents, tree.now + 1s);
    REQUIRE(indexing.requests.size() == 3);
    CHECK(indexing.requests.back().sessionId == "fast");
    CHECK(wait == 1000ms);

    watcher.runPass(*sessions, indexing, &documents, tree.now + 61s);
    REQUIRE(indexing.requests.size() == 4);
    CHECK(indexing.requests.back().sessionId == "slow");

    SECTION("admission is asked again before each target") {
        writeFile(fast / "f.cpp", "f changed once more, longer still");
        writeFile(slow / "s.cpp", "s changed once more, longer still");
        int asked = 0;
        options.admit = [&] { return ++asked == 1; };
        watcher.setOptions(options);
        const auto before = indexing.requests.size();
        watcher.runPass(*sessions, indexing, &documents, tree.now + 200s);
        CHECK(indexing.requests.size() == before + 1); // second target refused
    }
    SECTION("disabling watching stops the session") {
        sessions->enableWatch(false, "fast");
        writeFile(fast / "f.cpp", "f changed while unwatched, longer");
        const auto before = indexing.requests.size();
        watcher.runPass(*sessions, indexing, &documents, tree.now + 200s);
        CHECK(indexing.requests.size() == before);
    }
}
