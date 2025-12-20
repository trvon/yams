// CLI Stream Heuristics tests
// Catch2 migration from GTest (yams-3s4 / yams-cli)

#include <catch2/catch_test_macros.hpp>

#include <yams/cli/daemon_helpers.h>
#include <yams/daemon/ipc/ipc_protocol.h>

using namespace yams;

TEST_CASE("StreamHeuristics - Search should stream by limit", "[cli][stream_heuristics][catch2]") {
    daemon::SearchRequest r{};
    r.query = "q";
    r.limit = 5; // below default threshold
    CHECK_FALSE(cli::should_stream(r));

    r.limit = 100; // above default threshold (50)
    CHECK(cli::should_stream(r));
}

TEST_CASE("StreamHeuristics - Search should stream JSON output",
          "[cli][stream_heuristics][catch2]") {
    daemon::SearchRequest r{};
    r.query = "q";
    r.limit = 10;
    r.jsonOutput = true;
    CHECK(cli::should_stream(r));
}

TEST_CASE("StreamHeuristics - List should stream when snippets or paths only",
          "[cli][stream_heuristics][catch2]") {
    daemon::ListRequest r{};
    r.limit = 10; // below default threshold (100)
    r.showSnippets = false;
    r.pathsOnly = false;
    CHECK_FALSE(cli::should_stream(r));

    r.showSnippets = true;
    CHECK(cli::should_stream(r));

    r.showSnippets = false;
    r.pathsOnly = true;
    CHECK(cli::should_stream(r));
}

TEST_CASE("StreamHeuristics - Grep should stream for paths only or recursive",
          "[cli][stream_heuristics][catch2]") {
    daemon::GrepRequest r{};
    r.pattern = "foo";
    r.pathsOnly = true;
    CHECK(cli::should_stream(r));

    r.pathsOnly = false;
    r.recursive = true;
    CHECK(cli::should_stream(r));

    r.recursive = false;
    r.maxMatches = 0;
    r.paths.clear();
    r.path.clear();
    CHECK_FALSE(cli::should_stream(r));
}
