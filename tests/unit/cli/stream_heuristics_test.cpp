#include <gtest/gtest.h>

#include <yams/cli/daemon_helpers.h>
#include <yams/daemon/ipc/ipc_protocol.h>

using namespace yams;

namespace {

TEST(StreamHeuristicsTest, SearchShouldStreamByLimit) {
    daemon::SearchRequest r{};
    r.query = "q";
    r.limit = 5; // below default threshold
    EXPECT_FALSE(cli::should_stream(r));

    r.limit = 100; // above default threshold (50)
    EXPECT_TRUE(cli::should_stream(r));
}

TEST(StreamHeuristicsTest, SearchShouldStreamJsonOutput) {
    daemon::SearchRequest r{};
    r.query = "q";
    r.limit = 10;
    r.jsonOutput = true;
    EXPECT_TRUE(cli::should_stream(r));
}

TEST(StreamHeuristicsTest, ListShouldStreamWhenSnippetsOrPathsOnly) {
    daemon::ListRequest r{};
    r.limit = 10; // below default threshold (100)
    r.showSnippets = false;
    r.pathsOnly = false;
    EXPECT_FALSE(cli::should_stream(r));

    r.showSnippets = true;
    EXPECT_TRUE(cli::should_stream(r));

    r.showSnippets = false;
    r.pathsOnly = true;
    EXPECT_TRUE(cli::should_stream(r));
}

TEST(StreamHeuristicsTest, GrepShouldStreamForPathsOnlyOrRecursive) {
    daemon::GrepRequest r{};
    r.pattern = "foo";
    r.pathsOnly = true;
    EXPECT_TRUE(cli::should_stream(r));

    r.pathsOnly = false;
    r.recursive = true;
    EXPECT_TRUE(cli::should_stream(r));

    r.recursive = false;
    r.maxMatches = 0;
    r.paths.clear();
    r.path.clear();
    EXPECT_FALSE(cli::should_stream(r));
}

} // namespace
