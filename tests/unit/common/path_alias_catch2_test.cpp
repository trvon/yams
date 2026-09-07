// macOS spells /var and /tmp as symlinks into /private. Documents can be indexed under either
// spelling, so every consumer that compares, displays, or expands paths must agree on one alias
// table. These tests pin that table and the three policies built on it.

#include <catch2/catch_test_macros.hpp>

#include <yams/common/fs_utils.h>

#include <string>
#include <vector>

using yams::common::path_alias::canonicalSpelling;
using yams::common::path_alias::displaySpelling;
using yams::common::path_alias::otherSpelling;

TEST_CASE("otherSpelling maps between the /private and short forms of var and tmp",
          "[common][fs][alias][catch2]") {
    CHECK(otherSpelling("/var") == "/private/var");
    CHECK(otherSpelling("/var/folders/x") == "/private/var/folders/x");
    CHECK(otherSpelling("/private/var") == "/var");
    CHECK(otherSpelling("/private/var/folders/x") == "/var/folders/x");
    CHECK(otherSpelling("/tmp") == "/private/tmp");
    CHECK(otherSpelling("/tmp/a.txt") == "/private/tmp/a.txt");
    CHECK(otherSpelling("/private/tmp/a.txt") == "/tmp/a.txt");

    SECTION("only whole path components alias") {
        CHECK(otherSpelling("/variable/x").empty());
        CHECK(otherSpelling("/tmpfs/x").empty());
        CHECK(otherSpelling("/private/varnish").empty());
        CHECK(otherSpelling("/home/user/var/x").empty());
        CHECK(otherSpelling("").empty());
        CHECK(otherSpelling("relative/var/x").empty());
    }
}

TEST_CASE("canonical spelling is the /private form and display spelling is the short form",
          "[common][fs][alias][catch2]") {
    CHECK(canonicalSpelling("/var/folders/x") == "/private/var/folders/x");
    CHECK(canonicalSpelling("/tmp/a") == "/private/tmp/a");
    CHECK(canonicalSpelling("/private/tmp/a") == "/private/tmp/a");
    CHECK(canonicalSpelling("/usr/local/a") == "/usr/local/a");

    CHECK(displaySpelling("/private/var/folders/x") == "/var/folders/x");
    CHECK(displaySpelling("/private/tmp/a") == "/tmp/a");
    CHECK(displaySpelling("/tmp/a") == "/tmp/a");
    CHECK(displaySpelling("/usr/local/a") == "/usr/local/a");
}

TEST_CASE("platform wrappers alias only on macOS", "[common][fs][alias][catch2]") {
    const std::string canon = yams::common::canonicalizeMacPathAlias("/var/x");
    const std::string disp = yams::common::displayMacPathAlias("/private/var/x");
    std::vector<std::string> patterns{"/var/x", "/usr/y"};
    yams::common::appendMacPathAliases(patterns);
#if defined(__APPLE__)
    CHECK(canon == "/private/var/x");
    CHECK(disp == "/var/x");
    REQUIRE(patterns.size() == 3);
    CHECK(patterns[2] == "/private/var/x");
#else
    CHECK(canon == "/var/x");
    CHECK(disp == "/private/var/x");
    CHECK(patterns.size() == 2);
#endif
}
