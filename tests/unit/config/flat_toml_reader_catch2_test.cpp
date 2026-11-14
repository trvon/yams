// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <catch2/catch_test_macros.hpp>

#include <yams/config/config_helpers.h>

#include "../../common/test_helpers_catch2.h"

#include <filesystem>
#include <fstream>
#include <iterator>
#include <string>

namespace {

class FlatTomlFixture {
public:
    FlatTomlFixture() : directory_{"yams_flat_toml_"}, path_{directory_.path() / "config.toml"} {}

    std::filesystem::path write(std::string_view content) const {
        std::ofstream stream(path_);
        REQUIRE(stream.good());
        stream << content;
        stream.close();
        return path_;
    }

private:
    yams::test::TempDirGuard directory_;
    std::filesystem::path path_;
};

} // namespace

TEST_CASE_METHOD(FlatTomlFixture, "flat TOML reader preserves ordinary lookup semantics",
                 "[config][flat_toml]") {
    const auto path = write(R"toml(
root = "C:\\Users\\alice\\yams" # platform path
list = ["alpha", "beta#value"] # raw array

[search.path_tree]
mode = 'preferred'
quoted_hash = "# not a comment"
mixed_quote = "O'Brien#docs"
escaped_quote = "quoted \"#\" text" # trailing comment
single_mixed = 'say "hello"#inside' # trailing comment

[embeddings]
runtime.batch_size = 64
)toml");

    const auto values = yams::config::parse_simple_toml(path);
    CHECK((values.at("root") == R"(C:\Users\alice\yams)"));
    CHECK((values.at("list") == R"(["alpha", "beta#value"])"));
    CHECK((values.at("search.path_tree.mode") == "preferred"));
    CHECK((values.at("search.path_tree.quoted_hash") == "# not a comment"));
    CHECK((values.at("search.path_tree.mixed_quote") == "O'Brien#docs"));
    CHECK((values.at("search.path_tree.escaped_quote") == R"(quoted "#" text)"));
    CHECK((values.at("search.path_tree.single_mixed") == "say \"hello\"#inside"));
    CHECK((values.at("embeddings.runtime.batch_size") == "64"));
}

TEST_CASE("path-list decoding preserves quoted commas and platform paths",
          "[config][flat_toml][paths]") {
    const auto paths = yams::config::parse_path_list(R"(["C:\\corpus,old", 'D:\\corpus'])");
    REQUIRE((paths.size() == 2));
    CHECK((paths[0].string() == R"(C:\corpus,old)"));
    // A single-quoted TOML literal string has no escapes; its backslashes are kept as written.
    CHECK((paths[1].string() == R"(D:\\corpus)"));
}

TEST_CASE_METHOD(FlatTomlFixture,
                 "flat TOML reader decodes the escapes YAMS writes and keeps other backslashes",
                 "[config][flat_toml][escapes]") {
    // `yams init` writes `\\` and `\"` escapes; reading them back raw doubled every Windows
    // separator in the data directory. Hand-written Windows paths with single backslashes
    // (not valid TOML, but common) must keep working, so other sequences stay as written.
    const auto path = write(R"toml(
escaped_path = "C:\\Users\\alice\\yams"
unc_path = "\\\\server\\share"
hand_written = "C:\temp\new\Users"
quote_inside = "say \"hi\""
literal = 'C:\\kept\\as\\is'
)toml");

    const auto values = yams::config::parse_simple_toml(path);
    CHECK((values.at("escaped_path") == R"(C:\Users\alice\yams)"));
    CHECK((values.at("unc_path") == R"(\\server\share)"));
    CHECK((values.at("hand_written") == R"(C:\temp\new\Users)"));
    CHECK((values.at("quote_inside") == R"(say "hi")"));
    CHECK((values.at("literal") == R"(C:\\kept\\as\\is)"));
}

TEST_CASE_METHOD(FlatTomlFixture, "single-value lookup delegates to flat TOML semantics",
                 "[config][flat_toml]") {
    const auto path = write(R"toml(
[paths]
quoted_hash = "value#inside" # trailing comment
single_hash = 'single#inside' # trailing comment
)toml");

    CHECK((yams::config::parse_config_value(path, "paths", "quoted_hash") == "value#inside"));
    CHECK((yams::config::parse_config_value(path, "paths", "single_hash") == "single#inside"));
    CHECK(yams::config::parse_config_value(path, "paths", "missing").empty());
}

TEST_CASE_METHOD(FlatTomlFixture, "config path aliases share one documented precedence",
                 "[config][flat_toml][path]") {
    const auto canonical = write("[daemon]\nmode = \"canonical\"\n");
    const auto compatibility = canonical.parent_path() / "compatibility.toml";
    std::ofstream(compatibility) << "[daemon]\nmode = \"compatibility\"\n";

    yams::test::ScopedEnvVar canonicalEnvironment{"YAMS_CONFIG", canonical.string()};
    yams::test::ScopedEnvVar compatibilityEnvironment{"YAMS_CONFIG_PATH", compatibility.string()};
    yams::test::ScopedEnvVar embeddedMode{"YAMS_EMBEDDED", std::nullopt};

    CHECK((yams::config::get_config_path() == compatibility));
    CHECK((yams::config::resolve_daemon_mode_from_config() == "compatibility"));
    CHECK((yams::config::get_config_path("/explicit/config.toml") ==
           std::filesystem::path{"/explicit/config.toml"}));

    std::filesystem::remove(compatibility);
    CHECK((yams::config::get_config_path() == canonical));
    CHECK((yams::config::resolve_daemon_mode_from_config() == "canonical"));
    const auto runtimePaths = yams::config::resolve_runtime_paths();
    REQUIRE(runtimePaths.has_value());
    CHECK((runtimePaths.value().configFile.value == canonical));
    CHECK((runtimePaths.value().configFile.sourceName == "YAMS_CONFIG"));
}

TEST_CASE_METHOD(FlatTomlFixture,
                 "writing config values keeps existing escaped values and escapes new ones",
                 "[config][flat_toml][escapes][write]") {
    // write_config_values reads the file, merges, and rewrites every value. Once reads decode
    // escapes, writes must re-encode them, or setting one key corrupts an init-written path.
    const auto path = write(R"toml(
[core]
data_dir = "C:\\Users\\alice\\yams"
greeting = "say \"hi\""
)toml");

    REQUIRE(yams::config::write_config_values(path, {{"core.added", R"(D:\corpus "new")"}}));

    const auto values = yams::config::parse_simple_toml(path);
    CHECK((values.at("core.data_dir") == R"(C:\Users\alice\yams)"));
    CHECK((values.at("core.greeting") == R"(say "hi")"));
    CHECK((values.at("core.added") == R"(D:\corpus "new")"));

    std::ifstream in(path);
    const std::string text((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    CHECK(text.find(R"(data_dir = "C:\\Users\\alice\\yams")") != std::string::npos);
}
