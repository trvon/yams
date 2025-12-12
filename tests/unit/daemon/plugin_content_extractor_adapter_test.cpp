// SPDX-License-Identifier: Apache-2.0

#include <catch2/catch_test_macros.hpp>
#include <yams/daemon/resource/plugin_content_extractor_adapter.h>

using namespace yams::daemon;

TEST_CASE("PluginContentExtractorAdapter supportsExternal cached only", "[daemon][extractor]") {
    SECTION("Matches MIME type from cache") {
        PluginContentExtractorAdapter adapter(
            nullptr, "test_plugin",
            {"application/x-executable", "application/octet-stream"},
            {".exe", ".bin"},
            std::chrono::seconds{30});

        REQUIRE(adapter.supports("application/x-executable", ""));
        REQUIRE(adapter.supports("application/octet-stream", ""));
    }

    SECTION("Matches extension with dot") {
        PluginContentExtractorAdapter adapter(
            nullptr, "test_plugin",
            {"application/x-executable"},
            {".exe", ".bin"},
            std::chrono::seconds{30});

        REQUIRE(adapter.supports("", ".exe"));
        REQUIRE(adapter.supports("", ".bin"));
    }

    SECTION("Matches extension without dot") {
        PluginContentExtractorAdapter adapter(
            nullptr, "test_plugin",
            {},
            {".exe", ".bin"},
            std::chrono::seconds{30});

        REQUIRE(adapter.supports("", "exe"));
        REQUIRE(adapter.supports("", "bin"));
    }

    SECTION("Returns false for unsupported types without RPC") {
        PluginContentExtractorAdapter adapter(
            nullptr, "test_plugin",
            {"text/plain"},
            {".txt"},
            std::chrono::seconds{30});

        REQUIRE_FALSE(adapter.supports("application/pdf", ""));
        REQUIRE_FALSE(adapter.supports("", ".pdf"));
        REQUIRE_FALSE(adapter.supports("image/png", ".png"));
    }

    SECTION("Empty inputs return false") {
        PluginContentExtractorAdapter adapter(
            nullptr, "test_plugin",
            {"text/plain"},
            {".txt"},
            std::chrono::seconds{30});

        REQUIRE_FALSE(adapter.supports("", ""));
    }

    SECTION("isExternal returns true for external backend") {
        PluginContentExtractorAdapter adapter(
            nullptr, "test_plugin",
            {"text/plain"},
            {".txt"},
            std::chrono::seconds{30});

        REQUIRE(adapter.isExternal());
    }
}
