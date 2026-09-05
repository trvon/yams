// SPDX-License-Identifier: GPL-3.0-or-later

#include "../../../src/daemon/resource/extraction_response.h"
#include <catch2/catch_test_macros.hpp>
#include <yams/daemon/resource/plugin_content_extractor_adapter.h>

using namespace yams::daemon;

TEST_CASE("Native and external extraction preserve the same text and metadata",
          "[daemon][extractor][parity]") {
    yams_content_extractor_v1 table{};
    table.abi_version = YAMS_IFACE_CONTENT_EXTRACTOR_V1_VERSION;
    table.extract = [](const uint8_t*, size_t, yams_extraction_result_t** out) {
        static char text[] = "Document body";
        static char titleKey[] = "title";
        static char title[] = "Supplied title";
        static char authorKey[] = "author";
        static char author[] = "Ada";
        static yams_key_value_pair_t pairs[] = {{titleKey, title}, {authorKey, author}};
        *out = new yams_extraction_result_t{text, {pairs, 2}, nullptr};
        return YAMS_PLUGIN_OK;
    };
    table.free_result = [](yams_extraction_result_t* result) { delete result; };
    PluginContentExtractorAdapter adapter(&table, "fixture");
    const auto native = adapter.extractTextAndMetadata({}, "text/plain", ".txt");
    const auto external =
        decodeExtractionResponse({{"text", "Document body"},
                                  {"metadata", {{"title", "Supplied title"}, {"author", "Ada"}}}});
    REQUIRE(native);
    REQUIRE(external);
    CHECK(native->text == external->text);
    CHECK(native->metadata == external->metadata);
}

TEST_CASE("PluginContentExtractorAdapter supportsExternal cached only", "[daemon][extractor]") {
    SECTION("Matches MIME type from cache") {
        PluginContentExtractorAdapter adapter(
            nullptr, "test_plugin", {"application/x-executable", "application/octet-stream"},
            {".exe", ".bin"}, std::chrono::seconds{30});

        REQUIRE(adapter.supports("application/x-executable", ""));
        REQUIRE(adapter.supports("application/octet-stream", ""));
    }

    SECTION("Matches extension with dot") {
        PluginContentExtractorAdapter adapter(nullptr, "test_plugin", {"application/x-executable"},
                                              {".exe", ".bin"}, std::chrono::seconds{30});

        REQUIRE(adapter.supports("", ".exe"));
        REQUIRE(adapter.supports("", ".bin"));
    }

    SECTION("Matches extension without dot") {
        PluginContentExtractorAdapter adapter(nullptr, "test_plugin", {}, {".exe", ".bin"},
                                              std::chrono::seconds{30});

        REQUIRE(adapter.supports("", "exe"));
        REQUIRE(adapter.supports("", "bin"));
    }

    SECTION("Returns false for unsupported types without RPC") {
        PluginContentExtractorAdapter adapter(nullptr, "test_plugin", {"text/plain"}, {".txt"},
                                              std::chrono::seconds{30});

        REQUIRE_FALSE(adapter.supports("application/pdf", ""));
        REQUIRE_FALSE(adapter.supports("", ".pdf"));
        REQUIRE_FALSE(adapter.supports("image/png", ".png"));
    }

    SECTION("Empty inputs return false") {
        PluginContentExtractorAdapter adapter(nullptr, "test_plugin", {"text/plain"}, {".txt"},
                                              std::chrono::seconds{30});

        REQUIRE_FALSE(adapter.supports("", ""));
    }

    SECTION("isExternal returns true for external backend") {
        PluginContentExtractorAdapter adapter(nullptr, "test_plugin", {"text/plain"}, {".txt"},
                                              std::chrono::seconds{30});

        REQUIRE(adapter.isExternal());
    }
}
