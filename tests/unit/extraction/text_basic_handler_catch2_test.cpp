// SPDX-License-Identifier: GPL-3.0-or-later
// TextBasicHandler unit tests: capability reporting, format selection, and line-range extraction
// (the handler behind `get --range` and the search `lines:` qualifier).

#include <catch2/catch_test_macros.hpp>
#include <yams/extraction/format_handlers/text_basic_handler.hpp>

#include <cstring>
#include <set>
#include <string>
#include <string_view>
#include <vector>

using yams::extraction::format::ExtractionQuery;
using yams::extraction::format::HandlerRegistry;
using yams::extraction::format::Scope;
using yams::extraction::format::TextBasicHandler;

namespace {

std::vector<std::byte> bytesOf(std::string_view text) {
    std::vector<std::byte> out(text.size());
    std::memcpy(out.data(), text.data(), text.size());
    return out;
}

} // namespace

// capabilities() built its extension list from begin()/end() of two different temporary sets.
// libstdc++/libc++ release iterators happened to walk the first set to completion, but the MSVC
// debug STL rejects the mismatched range with a modal assertion dialog, which hung every
// `lines:` search on Windows Debug builds.
TEST_CASE("TextBasicHandler: the supported extension set is one shared instance",
          "[unit][extraction][text-basic-handler]") {
    const auto& first = TextBasicHandler::supportedExtensions();
    const auto& second = TextBasicHandler::supportedExtensions();
    CHECK((&first == &second));
    CHECK(first.contains(".md"));
    CHECK(first.contains(".txt"));
}

TEST_CASE("TextBasicHandler: capabilities list every supported extension exactly once",
          "[unit][extraction][text-basic-handler]") {
    TextBasicHandler handler;
    const auto caps = handler.capabilities();
    const auto& supported = TextBasicHandler::supportedExtensions();

    REQUIRE((caps.extensions.size() == supported.size()));
    const std::set<std::string> listed(caps.extensions.begin(), caps.extensions.end());
    CHECK((listed.size() == caps.extensions.size()));
    for (const auto& ext : caps.extensions) {
        CHECK(supported.contains(ext));
        CHECK(handler.supports("", ext));
    }
    CHECK(caps.supportsRange);
}

TEST_CASE("TextBasicHandler: registry selects it for text and not for binary formats",
          "[unit][extraction][text-basic-handler]") {
    HandlerRegistry registry;
    yams::extraction::format::registerTextBasicHandler(registry);

    CHECK(registry.selectBest("text/markdown", ".md"));
    CHECK(registry.selectBest("", ".TXT"));
    CHECK_FALSE(registry.selectBest("application/pdf", ".pdf"));
}

TEST_CASE("TextBasicHandler: a line range selects those lines",
          "[unit][extraction][text-basic-handler]") {
    HandlerRegistry registry;
    yams::extraction::format::registerTextBasicHandler(registry);
    ExtractionQuery query;
    query.scope = Scope::Range;

    query.range = "2-3";
    auto lines = registry.extract("text/plain", ".txt", bytesOf("one\ntwo\nthree\nfour\n"), query);
    REQUIRE(lines);
    REQUIRE(lines.value().text);
    CHECK((*lines.value().text == "two\nthree"));

    query.range = "two-five";
    auto invalid = registry.extract("text/plain", ".txt", bytesOf("one\ntwo\n"), query);
    REQUIRE_FALSE(invalid);
    CHECK((invalid.error().code == yams::ErrorCode::InvalidArgument));
}
