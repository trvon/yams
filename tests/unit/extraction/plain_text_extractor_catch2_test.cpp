// SPDX-License-Identifier: GPL-3.0-or-later
// PlainTextExtractor unit tests — covers extract(), extractFromBuffer(), and
// supportedExtensions(). Targets the most common extraction path in YAMS
// (any un-specialized MIME hits this).

#include <catch2/catch_test_macros.hpp>
#include <yams/extraction/plain_text_extractor.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <span>
#include <string>
#include <vector>

using namespace yams::extraction;

namespace {

std::filesystem::path makeTempDir() {
    static std::atomic<uint64_t> counter{0};
    auto base =
        std::filesystem::temp_directory_path() /
        ("yams_plain_text_extractor_" + std::to_string(reinterpret_cast<std::uintptr_t>(&counter)) +
         "_" + std::to_string(counter.fetch_add(1)));
    std::filesystem::create_directories(base);
    return base;
}

std::filesystem::path writeTempFile(const std::filesystem::path& dir, const std::string& name,
                                    const std::string& content) {
    auto p = dir / name;
    std::ofstream f(p, std::ios::binary);
    f.write(content.data(), static_cast<std::streamsize>(content.size()));
    return p;
}

std::span<const std::byte> asBytes(const std::string& s) {
    return std::span<const std::byte>(reinterpret_cast<const std::byte*>(s.data()), s.size());
}

} // namespace

TEST_CASE("PlainTextExtractor: extracts plain ASCII file", "[extraction][plain-text]") {
    auto dir = makeTempDir();
    auto file = writeTempFile(dir, "hello.txt", "hello world\nsecond line\n");

    PlainTextExtractor ex;
    auto r = ex.extract(file);
    REQUIRE(r);
    auto result = std::move(r).value();
    CHECK(result.isSuccess());
    CHECK(result.text.find("hello world") != std::string::npos);
    CHECK(result.extractionMethod == "plain_text");

    std::filesystem::remove_all(dir);
}

TEST_CASE("PlainTextExtractor: missing file returns failure without exception",
          "[extraction][plain-text]") {
    PlainTextExtractor ex;
    auto r = ex.extract("/nonexistent/path/definitely_not_here.txt");
    REQUIRE(r); // Result-wrapping succeeds; the error is in the ExtractionResult.
    auto result = std::move(r).value();
    CHECK_FALSE(result.isSuccess());
    CHECK(result.error.find("does not exist") != std::string::npos);
}

TEST_CASE("PlainTextExtractor: oversize file is rejected", "[extraction][plain-text]") {
    auto dir = makeTempDir();
    auto file = writeTempFile(dir, "big.txt", std::string(4096, 'a'));

    PlainTextExtractor ex;
    ExtractionConfig cfg;
    cfg.maxFileSize = 1024;
    auto r = ex.extract(file, cfg);
    REQUIRE(r);
    auto result = std::move(r).value();
    CHECK_FALSE(result.isSuccess());
    CHECK(result.error.find("too large") != std::string::npos);

    std::filesystem::remove_all(dir);
}

TEST_CASE("PlainTextExtractor: empty file succeeds with empty text", "[extraction][plain-text]") {
    auto dir = makeTempDir();
    auto file = writeTempFile(dir, "empty.txt", "");

    PlainTextExtractor ex;
    auto r = ex.extract(file);
    REQUIRE(r);
    auto result = std::move(r).value();
    CHECK(result.isSuccess());
    CHECK(result.text.empty());

    std::filesystem::remove_all(dir);
}

TEST_CASE("PlainTextExtractor: extractFromBuffer accepts UTF-8", "[extraction][plain-text]") {
    PlainTextExtractor ex;
    // "café résumé" in UTF-8
    std::string utf8 = "caf\xC3\xA9 r\xC3\xA9sum\xC3\xA9";
    auto r = ex.extractFromBuffer(asBytes(utf8));
    REQUIRE(r);
    auto result = std::move(r).value();
    CHECK(result.isSuccess());
    CHECK(result.text == utf8);
}

TEST_CASE("PlainTextExtractor: extractFromBuffer rejects data with NUL bytes",
          "[extraction][plain-text]") {
    PlainTextExtractor ex;
    std::string binary(64, 'a');
    binary[10] = '\0';
    binary[20] = '\x01';
    binary[30] = '\x02';
    auto r = ex.extractFromBuffer(asBytes(binary));
    REQUIRE(r);
    auto result = std::move(r).value();
    CHECK_FALSE(result.isSuccess());
}

TEST_CASE("PlainTextExtractor: extractFromBuffer handles empty input", "[extraction][plain-text]") {
    PlainTextExtractor ex;
    std::vector<std::byte> empty;
    auto r = ex.extractFromBuffer(std::span<const std::byte>(empty));
    REQUIRE(r);
    // Either a successful empty extraction or a binary-rejection is acceptable here;
    // the contract is that it must not throw and must not return uninitialized text.
    auto result = std::move(r).value();
    if (result.isSuccess()) {
        CHECK(result.text.empty());
    }
}

TEST_CASE("PlainTextExtractor: supportedExtensions covers common source/doc formats",
          "[extraction][plain-text]") {
    PlainTextExtractor ex;
    auto exts = ex.supportedExtensions();
    auto has = [&](std::string_view s) {
        return std::find(exts.begin(), exts.end(), s) != exts.end();
    };
    CHECK(has(".txt"));
    CHECK(has(".md"));
    CHECK(has(".json"));
    CHECK(has(".cpp"));
    CHECK(has(".py"));
    CHECK(has(".yaml"));
    CHECK_FALSE(has(".pdf"));
    CHECK_FALSE(has(".docx"));
}

TEST_CASE("PlainTextExtractor: name() is stable", "[extraction][plain-text]") {
    PlainTextExtractor ex;
    CHECK(ex.name() == "Plain Text Extractor");
}
