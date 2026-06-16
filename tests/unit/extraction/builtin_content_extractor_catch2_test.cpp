#include <catch2/catch_test_macros.hpp>

#include <cstddef>
#include <string>
#include <vector>

#include <yams/extraction/builtin_text_content_extractor.h>
#include <yams/extraction/content_extractor.h>

using namespace yams::extraction;

namespace {

std::vector<std::byte> toBytes(const std::string& s) {
    std::vector<std::byte> bytes;
    bytes.reserve(s.size());
    for (char c : s) {
        bytes.push_back(static_cast<std::byte>(static_cast<unsigned char>(c)));
    }
    return bytes;
}

} // namespace

// The daemon gates content_extractors_ready on the count of IContentExtractor
// instances. Plugin-sourced extractors (e.g. zyp/PDF) are optional and
// toolchain-gated (require Zig), so on platforms without that toolchain the count
// would be zero and readiness would never flip. The built-in text content extractor
// must therefore be constructible and functional unconditionally, on every platform.
TEST_CASE("BuiltinTextContentExtractor is always constructible as IContentExtractor",
          "[extraction][content_extractor][platform]") {
    std::shared_ptr<IContentExtractor> extractor = std::make_shared<BuiltinTextContentExtractor>();
    REQUIRE(extractor != nullptr);

    ContentExtractorList list{extractor};
    REQUIRE(list.size() == 1);
}

TEST_CASE("BuiltinTextContentExtractor supports text by extension and mime",
          "[extraction][content_extractor]") {
    BuiltinTextContentExtractor extractor;

    CHECK(extractor.supports("text/plain", ".txt"));
    CHECK(extractor.supports("text/markdown", ".md"));
    CHECK(extractor.supports("text/plain", ""));

    // Plain extension with no leading dot must normalize.
    CHECK(extractor.supports("", "txt"));
}

TEST_CASE("BuiltinTextContentExtractor extracts plain text bytes",
          "[extraction][content_extractor]") {
    BuiltinTextContentExtractor extractor;

    const std::string content = "hello world\nsecond line\n";
    auto bytes = toBytes(content);

    auto text = extractor.extractText(bytes, "text/plain", ".txt");
    REQUIRE(text.has_value());
    CHECK(text->find("hello world") != std::string::npos);
    CHECK(text->find("second line") != std::string::npos);
}

TEST_CASE("BuiltinTextContentExtractor falls back to raw bytes for text mime",
          "[extraction][content_extractor]") {
    BuiltinTextContentExtractor extractor;

    const std::string content = "raw text payload";
    auto bytes = toBytes(content);

    // Unknown extension but text mime: should still yield content.
    auto text = extractor.extractText(bytes, "text/plain", ".unknownext");
    REQUIRE(text.has_value());
    CHECK(text->find("raw text payload") != std::string::npos);
}
