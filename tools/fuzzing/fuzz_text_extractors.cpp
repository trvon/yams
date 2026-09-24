// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

// Text extraction from ingested file bytes: HTML and plain-text buffer extractors, HTML title
// and document-section detection. Oracles: no crash; extraction succeeds on arbitrary bytes;
// HTML output is not longer than its input plus one newline per tag; valid UTF-8 input yields
// valid UTF-8 text. Run with -timeout to catch super-linear scans (see html_text_extractor).

#include <yams/common/utf8_utils.h>
#include <yams/extraction/html_text_extractor.h>
#include <yams/extraction/plain_text_extractor.h>
#include <yams/extraction/title_util.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <string_view>

namespace {

void fuzzRequire(bool condition) {
    if (!condition) {
        __builtin_trap();
    }
}

bool wellFormed(std::string_view text) {
    return yams::common::sanitizeUtf8Strict(text) == text;
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size) {
    if (data == nullptr || size > std::size_t{1024} * 1024) {
        return 0;
    }
    const auto bytes = std::span<const std::byte>(reinterpret_cast<const std::byte*>(data), size);
    const std::string_view text(reinterpret_cast<const char*>(data), size);
    const bool inputValid = wellFormed(text);
    yams::extraction::ExtractionConfig config;

    yams::extraction::HtmlTextExtractor html;
    auto htmlResult = html.extractFromBuffer(bytes, config);
    fuzzRequire(htmlResult.has_value());
    if (htmlResult.value().success) {
        const auto tags = static_cast<std::size_t>(std::count(text.begin(), text.end(), '<'));
        fuzzRequire(htmlResult.value().text.size() <= size + tags + 1);
        fuzzRequire(!inputValid || wellFormed(htmlResult.value().text));
    }

    yams::extraction::PlainTextExtractor plain;
    auto plainResult = plain.extractFromBuffer(bytes, config);
    fuzzRequire(plainResult.has_value());

    (void)yams::extraction::util::extractHtmlTitle(text);
    (void)yams::extraction::util::detectDocumentSections(text);
    return 0;
}
