// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

// UTF-8 sanitizers and text encoding detection/conversion on ingested file bytes. Oracles:
// sanitizer output is well-formed UTF-8 (unchanged by the strict sanitizer) and leaves valid
// input intact; a "UTF-8" detection means the bytes really are well-formed; UTF-16 and
// Latin-1 conversions always produce well-formed UTF-8.

#include <yams/common/utf8_utils.h>
#include <yams/extraction/text_extractor.h>

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
    if (data == nullptr || size > std::size_t{256} * 1024) {
        return 0;
    }
    const std::string input(reinterpret_cast<const char*>(data), size);
    const bool inputValid = wellFormed(input);

    const std::string lenient = yams::common::sanitizeUtf8(input);
    fuzzRequire(wellFormed(lenient));
    fuzzRequire(!inputValid || lenient == input);

    const std::string strict = yams::common::sanitizeUtf8Strict(input);
    fuzzRequire(wellFormed(strict));

    std::string storage;
    const std::string_view ensured = yams::common::ensureValidUtf8(input, storage);
    fuzzRequire(wellFormed(ensured));
    fuzzRequire(!inputValid || ensured == input);

    using yams::extraction::EncodingDetector;
    const auto bytes = std::span<const std::byte>(reinterpret_cast<const std::byte*>(data), size);
    const std::string detected = EncodingDetector::detectEncoding(bytes, nullptr);
    const bool bom = size >= 3 && data[0] == 0xEF && data[1] == 0xBB && data[2] == 0xBF;
    fuzzRequire(detected != "UTF-8" || bom || inputValid);

    for (const char* encoding : {"UTF-16LE", "UTF-16BE", "ISO-8859-1"}) {
        auto converted = EncodingDetector::convertToUtf8(input, encoding);
        fuzzRequire(converted.has_value());
        fuzzRequire(wellFormed(converted.value()));
    }
    return 0;
}
