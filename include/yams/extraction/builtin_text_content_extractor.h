#pragma once

#include <yams/extraction/content_extractor.h>

namespace yams::extraction {

// Bridges the always-available built-in TextExtractorFactory (plain text, markdown,
// source code, HTML) to the IContentExtractor interface consumed by the daemon. This
// guarantees at least one content extractor exists regardless of platform or optional
// toolchain-gated plugins (e.g. zyp/PDF), keeping content-extractor readiness accurate.
class BuiltinTextContentExtractor : public IContentExtractor {
public:
    bool supports(const std::string& mime, const std::string& extension) const override;

    std::optional<std::string> extractText(const std::vector<std::byte>& bytes,
                                           const std::string& mime,
                                           const std::string& extension) override;
};

} // namespace yams::extraction
