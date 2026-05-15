#pragma once

#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace yams::extraction {

struct ExtractedContent {
    std::string text;
    std::unordered_map<std::string, std::string> metadata;
};

class IContentExtractor {
public:
    virtual ~IContentExtractor() = default;
    virtual bool supports(const std::string& mime, const std::string& extension) const = 0;
    virtual std::optional<std::string> extractText(const std::vector<std::byte>& bytes,
                                                   const std::string& mime,
                                                   const std::string& extension) = 0;

    virtual std::optional<ExtractedContent>
    extractTextAndMetadata(const std::vector<std::byte>& bytes, const std::string& mime,
                           const std::string& extension) {
        auto text = extractText(bytes, mime, extension);
        if (!text || text->empty())
            return std::nullopt;
        return ExtractedContent{std::move(*text), {}};
    }
};

using ContentExtractorList = std::vector<std::shared_ptr<IContentExtractor>>;

} // namespace yams::extraction
