#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace yams::extraction {

class IContentExtractor {
public:
    virtual ~IContentExtractor() = default;
    virtual bool supports(const std::string& mime, const std::string& extension) const = 0;
    virtual std::optional<std::string> extractText(const std::vector<std::byte>& bytes,
                                                   const std::string& mime,
                                                   const std::string& extension) = 0;
};

using ContentExtractorList = std::vector<std::shared_ptr<IContentExtractor>>;

} // namespace yams::extraction
