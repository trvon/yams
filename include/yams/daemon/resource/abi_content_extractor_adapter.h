#pragma once

#include <memory>
#include <string>
#include <vector>

#include <yams/extraction/content_extractor.h>
#include <yams/plugins/abi.h>
#include <yams/plugins/content_extractor_v1.h>

namespace yams::daemon {

class AbiContentExtractorAdapter : public yams::extraction::IContentExtractor {
public:
    explicit AbiContentExtractorAdapter(yams_content_extractor_v1* table) : table_(table) {}
    ~AbiContentExtractorAdapter() override = default;

    bool supports(const std::string& mime, const std::string& extension) const override {
        if (!table_ || !table_->supports)
            return false;
        return table_->supports(mime.c_str(), extension.c_str());
    }

    std::optional<std::string> extractText(const std::vector<std::byte>& bytes,
                                           const std::string& mime,
                                           const std::string& extension) override {
        (void)mime;
        (void)extension;
        if (!table_ || !table_->extract || !table_->free_result)
            return std::nullopt;
        yams_extraction_result_t* res = nullptr;
        int rc =
            table_->extract(reinterpret_cast<const uint8_t*>(bytes.data()), bytes.size(), &res);
        if (rc != YAMS_PLUGIN_OK || !res)
            return std::nullopt;
        std::optional<std::string> out;
        if (res->text)
            out = std::string(res->text);
        table_->free_result(res);
        return out;
    }

private:
    yams_content_extractor_v1* table_{};
};

} // namespace yams::daemon
