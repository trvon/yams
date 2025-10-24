#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <yams/plugins/symbol_extractor_v1.h>

namespace yams::daemon {

class AbiSymbolExtractorAdapter {
public:
    explicit AbiSymbolExtractorAdapter(yams_symbol_extractor_v1* table) : table_(table) {}

    bool supportsLanguage(const std::string& lang) const {
        return table_ && table_->supports_language &&
               table_->supports_language(table_->self, lang.c_str());
    }

    yams_symbol_extractor_v1* table() const { return table_; }

    /**
     * @brief Get plugin capabilities including supported languages and extensions
     * @return Map of file extensions to language identifiers
     *
     * Queries the plugin's get_capabilities_json() if available.
     */
    std::unordered_map<std::string, std::string> getSupportedExtensions() const;

private:
    yams_symbol_extractor_v1* table_{};
};

} // namespace yams::daemon
