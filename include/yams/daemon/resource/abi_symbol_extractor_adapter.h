#pragma once

#include <memory>
#include <string>
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

private:
    yams_symbol_extractor_v1* table_{};
};

} // namespace yams::daemon
