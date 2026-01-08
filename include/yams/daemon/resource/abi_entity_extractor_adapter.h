#pragma once

#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include <yams/plugins/entity_extractor_v2.h>

namespace yams::daemon {

/**
 * @brief Adapter for entity_extractor_v2 ABI plugins (like Glint).
 *
 * Wraps the C ABI vtable and provides a C++ interface for NL entity extraction.
 * Similar pattern to AbiSymbolExtractorAdapter.
 */
class AbiEntityExtractorAdapter {
public:
    explicit AbiEntityExtractorAdapter(yams_entity_extractor_v2* table) : table_(table) {}

    /**
     * @brief Check if the extractor supports a given content type.
     * @param contentType MIME type like "text/plain", "text/markdown"
     * @return true if supported
     */
    bool supportsContentType(const std::string& contentType) const {
        return table_ && table_->supports && table_->supports(table_->self, contentType.c_str());
    }

    /**
     * @brief Extract entities from text content.
     * @param content UTF-8 text to analyze
     * @param entityTypes Optional list of entity types to extract (nullptr for defaults)
     * @param entityTypeCount Number of entity types
     * @param language Optional language hint
     * @param filePath Optional file path for context
     * @return Extraction result (caller owns), or nullptr on error
     */
    yams_entity_extraction_result_v2* extract(std::string_view content,
                                              const char* const* entityTypes = nullptr,
                                              size_t entityTypeCount = 0,
                                              const char* language = nullptr,
                                              const char* filePath = nullptr) const {
        if (!table_ || !table_->extract)
            return nullptr;

        yams_entity_extraction_options_v2 options{};
        options.entity_types = entityTypes;
        options.entity_type_count = entityTypeCount;
        options.language = language;
        options.file_path = filePath;
        options.extract_relations = true;

        yams_entity_extraction_result_v2* result = nullptr;
        int rc = table_->extract(table_->self, content.data(), content.size(), &options, &result);
        if (rc != 0) {
            if (result) {
                freeResult(result);
            }
            return nullptr;
        }
        return result;
    }

    /**
     * @brief Free an extraction result.
     */
    void freeResult(yams_entity_extraction_result_v2* result) const {
        if (table_ && table_->free_result && result) {
            table_->free_result(table_->self, result);
        }
    }

    /**
     * @brief Get capabilities as JSON string.
     * @return JSON string (caller owns via freeString), or empty on error
     */
    std::string getCapabilitiesJson() const {
        if (!table_ || !table_->get_capabilities_json)
            return "{}";

        char* json = nullptr;
        int rc = table_->get_capabilities_json(table_->self, &json);
        if (rc != 0 || !json)
            return "{}";

        std::string result(json);
        if (table_->free_string) {
            table_->free_string(table_->self, json);
        }
        return result;
    }

    /**
     * @brief Get the underlying ABI table.
     */
    yams_entity_extractor_v2* table() const { return table_; }

    /**
     * @brief Get extractor ID for versioned state tracking.
     * @return String like "entity_extractor_glint:v0.1.0" or fallback
     */
    std::string getExtractorId() const;

private:
    yams_entity_extractor_v2* table_{};
};

} // namespace yams::daemon
