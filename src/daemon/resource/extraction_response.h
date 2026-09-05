#pragma once

#include <nlohmann/json.hpp>
#include <yams/extraction/content_extractor.h>

namespace yams::daemon {

// JSON-RPC result decoding is a pure seam, independent of subprocess lifecycle.
inline std::optional<extraction::ExtractedContent>
decodeExtractionResponse(const nlohmann::json& response) {
    if (!response.is_object() || (response.contains("error") && !response.at("error").is_null())) {
        return std::nullopt;
    }
    extraction::ExtractedContent result;
    if (response.contains("text") && response.at("text").is_string()) {
        result.text = response.at("text").get<std::string>();
    } else if (response.contains("content") && response.at("content").is_string()) {
        result.text = response.at("content").get<std::string>();
    } else {
        return std::nullopt;
    }
    if (const auto metadata = response.find("metadata");
        metadata != response.end() && metadata->is_object()) {
        for (const auto& [key, value] : metadata->items()) {
            // Match the ABI's string-valued metadata contract. Nested or typed
            // values must not silently become misleading serialized strings.
            if (value.is_string()) {
                result.metadata.emplace(key, value.get<std::string>());
            }
        }
    }
    return result;
}

} // namespace yams::daemon
