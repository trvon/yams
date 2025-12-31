/**
 * @file pdf_metadata.h
 * @brief Minimal PDF metadata extraction
 *
 * This provides basic PDF metadata extraction by parsing the trailer /Info dictionary.
 * zpdf focuses on text extraction and doesn't expose metadata, so we implement
 * a minimal parser for common metadata fields.
 */

#pragma once

#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <unordered_map>

namespace yams::zyp {

/**
 * PDF document metadata.
 */
struct PdfMetadata {
    std::optional<std::string> title;
    std::optional<std::string> author;
    std::optional<std::string> subject;
    std::optional<std::string> keywords;
    std::optional<std::string> creator;  /**< Application that created the original */
    std::optional<std::string> producer; /**< PDF producer application */
    std::optional<std::string> creationDate;
    std::optional<std::string> modDate;

    /**
     * Convert metadata to a key-value map.
     * Only includes fields that have values.
     */
    [[nodiscard]] std::unordered_map<std::string, std::string> toMap() const;

    /**
     * Check if any metadata fields are present.
     */
    [[nodiscard]] bool empty() const;
};

/**
 * Extract metadata from a PDF buffer.
 *
 * This is a minimal parser that:
 * 1. Finds the trailer dictionary
 * 2. Locates the /Info reference
 * 3. Parses the info dictionary for standard fields
 *
 * @param data PDF file content
 * @return Metadata if found, nullopt on parse error
 */
[[nodiscard]] std::optional<PdfMetadata> extractMetadata(std::span<const uint8_t> data);

} // namespace yams::zyp
