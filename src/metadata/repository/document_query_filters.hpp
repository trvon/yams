#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace yams::metadata {

struct DocumentQueryOptions;

namespace repository {

struct BindParam {
    enum class Type { Text, Int } type;
    std::string text;
    int64_t integer{0};
};

void addTextParam(std::vector<BindParam>& params, std::string value);
void addIntParam(std::vector<BindParam>& params, int64_t value);

void appendDocumentQueryFilters(const DocumentQueryOptions& options, bool joinFtsForContains,
                                bool hasPathIndexing, std::vector<std::string>& conditions,
                                std::vector<BindParam>& params, bool logExactPath);
void appendMetadataValueCountDocumentFilters(const DocumentQueryOptions& options,
                                             bool joinFtsForContains, bool hasPathIndexing,
                                             std::vector<std::string>& conditions,
                                             std::vector<BindParam>& params);

} // namespace repository
} // namespace yams::metadata
