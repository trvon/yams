#pragma once

#include <cstddef>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

namespace yams::metadata {
struct DocumentInfo;
} // namespace yams::metadata

namespace yams::cli::doctor {

struct SemanticDedupeMatch {
    size_t lhs{0};
    size_t rhs{0};
    double cosine{0.0};
    double titleOverlap{0.0};
    double pathOverlap{0.0};
    double score{0.0};
};

struct SemanticDedupeGroupPlan {
    std::vector<size_t> members;
    size_t canonicalIndex{0};
};

struct SemanticDedupeAnalysis {
    struct Row {
        yams::metadata::DocumentInfo doc;
        std::string normalizedTitle;
        std::string normalizedPath;
    };
    std::vector<Row> docs;
    std::vector<SemanticDedupeMatch> accepted;
    std::vector<SemanticDedupeGroupPlan> groups;
};

} // namespace yams::cli::doctor
