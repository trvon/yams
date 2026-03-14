// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#pragma once

#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <yams/metadata/knowledge_graph_store.h>
#include <yams/search/query_concept_extractor.h>

namespace yams::search {

struct GraphExpansionTerm {
    std::string text;
    float score = 0.0f;
};

struct GraphExpansionSeedDoc {
    std::string documentHash;
    std::string filePath;
    float score = 0.0f;
};

struct GraphExpansionConfig {
    size_t maxTerms = 0;
    size_t maxSeeds = 0;
    size_t maxNeighbors = 0;
};

std::vector<std::string> tokenizeKgQuery(std::string_view query);

float graphNodeExpansionWeight(const std::optional<std::string>& typeOpt,
                               std::string_view labelView);

std::vector<GraphExpansionTerm>
generateGraphExpansionTerms(const std::shared_ptr<metadata::KnowledgeGraphStore>& kgStore,
                            const std::string& query, const std::vector<QueryConcept>& concepts,
                            const GraphExpansionConfig& config);

std::vector<GraphExpansionTerm> generateGraphExpansionTermsFromDocuments(
    const std::shared_ptr<metadata::KnowledgeGraphStore>& kgStore,
    const std::vector<GraphExpansionSeedDoc>& seedDocs, const GraphExpansionConfig& config);

} // namespace yams::search
