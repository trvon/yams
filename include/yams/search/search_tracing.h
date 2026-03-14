// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

#include <yams/metadata/document_metadata.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/search/search_engine.h>

namespace yams::search {

std::string deriveDocIdFromPath(const std::string& path);
std::string documentIdForTrace(const std::string& filePath, const std::string& documentHash);

std::vector<std::string>
collectUniqueComponentDocIds(const std::vector<ComponentResult>& componentResults);

std::vector<std::string>
collectRankedResultDocIds(const std::vector<SearchResult>& results,
                          size_t maxCount = std::numeric_limits<size_t>::max());

std::vector<std::string> setDifferenceIds(const std::vector<std::string>& lhs,
                                          const std::vector<std::string>& rhs);

std::string joinWithTab(const std::vector<std::string>& values);

nlohmann::json buildComponentHitSummaryJson(const std::vector<ComponentResult>& componentResults,
                                            size_t topPerComponent);

nlohmann::json buildFusionTopSummaryJson(const std::vector<SearchResult>& results, size_t maxDocs);

std::optional<std::int64_t>
resolveKgDocumentId(const std::shared_ptr<metadata::KnowledgeGraphStore>& kgStore,
                    const metadata::DocumentInfo& doc);

nlohmann::json buildGraphDocProbeJson(const std::shared_ptr<metadata::KnowledgeGraphStore>& kgStore,
                                      const std::vector<SearchResult>& results, size_t limit);

} // namespace yams::search
