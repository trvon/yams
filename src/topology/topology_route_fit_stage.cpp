// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <yams/topology/topology_route_fit_stage.h>

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <string_view>
#include <unordered_map>
#include <unordered_set>

namespace yams::topology {

namespace {

std::string_view trimmed(std::string_view text) {
    const auto isSpace = [](char c) { return c == ' ' || c == '\t' || c == '\n' || c == '\r'; };
    while (!text.empty() && isSpace(text.front())) {
        text.remove_prefix(1);
    }
    while (!text.empty() && isSpace(text.back())) {
        text.remove_suffix(1);
    }
    return text;
}

// Appends `source` as a unit row of `matrix`; false when it has no direction.
bool appendUnitRow(std::vector<float>& matrix, std::span<const float> source) {
    double norm = 0.0;
    for (const float value : source) {
        norm += static_cast<double>(value) * static_cast<double>(value);
    }
    if (!(norm > 0.0) || !std::isfinite(norm)) {
        return false;
    }
    const auto scale = static_cast<float>(1.0 / std::sqrt(norm));
    for (const float value : source) {
        matrix.push_back(value * scale);
    }
    return true;
}

RouteFitStageStats skipped(std::string reason) {
    RouteFitStageStats stats;
    stats.skipReason = std::move(reason);
    return stats;
}

} // namespace

bool isRouteFitTrainingQuery(std::string_view query) {
    query = trimmed(query);
    if (query.size() < 3) {
        return false;
    }
    bool hasSpace = false;
    bool hasLetter = false;
    bool allHex = true;
    bool hasPathOrGlob = false;
    for (std::size_t i = 0; i < query.size(); ++i) {
        const char c = query[i];
        const bool digit = c >= '0' && c <= '9';
        const bool lower = c >= 'a' && c <= 'z';
        const bool upper = c >= 'A' && c <= 'Z';
        const bool nonAscii = static_cast<unsigned char>(c) >= 0x80;
        hasLetter = hasLetter || lower || upper || nonAscii;
        hasSpace = hasSpace || c == ' ' || c == '\t';
        allHex = allHex && (digit || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F'));
        // `?` is a glob wildcard except where it ends the token as punctuation.
        const bool wildcardMark = c == '?' && i + 1 < query.size();
        hasPathOrGlob = hasPathOrGlob || c == '/' || c == '\\' || c == '*' || wildcardMark;
    }
    if (!hasLetter) {
        return false; // numbers and punctuation only
    }
    if (hasSpace) {
        return true; // several words: a real question, even if it mentions a path
    }
    if (allHex && query.size() >= 8) {
        return false; // a content hash or a prefix of one
    }
    return !hasPathOrGlob; // a single token that is a path or a file pattern
}

Result<RouteFitStageStats> runRouteFitStage(TopologyArtifactBatch& batch,
                                            const RouteFitStageInput& input,
                                            const RouteFitOptions& options) {
    for (auto& cluster : batch.clusters) {
        cluster.fittedRouteVector.clear();
    }
    batch.routeFit = RouteFitProvenance{};
    batch.routeFitUnavailableReason.clear();

    if (!input.embed) {
        return skipped("no_embedder");
    }
    std::size_t dim = 0;
    for (const auto& cluster : batch.clusters) {
        if (!cluster.centroidEmbedding.empty()) {
            dim = cluster.centroidEmbedding.size();
            break;
        }
    }
    if (dim == 0 || batch.clusters.size() < 2) {
        return skipped("no_routable_clusters");
    }

    // Distinct queries, logged first so a short budget keeps the real ones.
    std::vector<std::string> queries;
    std::unordered_set<std::string_view> seen;
    std::size_t logged = 0;
    std::size_t generated = 0;
    const auto take = [&](std::span<const std::string> source, std::size_t& counter, bool filter) {
        for (const auto& text : source) {
            const auto query = trimmed(text);
            if (query.empty() || (filter && !isRouteFitTrainingQuery(query)) ||
                !seen.insert(query).second) {
                continue;
            }
            queries.emplace_back(query);
            ++counter;
        }
    };
    // `seen` views the callers' strings, which outlive this function body.
    take(input.loggedQueries, logged, /*filter=*/true);
    take(input.generatedQueries, generated, /*filter=*/false);
    const std::size_t holdoutEvery = std::max<std::size_t>(2, options.holdoutEvery);
    if (queries.size() - queries.size() / holdoutEvery < options.minFitQueries) {
        return skipped("too_few_queries");
    }

    // Cluster index of every cluster id, and unit centroids (a zero row for a cluster that has
    // none; such a cluster is never assigned a fitted vector).
    std::unordered_map<std::string_view, std::uint32_t> clusterIndex;
    clusterIndex.reserve(batch.clusters.size());
    std::vector<float> centroids;
    centroids.reserve(batch.clusters.size() * dim);
    for (std::size_t c = 0; c < batch.clusters.size(); ++c) {
        const auto& cluster = batch.clusters[c];
        clusterIndex.emplace(cluster.clusterId, static_cast<std::uint32_t>(c));
        if (cluster.centroidEmbedding.size() != dim ||
            !appendUnitRow(centroids, cluster.centroidEmbedding)) {
            centroids.resize((c + 1) * dim, 0.0F);
        }
    }

    // One row per document that has a usable embedding and a cluster. A document with several
    // memberships counts toward the first one listed, its primary cluster.
    std::unordered_map<std::string_view, std::uint32_t> clusterOfDocument;
    clusterOfDocument.reserve(batch.memberships.size());
    for (const auto& membership : batch.memberships) {
        if (const auto it = clusterIndex.find(membership.clusterId); it != clusterIndex.end()) {
            clusterOfDocument.emplace(membership.documentHash, it->second);
        }
    }
    std::vector<float> keys;
    std::vector<std::uint32_t> clusterOfKey;
    keys.reserve(input.documents.size() * dim);
    clusterOfKey.reserve(input.documents.size());
    for (const auto& document : input.documents) {
        const auto it = clusterOfDocument.find(document.documentHash);
        if (it == clusterOfDocument.end() || document.embedding.size() != dim) {
            continue;
        }
        if (appendUnitRow(keys, document.embedding)) {
            clusterOfKey.push_back(it->second);
        }
    }
    if (clusterOfKey.empty()) {
        return skipped("no_usable_documents");
    }

    // Labelling costs queries x documents; stay under the budget by dropping queries. Logged
    // queries are kept first, and the generated ones that still fit are taken evenly so they
    // keep covering the whole corpus.
    if (const std::size_t allowed = options.maxQueryDocumentPairs / clusterOfKey.size();
        queries.size() > allowed) {
        if (allowed - allowed / holdoutEvery < options.minFitQueries) {
            return skipped("corpus_too_large_for_budget");
        }
        std::vector<std::string> kept;
        kept.reserve(allowed);
        const std::size_t keptLogged = std::min(logged, allowed);
        for (std::size_t i = 0; i < keptLogged; ++i) {
            kept.push_back(std::move(queries[i]));
        }
        const std::size_t slots = allowed - keptLogged;
        for (std::size_t i = 0; i < slots; ++i) {
            kept.push_back(std::move(queries[logged + i * generated / slots]));
        }
        logged = keptLogged;
        generated = slots;
        queries = std::move(kept);
    }

    // Embedded in batches so a long embedding pass yields to pressure between calls.
    std::vector<float> queryRows;
    queryRows.reserve(queries.size() * dim);
    const std::size_t embedBatch = std::max<std::size_t>(1, options.embedBatchSize);
    for (std::size_t begin = 0; begin < queries.size(); begin += embedBatch) {
        if (options.cancelled && options.cancelled()) {
            return Error{ErrorCode::OperationCancelled, "route fit cancelled"};
        }
        const std::size_t count = std::min(embedBatch, queries.size() - begin);
        auto embedded = input.embed(std::span<const std::string>(queries).subspan(begin, count));
        if (!embedded) {
            return embedded.error();
        }
        if (embedded.value().size() != count) {
            return skipped("query_embedding_count_mismatch");
        }
        for (const auto& vector : embedded.value()) {
            if (vector.size() != dim) {
                return skipped("query_embedding_dimension_mismatch");
            }
            // A query with no direction gets a zero row so rows stay aligned with queries.
            if (!appendUnitRow(queryRows, vector)) {
                queryRows.resize(queryRows.size() + dim, 0.0F);
            }
        }
    }

    const RouteFitInput fitInput{.dim = dim,
                                 .keys = keys,
                                 .clusterOfKey = clusterOfKey,
                                 .centroids = centroids,
                                 .queries = queryRows};
    auto fitted = fitRouteVectors(fitInput, options);
    if (!fitted) {
        return fitted.error();
    }
    const auto& report = fitted.value();

    RouteFitStageStats stats;
    stats.queriesUsed = queryRows.size() / dim;
    stats.documentsUsed = clusterOfKey.size();
    stats.centroidHoldoutRecall = report.centroidHoldoutRecall;
    stats.fittedHoldoutRecall = report.fittedHoldoutRecall;
    if (!report.adopted) {
        stats.skipReason = report.fitQueries < options.minFitQueries ? "too_few_queries"
                                                                     : "no_gain_over_centroids";
        return stats;
    }
    for (std::size_t c = 0; c < batch.clusters.size(); ++c) {
        auto& cluster = batch.clusters[c];
        if (report.clusterFitted[c] == 0 || cluster.centroidEmbedding.size() != dim) {
            continue;
        }
        cluster.fittedRouteVector.assign(
            report.routeVectors.begin() + static_cast<std::ptrdiff_t>(c * dim),
            report.routeVectors.begin() + static_cast<std::ptrdiff_t>((c + 1) * dim));
        ++stats.fittedClusters;
    }
    if (stats.fittedClusters == 0) {
        stats.skipReason = "no_cluster_large_enough";
        return stats;
    }
    stats.adopted = true;
    batch.routeFit =
        RouteFitProvenance{.chosenStrength = report.chosenStrength,
                           .centroidHoldoutRecall = report.centroidHoldoutRecall,
                           .fittedHoldoutRecall = report.fittedHoldoutRecall,
                           .fitQueries = static_cast<std::uint32_t>(report.fitQueries),
                           .holdoutQueries = static_cast<std::uint32_t>(report.holdoutQueries),
                           .loggedQueries = static_cast<std::uint32_t>(logged),
                           .generatedQueries = static_cast<std::uint32_t>(generated),
                           .fittedAtEpoch = batch.topologyEpoch};
    return stats;
}

} // namespace yams::topology
