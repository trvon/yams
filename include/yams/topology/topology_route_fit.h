// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <span>
#include <vector>

#include <yams/core/types.h>

namespace yams::topology {

/// One partition of a corpus plus sample queries, all in the same embedding space.
/// Every matrix is row-major with `dim` columns, and every row must be unit length: the fit
/// treats the dot product as cosine.
struct RouteFitInput {
    std::size_t dim{0};
    /// Document embeddings, one row per document.
    std::span<const float> keys;
    /// Cluster index of each document, in [0, clusters).
    std::span<const std::uint32_t> clusterOfKey;
    /// Centroid direction of each cluster, one row per cluster.
    std::span<const float> centroids;
    /// Sample queries the router should serve well.
    std::span<const float> queries;
};

struct RouteFitOptions {
    /// Ridge strengths tried, as multiples of the mean diagonal of the query Gram matrix. The
    /// one with the best held-out recall is kept.
    std::vector<double> ridgeStrengths{0.1, 0.3, 1.0, 3.0, 10.0};
    /// Every n-th query is held out for model selection (the last of each group of n).
    std::size_t holdoutEvery{5};
    /// Fewer fitted queries than this and the fit is not attempted.
    std::size_t minFitQueries{64};
    /// Clusters with fewer documents keep their centroid. A one-document cluster's centroid is
    /// that document, so cosine to it already equals the support function; a correction could
    /// only add noise, and storing one would cost a vector per document.
    std::size_t minClusterDocuments{3};
    /// Upper bound on queries x documents, the term the labelling cost scales with. The stage
    /// drops queries (generated ones first, sampled evenly) to stay under it, and skips the fit
    /// when too few would remain. About ten million pairs took nine seconds at 1024 dimensions.
    std::size_t maxQueryDocumentPairs{40'000'000};
    /// Queries embedded per embedder call; `cancelled` is polled between calls.
    std::size_t embedBatchSize{256};
    /// Polled between queries and between strengths; returning true abandons the fit.
    std::function<bool()> cancelled{};
};

struct RouteFitReport {
    /// One row per cluster. Equal to the centroids when `adopted` is false.
    std::vector<float> routeVectors;
    /// One entry per cluster: nonzero where the row differs from the centroid. All zero when
    /// `adopted` is false.
    std::vector<std::uint8_t> clusterFitted;
    /// True when the fitted vectors beat the centroids on the held-out queries.
    bool adopted{false};
    double chosenStrength{0.0};
    /// Share of held-out queries whose best document's cluster ranks in the top
    /// `recallDepth`, under centroid routing and under the best fitted vectors.
    double centroidHoldoutRecall{0.0};
    double fittedHoldoutRecall{0.0};
    std::size_t recallDepth{0};
    std::size_t fitQueries{0};
    std::size_t holdoutQueries{0};
};

/// Fit one routing vector per cluster so that <query, vector_c> approximates the cluster's
/// support function h_c(query) = max over its documents of <query, document>.
///
/// Centroid routing scores a cluster by its mean direction, which says little about its best
/// member for a query drawn from a different distribution than the documents. The fit keeps
/// the centroid and adds a ridge-regressed correction learned from the sample queries (the
/// one-layer case of a learned support-function router; see "Amortizing Maximum Inner Product
/// Search with Learned Support Functions", ICML 2026).
///
/// The result is never worse than the centroids on the held-out queries: when no strength
/// beats them, or there are too few queries, the centroids are returned and `adopted` is false.
/// Fitted vectors are scored by dot product, not cosine; their length carries information.
///
/// Cost: one pass of queries x documents dot products for the exact support values, then one
/// dim x dim factorization per strength.
[[nodiscard]] Result<RouteFitReport> fitRouteVectors(const RouteFitInput& input,
                                                     const RouteFitOptions& options = {});

} // namespace yams::topology
