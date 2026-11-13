// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#pragma once

#include <cstddef>
#include <functional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include <yams/core/types.h>
#include <yams/topology/topology_artifacts.h>
#include <yams/topology/topology_route_fit.h>

namespace yams::topology {

/// What a topology rebuild hands the route fit: the documents it clustered and the sample
/// queries to learn from.
struct RouteFitStageInput {
    /// The rebuild's inputs. A document without an embedding of the batch's dimension is not
    /// used, and neither is one the batch does not assign to a cluster.
    std::span<const TopologyDocumentInput> documents;
    /// Queries people actually ran, newest first.
    std::span<const std::string> loggedQueries;
    /// Queries synthesized from the corpus (document titles), for when the log is short.
    std::span<const std::string> generatedQueries;
    /// Embeds a batch of query texts in the documents' embedding space.
    std::function<Result<std::vector<std::vector<float>>>(std::span<const std::string>)> embed;
};

struct RouteFitStageStats {
    /// True when fitted vectors were attached to the batch.
    bool adopted{false};
    std::size_t fittedClusters{0};
    std::size_t queriesUsed{0};
    std::size_t documentsUsed{0};
    /// Held-out routing recall under centroids and under the best fit tried, whenever the fit
    /// ran, adopted or not. Equal when no fit beat the centroids.
    double centroidHoldoutRecall{0.0};
    double fittedHoldoutRecall{0.0};
    /// Why nothing was attached, as a stable token; empty when adopted.
    std::string skipReason;
};

/// Whether a logged query is worth learning routing from. Lookups by content hash, by path or
/// by file pattern are common from agents and scripts; they say nothing about which cluster a
/// natural-language query belongs to, so the stage leaves them out.
[[nodiscard]] bool isRouteFitTrainingQuery(std::string_view query);

/// Fit route vectors for `batch` and attach them to its clusters.
///
/// Every cluster's fitted vector and the batch's route-fit provenance are cleared first, so the
/// batch never keeps vectors from an earlier fit. They are set again only when the fit beats
/// centroid routing on held-out queries; see fitRouteVectors. Skips, with a reason, when there
/// is no embedder, too few distinct queries, no usable documents, or an embedding of the wrong
/// dimension, or a corpus too large for `maxQueryDocumentPairs`. Errors are reserved for
/// cancellation and embedder failures.
[[nodiscard]] Result<RouteFitStageStats> runRouteFitStage(TopologyArtifactBatch& batch,
                                                          const RouteFitStageInput& input,
                                                          const RouteFitOptions& options = {});

} // namespace yams::topology
