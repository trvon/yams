// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <yams/topology/topology_route_fit.h>

#include <algorithm>
#include <cmath>
#include <string>

namespace yams::topology {

namespace {

constexpr std::size_t kRecallDepth = 5;

float dot(const float* a, const float* b, std::size_t n) noexcept {
    float acc[8] = {0, 0, 0, 0, 0, 0, 0, 0};
    std::size_t i = 0;
    for (; i + 8 <= n; i += 8) {
        for (std::size_t j = 0; j < 8; ++j) {
            acc[j] += a[i + j] * b[i + j];
        }
    }
    float sum = 0.0F;
    for (; i < n; ++i) {
        sum += a[i] * b[i];
    }
    for (const float part : acc) {
        sum += part;
    }
    return sum;
}

// Share of the given queries whose true cluster ranks within `depth` under score = <q, v_c>.
double recallWithin(std::span<const float> queries, std::span<const std::size_t> rows,
                    std::span<const std::uint32_t> trueCluster, std::span<const float> vectors,
                    std::size_t dim, std::size_t clusters, std::size_t depth) {
    if (rows.empty()) {
        return 0.0;
    }
    std::size_t hits = 0;
    for (const auto row : rows) {
        const float* q = queries.data() + row * dim;
        const std::uint32_t truth = trueCluster[row];
        const float truthScore = dot(q, vectors.data() + truth * dim, dim);
        std::size_t better = 0;
        for (std::size_t c = 0; c < clusters && better < depth; ++c) {
            if (c == truth) {
                continue;
            }
            const float score = dot(q, vectors.data() + c * dim, dim);
            if (score > truthScore || (score == truthScore && c < truth)) {
                ++better;
            }
        }
        hits += better < depth ? 1 : 0;
    }
    return static_cast<double>(hits) / static_cast<double>(rows.size());
}

// Lower Cholesky factor of (gram + shift * I), in place. Returns false when the matrix is not
// positive definite even after the shift.
bool factorize(std::vector<double>& matrix, std::size_t d, double shift) {
    for (std::size_t i = 0; i < d; ++i) {
        matrix[i * d + i] += shift;
    }
    for (std::size_t i = 0; i < d; ++i) {
        for (std::size_t j = 0; j <= i; ++j) {
            double sum = matrix[i * d + j];
            for (std::size_t p = 0; p < j; ++p) {
                sum -= matrix[i * d + p] * matrix[j * d + p];
            }
            if (i == j) {
                if (!(sum > 0.0)) {
                    return false;
                }
                matrix[i * d + j] = std::sqrt(sum);
            } else {
                matrix[i * d + j] = sum / matrix[j * d + j];
            }
        }
    }
    return true;
}

void solveInPlace(const std::vector<double>& factor, std::size_t d, std::vector<double>& rhs) {
    for (std::size_t i = 0; i < d; ++i) {
        double sum = rhs[i];
        for (std::size_t p = 0; p < i; ++p) {
            sum -= factor[i * d + p] * rhs[p];
        }
        rhs[i] = sum / factor[i * d + i];
    }
    for (std::size_t i = d; i-- > 0;) {
        double sum = rhs[i];
        for (std::size_t p = i + 1; p < d; ++p) {
            sum -= factor[p * d + i] * rhs[p];
        }
        rhs[i] = sum / factor[i * d + i];
    }
}

} // namespace

Result<RouteFitReport> fitRouteVectors(const RouteFitInput& input, const RouteFitOptions& options) {
    const std::size_t d = input.dim;
    if (d == 0 || input.keys.size() % d != 0 || input.centroids.size() % d != 0 ||
        input.queries.size() % d != 0) {
        return Error{ErrorCode::InvalidArgument, "route fit: matrix sizes do not match dim"};
    }
    const std::size_t documents = input.keys.size() / d;
    const std::size_t clusters = input.centroids.size() / d;
    const std::size_t queries = input.queries.size() / d;
    if (clusters == 0 || documents == 0 || input.clusterOfKey.size() != documents) {
        return Error{ErrorCode::InvalidArgument, "route fit: empty or inconsistent partition"};
    }
    for (const auto cluster : input.clusterOfKey) {
        if (cluster >= clusters) {
            return Error{ErrorCode::InvalidArgument, "route fit: document assigned to cluster " +
                                                         std::to_string(cluster) + " of " +
                                                         std::to_string(clusters)};
        }
    }
    const auto cancelled = [&options] { return options.cancelled && options.cancelled(); };

    RouteFitReport report;
    report.routeVectors.assign(input.centroids.begin(), input.centroids.end());
    report.clusterFitted.assign(clusters, 0);
    std::vector<std::size_t> clusterDocuments(clusters, 0);
    for (const auto cluster : input.clusterOfKey) {
        ++clusterDocuments[cluster];
    }
    std::vector<std::uint8_t> eligible(clusters, 0);
    for (std::size_t c = 0; c < clusters; ++c) {
        eligible[c] = clusterDocuments[c] >= options.minClusterDocuments ? 1 : 0;
    }
    report.recallDepth = std::min(kRecallDepth, clusters);

    const std::size_t holdoutEvery = std::max<std::size_t>(2, options.holdoutEvery);
    std::vector<std::size_t> fitRows;
    std::vector<std::size_t> holdoutRows;
    for (std::size_t q = 0; q < queries; ++q) {
        (q % holdoutEvery == holdoutEvery - 1 ? holdoutRows : fitRows).push_back(q);
    }
    report.fitQueries = fitRows.size();
    report.holdoutQueries = holdoutRows.size();
    if (fitRows.size() < options.minFitQueries || holdoutRows.empty() || clusters < 2) {
        return report;
    }

    // Exact support values: h[q][c] = best document of cluster c for query q.
    std::vector<float> support(queries * clusters, -2.0F);
    std::vector<std::uint32_t> trueCluster(queries, 0);
    for (std::size_t q = 0; q < queries; ++q) {
        if (cancelled()) {
            return Error{ErrorCode::OperationCancelled, "route fit cancelled"};
        }
        const float* query = input.queries.data() + q * d;
        float* row = support.data() + q * clusters;
        float best = -2.0F;
        for (std::size_t k = 0; k < documents; ++k) {
            const float score = dot(query, input.keys.data() + k * d, d);
            const std::uint32_t cluster = input.clusterOfKey[k];
            row[cluster] = std::max(row[cluster], score);
            if (score > best) {
                best = score;
                trueCluster[q] = cluster;
            }
        }
    }

    report.centroidHoldoutRecall = recallWithin(input.queries, holdoutRows, trueCluster,
                                                input.centroids, d, clusters, report.recallDepth);
    report.fittedHoldoutRecall = report.centroidHoldoutRecall;

    // Normal equations for the correction: minimize sum (<q, centroid_c + w_c> - h_c(q))^2.
    std::vector<double> gram(d * d, 0.0);
    std::vector<double> moments(d * clusters, 0.0);
    std::vector<double> residual(clusters);
    for (const auto q : fitRows) {
        if (cancelled()) {
            return Error{ErrorCode::OperationCancelled, "route fit cancelled"};
        }
        const float* query = input.queries.data() + q * d;
        for (std::size_t c = 0; c < clusters; ++c) {
            const float h = support[q * clusters + c];
            // Small and empty clusters keep their centroid: a zero residual fits no correction.
            residual[c] =
                (eligible[c] == 0 || h < -1.5F)
                    ? 0.0
                    : static_cast<double>(h) -
                          static_cast<double>(dot(query, input.centroids.data() + c * d, d));
        }
        for (std::size_t i = 0; i < d; ++i) {
            const double qi = query[i];
            double* gramRow = gram.data() + i * d;
            for (std::size_t j = 0; j <= i; ++j) {
                gramRow[j] += qi * static_cast<double>(query[j]);
            }
            double* momentRow = moments.data() + i * clusters;
            for (std::size_t c = 0; c < clusters; ++c) {
                momentRow[c] += qi * residual[c];
            }
        }
    }
    double meanDiagonal = 0.0;
    for (std::size_t i = 0; i < d; ++i) {
        meanDiagonal += gram[i * d + i];
    }
    meanDiagonal /= static_cast<double>(d);
    if (!(meanDiagonal > 0.0)) {
        return report;
    }

    std::vector<float> candidate(clusters * d);
    std::vector<double> rhs(d);
    for (const double strength : options.ridgeStrengths) {
        if (cancelled()) {
            return Error{ErrorCode::OperationCancelled, "route fit cancelled"};
        }
        if (!(strength > 0.0)) {
            continue;
        }
        std::vector<double> factor = gram;
        if (!factorize(factor, d, strength * meanDiagonal)) {
            continue;
        }
        for (std::size_t c = 0; c < clusters; ++c) {
            if (eligible[c] == 0) {
                std::copy_n(input.centroids.data() + c * d, d, candidate.data() + c * d);
                continue;
            }
            for (std::size_t i = 0; i < d; ++i) {
                rhs[i] = moments[i * clusters + c];
            }
            solveInPlace(factor, d, rhs);
            for (std::size_t i = 0; i < d; ++i) {
                candidate[c * d + i] = input.centroids[c * d + i] + static_cast<float>(rhs[i]);
            }
        }
        const double recall = recallWithin(input.queries, holdoutRows, trueCluster, candidate, d,
                                           clusters, report.recallDepth);
        if (recall > report.fittedHoldoutRecall) {
            report.fittedHoldoutRecall = recall;
            report.chosenStrength = strength;
            report.routeVectors = candidate;
            report.clusterFitted = eligible;
            report.adopted = true;
        }
    }
    return report;
}

} // namespace yams::topology
