#include <yams/topology/protected_relation_cover.h>

#include "protected_relation_identity_internal.h"

#include <algorithm>
#include <array>
#include <charconv>
#include <cmath>
#include <limits>
#include <ranges>
#include <string_view>
#include <unordered_set>

namespace yams::topology {

namespace {

class Fnv1a64Builder {
public:
    void append(std::string_view value) noexcept {
        for (const unsigned char byte : value) {
            fingerprint_ ^= byte;
            fingerprint_ *= kPrime;
        }
    }

    void append(std::size_t value) noexcept {
        std::array<char, std::numeric_limits<std::size_t>::digits10 + 1> buffer{};
        const auto result = std::to_chars(buffer.data(), buffer.data() + buffer.size(), value);
        append(
            std::string_view{buffer.data(), static_cast<std::size_t>(result.ptr - buffer.data())});
    }

    void append(float value) noexcept {
        std::array<char, 64> buffer{};
        const auto result =
            std::to_chars(buffer.data(), buffer.data() + buffer.size(), value,
                          std::chars_format::general, std::numeric_limits<float>::max_digits10);
        append(
            std::string_view{buffer.data(), static_cast<std::size_t>(result.ptr - buffer.data())});
    }

    [[nodiscard]] std::uint64_t fingerprint() const noexcept { return fingerprint_; }

private:
    static constexpr std::uint64_t kOffset = 14695981039346656037ULL;
    static constexpr std::uint64_t kPrime = 1099511628211ULL;
    std::uint64_t fingerprint_{kOffset};
};

std::string formatProtectedRelationIdentity(std::uint64_t fingerprint) {
    constexpr std::string_view prefix = "semantic_neighbor:v1:construction:fnv1a64:";
    std::array<char, 16> encoded{};
    const auto result =
        std::to_chars(encoded.data(), encoded.data() + encoded.size(), fingerprint, 16);

    std::string identity{prefix};
    identity.append(encoded.size() - static_cast<std::size_t>(result.ptr - encoded.data()), '0');
    identity.append(encoded.data(), static_cast<std::size_t>(result.ptr - encoded.data()));
    return identity;
}

} // namespace

std::string detail::protectedRelationIdentityFromObservations(
    std::vector<ProtectedRelationObservation> observations) {
    std::ranges::stable_sort(observations, [](const auto& lhs, const auto& rhs) {
        if (lhs.lhs != rhs.lhs) {
            return lhs.lhs < rhs.lhs;
        }
        return lhs.rhs < rhs.rhs;
    });

    Fnv1a64Builder descriptor;
    descriptor.append(
        "relation=semantic_neighbor;provenance=topology_input;version=1;split=construction");
    for (std::size_t index = 0; index < observations.size();) {
        const auto& observation = observations[index];
        float score = observation.score;
        std::size_t next = index + 1;
        while (next < observations.size() && observations[next].lhs == observation.lhs &&
               observations[next].rhs == observation.rhs) {
            score = std::max(score, observations[next].score);
            ++next;
        }

        descriptor.append(";lhs=");
        descriptor.append(observation.lhs.size());
        descriptor.append(":");
        descriptor.append(observation.lhs);
        descriptor.append(";rhs=");
        descriptor.append(observation.rhs.size());
        descriptor.append(":");
        descriptor.append(observation.rhs);
        descriptor.append(";score=");
        descriptor.append(score);
        index = next;
    }
    return formatProtectedRelationIdentity(descriptor.fingerprint());
}

std::string protectedRelationConstructionIdentity(std::span<const TopologyDocumentInput> documents,
                                                  const TopologyBuildConfig& config) {
    std::unordered_set<std::string_view> documentHashes;
    documentHashes.reserve(documents.size());
    for (const auto& document : documents) {
        if (!document.documentHash.empty()) {
            documentHashes.insert(document.documentHash);
        }
    }

    std::size_t observationCapacity = 0;
    for (const auto& document : documents) {
        observationCapacity += document.neighbors.size();
    }
    std::vector<detail::ProtectedRelationObservation> observations;
    observations.reserve(observationCapacity);
    for (const auto& document : documents) {
        const std::string_view documentHash = document.documentHash;
        for (const auto& neighbor : document.neighbors) {
            const std::string_view neighborHash = neighbor.documentHash;
            if (documentHash.empty() || neighborHash.empty() || documentHash == neighborHash ||
                !documentHashes.contains(neighborHash) ||
                (config.reciprocalOnly && !neighbor.reciprocal) || !std::isfinite(neighbor.score) ||
                neighbor.score < static_cast<float>(config.minEdgeScore)) {
                continue;
            }
            const auto endpoints = std::minmax(documentHash, neighborHash);
            observations.push_back(detail::ProtectedRelationObservation{
                .lhs = endpoints.first,
                .rhs = endpoints.second,
                .score = neighbor.score,
            });
        }
    }
    return detail::protectedRelationIdentityFromObservations(std::move(observations));
}

Result<ProtectedRelationCoverIndex>
buildProtectedRelationCoverIndex(const TopologyArtifactBatch& artifacts) {
    ProtectedRelationCoverIndex index;
    index.fibers.reserve(artifacts.clusters.size());
    index.fiberById.reserve(artifacts.clusters.size());
    index.fiberIndicesByDocumentHash.reserve(artifacts.memberships.size());

    std::vector<const ClusterArtifact*> clusters;
    clusters.reserve(artifacts.clusters.size());
    for (const auto& cluster : artifacts.clusters) {
        clusters.push_back(&cluster);
    }
    std::ranges::sort(clusters, {}, &ClusterArtifact::clusterId);

    for (const auto* cluster : clusters) {
        if (cluster->clusterId.empty()) {
            return Error{ErrorCode::InvalidData, "protected relation fiber has no id"};
        }
        if (index.fiberById.contains(cluster->clusterId)) {
            return Error{ErrorCode::InvalidData, "protected relation cover has duplicate fiber id"};
        }

        auto documentHashes = cluster->memberDocumentHashes;
        std::ranges::sort(documentHashes);
        const auto uniqueEnd = std::ranges::unique(documentHashes).begin();
        if (uniqueEnd != documentHashes.end()) {
            return Error{ErrorCode::InvalidData,
                         "protected relation fiber has duplicate document membership"};
        }
        if (documentHashes.empty()) {
            return Error{ErrorCode::InvalidData, "protected relation fiber is empty"};
        }

        const auto fiberIndex = index.fibers.size();
        index.fiberById.emplace(cluster->clusterId, fiberIndex);
        for (const auto& documentHash : documentHashes) {
            if (documentHash.empty()) {
                return Error{ErrorCode::InvalidData,
                             "protected relation fiber has an empty document"};
            }
            index.fiberIndicesByDocumentHash[documentHash].push_back(fiberIndex);
        }
        index.fibers.emplace_back();
        auto& fiber = index.fibers.back();
        fiber.fiberId = cluster->clusterId;
        fiber.documentHashes.swap(documentHashes);
    }

    for (const auto& membership : artifacts.memberships) {
        if (membership.documentHash.empty() ||
            !index.fiberIndicesByDocumentHash.contains(membership.documentHash)) {
            return Error{ErrorCode::InvalidData,
                         "protected relation membership is absent from the cover"};
        }
    }
    return index;
}

} // namespace yams::topology
