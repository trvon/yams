#pragma once

#include <yams/core/types.h>
#include <yams/topology/topology_artifacts.h>

#include <cstddef>
#include <string>
#include <unordered_map>
#include <vector>

namespace yams::topology {

/// One explicit element of the protected-relation cover. A document may belong to several fibers.
struct ProtectedRelationFiber {
    std::string fiberId;
    std::vector<std::string> documentHashes;
};

/// Immutable query-time index derived from the construction's overlapping relation cover.
struct ProtectedRelationCoverIndex {
    std::vector<ProtectedRelationFiber> fibers;
    std::unordered_map<std::string, std::size_t> fiberById;
    std::unordered_map<std::string, std::vector<std::size_t>> fiberIndicesByDocumentHash;
};

/// Materialize the construction-owned cover. Fibers are explicit cover elements, so overlapping
/// membership is preserved instead of collapsed into one membership signature.
[[nodiscard]] Result<ProtectedRelationCoverIndex>
buildProtectedRelationCoverIndex(const TopologyArtifactBatch& artifacts);

} // namespace yams::topology
