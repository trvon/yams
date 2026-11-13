#pragma once

#include <yams/core/types.h>

#include <cstddef>
#include <string>
#include <unordered_set>

namespace yams::metadata {
class MetadataRepository;
} // namespace yams::metadata

namespace yams::vector {
class VectorDatabase;
} // namespace yams::vector

namespace yams::daemon {

struct LegacyVectorCleanupResult {
    std::size_t documentsPurged{0};
    std::size_t documentsFailed{0};
};

/// Delete vectors that embed text the document no longer has. Builds before model ids were
/// recorded wrote vectors with an empty model_id; when MIME repair or re-extraction later
/// removed the document's text, those vectors stayed and kept surfacing in search. A document
/// qualifies only when it has no content row and every one of its vectors has an empty
/// model_id: replicated or current vectors always carry a model id and are left alone, since a
/// peer's embedding can legitimately arrive before local extraction runs.
Result<LegacyVectorCleanupResult>
purgeLegacyVectorsWithoutText(metadata::MetadataRepository& metadata,
                              vector::VectorDatabase& vectors,
                              const std::unordered_set<std::string>& vectorDocumentHashes);

} // namespace yams::daemon
