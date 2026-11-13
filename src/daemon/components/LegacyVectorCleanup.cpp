#include <yams/daemon/components/LegacyVectorCleanup.h>

#include <yams/metadata/metadata_repository.h>
#include <yams/vector/vector_database.h>

#include <algorithm>

namespace yams::daemon {

Result<LegacyVectorCleanupResult>
purgeLegacyVectorsWithoutText(metadata::MetadataRepository& metadata,
                              vector::VectorDatabase& vectors,
                              const std::unordered_set<std::string>& vectorDocumentHashes) {
    auto withoutContent = metadata.getDocumentHashesWithoutContent();
    if (!withoutContent) {
        return withoutContent.error();
    }
    LegacyVectorCleanupResult result;
    for (const auto& hash : withoutContent.value()) {
        if (!vectorDocumentHashes.contains(hash)) {
            continue;
        }
        const auto records = vectors.getVectorsByDocument(hash);
        if (records.empty() || !std::all_of(records.begin(), records.end(),
                                            [](const auto& r) { return r.model_id.empty(); })) {
            continue;
        }
        if (vectors.deleteVectorsByDocument(hash)) {
            ++result.documentsPurged;
        } else {
            ++result.documentsFailed;
        }
    }
    return result;
}

} // namespace yams::daemon
