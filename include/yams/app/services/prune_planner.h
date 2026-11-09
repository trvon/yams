#pragma once

#include <yams/core/types.h>

#include <chrono>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <vector>

namespace yams::metadata {
class MetadataRepository;
} // namespace yams::metadata

namespace yams::api {
class IContentStore;
} // namespace yams::api

namespace yams::app::services {

/// Filters that select which indexed documents a prune would remove.
struct PruneConfig {
    std::vector<std::string> categories; ///< Prune categories (build-artifacts, logs, etc.)
    std::vector<std::string> extensions; ///< File extensions; case and leading dot ignored
    std::chrono::seconds minAge{0};      ///< Minimum file age to prune
    int64_t minSize{0};                  ///< Minimum file size to prune
    int64_t maxSize{std::numeric_limits<int64_t>::max()}; ///< Maximum file size to prune
};

/// Document selected for pruning.
struct PruneCandidate {
    std::string hash;
    std::string path;
    std::string category;
    int64_t fileSize{0};
    std::chrono::system_clock::time_point modifiedTime;
};

/// Scan the metadata repository and return the documents that match `config`.
///
/// Documents are classified by path, extension and MIME type. When `contentStore` is provided,
/// extensionless documents are also classified by their leading content signature. Fails when
/// the repository cannot be queried.
[[nodiscard]] Result<std::vector<PruneCandidate>>
queryPruneCandidates(metadata::MetadataRepository& repo, const PruneConfig& config,
                     const std::shared_ptr<api::IContentStore>& contentStore = nullptr);

} // namespace yams::app::services
