#include <yams/api/content_store.h>
#include <yams/app/services/prune_planner.h>
#include <yams/core/magic_numbers.hpp>
#include <yams/metadata/document_metadata.h>
#include <yams/metadata/metadata_repository.h>

#include <spdlog/spdlog.h>

#include <algorithm>
#include <filesystem>
#include <ranges>
#include <span>

namespace yams::app::services {

namespace {

constexpr std::size_t kPruneSignatureBytes = 4096;

magic::PruneCategory
classifyPruneCandidate(const metadata::DocumentInfo& doc,
                       const std::shared_ptr<api::IContentStore>& contentStore) {
    auto category = magic::getPruneCategory(doc.filePath, doc.fileExtension, doc.mimeType);
    // For extensionless files, a definitive signature is more specific than a generic path rule
    // such as a parent build directory.
    const bool needsSignature =
        doc.fileExtension.empty() || std::filesystem::path(doc.filePath).extension().empty();
    if (!needsSignature || !contentStore || doc.sha256Hash.empty()) {
        return category;
    }

    auto bytes = contentStore->retrieveBytesPrefix(doc.sha256Hash, kPruneSignatureBytes);
    if (!bytes || bytes.value().empty()) {
        return category;
    }
    const auto* data = reinterpret_cast<const uint8_t*>(bytes.value().data());
    const auto detectedMime =
        magic::detect_mime_type(std::span<const uint8_t>(data, bytes.value().size()));
    const auto signatureCategory = magic::getPruneCategory({}, {}, detectedMime);
    return signatureCategory != magic::PruneCategory::None ? signatureCategory : category;
}

std::vector<PruneCandidate>
buildPruneCandidates(std::span<const metadata::DocumentInfo> docs, const PruneConfig& config,
                     const std::shared_ptr<api::IContentStore>& contentStore) {
    std::vector<PruneCandidate> candidates;

    auto matchesCategory = [&](magic::PruneCategory category) -> bool {
        if (config.categories.empty())
            return true;
        return std::ranges::any_of(config.categories, [category](const auto& cat) {
            return magic::matchesPruneGroup(category, cat);
        });
    };

    auto meetsAgeCriteria = [&](const metadata::DocumentInfo& doc) -> bool {
        if (config.minAge.count() == 0)
            return true;
        auto now = std::chrono::system_clock::now();
        auto fileAge = std::chrono::duration_cast<std::chrono::seconds>(now - doc.modifiedTime);
        return fileAge >= config.minAge;
    };

    auto meetsSizeCriteria = [&](const metadata::DocumentInfo& doc) -> bool {
        return doc.fileSize >= config.minSize && doc.fileSize <= config.maxSize;
    };

    auto meetsExtensionCriteria = [&](const metadata::DocumentInfo& doc) -> bool {
        if (config.extensions.empty())
            return true;
        return std::ranges::any_of(config.extensions, [&](const auto& ext) {
            return doc.fileExtension == ext || doc.fileExtension == ("." + ext);
        });
    };

    for (const auto& doc : docs) {
        if (!meetsAgeCriteria(doc))
            continue;
        if (!meetsSizeCriteria(doc))
            continue;
        if (!meetsExtensionCriteria(doc))
            continue;
        const auto category = classifyPruneCandidate(doc, contentStore);
        if (!matchesCategory(category))
            continue;

        PruneCandidate candidate;
        candidate.hash = doc.sha256Hash;
        candidate.path = doc.filePath;
        candidate.fileSize = doc.fileSize;
        candidate.modifiedTime = doc.modifiedTime;
        candidate.category = magic::getPruneCategoryName(category);

        candidates.push_back(std::move(candidate));
    }

    return candidates;
}

} // namespace

std::vector<PruneCandidate>
queryPruneCandidates(metadata::MetadataRepository& repo, const PruneConfig& config,
                     const std::shared_ptr<api::IContentStore>& contentStore) {
    auto docsResult = repo.queryDocuments(metadata::DocumentQueryOptions{});
    if (!docsResult) {
        spdlog::error("Failed to query documents: {}", docsResult.error().message);
        return {};
    }

    auto& allDocs = docsResult.value();
    spdlog::debug("Prune query: scanning {} documents", allDocs.size());

    auto candidates = buildPruneCandidates(allDocs, config, contentStore);

    spdlog::info("Prune query: found {} candidates", candidates.size());
    return candidates;
}

} // namespace yams::app::services
