#include <spdlog/spdlog.h>
#include <algorithm>
#include <cstdlib>
#include <iterator>
#include <stdexcept>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/query_helpers.h>
#include <yams/metadata/versioning_util.h>

namespace yams::metadata {

Result<std::vector<VersionRetentionCandidate>>
selectVersionRetentionCandidates(IMetadataRepository& repo, const VersionRetentionPolicy& policy) {
    if (policy.keepLatest == 0) {
        return Error{ErrorCode::InvalidArgument, "keepLatest must be at least one"};
    }

    DocumentQueryOptions options;
    if (policy.seriesKey) {
        options.metadataFilters.emplace_back("series_key", *policy.seriesKey);
    }
    auto documentsResult = repo.queryDocuments(options);
    if (!documentsResult) {
        return documentsResult.error();
    }

    std::vector<int64_t> ids;
    ids.reserve(documentsResult.value().size());
    for (const auto& document : documentsResult.value()) {
        ids.push_back(document.id);
    }
    auto metadataResult = repo.getMetadataForDocuments(ids);
    if (!metadataResult) {
        return metadataResult.error();
    }

    std::unordered_map<std::string, std::vector<VersionRetentionCandidate>> series;
    std::unordered_set<int64_t> latestDocumentIds;
    for (const auto& document : documentsResult.value()) {
        const auto metadataIt = metadataResult.value().find(document.id);
        if (metadataIt == metadataResult.value().end()) {
            continue;
        }
        const auto seriesIt = metadataIt->second.find("series_key");
        if (seriesIt == metadataIt->second.end() || seriesIt->second.asString().empty()) {
            continue;
        }

        int64_t version = 0;
        if (const auto latestIt = metadataIt->second.find("is_latest");
            latestIt != metadataIt->second.end() && latestIt->second.asBoolean()) {
            latestDocumentIds.insert(document.id);
        }
        if (const auto versionIt = metadataIt->second.find("version");
            versionIt != metadataIt->second.end()) {
            try {
                version = versionIt->second.asInteger();
            } catch (const std::exception&) {
                // Sort malformed versions behind well-formed history. The latest
                // marker below still ensures the current document is protected.
            }
        }
        series[seriesIt->second.asString()].push_back(
            {document, version, seriesIt->second.asString()});
    }

    std::vector<VersionRetentionCandidate> candidates;
    for (auto& entry : series) {
        auto& versions = entry.second;
        std::sort(versions.begin(), versions.end(),
                  [&latestDocumentIds](const auto& left, const auto& right) {
                      const bool leftLatest = latestDocumentIds.contains(left.document.id);
                      const bool rightLatest = latestDocumentIds.contains(right.document.id);
                      if (leftLatest != rightLatest) {
                          return leftLatest;
                      }
                      if (left.version != right.version) {
                          return left.version > right.version;
                      }
                      return left.document.indexedTime > right.document.indexedTime;
                  });
        if (versions.size() > policy.keepLatest) {
            for (auto it =
                     std::next(versions.begin(), static_cast<std::ptrdiff_t>(policy.keepLatest));
                 it != versions.end(); ++it) {
                if (!latestDocumentIds.contains(it->document.id)) {
                    candidates.push_back(*it);
                }
            }
        }
    }
    return candidates;
}

static bool versioningEnabled() {
    if (const char* env = std::getenv("YAMS_ENABLE_VERSIONING")) { // NOLINT(concurrency-mt-unsafe)
        std::string_view v(env);
        return !(v == "0" || v == "false" || v == "FALSE");
    }
    return true;
}

static void applyVersioningMetadataWrites(
    MetadataRepository& repo,
    const std::vector<std::tuple<int64_t, std::string, MetadataValue>>& entries) {
    if (entries.empty()) {
        return;
    }

    if (entries.size() > 1) {
        auto batchResult = repo.setMetadataBatch(entries);
        if (batchResult) {
            return;
        }
        spdlog::debug("Versioning: metadata batch failed: {}; falling back to per-entry writes",
                      batchResult.error().message);
    }

    for (const auto& [documentId, key, value] : entries) {
        auto setResult = repo.setMetadata(documentId, key, value);
        if (!setResult) {
            spdlog::debug("Versioning: failed to set metadata doc={} key='{}': {}", documentId, key,
                          setResult.error().message);
        }
    }
}

int64_t applyPathSeriesVersioning(MetadataRepository& repo, const std::string& filePath,
                                  int64_t newDocumentId,
                                  std::optional<DocumentInfo> prevLatestHint) {
    if (!versioningEnabled()) {
        return 0;
    }

    int64_t maxVersion = 0;
    auto updateVersionFromDoc = [&](const DocumentInfo& doc) {
        auto verRes = repo.getMetadata(doc.id, "version");
        if (verRes && verRes.value().has_value()) {
            try {
                const int64_t v = verRes.value().value().asInteger();
                if (v > maxVersion || !prevLatestHint.has_value()) {
                    maxVersion = v;
                    prevLatestHint = doc;
                }
            } catch (const std::exception& ex) {
                spdlog::debug("Versioning: ignoring invalid prior version metadata for doc {}: {}",
                              doc.id, ex.what());
            } catch (...) {
                spdlog::debug("Versioning: ignoring invalid prior version metadata for doc {}",
                              doc.id);
            }
        }
    };

    if (prevLatestHint.has_value()) {
        updateVersionFromDoc(*prevLatestHint);
    }

    try {
        const auto seriesKey = filePath;
        auto prevListRes = queryDocumentsByPattern(repo, seriesKey);
        if (!prevListRes) {
            spdlog::warn("Versioning: queryDocumentsByPattern failed for '{}': {}", filePath,
                         prevListRes.error().message);
            return 0;
        }

        const auto& prevList = prevListRes.value();
        for (const auto& d : prevList) {
            if (d.id == newDocumentId)
                continue;
            if (prevLatestHint.has_value() && d.id == prevLatestHint->id)
                continue;
            spdlog::info("Versioning: existing candidate id={} path={}", d.id, d.filePath);

            auto isLatestRes = repo.getMetadata(d.id, "is_latest");
            if (isLatestRes && isLatestRes.value().has_value() &&
                isLatestRes.value().value().asBoolean()) {
                prevLatestHint = d;
                updateVersionFromDoc(d);
                break;
            }

            updateVersionFromDoc(d);
        }

        int64_t newVersion = 1;
        std::vector<std::tuple<int64_t, std::string, MetadataValue>> metadataEntries;
        metadataEntries.reserve(prevLatestHint.has_value() ? 4U : 3U);

        if (prevLatestHint.has_value()) {
            metadataEntries.emplace_back(prevLatestHint->id, "is_latest", MetadataValue(false));

            DocumentRelationship rel;
            rel.parentId = prevLatestHint->id;
            rel.childId = newDocumentId;
            rel.relationshipType = RelationshipType::VersionOf;
            rel.createdTime =
                std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
            (void)repo.insertRelationship(rel);

            newVersion = (maxVersion > 0 ? maxVersion : 1) + 1;
        }

        metadataEntries.emplace_back(newDocumentId, "version", MetadataValue(newVersion));
        metadataEntries.emplace_back(newDocumentId, "is_latest", MetadataValue(true));
        metadataEntries.emplace_back(newDocumentId, "series_key", MetadataValue(seriesKey));
        applyVersioningMetadataWrites(repo, metadataEntries);

        if (auto latestRes = repo.getMetadata(newDocumentId, "is_latest");
            latestRes && latestRes.value().has_value()) {
            spdlog::info("Versioning: verification new_id={} latest={}", newDocumentId,
                         latestRes.value()->asBoolean());
        }

        if (prevLatestHint.has_value()) {
            if (auto prevRes = repo.getMetadata(prevLatestHint->id, "is_latest");
                prevRes && prevRes.value().has_value()) {
                spdlog::info("Versioning: verification prev_id={} latest={}", prevLatestHint->id,
                             prevRes.value()->asBoolean());
            }
        }

        spdlog::info("Versioning: path='{}' new_id={} version={} prev_latest={}", seriesKey,
                     newDocumentId, newVersion,
                     prevLatestHint.has_value() ? prevLatestHint->id : 0);

        return newVersion;
    } catch (const std::exception& ex) {
        spdlog::warn("Versioning: exception while updating lineage for '{}': {}", filePath,
                     ex.what());
        return 0;
    }
}

} // namespace yams::metadata
