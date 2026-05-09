#include <spdlog/spdlog.h>
#include <cstdlib>
#include <string_view>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/query_helpers.h>
#include <yams/metadata/versioning_util.h>

namespace yams::metadata {

static bool versioningEnabled() {
    if (const char* env = std::getenv("YAMS_ENABLE_VERSIONING")) {
        std::string_view v(env);
        return !(v == "0" || v == "false" || v == "FALSE");
    }
    return true;
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
            } catch (...) {
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
        if (prevLatestHint.has_value()) {
            (void)repo.setMetadata(prevLatestHint->id, "is_latest", MetadataValue(false));

            DocumentRelationship rel;
            rel.parentId = prevLatestHint->id;
            rel.childId = newDocumentId;
            rel.relationshipType = RelationshipType::VersionOf;
            rel.createdTime =
                std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
            (void)repo.insertRelationship(rel);

            newVersion = (maxVersion > 0 ? maxVersion : 1) + 1;
        }

        (void)repo.setMetadata(newDocumentId, "version", MetadataValue(newVersion));
        (void)repo.setMetadata(newDocumentId, "is_latest", MetadataValue(true));
        (void)repo.setMetadata(newDocumentId, "series_key", MetadataValue(seriesKey));

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
