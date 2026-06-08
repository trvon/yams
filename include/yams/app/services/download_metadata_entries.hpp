#pragma once

#include <optional>
#include <string>
#include <tuple>
#include <vector>

#include "services.hpp"

#include <yams/core/assert.hpp>
#include <yams/downloader/downloader.hpp>
#include <yams/metadata/document_metadata.h>

namespace yams::app::services {

using DownloadMetadataEntry = std::tuple<int64_t, std::string, metadata::MetadataValue>;
using DownloadMetadataEntries = std::vector<DownloadMetadataEntry>;

inline DownloadMetadataEntries buildDownloadMetadataEntries(int64_t documentId,
                                                            const DownloadServiceRequest& req,
                                                            const downloader::FinalResult& result) {
    YAMS_DCHECK(documentId > 0, "download metadata batch should target a persisted document id");

    DownloadMetadataEntries entries;
    entries.reserve(static_cast<std::size_t>(7 + req.tags.size() + req.metadata.size() + 4));

    entries.emplace_back(documentId, "source_url", metadata::MetadataValue(result.url));
    if (result.etag) {
        entries.emplace_back(documentId, "etag", metadata::MetadataValue(*result.etag));
    }
    if (result.lastModified) {
        entries.emplace_back(documentId, "last_modified",
                             metadata::MetadataValue(*result.lastModified));
    }
    if (result.contentType) {
        entries.emplace_back(documentId, "content_type",
                             metadata::MetadataValue(*result.contentType));
    }
    if (result.suggestedName) {
        entries.emplace_back(documentId, "suggested_name",
                             metadata::MetadataValue(*result.suggestedName));
    }
    if (result.httpStatus) {
        entries.emplace_back(documentId, "http_status",
                             metadata::MetadataValue(std::to_string(*result.httpStatus)));
    }
    if (result.checksumOk) {
        entries.emplace_back(documentId, "checksum_ok",
                             metadata::MetadataValue(*result.checksumOk ? "true" : "false"));
    }

    std::optional<std::string> winningTag;
    auto setWinningTag = [&](std::string value) {
        if (!value.empty()) {
            winningTag = std::move(value);
        }
    };

    for (const auto& tag : req.tags) {
        setWinningTag(tag);
    }
    for (const auto& [key, value] : req.metadata) {
        if (!key.empty()) {
            entries.emplace_back(documentId, key, metadata::MetadataValue(value));
        }
    }

    setWinningTag("downloaded");

    std::string scheme;
    std::string host;
    const auto pos = result.url.find("://");
    if (pos != std::string::npos) {
        scheme = result.url.substr(0, pos);
        auto rest = result.url.substr(pos + 3);
        const auto slash = rest.find('/');
        host = slash == std::string::npos ? rest : rest.substr(0, slash);
    }
    if (!host.empty()) {
        setWinningTag("host:" + host);
    }
    if (!scheme.empty()) {
        setWinningTag("scheme:" + scheme);
    }
    if (result.httpStatus) {
        const int code = *result.httpStatus;
        const std::string bucket =
            (code >= 200 && code < 300) ? "2xx" : ((code >= 400 && code < 500) ? "4xx" : "5xx");
        setWinningTag("status:" + bucket);
    }
    if (winningTag.has_value()) {
        entries.emplace_back(documentId, "tag", metadata::MetadataValue(*winningTag));
    }

    return entries;
}

} // namespace yams::app::services
