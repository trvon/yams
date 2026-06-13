#pragma once

#include <yams/daemon/ipc/ipc_protocol_common.h>

namespace yams::daemon {

// ============================================================================
// Response Types
// ============================================================================

struct GetInitResponse {
    uint64_t transferId = 0;                     // non-zero when a session is created
    uint64_t totalSize = 0;                      // total content size
    uint32_t chunkSize = 0;                      // agreed chunk size
    std::map<std::string, std::string> metadata; // optional document metadata

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << static_cast<uint64_t>(transferId) << static_cast<uint64_t>(totalSize)
            << static_cast<uint32_t>(chunkSize) << metadata;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<GetInitResponse> deserialize(Deserializer& deser) {
        GetInitResponse res;
        auto id = deser.template read<uint64_t>();
        if (!id)
            return id.error();
        res.transferId = id.value();
        auto ts = deser.template read<uint64_t>();
        if (!ts)
            return ts.error();
        res.totalSize = ts.value();
        auto cs = deser.template read<uint32_t>();
        if (!cs)
            return cs.error();
        res.chunkSize = cs.value();
        auto md = deser.readStringMap();
        if (!md)
            return md.error();
        res.metadata = std::move(md.value());
        return res;
    }
};

struct GetChunkResponse {
    std::string data; // raw bytes chunk
    uint64_t bytesRemaining = 0;

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << data << static_cast<uint64_t>(bytesRemaining);
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<GetChunkResponse> deserialize(Deserializer& deser) {
        GetChunkResponse res;
        auto d = deser.readString();
        if (!d)
            return d.error();
        res.data = std::move(d.value());
        auto rem = deser.template read<uint64_t>();
        if (!rem)
            return rem.error();
        res.bytesRemaining = rem.value();
        return res;
    }
};

// Enhanced list response for full feature parity
struct ListEntry {
    // Basic file information
    std::string hash;
    std::string path;
    std::string name;
    std::string fileName;
    uint64_t size = 0;

    // File type and format information
    std::string mimeType;
    std::string fileType; // "text" | "binary" | "image" | "document" | etc.
    std::string extension;

    // Timestamps (Unix epoch seconds)
    int64_t created = 0;
    int64_t modified = 0;
    int64_t indexed = 0;

    // Content and metadata
    std::string snippet;  // content preview
    std::string language; // detected language
    std::string extractionMethod;
    std::vector<std::string> tags;
    std::map<std::string, std::string> metadata;

    // Change tracking info
    std::string changeType; // "added" | "modified" | "deleted" | ""
    int64_t changeTime = 0; // when the change was detected

    // Display helpers
    double relevanceScore = 0.0;
    std::string matchReason; // why this document matched

    // Extraction status: "pending", "success", "failed", "skipped"
    std::string extractionStatus = "pending";

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        // Basic file information
        ser << hash << path << name << fileName << static_cast<uint64_t>(size);

        // File type and format
        ser << mimeType << fileType << extension;

        // Timestamps
        ser << static_cast<int64_t>(created) << static_cast<int64_t>(modified)
            << static_cast<int64_t>(indexed);

        // Content and metadata
        ser << snippet << language << extractionMethod << tags << metadata;

        // Change tracking
        ser << changeType << static_cast<int64_t>(changeTime);

        // Display helpers
        ser << relevanceScore << matchReason;

        // Extraction status
        ser << extractionStatus;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<ListEntry> deserialize(Deserializer& deser) {
        ListEntry e;

        // Basic file information
        auto h = deser.readString();
        if (!h)
            return h.error();
        e.hash = std::move(h.value());

        auto p = deser.readString();
        if (!p)
            return p.error();
        e.path = std::move(p.value());

        auto n = deser.readString();
        if (!n)
            return n.error();
        e.name = std::move(n.value());

        auto fn = deser.readString();
        if (!fn)
            return fn.error();
        e.fileName = std::move(fn.value());

        auto s = deser.template read<uint64_t>();
        if (!s)
            return s.error();
        e.size = s.value();

        // File type and format
        auto mt = deser.readString();
        if (!mt)
            return mt.error();
        e.mimeType = std::move(mt.value());

        auto ft = deser.readString();
        if (!ft)
            return ft.error();
        e.fileType = std::move(ft.value());

        auto ext = deser.readString();
        if (!ext)
            return ext.error();
        e.extension = std::move(ext.value());

        // Timestamps
        auto cr = deser.template read<int64_t>();
        if (!cr)
            return cr.error();
        e.created = cr.value();

        auto mo = deser.template read<int64_t>();
        if (!mo)
            return mo.error();
        e.modified = mo.value();

        auto idx = deser.template read<int64_t>();
        if (!idx)
            return idx.error();
        e.indexed = idx.value();

        // Content and metadata
        auto snip = deser.readString();
        if (!snip)
            return snip.error();
        e.snippet = std::move(snip.value());

        auto lang = deser.readString();
        if (!lang)
            return lang.error();
        e.language = std::move(lang.value());

        auto em = deser.readString();
        if (!em)
            return em.error();
        e.extractionMethod = std::move(em.value());

        auto tags = deser.readStringVector();
        if (!tags)
            return tags.error();
        e.tags = std::move(tags.value());

        auto meta = deser.readStringMap();
        if (!meta)
            return meta.error();
        e.metadata = std::move(meta.value());

        // Change tracking
        auto ct = deser.readString();
        if (!ct)
            return ct.error();
        e.changeType = std::move(ct.value());

        auto chTime = deser.template read<int64_t>();
        if (!chTime)
            return chTime.error();
        e.changeTime = chTime.value();

        // Display helpers
        auto rel = deser.template read<double>();
        if (!rel)
            return rel.error();
        e.relevanceScore = rel.value();

        auto mr = deser.readString();
        if (!mr)
            return mr.error();
        e.matchReason = std::move(mr.value());

        // Extraction status (backward compatible)
        auto es = deser.readString();
        if (es) {
            e.extractionStatus = std::move(es.value());
        }

        return e;
    }
};

struct ListResponse {
    std::vector<ListEntry> items;
    uint64_t totalCount = 0;
    std::string queryInfo;
    std::map<std::string, std::string> listStats;

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << static_cast<uint32_t>(items.size());
        for (const auto& it : items)
            it.serialize(ser);
        ser << static_cast<uint64_t>(totalCount) << queryInfo << listStats;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<ListResponse> deserialize(Deserializer& deser) {
        ListResponse r;
        auto cnt = deser.template read<uint32_t>();
        if (!cnt)
            return cnt.error();
        r.items.reserve(cnt.value());
        for (uint32_t i = 0; i < cnt.value(); ++i) {
            auto e = ListEntry::deserialize(deser);
            if (!e)
                return e.error();
            r.items.push_back(std::move(e.value()));
        }
        auto tot = deser.template read<uint64_t>();
        if (!tot)
            return tot.error();
        r.totalCount = tot.value();
        if (auto qi = deser.readString(); qi)
            r.queryInfo = std::move(qi.value());
        if (auto ls = deser.readStringMap(); ls)
            r.listStats = std::move(ls.value());
        return r;
    }
};

struct SearchResult {
    std::string id;
    std::string path;
    std::string title;
    std::string snippet;
    double score;
    std::map<std::string, std::string> metadata;

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << id << path << title << snippet << score << metadata;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<SearchResult> deserialize(Deserializer& deser) {
        SearchResult result;

        auto idResult = deser.readString();
        if (!idResult)
            return idResult.error();
        result.id = std::move(idResult.value());

        auto pathResult = deser.readString();
        if (!pathResult)
            return pathResult.error();
        result.path = std::move(pathResult.value());

        auto titleResult = deser.readString();
        if (!titleResult)
            return titleResult.error();
        result.title = std::move(titleResult.value());

        auto snippetResult = deser.readString();
        if (!snippetResult)
            return snippetResult.error();
        result.snippet = std::move(snippetResult.value());

        auto scoreResult = deser.template read<double>();
        if (!scoreResult)
            return scoreResult.error();
        result.score = scoreResult.value();

        auto metadataResult = deser.readStringMap();
        if (!metadataResult)
            return metadataResult.error();
        result.metadata = std::move(metadataResult.value());

        return result;
    }
};

struct SearchResponse {
    std::vector<SearchResult> results;
    size_t totalCount = 0;
    std::chrono::milliseconds elapsed{0};
    std::string traceId;
    std::string queryInfo;
    std::map<std::string, std::string> searchStats;

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << static_cast<uint32_t>(results.size());
        for (const auto& result : results) {
            result.serialize(ser);
        }
        ser << static_cast<uint64_t>(totalCount) << elapsed << traceId << queryInfo << searchStats;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<SearchResponse> deserialize(Deserializer& deser) {
        SearchResponse res;

        auto countResult = deser.template read<uint32_t>();
        if (!countResult)
            return countResult.error();

        res.results.reserve(countResult.value());
        for (uint32_t i = 0; i < countResult.value(); ++i) {
            auto resultResult = SearchResult::deserialize(deser);
            if (!resultResult)
                return resultResult.error();
            res.results.push_back(std::move(resultResult.value()));
        }

        auto totalCountResult = deser.template read<uint64_t>();
        if (!totalCountResult)
            return totalCountResult.error();
        res.totalCount = totalCountResult.value();

        auto elapsedResult = deser.template readDuration<std::chrono::milliseconds>();
        if (!elapsedResult)
            return elapsedResult.error();
        res.elapsed = elapsedResult.value();

        auto traceIdResult = deser.readString();
        if (!traceIdResult)
            return traceIdResult.error();
        res.traceId = std::move(traceIdResult.value());

        if (auto queryInfoResult = deser.readString(); queryInfoResult)
            res.queryInfo = std::move(queryInfoResult.value());
        if (auto searchStatsResult = deser.readStringMap(); searchStatsResult)
            res.searchStats = std::move(searchStatsResult.value());

        return res;
    }
};

struct AddResponse {
    std::string hash;
    size_t bytesStored;
    size_t bytesDeduped;
    double dedupRatio;

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << hash << static_cast<uint64_t>(bytesStored) << static_cast<uint64_t>(bytesDeduped)
            << dedupRatio;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<AddResponse> deserialize(Deserializer& deser) {
        AddResponse res;

        auto hashResult = deser.readString();
        if (!hashResult)
            return hashResult.error();
        res.hash = std::move(hashResult.value());

        auto bytesStoredResult = deser.template read<uint64_t>();
        if (!bytesStoredResult)
            return bytesStoredResult.error();
        res.bytesStored = bytesStoredResult.value();

        auto bytesDedupedResult = deser.template read<uint64_t>();
        if (!bytesDedupedResult)
            return bytesDedupedResult.error();
        res.bytesDeduped = bytesDedupedResult.value();

        auto dedupRatioResult = deser.template read<double>();
        if (!dedupRatioResult)
            return dedupRatioResult.error();
        res.dedupRatio = dedupRatioResult.value();

        return res;
    }
};

// Related document entry for knowledge graph results
struct RelatedDocumentEntry {
    std::string hash;
    std::string path;
    std::string name;
    std::string relationship;   // type of relationship
    int distance{1};            // distance in graph
    double relevanceScore{0.0}; // relevance score

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << hash << path << name << relationship << static_cast<int32_t>(distance)
            << relevanceScore;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<RelatedDocumentEntry> deserialize(Deserializer& deser) {
        RelatedDocumentEntry entry;

        auto hashResult = deser.readString();
        if (!hashResult)
            return hashResult.error();
        entry.hash = std::move(hashResult.value());

        auto pathResult = deser.readString();
        if (!pathResult)
            return pathResult.error();
        entry.path = std::move(pathResult.value());

        auto nameResult = deser.readString();
        if (!nameResult)
            return nameResult.error();
        entry.name = std::move(nameResult.value());

        auto relationshipResult = deser.readString();
        if (!relationshipResult)
            return relationshipResult.error();
        entry.relationship = std::move(relationshipResult.value());

        auto distanceResult = deser.template read<int32_t>();
        if (!distanceResult)
            return distanceResult.error();
        entry.distance = distanceResult.value();

        auto relevanceScoreResult = deser.template read<double>();
        if (!relevanceScoreResult)
            return relevanceScoreResult.error();
        entry.relevanceScore = relevanceScoreResult.value();

        return entry;
    }
};

// Enhanced GetResponse supporting rich document information and knowledge graph
struct GetResponse {
    bool compressed{false};
    std::optional<uint8_t> compressionAlgorithm;
    std::optional<uint8_t> compressionLevel;
    std::optional<uint64_t> uncompressedSize;
    std::optional<uint32_t> compressedCrc32;
    std::optional<uint32_t> uncompressedCrc32;
    std::vector<uint8_t> compressionHeader;
    std::optional<uint32_t> centroidWeight;
    std::optional<uint32_t> centroidDims;
    std::vector<float> centroidPreview;
    // Single document result (for hash/name queries)
    std::string hash;
    std::string path;
    std::string name;
    std::string fileName;
    uint64_t size{0};
    std::string mimeType;
    std::string fileType;

    // Time information
    int64_t created{0}; // Unix epoch seconds
    int64_t modified{0};
    int64_t indexed{0};

    // Content (conditionally included)
    std::string content;    // document content (if not metadataOnly)
    bool hasContent{false}; // indicates if content is included

    // Metadata
    std::map<std::string, std::string> metadata;

    // Knowledge graph results (when showGraph=true)
    bool graphEnabled{false};
    std::vector<RelatedDocumentEntry> related;

    // Result information
    uint64_t totalBytes{0};    // total bytes processed
    bool outputWritten{false}; // true if written to output file

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        // Basic document information
        ser << hash << path << name << fileName << static_cast<uint64_t>(size) << mimeType
            << fileType;

        // Compression metadata
        ser << compressed;

        const bool hasAlgorithm = compressionAlgorithm.has_value();
        ser << hasAlgorithm;
        if (hasAlgorithm) {
            ser << static_cast<uint32_t>(*compressionAlgorithm);
        }

        const bool hasLevel = compressionLevel.has_value();
        ser << hasLevel;
        if (hasLevel) {
            ser << static_cast<uint32_t>(*compressionLevel);
        }

        const bool hasUncompressedSize = uncompressedSize.has_value();
        ser << hasUncompressedSize;
        if (hasUncompressedSize) {
            ser << static_cast<uint64_t>(*uncompressedSize);
        }

        const bool hasCompressedCrc32 = compressedCrc32.has_value();
        ser << hasCompressedCrc32;
        if (hasCompressedCrc32) {
            ser << static_cast<uint32_t>(*compressedCrc32);
        }

        const bool hasUncompressedCrc32 = uncompressedCrc32.has_value();
        ser << hasUncompressedCrc32;
        if (hasUncompressedCrc32) {
            ser << static_cast<uint32_t>(*uncompressedCrc32);
        }

        std::string headerBlob(compressionHeader.begin(), compressionHeader.end());
        ser << headerBlob;

        // Time information
        ser << static_cast<int64_t>(created) << static_cast<int64_t>(modified)
            << static_cast<int64_t>(indexed);

        const bool hasCentroidWeight = centroidWeight.has_value();
        ser << hasCentroidWeight;
        if (hasCentroidWeight) {
            ser << static_cast<uint32_t>(*centroidWeight);
        }

        const bool hasCentroidDims = centroidDims.has_value();
        ser << hasCentroidDims;
        if (hasCentroidDims) {
            ser << static_cast<uint32_t>(*centroidDims);
        }

        std::string centroidBlob;
        centroidBlob.resize(centroidPreview.size() * sizeof(float));
        if (!centroidBlob.empty()) {
            std::memcpy(centroidBlob.data(), centroidPreview.data(), centroidBlob.size());
        }
        ser << centroidBlob;

        // Content
        ser << content << hasContent;

        // Metadata
        ser << metadata;

        // Knowledge graph
        ser << graphEnabled << static_cast<uint32_t>(related.size());
        for (const auto& rel : related)
            rel.serialize(ser);

        // Result information
        ser << static_cast<uint64_t>(totalBytes) << outputWritten;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<GetResponse> deserialize(Deserializer& deser) {
        GetResponse res;

        // Basic document information
        auto hashResult = deser.readString();
        if (!hashResult)
            return hashResult.error();
        res.hash = std::move(hashResult.value());

        auto pathResult = deser.readString();
        if (!pathResult)
            return pathResult.error();
        res.path = std::move(pathResult.value());

        auto nameResult = deser.readString();
        if (!nameResult)
            return nameResult.error();
        res.name = std::move(nameResult.value());

        auto fileNameResult = deser.readString();
        if (!fileNameResult)
            return fileNameResult.error();
        res.fileName = std::move(fileNameResult.value());

        auto sizeResult = deser.template read<uint64_t>();
        if (!sizeResult)
            return sizeResult.error();
        res.size = sizeResult.value();

        auto mimeTypeResult = deser.readString();
        if (!mimeTypeResult)
            return mimeTypeResult.error();
        res.mimeType = std::move(mimeTypeResult.value());

        auto fileTypeResult = deser.readString();
        if (!fileTypeResult)
            return fileTypeResult.error();
        res.fileType = std::move(fileTypeResult.value());

        auto compressedResult = deser.template read<bool>();
        if (!compressedResult)
            return compressedResult.error();
        res.compressed = compressedResult.value();

        auto hasAlgorithmResult = deser.template read<bool>();
        if (!hasAlgorithmResult)
            return hasAlgorithmResult.error();
        if (hasAlgorithmResult.value()) {
            auto valueResult = deser.template read<uint32_t>();
            if (!valueResult)
                return valueResult.error();
            res.compressionAlgorithm = static_cast<uint8_t>(valueResult.value());
        }

        auto hasLevelResult = deser.template read<bool>();
        if (!hasLevelResult)
            return hasLevelResult.error();
        if (hasLevelResult.value()) {
            auto valueResult = deser.template read<uint32_t>();
            if (!valueResult)
                return valueResult.error();
            res.compressionLevel = static_cast<uint8_t>(valueResult.value());
        }

        auto hasUncompressedSizeResult = deser.template read<bool>();
        if (!hasUncompressedSizeResult)
            return hasUncompressedSizeResult.error();
        if (hasUncompressedSizeResult.value()) {
            auto valueResult = deser.template read<uint64_t>();
            if (!valueResult)
                return valueResult.error();
            res.uncompressedSize = valueResult.value();
        }

        auto hasCompressedCrc32Result = deser.template read<bool>();
        if (!hasCompressedCrc32Result)
            return hasCompressedCrc32Result.error();
        if (hasCompressedCrc32Result.value()) {
            auto valueResult = deser.template read<uint32_t>();
            if (!valueResult)
                return valueResult.error();
            res.compressedCrc32 = valueResult.value();
        }

        auto hasUncompressedCrc32Result = deser.template read<bool>();
        if (!hasUncompressedCrc32Result)
            return hasUncompressedCrc32Result.error();
        if (hasUncompressedCrc32Result.value()) {
            auto valueResult = deser.template read<uint32_t>();
            if (!valueResult)
                return valueResult.error();
            res.uncompressedCrc32 = valueResult.value();
        }

        auto headerResult = deser.readString();
        if (!headerResult)
            return headerResult.error();
        auto headerStr = std::move(headerResult.value());
        res.compressionHeader.assign(headerStr.begin(), headerStr.end());

        // Time information
        auto createdResult = deser.template read<int64_t>();
        if (!createdResult)
            return createdResult.error();
        res.created = createdResult.value();

        auto modifiedResult = deser.template read<int64_t>();
        if (!modifiedResult)
            return modifiedResult.error();
        res.modified = modifiedResult.value();

        auto indexedResult = deser.template read<int64_t>();
        if (!indexedResult)
            return indexedResult.error();
        res.indexed = indexedResult.value();

        auto hasCentroidWeightResult = deser.template read<bool>();
        if (!hasCentroidWeightResult)
            return hasCentroidWeightResult.error();
        if (hasCentroidWeightResult.value()) {
            auto valueResult = deser.template read<uint32_t>();
            if (!valueResult)
                return valueResult.error();
            res.centroidWeight = valueResult.value();
        }

        auto hasCentroidDimsResult = deser.template read<bool>();
        if (!hasCentroidDimsResult)
            return hasCentroidDimsResult.error();
        if (hasCentroidDimsResult.value()) {
            auto valueResult = deser.template read<uint32_t>();
            if (!valueResult)
                return valueResult.error();
            res.centroidDims = valueResult.value();
        }

        auto centroidBlobResult = deser.readString();
        if (!centroidBlobResult)
            return centroidBlobResult.error();
        auto centroidStr = std::move(centroidBlobResult.value());
        if (!centroidStr.empty()) {
            if ((centroidStr.size() % sizeof(float)) != 0)
                return Error{ErrorCode::DataCorruption, "Invalid centroid preview blob"};
            res.centroidPreview.resize(centroidStr.size() / sizeof(float));
            std::memcpy(res.centroidPreview.data(), centroidStr.data(), centroidStr.size());
        }

        // Content
        auto contentResult = deser.readString();
        if (!contentResult)
            return contentResult.error();
        res.content = std::move(contentResult.value());

        auto hasContentResult = deser.template read<bool>();
        if (!hasContentResult)
            return hasContentResult.error();
        res.hasContent = hasContentResult.value();

        // Metadata
        auto metadataResult = deser.readStringMap();
        if (!metadataResult)
            return metadataResult.error();
        res.metadata = std::move(metadataResult.value());

        // Knowledge graph
        auto graphEnabledResult = deser.template read<bool>();
        if (!graphEnabledResult)
            return graphEnabledResult.error();
        res.graphEnabled = graphEnabledResult.value();

        auto relatedCountResult = deser.template read<uint32_t>();
        if (!relatedCountResult)
            return relatedCountResult.error();
        res.related.reserve(relatedCountResult.value());
        for (uint32_t i = 0; i < relatedCountResult.value(); ++i) {
            auto relResult = RelatedDocumentEntry::deserialize(deser);
            if (!relResult)
                return relResult.error();
            res.related.push_back(std::move(relResult.value()));
        }

        // Result information
        auto totalBytesResult = deser.template read<uint64_t>();
        if (!totalBytesResult)
            return totalBytesResult.error();
        res.totalBytes = totalBytesResult.value();

        auto outputWrittenResult = deser.template read<bool>();
        if (!outputWrittenResult)
            return outputWrittenResult.error();
        res.outputWritten = outputWrittenResult.value();

        return res;
    }
};

struct StatusResponse {
    bool running = true;
    bool ready; // Overall readiness (backward compatibility)
    size_t uptimeSeconds;
    size_t requestsProcessed;
    size_t activeConnections;
    size_t maxConnections{0};        // Connection slot limit
    size_t connectionSlotsFree{0};   // Available connection slots
    uint64_t oldestConnectionAge{0}; // Age of oldest active connection (seconds)
    uint64_t forcedCloseCount{0};    // Connections closed due to lifetime exceeded
    // Proxy socket metrics
    size_t proxyActiveConnections{0};
    std::string proxySocketPath;
    double memoryUsageMb;
    double cpuUsagePercent;
    std::string version;
    // Vector DB health metrics
    bool vectorDbInitAttempted{false};
    bool vectorDbReady{false};
    uint32_t vectorDbDim{0};
    std::string vectorIndexEngine;
    // Database init phase visibility for `yams daemon status` rendering.
    // Empty when not set; otherwise "opening" | "recovering" | "migrating" | "ready".
    std::string databasePhase;
    uint64_t databasePhaseElapsedMs{0};
    // Post-startup maintenance phase, separate from database readiness.
    std::string maintenancePhase;
    uint64_t maintenancePhaseElapsedMs{0};
    std::string databaseRecoveredAt;
    std::string databaseRecoveredFrom;
    std::string storageWarning;
    // FSM metrics (transport state machine), exposed in daemon status
    uint64_t fsmTransitions = 0;
    uint64_t fsmHeaderReads = 0;
    uint64_t fsmPayloadReads = 0;
    uint64_t fsmPayloadWrites = 0;
    uint64_t fsmBytesSent = 0;
    uint64_t fsmBytesReceived = 0;
    // Multiplexing metrics (transport fairness/queues)
    uint64_t muxActiveHandlers = 0;
    int64_t muxQueuedBytes = 0;
    uint64_t muxWriterBudgetBytes = 0;
    uint32_t retryAfterMs = 0; // optional backpressure hint
    // Centralized tuning pool sizes (from FSM metrics via TuningManager)
    uint32_t ipcPoolSize{0};
    uint32_t ioPoolSize{0};
    struct SearchMetrics {
        std::uint32_t active{0};
        std::uint32_t queued{0};
        std::uint64_t executed{0};
        double cacheHitRate{0.0};
        std::uint64_t avgLatencyUs{0};
        std::uint32_t concurrencyLimit{0};

        template <typename Serializer>
        requires IsSerializer<Serializer>
        void serialize(Serializer& ser) const {
            ser << active << queued << executed << cacheHitRate << avgLatencyUs << concurrencyLimit;
        }

        template <typename Deserializer>
        requires IsDeserializer<Deserializer>
        static Result<SearchMetrics> deserialize(Deserializer& deser) {
            SearchMetrics metrics;

            auto activeRes = deser.readUint32();
            if (!activeRes)
                return activeRes.error();
            metrics.active = activeRes.value();

            auto queuedRes = deser.readUint32();
            if (!queuedRes)
                return queuedRes.error();
            metrics.queued = queuedRes.value();

            auto executedRes = deser.readUint64();
            if (!executedRes)
                return executedRes.error();
            metrics.executed = executedRes.value();

            auto cacheHitRes = deser.readDouble();
            if (!cacheHitRes)
                return cacheHitRes.error();
            metrics.cacheHitRate = cacheHitRes.value();

            auto latencyRes = deser.readUint64();
            if (!latencyRes)
                return latencyRes.error();
            metrics.avgLatencyUs = latencyRes.value();

            auto limitRes = deser.readUint32();
            if (!limitRes)
                return limitRes.error();
            metrics.concurrencyLimit = limitRes.value();

            return metrics;
        }
    };
    SearchMetrics searchMetrics{};
    std::map<std::string, size_t> requestCounts;

    // Readiness state tracking (new fields)
    std::map<std::string, bool> readinessStates; // Subsystem -> ready
    std::map<std::string, uint8_t> initProgress; // Subsystem -> progress (0-100)
    std::string overallStatus;                   // "ready", "degraded", "initializing", "starting"
    // Explicit lifecycle fields (preferred by CLI/clients)
    std::string lifecycleState; // mirrors FSM state string when available
    std::string lastError;      // last lifecycle error if any (empty when none)

    // Content store diagnostics
    std::string dataDir;           // absolute daemon-resolved active data dir
    std::string metadataDbPath;    // absolute live metadata DB file path
    std::string vectorDbPath;      // absolute live vector DB file path
    std::string contentStoreRoot;  // absolute path to storage root (daemon-resolved)
    std::string contentStoreError; // last initialization error (if any)

    // WAL file sizes (bytes); zero when the file does not exist or is empty.
    uint64_t metadataWalBytes{0};
    uint64_t vectorWalBytes{0};

    // Embedding runtime details (best-effort)
    bool embeddingAvailable{false};
    std::string embeddingBackend;   // provider|daemon|local|hybrid|unknown
    std::string embeddingModel;     // preferred/active model name if known
    std::string embeddingModelPath; // resolved model path, when known
    uint32_t embeddingDim{0};
    int32_t embeddingThreadsIntra{0};
    int32_t embeddingThreadsInter{0};

    // Model information
    struct ModelInfo {
        std::string name;
        std::string type;
        size_t memoryMb;
        uint64_t requestCount;

        template <typename Serializer>
        requires IsSerializer<Serializer>
        void serialize(Serializer& ser) const {
            ser << name << type << static_cast<uint64_t>(memoryMb) << requestCount;
        }

        template <typename Deserializer>
        requires IsDeserializer<Deserializer>
        static Result<ModelInfo> deserialize(Deserializer& deser) {
            ModelInfo info;

            auto nameResult = deser.readString();
            if (!nameResult)
                return nameResult.error();
            info.name = std::move(nameResult.value());

            auto typeResult = deser.readString();
            if (!typeResult)
                return typeResult.error();
            info.type = std::move(typeResult.value());

            auto memoryResult = deser.template read<uint64_t>();
            if (!memoryResult)
                return memoryResult.error();
            info.memoryMb = memoryResult.value();

            auto requestCountResult = deser.template read<uint64_t>();
            if (!requestCountResult)
                return requestCountResult.error();
            info.requestCount = requestCountResult.value();

            return info;
        }
    };
    std::vector<ModelInfo> models;

    // Plugin/model provider details (typed, preferred over JSON snapshots)
    struct ProviderInfo {
        std::string name;
        bool ready{false};
        bool degraded{false};
        std::string error; // if degraded
        uint32_t modelsLoaded{0};
        bool isProvider{false};                // true if this plugin is the adopted model provider
        std::vector<std::string> interfaces;   // plugin interfaces (e.g., content_extractor_v1)
        std::vector<std::string> capabilities; // capability categories (e.g., content_extraction)

        template <typename Serializer>
        requires IsSerializer<Serializer>
        void serialize(Serializer& ser) const {
            ser << name << ready << degraded << error << static_cast<uint32_t>(modelsLoaded)
                << isProvider;
            // Serialize interfaces
            ser << static_cast<uint32_t>(interfaces.size());
            for (const auto& iface : interfaces) {
                ser << iface;
            }
            // Serialize capabilities
            ser << static_cast<uint32_t>(capabilities.size());
            for (const auto& cap : capabilities) {
                ser << cap;
            }
        }

        template <typename Deserializer>
        requires IsDeserializer<Deserializer>
        static Result<ProviderInfo> deserialize(Deserializer& deser) {
            ProviderInfo p;
            auto nameRes = deser.readString();
            if (!nameRes)
                return nameRes.error();
            p.name = std::move(nameRes.value());
            auto rdy = deser.template read<bool>();
            if (!rdy)
                return rdy.error();
            p.ready = rdy.value();
            auto deg = deser.template read<bool>();
            if (!deg)
                return deg.error();
            p.degraded = deg.value();
            auto err = deser.readString();
            if (!err)
                return err.error();
            p.error = std::move(err.value());
            auto ml = deser.template read<uint32_t>();
            if (!ml)
                return ml.error();
            p.modelsLoaded = ml.value();
            auto ip = deser.template read<bool>();
            if (!ip)
                return ip.error();
            p.isProvider = ip.value();
            // Deserialize interfaces
            auto ifaceCount = deser.template read<uint32_t>();
            if (!ifaceCount)
                return ifaceCount.error();
            p.interfaces.reserve(ifaceCount.value());
            for (uint32_t i = 0; i < ifaceCount.value(); ++i) {
                auto iface = deser.readString();
                if (!iface)
                    return iface.error();
                p.interfaces.push_back(std::move(iface.value()));
            }
            // Deserialize capabilities
            auto capCount = deser.template read<uint32_t>();
            if (!capCount)
                return capCount.error();
            p.capabilities.reserve(capCount.value());
            for (uint32_t i = 0; i < capCount.value(); ++i) {
                auto cap = deser.readString();
                if (!cap)
                    return cap.error();
                p.capabilities.push_back(std::move(cap.value()));
            }
            return p;
        }
    };
    std::vector<ProviderInfo> providers;
    struct PluginSkipInfo {
        std::string path;
        std::string reason;
        template <typename Serializer>
        requires IsSerializer<Serializer>
        void serialize(Serializer& ser) const {
            ser << path << reason;
        }
        template <typename Deserializer>
        requires IsDeserializer<Deserializer>
        static Result<PluginSkipInfo> deserialize(Deserializer& d) {
            PluginSkipInfo s;
            auto p = d.readString();
            if (!p)
                return p.error();
            s.path = std::move(p.value());
            auto r = d.readString();
            if (!r)
                return r.error();
            s.reason = std::move(r.value());
            return s;
        }
    };
    std::vector<PluginSkipInfo> skippedPlugins;

    // PBI-040, task 040-1: PostIngestQueue depth for FTS5 readiness checks
    uint32_t postIngestQueueDepth{0};

    // Search tuning state (from SearchTuner FSM - epic yams-7ez4)
    std::string searchTuningState;  // e.g., "SMALL_CODE", "SCIENTIFIC", "MIXED"
    std::string searchTuningReason; // Human-readable explanation of state selection
    std::map<std::string, double> searchTuningParams; // e.g., {"textWeight": 0.55, ...}

    // ResourceGovernor metrics (memory pressure management)
    uint64_t governorRssBytes{0};     // Current process RSS
    uint64_t governorBudgetBytes{0};  // Memory budget limit
    uint8_t governorPressureLevel{0}; // 0=Normal, 1=Warning, 2=Critical, 3=Emergency
    uint8_t governorHeadroomPct{100}; // Scaling headroom (0-100%)

    // ONNX concurrency metrics
    uint32_t onnxTotalSlots{0};
    uint32_t onnxUsedSlots{0};
    uint32_t onnxGlinerUsed{0};
    uint32_t onnxEmbedUsed{0};
    uint32_t onnxRerankerUsed{0};

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << running << ready << static_cast<uint64_t>(uptimeSeconds)
            << static_cast<uint64_t>(requestsProcessed) << static_cast<uint64_t>(activeConnections)
            << static_cast<uint64_t>(maxConnections) << static_cast<uint64_t>(connectionSlotsFree)
            << static_cast<uint64_t>(oldestConnectionAge) << static_cast<uint64_t>(forcedCloseCount)
            << memoryUsageMb << cpuUsagePercent << version << static_cast<uint64_t>(fsmTransitions)
            << static_cast<uint64_t>(fsmHeaderReads) << static_cast<uint64_t>(fsmPayloadReads)
            << static_cast<uint64_t>(fsmPayloadWrites) << static_cast<uint64_t>(fsmBytesSent)
            << static_cast<uint64_t>(fsmBytesReceived) << static_cast<uint64_t>(muxActiveHandlers)
            << static_cast<int64_t>(muxQueuedBytes) << static_cast<uint64_t>(muxWriterBudgetBytes)
            << static_cast<uint32_t>(retryAfterMs);

        // Vector DB metrics
        ser << vectorDbInitAttempted << vectorDbReady << static_cast<uint32_t>(vectorDbDim)
            << vectorIndexEngine;

        // Serialize request counts map
        ser << static_cast<uint32_t>(requestCounts.size());
        for (const auto& [key, value] : requestCounts) {
            ser << key << static_cast<uint64_t>(value);
        }

        // Serialize readiness states map
        ser << static_cast<uint32_t>(readinessStates.size());
        for (const auto& [key, value] : readinessStates) {
            ser << key << value;
        }

        // Serialize init progress map
        ser << static_cast<uint32_t>(initProgress.size());
        for (const auto& [key, value] : initProgress) {
            ser << key << static_cast<uint8_t>(value);
        }

        // Serialize overall status and lifecycle fields
        ser << overallStatus << lifecycleState << lastError;

        // Serialize models
        ser << static_cast<uint32_t>(models.size());
        for (const auto& model : models) {
            model.serialize(ser);
        }

        // Serialize providers
        ser << static_cast<uint32_t>(providers.size());
        for (const auto& p : providers) {
            p.serialize(ser);
        }
        // Serialize skipped plugins
        ser << static_cast<uint32_t>(skippedPlugins.size());
        for (const auto& s : skippedPlugins)
            s.serialize(ser);

        // Serialize content store and database diagnostics (as strings)
        ser << dataDir << metadataDbPath << vectorDbPath << contentStoreRoot << contentStoreError;

        // Serialize embedding runtime details
        ser << embeddingAvailable << embeddingBackend << embeddingModel << embeddingModelPath
            << static_cast<uint32_t>(embeddingDim) << static_cast<int32_t>(embeddingThreadsIntra)
            << static_cast<int32_t>(embeddingThreadsInter);

        // Serialize centralized tuning pool sizes
        ser << static_cast<uint32_t>(ipcPoolSize) << static_cast<uint32_t>(ioPoolSize);

        // Serialize search tuning state (epic yams-7ez4)
        ser << searchTuningState << searchTuningReason;
        ser << static_cast<uint32_t>(searchTuningParams.size());
        for (const auto& [key, value] : searchTuningParams) {
            ser << key << value;
        }

        // Serialize ResourceGovernor metrics
        ser << static_cast<uint64_t>(governorRssBytes) << static_cast<uint64_t>(governorBudgetBytes)
            << static_cast<uint8_t>(governorPressureLevel)
            << static_cast<uint8_t>(governorHeadroomPct);

        // Serialize ONNX concurrency metrics
        ser << static_cast<uint32_t>(onnxTotalSlots) << static_cast<uint32_t>(onnxUsedSlots)
            << static_cast<uint32_t>(onnxGlinerUsed) << static_cast<uint32_t>(onnxEmbedUsed)
            << static_cast<uint32_t>(onnxRerankerUsed);

        // Proxy socket metrics
        ser << static_cast<uint64_t>(proxyActiveConnections) << proxySocketPath;

        // Database phase + recovery + storage warning (appended; older daemons
        // omit these and older clients tolerate missing tail fields).
        ser << databasePhase << static_cast<uint64_t>(databasePhaseElapsedMs) << databaseRecoveredAt
            << databaseRecoveredFrom << storageWarning;

        // WAL file sizes (appended; older clients tolerate missing tail fields)
        ser << static_cast<uint64_t>(metadataWalBytes) << static_cast<uint64_t>(vectorWalBytes);

        // Maintenance phase (appended; older clients tolerate missing tail fields)
        ser << maintenancePhase << static_cast<uint64_t>(maintenancePhaseElapsedMs);
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<StatusResponse> deserialize(Deserializer& deser) {
        StatusResponse res;

        auto runningResult = deser.template read<bool>();
        if (!runningResult)
            return runningResult.error();
        res.running = runningResult.value();

        auto readyResult = deser.template read<bool>();
        if (!readyResult)
            return readyResult.error();
        res.ready = readyResult.value();

        auto uptimeResult = deser.template read<uint64_t>();
        if (!uptimeResult)
            return uptimeResult.error();
        res.uptimeSeconds = uptimeResult.value();

        auto requestsResult = deser.template read<uint64_t>();
        if (!requestsResult)
            return requestsResult.error();
        res.requestsProcessed = requestsResult.value();

        auto connectionsResult = deser.template read<uint64_t>();
        if (!connectionsResult)
            return connectionsResult.error();
        res.activeConnections = connectionsResult.value();

        auto maxConnsResult = deser.template read<uint64_t>();
        if (!maxConnsResult)
            return maxConnsResult.error();
        res.maxConnections = maxConnsResult.value();

        auto slotsFreeResult = deser.template read<uint64_t>();
        if (!slotsFreeResult)
            return slotsFreeResult.error();
        res.connectionSlotsFree = slotsFreeResult.value();

        auto oldestAgeResult = deser.template read<uint64_t>();
        if (!oldestAgeResult)
            return oldestAgeResult.error();
        res.oldestConnectionAge = oldestAgeResult.value();

        auto forcedCloseResult = deser.template read<uint64_t>();
        if (!forcedCloseResult)
            return forcedCloseResult.error();
        res.forcedCloseCount = forcedCloseResult.value();

        auto memoryResult = deser.template read<double>();
        if (!memoryResult)
            return memoryResult.error();
        res.memoryUsageMb = memoryResult.value();

        auto cpuResult = deser.template read<double>();
        if (!cpuResult)
            return cpuResult.error();
        res.cpuUsagePercent = cpuResult.value();

        auto versionResult = deser.readString();
        if (!versionResult)
            return versionResult.error();
        res.version = std::move(versionResult.value());
        // Deserialize FSM metrics
        auto fsmTransResult = deser.template read<uint64_t>();
        if (!fsmTransResult)
            return fsmTransResult.error();
        res.fsmTransitions = fsmTransResult.value();

        auto fsmHdrReadsResult = deser.template read<uint64_t>();
        if (!fsmHdrReadsResult)
            return fsmHdrReadsResult.error();
        res.fsmHeaderReads = fsmHdrReadsResult.value();

        auto fsmPayloadReadsResult = deser.template read<uint64_t>();
        if (!fsmPayloadReadsResult)
            return fsmPayloadReadsResult.error();
        res.fsmPayloadReads = fsmPayloadReadsResult.value();

        auto fsmPayloadWritesResult = deser.template read<uint64_t>();
        if (!fsmPayloadWritesResult)
            return fsmPayloadWritesResult.error();
        res.fsmPayloadWrites = fsmPayloadWritesResult.value();

        auto fsmBytesSentResult = deser.template read<uint64_t>();
        if (!fsmBytesSentResult)
            return fsmBytesSentResult.error();
        res.fsmBytesSent = fsmBytesSentResult.value();

        auto fsmBytesReceivedResult = deser.template read<uint64_t>();
        if (!fsmBytesReceivedResult)
            return fsmBytesReceivedResult.error();
        res.fsmBytesReceived = fsmBytesReceivedResult.value();

        // Multiplexing metrics
        auto muxActiveHandlersResult = deser.template read<uint64_t>();
        if (!muxActiveHandlersResult)
            return muxActiveHandlersResult.error();
        res.muxActiveHandlers = muxActiveHandlersResult.value();

        auto muxQueuedBytesResult = deser.template read<int64_t>();
        if (!muxQueuedBytesResult)
            return muxQueuedBytesResult.error();
        res.muxQueuedBytes = muxQueuedBytesResult.value();

        auto muxWriterBudgetBytesResult = deser.template read<uint64_t>();
        if (!muxWriterBudgetBytesResult)
            return muxWriterBudgetBytesResult.error();
        res.muxWriterBudgetBytes = muxWriterBudgetBytesResult.value();

        // retryAfterMs (optional; default 0 if older streams)
        auto retryAfterMsResult = deser.template read<uint32_t>();
        if (!retryAfterMsResult)
            return retryAfterMsResult.error();
        res.retryAfterMs = retryAfterMsResult.value();

        // Vector DB metrics
        auto vInitAttempted = deser.template read<bool>();
        if (!vInitAttempted)
            return vInitAttempted.error();
        res.vectorDbInitAttempted = vInitAttempted.value();
        auto vReady = deser.template read<bool>();
        if (!vReady)
            return vReady.error();
        res.vectorDbReady = vReady.value();
        auto vDim = deser.template read<uint32_t>();
        if (!vDim)
            return vDim.error();
        res.vectorDbDim = vDim.value();
        auto vectorIndexEngineResult = deser.readString();
        if (!vectorIndexEngineResult)
            return vectorIndexEngineResult.error();
        res.vectorIndexEngine = std::move(vectorIndexEngineResult.value());

        // Deserialize request counts
        auto countsCountResult = deser.template read<uint32_t>();
        if (!countsCountResult)
            return countsCountResult.error();

        for (uint32_t i = 0; i < countsCountResult.value(); ++i) {
            auto keyResult = deser.readString();
            if (!keyResult)
                return keyResult.error();
            auto valueResult = deser.template read<uint64_t>();
            if (!valueResult)
                return valueResult.error();
            res.requestCounts[std::move(keyResult.value())] = valueResult.value();
        }

        // Deserialize readiness states
        auto readinessCountResult = deser.template read<uint32_t>();
        if (!readinessCountResult)
            return readinessCountResult.error();

        for (uint32_t i = 0; i < readinessCountResult.value(); ++i) {
            auto keyResult = deser.readString();
            if (!keyResult)
                return keyResult.error();
            auto valueResult = deser.template read<bool>();
            if (!valueResult)
                return valueResult.error();
            res.readinessStates[std::move(keyResult.value())] = valueResult.value();
        }

        // Deserialize init progress
        auto progressCountResult = deser.template read<uint32_t>();
        if (!progressCountResult)
            return progressCountResult.error();

        for (uint32_t i = 0; i < progressCountResult.value(); ++i) {
            auto keyResult = deser.readString();
            if (!keyResult)
                return keyResult.error();
            auto valueResult = deser.template read<uint8_t>();
            if (!valueResult)
                return valueResult.error();
            res.initProgress[std::move(keyResult.value())] = valueResult.value();
        }

        // Deserialize overall status and lifecycle fields
        auto statusResult = deser.readString();
        if (!statusResult)
            return statusResult.error();
        res.overallStatus = std::move(statusResult.value());

        auto lifecycleRes = deser.readString();
        if (!lifecycleRes)
            return lifecycleRes.error();
        res.lifecycleState = std::move(lifecycleRes.value());

        auto lastErrRes = deser.readString();
        if (!lastErrRes)
            return lastErrRes.error();
        res.lastError = std::move(lastErrRes.value());

        // Deserialize models
        auto modelsCountResult = deser.template read<uint32_t>();
        if (!modelsCountResult)
            return modelsCountResult.error();

        res.models.reserve(modelsCountResult.value());
        for (uint32_t i = 0; i < modelsCountResult.value(); ++i) {
            auto modelResult = ModelInfo::deserialize(deser);
            if (!modelResult)
                return modelResult.error();
            res.models.push_back(std::move(modelResult.value()));
        }

        // Deserialize providers
        auto providersCountResult = deser.template read<uint32_t>();
        if (!providersCountResult)
            return providersCountResult.error();
        res.providers.reserve(providersCountResult.value());
        for (uint32_t i = 0; i < providersCountResult.value(); ++i) {
            auto pRes = ProviderInfo::deserialize(deser);
            if (!pRes)
                return pRes.error();
            res.providers.push_back(std::move(pRes.value()));
        }
        // Deserialize skipped plugins
        auto skipCount = deser.template read<uint32_t>();
        if (!skipCount)
            return skipCount.error();
        res.skippedPlugins.reserve(skipCount.value());
        for (uint32_t i = 0; i < skipCount.value(); ++i) {
            auto s = PluginSkipInfo::deserialize(deser);
            if (!s)
                return s.error();
            res.skippedPlugins.push_back(std::move(s.value()));
        }

        // Deserialize content store and database diagnostics
        auto rdd = deser.readString();
        if (!rdd)
            return rdd.error();
        res.dataDir = std::move(rdd.value());
        auto mdp = deser.readString();
        if (!mdp)
            return mdp.error();
        res.metadataDbPath = std::move(mdp.value());
        auto vdp = deser.readString();
        if (!vdp)
            return vdp.error();
        res.vectorDbPath = std::move(vdp.value());
        auto csr = deser.readString();
        if (!csr)
            return csr.error();
        res.contentStoreRoot = std::move(csr.value());
        auto cse = deser.readString();
        if (!cse)
            return cse.error();
        res.contentStoreError = std::move(cse.value());

        // Deserialize embedding runtime details
        auto eavail = deser.template read<bool>();
        if (!eavail)
            return eavail.error();
        res.embeddingAvailable = eavail.value();
        auto eb = deser.readString();
        if (!eb)
            return eb.error();
        res.embeddingBackend = std::move(eb.value());
        auto em = deser.readString();
        if (!em)
            return em.error();
        res.embeddingModel = std::move(em.value());
        // embeddingModelPath (added after embeddingModel)
        if (auto emp = deser.readString(); emp) {
            res.embeddingModelPath = std::move(emp.value());
        } else {
            return emp.error();
        }
        auto ed = deser.template read<uint32_t>();
        if (!ed)
            return ed.error();
        res.embeddingDim = ed.value();
        auto ei = deser.template read<int32_t>();
        if (!ei)
            return ei.error();
        res.embeddingThreadsIntra = ei.value();
        auto ej = deser.template read<int32_t>();
        if (!ej)
            return ej.error();
        res.embeddingThreadsInter = ej.value();

        // Centralized tuning pool sizes
        auto ipcSz = deser.template read<uint32_t>();
        if (!ipcSz)
            return ipcSz.error();
        res.ipcPoolSize = ipcSz.value();
        auto ioSz = deser.template read<uint32_t>();
        if (!ioSz)
            return ioSz.error();
        res.ioPoolSize = ioSz.value();

        // Deserialize search tuning state (epic yams-7ez4)
        auto tuningState = deser.readString();
        if (!tuningState)
            return tuningState.error();
        res.searchTuningState = std::move(tuningState.value());
        auto tuningReason = deser.readString();
        if (!tuningReason)
            return tuningReason.error();
        res.searchTuningReason = std::move(tuningReason.value());
        auto tuningParamsCount = deser.template read<uint32_t>();
        if (!tuningParamsCount)
            return tuningParamsCount.error();
        for (uint32_t i = 0; i < tuningParamsCount.value(); ++i) {
            auto keyRes = deser.readString();
            if (!keyRes)
                return keyRes.error();
            auto valRes = deser.template read<double>();
            if (!valRes)
                return valRes.error();
            res.searchTuningParams[std::move(keyRes.value())] = valRes.value();
        }

        // Deserialize ResourceGovernor metrics
        auto govRss = deser.template read<uint64_t>();
        if (!govRss)
            return govRss.error();
        res.governorRssBytes = govRss.value();

        auto govBudget = deser.template read<uint64_t>();
        if (!govBudget)
            return govBudget.error();
        res.governorBudgetBytes = govBudget.value();

        auto govLevel = deser.template read<uint8_t>();
        if (!govLevel)
            return govLevel.error();
        res.governorPressureLevel = govLevel.value();

        auto govHeadroom = deser.template read<uint8_t>();
        if (!govHeadroom)
            return govHeadroom.error();
        res.governorHeadroomPct = govHeadroom.value();

        // Deserialize ONNX concurrency metrics
        auto onnxTotal = deser.template read<uint32_t>();
        if (!onnxTotal)
            return onnxTotal.error();
        res.onnxTotalSlots = onnxTotal.value();

        auto onnxUsed = deser.template read<uint32_t>();
        if (!onnxUsed)
            return onnxUsed.error();
        res.onnxUsedSlots = onnxUsed.value();

        auto onnxGliner = deser.template read<uint32_t>();
        if (!onnxGliner)
            return onnxGliner.error();
        res.onnxGlinerUsed = onnxGliner.value();

        auto onnxEmbed = deser.template read<uint32_t>();
        if (!onnxEmbed)
            return onnxEmbed.error();
        res.onnxEmbedUsed = onnxEmbed.value();

        auto onnxReranker = deser.template read<uint32_t>();
        if (!onnxReranker)
            return onnxReranker.error();
        res.onnxRerankerUsed = onnxReranker.value();

        // Proxy socket metrics (optional — tolerate missing for backward compat)
        auto proxyConnResult = deser.template read<uint64_t>();
        if (proxyConnResult)
            res.proxyActiveConnections = proxyConnResult.value();
        auto proxyPathResult = deser.template read<std::string>();
        if (proxyPathResult)
            res.proxySocketPath = proxyPathResult.value();

        // Database phase + recovery + storage warning (appended; tolerate missing
        // for backward compat with older daemons).
        auto dbPhaseRes = deser.template read<std::string>();
        if (dbPhaseRes)
            res.databasePhase = std::move(dbPhaseRes.value());
        auto dbPhaseElapsedRes = deser.template read<uint64_t>();
        if (dbPhaseElapsedRes)
            res.databasePhaseElapsedMs = dbPhaseElapsedRes.value();
        auto dbRecoveredAtRes = deser.template read<std::string>();
        if (dbRecoveredAtRes)
            res.databaseRecoveredAt = std::move(dbRecoveredAtRes.value());
        auto dbRecoveredFromRes = deser.template read<std::string>();
        if (dbRecoveredFromRes)
            res.databaseRecoveredFrom = std::move(dbRecoveredFromRes.value());
        auto storageWarnRes = deser.template read<std::string>();
        if (storageWarnRes)
            res.storageWarning = std::move(storageWarnRes.value());

        // WAL file sizes (appended; tolerate missing for backward compat)
        auto metaWalRes = deser.template read<uint64_t>();
        if (metaWalRes)
            res.metadataWalBytes = metaWalRes.value();
        auto vecWalRes = deser.template read<uint64_t>();
        if (vecWalRes)
            res.vectorWalBytes = vecWalRes.value();

        auto maintenancePhaseRes = deser.template read<std::string>();
        if (maintenancePhaseRes)
            res.maintenancePhase = std::move(maintenancePhaseRes.value());
        auto maintenanceElapsedRes = deser.template read<uint64_t>();
        if (maintenanceElapsedRes)
            res.maintenancePhaseElapsedMs = maintenanceElapsedRes.value();

        return res;
    }
};

struct DeleteResponse {
    bool dryRun = false;
    size_t successCount = 0;
    size_t failureCount = 0;

    struct DeleteResult {
        std::string name;
        std::string hash;
        bool success = false;
        std::string error; // if failed

        template <typename Serializer>
        requires IsSerializer<Serializer>
        void serialize(Serializer& ser) const {
            ser << name << hash << success << error;
        }

        template <typename Deserializer>
        requires IsDeserializer<Deserializer>
        static Result<DeleteResult> deserialize(Deserializer& deser) {
            DeleteResult result;

            auto nameRes = deser.readString();
            if (!nameRes)
                return nameRes.error();
            result.name = std::move(nameRes.value());

            auto hashRes = deser.readString();
            if (!hashRes)
                return hashRes.error();
            result.hash = std::move(hashRes.value());

            auto successRes = deser.template read<bool>();
            if (!successRes)
                return successRes.error();
            result.success = successRes.value();

            auto errorRes = deser.readString();
            if (!errorRes)
                return errorRes.error();
            result.error = std::move(errorRes.value());

            return result;
        }
    };

    std::vector<DeleteResult> results;

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << dryRun << successCount << failureCount;
        ser << static_cast<uint32_t>(results.size());
        for (const auto& result : results) {
            result.serialize(ser);
        }
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<DeleteResponse> deserialize(Deserializer& deser) {
        DeleteResponse resp;

        auto dryRunRes = deser.template read<bool>();
        if (!dryRunRes)
            return dryRunRes.error();
        resp.dryRun = dryRunRes.value();

        auto successRes = deser.template read<uint32_t>();
        if (!successRes)
            return successRes.error();
        resp.successCount = successRes.value();

        auto failureRes = deser.template read<uint32_t>();
        if (!failureRes)
            return failureRes.error();
        resp.failureCount = failureRes.value();

        auto sizeRes = deser.template read<uint32_t>();
        if (!sizeRes)
            return sizeRes.error();
        size_t size = sizeRes.value();

        resp.results.reserve(size);
        for (size_t i = 0; i < size; ++i) {
            auto resultRes = DeleteResult::deserialize(deser);
            if (!resultRes)
                return resultRes.error();
            resp.results.push_back(std::move(resultRes.value()));
        }

        return resp;
    }
};

struct SuccessResponse {
    std::string message;

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << message;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<SuccessResponse> deserialize(Deserializer& deser) {
        SuccessResponse res;

        auto messageResult = deser.readString();
        if (!messageResult)
            return messageResult.error();
        res.message = std::move(messageResult.value());

        return res;
    }
};

struct ErrorResponse {
    struct RetryInfo {
        std::uint32_t retryAfterMs{0};
        std::string reason;

        template <typename Serializer>
        requires IsSerializer<Serializer>
        void serialize(Serializer& ser) const {
            ser << retryAfterMs << reason;
        }

        template <typename Deserializer>
        requires IsDeserializer<Deserializer>
        static Result<RetryInfo> deserialize(Deserializer& deser) {
            RetryInfo info;
            auto retryResult = deser.template read<uint32_t>();
            if (!retryResult)
                return retryResult.error();
            info.retryAfterMs = retryResult.value();

            auto reasonResult = deser.readString();
            if (!reasonResult)
                return reasonResult.error();
            info.reason = std::move(reasonResult.value());
            return info;
        }
    };

    ErrorCode code;
    std::string message;
    std::optional<RetryInfo> retry;

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << static_cast<uint32_t>(code) << message;
        const bool hasRetry = retry.has_value();
        ser << hasRetry;
        if (hasRetry) {
            retry->serialize(ser);
        }
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<ErrorResponse> deserialize(Deserializer& deser) {
        ErrorResponse res;

        auto codeResult = deser.template read<uint32_t>();
        if (!codeResult)
            return codeResult.error();
        res.code = static_cast<ErrorCode>(codeResult.value());

        auto messageResult = deser.readString();
        if (!messageResult)
            return messageResult.error();
        res.message = std::move(messageResult.value());

        auto hasRetryResult = deser.readBool();
        if (!hasRetryResult)
            return hasRetryResult.error();
        if (hasRetryResult.value()) {
            auto retryResult = RetryInfo::deserialize(deser);
            if (!retryResult)
                return retryResult.error();
            res.retry = std::move(retryResult.value());
        }

        return res;
    }
};

struct PongResponse {
    std::chrono::steady_clock::time_point serverTime;
    std::chrono::milliseconds roundTrip;

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << static_cast<uint64_t>(serverTime.time_since_epoch().count()) << roundTrip;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<PongResponse> deserialize(Deserializer& deser) {
        PongResponse res;

        auto timeResult = deser.template read<uint64_t>();
        if (!timeResult)
            return timeResult.error();
        res.serverTime = std::chrono::steady_clock::time_point{
            std::chrono::steady_clock::duration{timeResult.value()}};

        auto roundTripResult = deser.template readDuration<std::chrono::milliseconds>();
        if (!roundTripResult)
            return roundTripResult.error();
        res.roundTrip = roundTripResult.value();

        return res;
    }
};

// ============================================================================
// Embedding Response Types
// ============================================================================

struct EmbeddingResponse {
    std::vector<float> embedding;
    size_t dimensions;
    std::string modelUsed;
    uint32_t processingTimeMs;

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << static_cast<uint32_t>(embedding.size());
        for (float value : embedding) {
            ser << value;
        }
        ser << static_cast<uint64_t>(dimensions) << modelUsed << processingTimeMs;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<EmbeddingResponse> deserialize(Deserializer& deser) {
        EmbeddingResponse res;

        auto sizeResult = deser.template read<uint32_t>();
        if (!sizeResult)
            return sizeResult.error();

        res.embedding.resize(sizeResult.value());
        for (size_t i = 0; i < sizeResult.value(); ++i) {
            auto valueResult = deser.template read<float>();
            if (!valueResult)
                return valueResult.error();
            res.embedding[i] = valueResult.value();
        }

        auto dimensionsResult = deser.template read<uint64_t>();
        if (!dimensionsResult)
            return dimensionsResult.error();
        res.dimensions = dimensionsResult.value();

        auto modelResult = deser.readString();
        if (!modelResult)
            return modelResult.error();
        res.modelUsed = std::move(modelResult.value());

        auto timeResult = deser.template read<uint32_t>();
        if (!timeResult)
            return timeResult.error();
        res.processingTimeMs = timeResult.value();

        return res;
    }
};

struct BatchEmbeddingResponse {
    std::vector<std::vector<float>> embeddings;
    size_t dimensions;
    std::string modelUsed;
    uint32_t processingTimeMs;
    size_t successCount;
    size_t failureCount;

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << static_cast<uint32_t>(embeddings.size());
        for (const auto& embedding : embeddings) {
            ser << static_cast<uint32_t>(embedding.size());
            for (float value : embedding) {
                ser << value;
            }
        }
        ser << static_cast<uint64_t>(dimensions) << modelUsed << processingTimeMs
            << static_cast<uint64_t>(successCount) << static_cast<uint64_t>(failureCount);
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<BatchEmbeddingResponse> deserialize(Deserializer& deser) {
        BatchEmbeddingResponse res;

        auto embeddingsCountResult = deser.template read<uint32_t>();
        if (!embeddingsCountResult)
            return embeddingsCountResult.error();

        res.embeddings.reserve(embeddingsCountResult.value());
        for (uint32_t i = 0; i < embeddingsCountResult.value(); ++i) {
            auto embeddingSizeResult = deser.template read<uint32_t>();
            if (!embeddingSizeResult)
                return embeddingSizeResult.error();

            std::vector<float> embedding;
            embedding.resize(embeddingSizeResult.value());
            for (uint32_t j = 0; j < embeddingSizeResult.value(); ++j) {
                auto valueResult = deser.template read<float>();
                if (!valueResult)
                    return valueResult.error();
                embedding[j] = valueResult.value();
            }
            res.embeddings.push_back(std::move(embedding));
        }

        auto dimensionsResult = deser.template read<uint64_t>();
        if (!dimensionsResult)
            return dimensionsResult.error();
        res.dimensions = dimensionsResult.value();

        auto modelResult = deser.readString();
        if (!modelResult)
            return modelResult.error();
        res.modelUsed = std::move(modelResult.value());

        auto timeResult = deser.template read<uint32_t>();
        if (!timeResult)
            return timeResult.error();
        res.processingTimeMs = timeResult.value();

        auto successResult = deser.template read<uint64_t>();
        if (!successResult)
            return successResult.error();
        res.successCount = successResult.value();

        auto failureResult = deser.template read<uint64_t>();
        if (!failureResult)
            return failureResult.error();
        res.failureCount = failureResult.value();

        return res;
    }
};

// ============================================================================
// Streaming Event Types
// ============================================================================

struct EmbedDocumentsResponse {
    size_t requested{0};
    size_t embedded{0};
    size_t skipped{0};
    size_t failed{0};

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << static_cast<uint64_t>(requested) << static_cast<uint64_t>(embedded)
            << static_cast<uint64_t>(skipped) << static_cast<uint64_t>(failed);
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<EmbedDocumentsResponse> deserialize(Deserializer& deser) {
        EmbedDocumentsResponse res;

        auto r = deser.template read<uint64_t>();
        if (!r)
            return r.error();
        res.requested = r.value();

        auto e = deser.template read<uint64_t>();
        if (!e)
            return e.error();
        res.embedded = e.value();

        auto s = deser.template read<uint64_t>();
        if (!s)
            return s.error();
        res.skipped = s.value();

        auto f = deser.template read<uint64_t>();
        if (!f)
            return f.error();
        res.failed = f.value();

        return res;
    }
};

struct EmbeddingEvent {
    std::string modelName;
    size_t processed{0};
    size_t total{0};
    size_t success{0};
    size_t failure{0};
    size_t inserted{0};
    std::string phase;   // started|working|completed|error
    std::string message; // optional status

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << modelName << static_cast<uint64_t>(processed) << static_cast<uint64_t>(total)
            << static_cast<uint64_t>(success) << static_cast<uint64_t>(failure)
            << static_cast<uint64_t>(inserted) << phase << message;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<EmbeddingEvent> deserialize(Deserializer& deser) {
        EmbeddingEvent ev;
        auto m = deser.readString();
        if (!m)
            return m.error();
        ev.modelName = std::move(m.value());
        auto p = deser.template read<uint64_t>();
        if (!p)
            return p.error();
        ev.processed = p.value();
        auto t = deser.template read<uint64_t>();
        if (!t)
            return t.error();
        ev.total = t.value();
        auto s = deser.template read<uint64_t>();
        if (!s)
            return s.error();
        ev.success = s.value();
        auto f = deser.template read<uint64_t>();
        if (!f)
            return f.error();
        ev.failure = f.value();
        auto ins = deser.template read<uint64_t>();
        if (!ins)
            return ins.error();
        ev.inserted = ins.value();
        auto ph = deser.readString();
        if (!ph)
            return ph.error();
        ev.phase = std::move(ph.value());
        auto msg = deser.readString();
        if (!msg)
            return msg.error();
        ev.message = std::move(msg.value());
        return ev;
    }
};

struct ModelLoadEvent {
    std::string modelName;
    std::string phase; // started|downloading|initializing|warming|completed|error
    uint64_t bytesTotal{0};
    uint64_t bytesLoaded{0};
    std::string message;

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << modelName << phase << bytesTotal << bytesLoaded << message;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<ModelLoadEvent> deserialize(Deserializer& deser) {
        ModelLoadEvent ev;
        auto m = deser.readString();
        if (!m)
            return m.error();
        ev.modelName = std::move(m.value());
        auto ph = deser.readString();
        if (!ph)
            return ph.error();
        ev.phase = std::move(ph.value());
        auto bt = deser.template read<uint64_t>();
        if (!bt)
            return bt.error();
        ev.bytesTotal = bt.value();
        auto bl = deser.template read<uint64_t>();
        if (!bl)
            return bl.error();
        ev.bytesLoaded = bl.value();
        auto msg = deser.readString();
        if (!msg)
            return msg.error();
        ev.message = std::move(msg.value());
        return ev;
    }
};

struct ModelLoadResponse {
    bool success;
    std::string modelName;
    size_t memoryUsageMb;
    uint32_t loadTimeMs;

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << success << modelName << static_cast<uint64_t>(memoryUsageMb) << loadTimeMs;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<ModelLoadResponse> deserialize(Deserializer& deser) {
        ModelLoadResponse res;

        auto successResult = deser.template read<bool>();
        if (!successResult)
            return successResult.error();
        res.success = successResult.value();

        auto modelResult = deser.readString();
        if (!modelResult)
            return modelResult.error();
        res.modelName = std::move(modelResult.value());

        auto memoryResult = deser.template read<uint64_t>();
        if (!memoryResult)
            return memoryResult.error();
        res.memoryUsageMb = memoryResult.value();

        auto timeResult = deser.template read<uint32_t>();
        if (!timeResult)
            return timeResult.error();
        res.loadTimeMs = timeResult.value();

        return res;
    }
};

struct ModelStatusResponse {
    struct ModelDetails {
        std::string name;
        std::string path;
        bool loaded;
        bool isHot; // Part of hot pool
        size_t memoryMb;
        size_t embeddingDim;
        size_t maxSequenceLength;
        uint64_t requestCount;
        uint64_t errorCount;
        std::chrono::system_clock::time_point loadTime;
        std::chrono::steady_clock::time_point lastAccess;

        template <typename Serializer>
        requires IsSerializer<Serializer>
        void serialize(Serializer& ser) const {
            ser << name << path << loaded << isHot << static_cast<uint64_t>(memoryMb)
                << static_cast<uint64_t>(embeddingDim) << static_cast<uint64_t>(maxSequenceLength)
                << requestCount << errorCount
                << static_cast<uint64_t>(loadTime.time_since_epoch().count())
                << static_cast<uint64_t>(lastAccess.time_since_epoch().count());
        }

        template <typename Deserializer>
        requires IsDeserializer<Deserializer>
        static Result<ModelDetails> deserialize(Deserializer& deser) {
            ModelDetails details;

            auto nameResult = deser.readString();
            if (!nameResult)
                return nameResult.error();
            details.name = std::move(nameResult.value());

            auto pathResult = deser.readString();
            if (!pathResult)
                return pathResult.error();
            details.path = std::move(pathResult.value());

            auto loadedResult = deser.template read<bool>();
            if (!loadedResult)
                return loadedResult.error();
            details.loaded = loadedResult.value();

            auto isHotResult = deser.template read<bool>();
            if (!isHotResult)
                return isHotResult.error();
            details.isHot = isHotResult.value();

            auto memoryResult = deser.template read<uint64_t>();
            if (!memoryResult)
                return memoryResult.error();
            details.memoryMb = memoryResult.value();

            auto embeddingDimResult = deser.template read<uint64_t>();
            if (!embeddingDimResult)
                return embeddingDimResult.error();
            details.embeddingDim = embeddingDimResult.value();

            auto maxSeqResult = deser.template read<uint64_t>();
            if (!maxSeqResult)
                return maxSeqResult.error();
            details.maxSequenceLength = maxSeqResult.value();

            auto requestCountResult = deser.template read<uint64_t>();
            if (!requestCountResult)
                return requestCountResult.error();
            details.requestCount = requestCountResult.value();

            auto errorCountResult = deser.template read<uint64_t>();
            if (!errorCountResult)
                return errorCountResult.error();
            details.errorCount = errorCountResult.value();

            auto loadTimeResult = deser.template read<uint64_t>();
            if (!loadTimeResult)
                return loadTimeResult.error();
            details.loadTime = std::chrono::system_clock::time_point{
                std::chrono::system_clock::duration{loadTimeResult.value()}};

            auto lastAccessResult = deser.template read<uint64_t>();
            if (!lastAccessResult)
                return lastAccessResult.error();
            details.lastAccess = std::chrono::steady_clock::time_point{
                std::chrono::steady_clock::duration{lastAccessResult.value()}};

            return details;
        }
    };

    std::vector<ModelDetails> models;
    size_t totalMemoryMb;
    size_t maxMemoryMb;

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << static_cast<uint32_t>(models.size());
        for (const auto& model : models) {
            model.serialize(ser);
        }
        ser << static_cast<uint64_t>(totalMemoryMb) << static_cast<uint64_t>(maxMemoryMb);
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<ModelStatusResponse> deserialize(Deserializer& deser) {
        ModelStatusResponse res;

        auto modelsCountResult = deser.template read<uint32_t>();
        if (!modelsCountResult)
            return modelsCountResult.error();

        res.models.reserve(modelsCountResult.value());
        for (uint32_t i = 0; i < modelsCountResult.value(); ++i) {
            auto modelResult = ModelDetails::deserialize(deser);
            if (!modelResult)
                return modelResult.error();
            res.models.push_back(std::move(modelResult.value()));
        }

        auto totalMemoryResult = deser.template read<uint64_t>();
        if (!totalMemoryResult)
            return totalMemoryResult.error();
        res.totalMemoryMb = totalMemoryResult.value();

        auto maxMemoryResult = deser.template read<uint64_t>();
        if (!maxMemoryResult)
            return maxMemoryResult.error();
        res.maxMemoryMb = maxMemoryResult.value();

        return res;
    }
};

// ============================================================================
// Additional Response Types
// ============================================================================

struct AddDocumentResponse {
    std::string hash;
    std::string path;
    std::string message;
    size_t documentsAdded = 0;
    size_t documentsUpdated = 0;
    size_t documentsSkipped = 0;
    size_t size = 0;
    std::string snapshotId;    // Auto-generated timestamp ID for directory operations
    std::string snapshotLabel; // Optional human-friendly label
    std::string extractionStatus =
        "pending"; // Text extraction status: pending, success, failed, skipped

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << hash << path << documentsAdded << documentsUpdated << documentsSkipped << message
            << static_cast<uint64_t>(size) << snapshotId << snapshotLabel << extractionStatus;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<AddDocumentResponse> deserialize(Deserializer& deser) {
        AddDocumentResponse res;
        auto h = deser.readString();
        if (!h)
            return h.error();
        res.hash = std::move(h.value());
        auto p = deser.readString();
        if (!p)
            return p.error();
        res.path = std::move(p.value());
        auto da = deser.template read<uint64_t>();
        if (!da)
            return da.error();
        res.documentsAdded = static_cast<size_t>(da.value());
        auto du = deser.template read<uint64_t>();
        if (!du) {
            // For backward compatibility, assume 0 if read fails
            res.documentsUpdated = 0;
        } else {
            res.documentsUpdated = static_cast<size_t>(du.value());
        }
        auto ds = deser.template read<uint64_t>();
        if (!ds) {
            // For backward compatibility, assume 0 if read fails
            res.documentsSkipped = 0;
        } else {
            res.documentsSkipped = static_cast<size_t>(ds.value());
        }
        auto m = deser.readString();
        if (!m)
            return m.error();
        res.message = std::move(m.value());
        auto s = deser.template read<uint64_t>();
        if (!s)
            return s.error();
        res.size = static_cast<size_t>(s.value());
        auto sid = deser.readString();
        if (!sid)
            return sid.error();
        res.snapshotId = std::move(sid.value());
        auto slbl = deser.readString();
        if (!slbl)
            return slbl.error();
        res.snapshotLabel = std::move(slbl.value());

        // Read extraction status (backward compatible - default to pending if not present)
        auto es = deser.readString();
        if (es) {
            res.extractionStatus = std::move(es.value());
        }

        return res;
    }
};

struct GrepMatch {
    std::string file;
    size_t lineNumber = 0;
    std::string line;
    std::vector<std::string> contextBefore;
    std::vector<std::string> contextAfter;
    std::string matchType = "regex"; // "regex" | "semantic" | "hybrid"
    double confidence = 1.0;         // Match confidence (1.0 for regex, variable for semantic)
    std::string diff;

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << file << static_cast<uint64_t>(lineNumber) << line << contextBefore << contextAfter
            << matchType << confidence;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<GrepMatch> deserialize(Deserializer& deser) {
        GrepMatch m;
        auto f = deser.readString();
        if (!f)
            return f.error();
        m.file = std::move(f.value());
        auto ln = deser.template read<uint64_t>();
        if (!ln)
            return ln.error();
        m.lineNumber = ln.value();
        auto l = deser.readString();
        if (!l)
            return l.error();
        m.line = std::move(l.value());
        auto cb = deser.readStringVector();
        if (!cb)
            return cb.error();
        m.contextBefore = std::move(cb.value());
        auto ca = deser.readStringVector();
        if (!ca)
            return ca.error();
        m.contextAfter = std::move(ca.value());

        // Deserialize new fields
        auto mt = deser.readString();
        if (!mt)
            return mt.error();
        m.matchType = std::move(mt.value());
        auto conf = deser.template read<double>();
        if (!conf)
            return conf.error();
        m.confidence = conf.value();

        return m;
    }
};

struct GrepResponse {
    std::vector<GrepMatch> matches;
    uint64_t totalMatches = 0;
    uint64_t filesSearched = 0;
    uint64_t regexMatches = 0;
    uint64_t semanticMatches = 0;
    int64_t executionTimeMs = 0;
    std::string queryInfo;
    std::map<std::string, std::string> searchStats;
    std::vector<std::string> filesWith;
    std::vector<std::string> filesWithout;
    std::vector<std::string> pathsOnly;

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << static_cast<uint32_t>(matches.size());
        for (const auto& it : matches)
            it.serialize(ser);
        ser << totalMatches << filesSearched << regexMatches << semanticMatches << executionTimeMs
            << queryInfo << searchStats << filesWith << filesWithout << pathsOnly;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<GrepResponse> deserialize(Deserializer& deser) {
        GrepResponse r;
        auto cnt = deser.template read<uint32_t>();
        if (!cnt)
            return cnt.error();
        r.matches.reserve(cnt.value());
        for (uint32_t i = 0; i < cnt.value(); ++i) {
            auto e = GrepMatch::deserialize(deser);
            if (!e)
                return e.error();
            r.matches.push_back(std::move(e.value()));
        }

        auto totalMatches_ = deser.template read<uint64_t>();
        if (!totalMatches_)
            return totalMatches_.error();
        r.totalMatches = totalMatches_.value();

        auto filesSearched_ = deser.template read<uint64_t>();
        if (!filesSearched_)
            return filesSearched_.error();
        r.filesSearched = filesSearched_.value();

        // For backward compatibility, new fields are optional
        auto regexMatches_ = deser.template read<uint64_t>();
        if (regexMatches_)
            r.regexMatches = regexMatches_.value();

        auto semanticMatches_ = deser.template read<uint64_t>();
        if (semanticMatches_)
            r.semanticMatches = semanticMatches_.value();

        auto executionTimeMs_ = deser.template read<int64_t>();
        if (executionTimeMs_)
            r.executionTimeMs = executionTimeMs_.value();

        auto queryInfo_ = deser.readString();
        if (queryInfo_)
            r.queryInfo = queryInfo_.value();

        auto searchStats_ = deser.readStringMap();
        if (searchStats_)
            r.searchStats = searchStats_.value();

        auto filesWith_ = deser.readStringVector();
        if (filesWith_)
            r.filesWith = filesWith_.value();

        auto filesWithout_ = deser.readStringVector();
        if (filesWithout_)
            r.filesWithout = filesWithout_.value();

        auto pathsOnly_ = deser.readStringVector();
        if (pathsOnly_)
            r.pathsOnly = pathsOnly_.value();

        return r;
    }
};

struct UpdateDocumentResponse {
    std::string hash; // Updated document hash
    bool contentUpdated = false;
    bool metadataUpdated = false;
    bool tagsUpdated = false;
    uint64_t updatesApplied = 0;
    uint64_t tagsAdded = 0;
    uint64_t tagsRemoved = 0;

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << hash << contentUpdated << metadataUpdated << tagsUpdated << updatesApplied
            << tagsAdded << tagsRemoved;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<UpdateDocumentResponse> deserialize(Deserializer& deser) {
        UpdateDocumentResponse res;
        auto h = deser.readString();
        if (!h)
            return h.error();
        res.hash = std::move(h.value());
        auto c = deser.template read<bool>();
        if (!c)
            return c.error();
        res.contentUpdated = c.value();
        auto m = deser.template read<bool>();
        if (!m)
            return m.error();
        res.metadataUpdated = m.value();
        auto t = deser.template read<bool>();
        if (!t)
            return t.error();
        res.tagsUpdated = t.value();
        if (auto ua = deser.template read<uint64_t>(); ua)
            res.updatesApplied = ua.value();
        if (auto ta = deser.template read<uint64_t>(); ta)
            res.tagsAdded = ta.value();
        if (auto tr = deser.template read<uint64_t>(); tr)
            res.tagsRemoved = tr.value();
        return res;
    }
};

struct GetStatsResponse {
    size_t totalDocuments = 0;
    size_t totalSize = 0;
    size_t indexedDocuments = 0;
    size_t vectorIndexSize = 0;
    double compressionRatio = 0.0;
    std::map<std::string, size_t> documentsByType;
    std::map<std::string, std::string> additionalStats;

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << static_cast<uint64_t>(totalDocuments) << static_cast<uint64_t>(totalSize)
            << static_cast<uint64_t>(indexedDocuments) << static_cast<uint64_t>(vectorIndexSize)
            << compressionRatio;

        // Serialize documentsByType map manually (string -> size_t)
        ser << static_cast<uint32_t>(documentsByType.size());
        for (const auto& [key, value] : documentsByType) {
            ser << key << static_cast<uint64_t>(value);
        }

        ser << additionalStats;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<GetStatsResponse> deserialize(Deserializer& deser) {
        GetStatsResponse res;
        auto td = deser.template read<uint64_t>();
        if (!td)
            return td.error();
        res.totalDocuments = td.value();
        auto ts = deser.template read<uint64_t>();
        if (!ts)
            return ts.error();
        res.totalSize = ts.value();
        auto id = deser.template read<uint64_t>();
        if (!id)
            return id.error();
        res.indexedDocuments = id.value();
        auto vs = deser.template read<uint64_t>();
        if (!vs)
            return vs.error();
        res.vectorIndexSize = vs.value();
        auto cr = deser.template read<double>();
        if (!cr)
            return cr.error();
        res.compressionRatio = cr.value();

        // Read documentsByType map
        auto typeCount = deser.template read<uint32_t>();
        if (!typeCount)
            return typeCount.error();
        for (uint32_t i = 0; i < typeCount.value(); ++i) {
            auto key = deser.readString();
            if (!key)
                return key.error();
            auto val = deser.template read<uint64_t>();
            if (!val)
                return val.error();
            res.documentsByType[key.value()] = val.value();
        }

        auto as = deser.readStringMap();
        if (!as)
            return as.error();
        res.additionalStats = std::move(as.value());
        return res;
    }
};

// File history response (PBI-043 enhancement)
struct FileVersion {
    std::string snapshotId;
    std::string hash;
    uint64_t size = 0;
    int64_t indexedTimestamp = 0; // Unix timestamp in seconds

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << snapshotId << hash << size << indexedTimestamp;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<FileVersion> deserialize(Deserializer& deser) {
        FileVersion fv;
        auto sid = deser.readString();
        if (!sid)
            return sid.error();
        fv.snapshotId = std::move(sid.value());

        auto h = deser.readString();
        if (!h)
            return h.error();
        fv.hash = std::move(h.value());

        auto s = deser.template read<uint64_t>();
        if (!s)
            return s.error();
        fv.size = s.value();

        auto ts = deser.template read<int64_t>();
        if (!ts)
            return ts.error();
        fv.indexedTimestamp = ts.value();

        return fv;
    }
};

struct FileHistoryResponse {
    std::string filepath;
    std::vector<FileVersion> versions;
    uint32_t totalVersions = 0;
    bool found = false;
    std::string message; // User-friendly message (e.g., "File not found in any snapshot")

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << filepath << static_cast<uint32_t>(versions.size());
        for (const auto& v : versions)
            v.serialize(ser);
        ser << totalVersions << found << message;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<FileHistoryResponse> deserialize(Deserializer& deser) {
        FileHistoryResponse res;

        auto fp = deser.readString();
        if (!fp)
            return fp.error();
        res.filepath = std::move(fp.value());

        auto cnt = deser.template read<uint32_t>();
        if (!cnt)
            return cnt.error();
        res.versions.reserve(cnt.value());
        for (uint32_t i = 0; i < cnt.value(); ++i) {
            auto fv = FileVersion::deserialize(deser);
            if (!fv)
                return fv.error();
            res.versions.push_back(std::move(fv.value()));
        }

        auto tv = deser.template read<uint32_t>();
        if (!tv)
            return tv.error();
        res.totalVersions = tv.value();

        auto f = deser.template read<bool>();
        if (!f)
            return f.error();
        res.found = f.value();

        auto msg = deser.readString();
        if (!msg)
            return msg.error();
        res.message = std::move(msg.value());

        return res;
    }
};

// Prune response (PBI-062)
struct PruneResponse {
    uint64_t filesDeleted{0};
    uint64_t filesFailed{0};
    uint64_t totalBytesFreed{0};
    std::unordered_map<std::string, uint64_t> categoryCounts; // Category -> file count
    std::unordered_map<std::string, uint64_t> categorySizes;  // Category -> bytes
    std::vector<std::string> deletedPaths;                    // If verbose
    std::vector<std::string> failedPaths;
    std::string statusMessage;
    std::string errorMessage;

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << filesDeleted << filesFailed << totalBytesFreed;

        ser << static_cast<uint32_t>(categoryCounts.size());
        for (const auto& [cat, count] : categoryCounts) {
            ser << cat << count;
        }

        ser << static_cast<uint32_t>(categorySizes.size());
        for (const auto& [cat, size] : categorySizes) {
            ser << cat << size;
        }

        ser << deletedPaths << failedPaths << statusMessage << errorMessage;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<PruneResponse> deserialize(Deserializer& deser) {
        PruneResponse res;

        if (auto r = deser.template read<uint64_t>(); r)
            res.filesDeleted = r.value();
        else
            return r.error();

        if (auto r = deser.template read<uint64_t>(); r)
            res.filesFailed = r.value();
        else
            return r.error();

        if (auto r = deser.template read<uint64_t>(); r)
            res.totalBytesFreed = r.value();
        else
            return r.error();

        // Read categoryCounts map
        if (auto cnt = deser.template read<uint32_t>(); cnt) {
            for (uint32_t i = 0; i < cnt.value(); ++i) {
                auto key = deser.readString();
                if (!key)
                    return key.error();
                auto val = deser.template read<uint64_t>();
                if (!val)
                    return val.error();
                res.categoryCounts[key.value()] = val.value();
            }
        } else
            return cnt.error();

        // Read categorySizes map
        if (auto cnt = deser.template read<uint32_t>(); cnt) {
            for (uint32_t i = 0; i < cnt.value(); ++i) {
                auto key = deser.readString();
                if (!key)
                    return key.error();
                auto val = deser.template read<uint64_t>();
                if (!val)
                    return val.error();
                res.categorySizes[key.value()] = val.value();
            }
        } else
            return cnt.error();

        if (auto r = deser.readStringVector(); r)
            res.deletedPaths = std::move(r.value());
        else
            return r.error();

        if (auto r = deser.readStringVector(); r)
            res.failedPaths = std::move(r.value());
        else
            return r.error();

        if (auto r = deser.readString(); r)
            res.statusMessage = std::move(r.value());
        else
            return r.error();

        if (auto r = deser.readString(); r)
            res.errorMessage = std::move(r.value());
        else
            return r.error();

        return res;
    }
};

// Snapshot responses (collections use generic metadata query via getMetadataValueCounts)
struct SnapshotInfo {
    std::string id;
    std::string label;
    std::string createdAt;
    uint64_t documentCount{0};

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << id << label << createdAt << documentCount;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<SnapshotInfo> deserialize(Deserializer& deser) {
        SnapshotInfo info;

        if (auto r = deser.readString(); r)
            info.id = std::move(r.value());
        else
            return r.error();

        if (auto r = deser.readString(); r)
            info.label = std::move(r.value());
        else
            return r.error();

        if (auto r = deser.readString(); r)
            info.createdAt = std::move(r.value());
        else
            return r.error();

        if (auto r = deser.template read<uint64_t>(); r)
            info.documentCount = r.value();
        else
            return r.error();

        return info;
    }
};

struct ListSnapshotsResponse {
    std::vector<SnapshotInfo> snapshots;
    uint64_t totalCount{0};

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << static_cast<uint32_t>(snapshots.size());
        for (const auto& snap : snapshots) {
            snap.serialize(ser);
        }
        ser << totalCount;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<ListSnapshotsResponse> deserialize(Deserializer& deser) {
        ListSnapshotsResponse res;

        if (auto cnt = deser.template read<uint32_t>(); cnt) {
            res.snapshots.reserve(cnt.value());
            for (uint32_t i = 0; i < cnt.value(); ++i) {
                auto snap = SnapshotInfo::deserialize(deser);
                if (!snap)
                    return snap.error();
                res.snapshots.push_back(std::move(snap.value()));
            }
        } else
            return cnt.error();

        if (auto r = deser.template read<uint64_t>(); r)
            res.totalCount = r.value();
        else
            return r.error();

        return res;
    }
};

struct RestoredFile {
    std::string path;
    std::string hash;
    uint64_t size{0};
    bool skipped{false};
    std::string skipReason;

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << path << hash << size << skipped << skipReason;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<RestoredFile> deserialize(Deserializer& deser) {
        RestoredFile file;

        if (auto r = deser.readString(); r)
            file.path = std::move(r.value());
        else
            return r.error();

        if (auto r = deser.readString(); r)
            file.hash = std::move(r.value());
        else
            return r.error();

        if (auto r = deser.template read<uint64_t>(); r)
            file.size = r.value();
        else
            return r.error();

        if (auto r = deser.readBool(); r)
            file.skipped = r.value();
        else
            return r.error();

        if (auto r = deser.readString(); r)
            file.skipReason = std::move(r.value());
        else
            return r.error();

        return file;
    }
};

struct RestoreCollectionResponse {
    uint64_t filesRestored{0};
    std::vector<RestoredFile> files;
    bool dryRun{false};

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << filesRestored;
        ser << static_cast<uint32_t>(files.size());
        for (const auto& file : files) {
            file.serialize(ser);
        }
        ser << dryRun;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<RestoreCollectionResponse> deserialize(Deserializer& deser) {
        RestoreCollectionResponse res;

        if (auto r = deser.template read<uint64_t>(); r)
            res.filesRestored = r.value();
        else
            return r.error();

        if (auto cnt = deser.template read<uint32_t>(); cnt) {
            res.files.reserve(cnt.value());
            for (uint32_t i = 0; i < cnt.value(); ++i) {
                auto file = RestoredFile::deserialize(deser);
                if (!file)
                    return file.error();
                res.files.push_back(std::move(file.value()));
            }
        } else
            return cnt.error();

        if (auto r = deser.readBool(); r)
            res.dryRun = r.value();
        else
            return r.error();

        return res;
    }
};

struct RestoreSnapshotResponse {
    uint64_t filesRestored{0};
    std::vector<RestoredFile> files;
    bool dryRun{false};

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << filesRestored;
        ser << static_cast<uint32_t>(files.size());
        for (const auto& file : files) {
            file.serialize(ser);
        }
        ser << dryRun;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<RestoreSnapshotResponse> deserialize(Deserializer& deser) {
        RestoreSnapshotResponse res;

        if (auto r = deser.template read<uint64_t>(); r)
            res.filesRestored = r.value();
        else
            return r.error();

        if (auto cnt = deser.template read<uint32_t>(); cnt) {
            res.files.reserve(cnt.value());
            for (uint32_t i = 0; i < cnt.value(); ++i) {
                auto file = RestoredFile::deserialize(deser);
                if (!file)
                    return file.error();
                res.files.push_back(std::move(file.value()));
            }
        } else
            return cnt.error();

        if (auto r = deser.readBool(); r)
            res.dryRun = r.value();
        else
            return r.error();

        return res;
    }
};

// Graph query responses (PBI-009)
struct GraphNode {
    int64_t nodeId{0};
    std::string nodeKey;
    std::string label;
    std::string type;
    std::string documentHash;
    std::string documentPath;
    std::string snapshotId;
    int32_t distance{0};
    std::string properties; // JSON string for node properties (optional)

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << nodeId << nodeKey << label << type << documentHash << documentPath << snapshotId
            << distance << properties;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<GraphNode> deserialize(Deserializer& deser) {
        GraphNode node;

        if (auto r = deser.template read<int64_t>(); r)
            node.nodeId = r.value();
        else
            return r.error();

        if (auto r = deser.readString(); r)
            node.nodeKey = std::move(r.value());
        else
            return r.error();

        if (auto r = deser.readString(); r)
            node.label = std::move(r.value());
        else
            return r.error();

        if (auto r = deser.readString(); r)
            node.type = std::move(r.value());
        else
            return r.error();

        if (auto r = deser.readString(); r)
            node.documentHash = std::move(r.value());
        else
            return r.error();

        if (auto r = deser.readString(); r)
            node.documentPath = std::move(r.value());
        else
            return r.error();

        if (auto r = deser.readString(); r)
            node.snapshotId = std::move(r.value());
        else
            return r.error();

        if (auto r = deser.template read<int32_t>(); r)
            node.distance = r.value();
        else
            return r.error();

        if (auto r = deser.readString(); r)
            node.properties = std::move(r.value());
        else
            return r.error();

        return node;
    }
};

struct GraphEdge {
    int64_t edgeId{0};
    int64_t srcNodeId{0};
    int64_t dstNodeId{0};
    std::string relation;
    float weight{1.0f};
    std::string properties; // JSON string for edge properties (optional)

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << edgeId << srcNodeId << dstNodeId << relation << weight << properties;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<GraphEdge> deserialize(Deserializer& deser) {
        GraphEdge edge;

        if (auto r = deser.template read<int64_t>(); r)
            edge.edgeId = r.value();
        else
            return r.error();

        if (auto r = deser.template read<int64_t>(); r)
            edge.srcNodeId = r.value();
        else
            return r.error();

        if (auto r = deser.template read<int64_t>(); r)
            edge.dstNodeId = r.value();
        else
            return r.error();

        if (auto r = deser.readString(); r)
            edge.relation = std::move(r.value());
        else
            return r.error();

        if (auto r = deser.template read<float>(); r)
            edge.weight = r.value();
        else
            return r.error();

        if (auto r = deser.readString(); r)
            edge.properties = std::move(r.value());
        else
            return r.error();

        return edge;
    }
};

struct GraphQueryResponse {
    GraphNode originNode;
    std::vector<GraphNode> connectedNodes;
    std::vector<GraphEdge> edges;
    uint64_t totalNodesFound{0};
    uint64_t totalEdgesTraversed{0};
    bool truncated{false};
    int32_t maxDepthReached{0};
    int64_t queryTimeMs{0};
    bool kgAvailable{true};
    std::string warning;

    // yams-66h: Node type counts for --list-types mode
    std::vector<std::pair<std::string, uint64_t>> nodeTypeCounts;

    // Relation type counts for --relations mode
    std::vector<std::pair<std::string, uint64_t>> relationTypeCounts;

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        originNode.serialize(ser);
        ser << static_cast<uint32_t>(connectedNodes.size());
        for (const auto& node : connectedNodes) {
            node.serialize(ser);
        }
        ser << static_cast<uint32_t>(edges.size());
        for (const auto& edge : edges) {
            edge.serialize(ser);
        }
        ser << totalNodesFound << totalEdgesTraversed << truncated << maxDepthReached << queryTimeMs
            << kgAvailable << warning;
        // yams-66h: Serialize node type counts
        ser << static_cast<uint32_t>(nodeTypeCounts.size());
        for (const auto& [type, count] : nodeTypeCounts) {
            ser << type << count;
        }
        // Serialize relation type counts
        ser << static_cast<uint32_t>(relationTypeCounts.size());
        for (const auto& [rel, count] : relationTypeCounts) {
            ser << rel << count;
        }
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<GraphQueryResponse> deserialize(Deserializer& deser) {
        GraphQueryResponse res;

        auto origin = GraphNode::deserialize(deser);
        if (!origin)
            return origin.error();
        res.originNode = std::move(origin.value());

        if (auto cnt = deser.template read<uint32_t>(); cnt) {
            res.connectedNodes.reserve(cnt.value());
            for (uint32_t i = 0; i < cnt.value(); ++i) {
                auto node = GraphNode::deserialize(deser);
                if (!node)
                    return node.error();
                res.connectedNodes.push_back(std::move(node.value()));
            }
        } else
            return cnt.error();

        if (auto cnt = deser.template read<uint32_t>(); cnt) {
            res.edges.reserve(cnt.value());
            for (uint32_t i = 0; i < cnt.value(); ++i) {
                auto edge = GraphEdge::deserialize(deser);
                if (!edge)
                    return edge.error();
                res.edges.push_back(std::move(edge.value()));
            }
        } else
            return cnt.error();

        if (auto r = deser.template read<uint64_t>(); r)
            res.totalNodesFound = r.value();
        else
            return r.error();

        if (auto r = deser.template read<uint64_t>(); r)
            res.totalEdgesTraversed = r.value();
        else
            return r.error();

        if (auto r = deser.template read<bool>(); r)
            res.truncated = r.value();
        else
            return r.error();

        if (auto r = deser.template read<int32_t>(); r)
            res.maxDepthReached = r.value();
        else
            return r.error();

        if (auto r = deser.template read<int64_t>(); r)
            res.queryTimeMs = r.value();
        else
            return r.error();

        if (auto r = deser.template read<bool>(); r)
            res.kgAvailable = r.value();
        else
            return r.error();

        if (auto r = deser.readString(); r)
            res.warning = std::move(r.value());
        else
            return r.error();

        // yams-66h: Deserialize node type counts
        if (auto cnt = deser.template read<uint32_t>(); cnt) {
            res.nodeTypeCounts.reserve(cnt.value());
            for (uint32_t i = 0; i < cnt.value(); ++i) {
                std::string type;
                uint64_t count{0};
                if (auto r = deser.readString(); r)
                    type = std::move(r.value());
                else
                    return r.error();
                if (auto r = deser.template read<uint64_t>(); r)
                    count = r.value();
                else
                    return r.error();
                res.nodeTypeCounts.emplace_back(std::move(type), count);
            }
        } else
            return cnt.error();

        // Deserialize relation type counts
        if (auto cnt = deser.template read<uint32_t>(); cnt) {
            res.relationTypeCounts.reserve(cnt.value());
            for (uint32_t i = 0; i < cnt.value(); ++i) {
                std::string rel;
                uint64_t count{0};
                if (auto r = deser.readString(); r)
                    rel = std::move(r.value());
                else
                    return r.error();
                if (auto r = deser.template read<uint64_t>(); r)
                    count = r.value();
                else
                    return r.error();
                res.relationTypeCounts.emplace_back(std::move(rel), count);
            }
        } else
            return cnt.error();

        return res;
    }
};

struct GraphExploreSymbol {
    std::string nodeKey;
    std::string label;
    std::string qualifiedName;
    std::string kind;
    std::string filePath;
    std::optional<int32_t> startLine;
    std::optional<int32_t> endLine;
    double score{0.0};
    bool exactMatch{false};
    bool generatedOrCache{false};
    bool testFile{false};

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << nodeKey << label << qualifiedName << kind << filePath << startLine.has_value();
        if (startLine)
            ser << *startLine;
        ser << endLine.has_value();
        if (endLine)
            ser << *endLine;
        ser << score << exactMatch << generatedOrCache << testFile;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<GraphExploreSymbol> deserialize(Deserializer& deser) {
        GraphExploreSymbol symbol;
        if (auto r = deser.readString(); r)
            symbol.nodeKey = std::move(r.value());
        else
            return r.error();
        if (auto r = deser.readString(); r)
            symbol.label = std::move(r.value());
        else
            return r.error();
        if (auto r = deser.readString(); r)
            symbol.qualifiedName = std::move(r.value());
        else
            return r.error();
        if (auto r = deser.readString(); r)
            symbol.kind = std::move(r.value());
        else
            return r.error();
        if (auto r = deser.readString(); r)
            symbol.filePath = std::move(r.value());
        else
            return r.error();
        if (auto has = deser.template read<bool>(); has) {
            if (has.value()) {
                if (auto r = deser.template read<int32_t>(); r)
                    symbol.startLine = r.value();
                else
                    return r.error();
            }
        } else
            return has.error();
        if (auto has = deser.template read<bool>(); has) {
            if (has.value()) {
                if (auto r = deser.template read<int32_t>(); r)
                    symbol.endLine = r.value();
                else
                    return r.error();
            }
        } else
            return has.error();
        if (auto r = deser.template read<double>(); r)
            symbol.score = r.value();
        else
            return r.error();
        if (auto r = deser.template read<bool>(); r)
            symbol.exactMatch = r.value();
        else
            return r.error();
        if (auto r = deser.template read<bool>(); r)
            symbol.generatedOrCache = r.value();
        else
            return r.error();
        if (auto r = deser.template read<bool>(); r)
            symbol.testFile = r.value();
        else
            return r.error();
        return symbol;
    }
};

struct GraphExploreRelation {
    std::string relation;
    std::string sourceNodeKey;
    std::string sourceLabel;
    std::string targetNodeKey;
    std::string targetLabel;
    float weight{1.0F};
    double confidence{1.0};
    std::string provenance;

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << relation << sourceNodeKey << sourceLabel << targetNodeKey << targetLabel << weight
            << confidence << provenance;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<GraphExploreRelation> deserialize(Deserializer& deser) {
        GraphExploreRelation relation;
        if (auto r = deser.readString(); r)
            relation.relation = std::move(r.value());
        else
            return r.error();
        if (auto r = deser.readString(); r)
            relation.sourceNodeKey = std::move(r.value());
        else
            return r.error();
        if (auto r = deser.readString(); r)
            relation.sourceLabel = std::move(r.value());
        else
            return r.error();
        if (auto r = deser.readString(); r)
            relation.targetNodeKey = std::move(r.value());
        else
            return r.error();
        if (auto r = deser.readString(); r)
            relation.targetLabel = std::move(r.value());
        else
            return r.error();
        if (auto r = deser.template read<float>(); r)
            relation.weight = r.value();
        else
            return r.error();
        if (auto r = deser.template read<double>(); r)
            relation.confidence = r.value();
        else
            return r.error();
        if (auto r = deser.readString(); r)
            relation.provenance = std::move(r.value());
        else
            return r.error();
        return relation;
    }
};

struct GraphExploreSnippet {
    std::string filePath;
    std::string language;
    std::string mode{"full"};
    std::optional<int32_t> startLine;
    std::optional<int32_t> endLine;
    std::string heading;
    std::string content;
    std::vector<GraphExploreSymbol> symbols;
    bool truncated{false};

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << filePath << language << mode << startLine.has_value();
        if (startLine)
            ser << *startLine;
        ser << endLine.has_value();
        if (endLine)
            ser << *endLine;
        ser << heading << content << static_cast<uint32_t>(symbols.size());
        for (const auto& symbol : symbols) {
            symbol.serialize(ser);
        }
        ser << truncated;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<GraphExploreSnippet> deserialize(Deserializer& deser) {
        GraphExploreSnippet snippet;
        if (auto r = deser.readString(); r)
            snippet.filePath = std::move(r.value());
        else
            return r.error();
        if (auto r = deser.readString(); r)
            snippet.language = std::move(r.value());
        else
            return r.error();
        if (auto r = deser.readString(); r)
            snippet.mode = std::move(r.value());
        else
            return r.error();
        if (auto has = deser.template read<bool>(); has) {
            if (has.value()) {
                if (auto r = deser.template read<int32_t>(); r)
                    snippet.startLine = r.value();
                else
                    return r.error();
            }
        } else
            return has.error();
        if (auto has = deser.template read<bool>(); has) {
            if (has.value()) {
                if (auto r = deser.template read<int32_t>(); r)
                    snippet.endLine = r.value();
                else
                    return r.error();
            }
        } else
            return has.error();
        if (auto r = deser.readString(); r)
            snippet.heading = std::move(r.value());
        else
            return r.error();
        if (auto r = deser.readString(); r)
            snippet.content = std::move(r.value());
        else
            return r.error();
        if (auto cnt = deser.template read<uint32_t>(); cnt) {
            snippet.symbols.reserve(cnt.value());
            for (uint32_t i = 0; i < cnt.value(); ++i) {
                auto symbol = GraphExploreSymbol::deserialize(deser);
                if (!symbol)
                    return symbol.error();
                snippet.symbols.push_back(std::move(symbol.value()));
            }
        } else
            return cnt.error();
        if (auto r = deser.template read<bool>(); r)
            snippet.truncated = r.value();
        else
            return r.error();
        return snippet;
    }
};

struct GraphExploreResponse {
    std::string query;
    std::vector<GraphExploreSymbol> entrySymbols;
    std::vector<GraphExploreSnippet> files;
    std::vector<GraphExploreRelation> relationships;
    std::vector<std::string> warnings;
    uint64_t totalSymbolsConsidered{0};
    uint64_t totalFilesConsidered{0};
    uint64_t emittedChars{0};
    bool kgAvailable{true};
    bool truncated{false};

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << query << static_cast<uint32_t>(entrySymbols.size());
        for (const auto& symbol : entrySymbols) {
            symbol.serialize(ser);
        }
        ser << static_cast<uint32_t>(files.size());
        for (const auto& file : files) {
            file.serialize(ser);
        }
        ser << static_cast<uint32_t>(relationships.size());
        for (const auto& relation : relationships) {
            relation.serialize(ser);
        }
        ser << warnings << totalSymbolsConsidered << totalFilesConsidered << emittedChars
            << kgAvailable << truncated;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<GraphExploreResponse> deserialize(Deserializer& deser) {
        GraphExploreResponse response;
        if (auto r = deser.readString(); r)
            response.query = std::move(r.value());
        else
            return r.error();
        if (auto cnt = deser.template read<uint32_t>(); cnt) {
            response.entrySymbols.reserve(cnt.value());
            for (uint32_t i = 0; i < cnt.value(); ++i) {
                auto symbol = GraphExploreSymbol::deserialize(deser);
                if (!symbol)
                    return symbol.error();
                response.entrySymbols.push_back(std::move(symbol.value()));
            }
        } else
            return cnt.error();
        if (auto cnt = deser.template read<uint32_t>(); cnt) {
            response.files.reserve(cnt.value());
            for (uint32_t i = 0; i < cnt.value(); ++i) {
                auto file = GraphExploreSnippet::deserialize(deser);
                if (!file)
                    return file.error();
                response.files.push_back(std::move(file.value()));
            }
        } else
            return cnt.error();
        if (auto cnt = deser.template read<uint32_t>(); cnt) {
            response.relationships.reserve(cnt.value());
            for (uint32_t i = 0; i < cnt.value(); ++i) {
                auto relation = GraphExploreRelation::deserialize(deser);
                if (!relation)
                    return relation.error();
                response.relationships.push_back(std::move(relation.value()));
            }
        } else
            return cnt.error();
        if (auto r = deser.readStringVector(); r)
            response.warnings = std::move(r.value());
        else
            return r.error();
        if (auto r = deser.template read<uint64_t>(); r)
            response.totalSymbolsConsidered = r.value();
        else
            return r.error();
        if (auto r = deser.template read<uint64_t>(); r)
            response.totalFilesConsidered = r.value();
        else
            return r.error();
        if (auto r = deser.template read<uint64_t>(); r)
            response.emittedChars = r.value();
        else
            return r.error();
        if (auto r = deser.template read<bool>(); r)
            response.kgAvailable = r.value();
        else
            return r.error();
        if (auto r = deser.template read<bool>(); r)
            response.truncated = r.value();
        else
            return r.error();
        return response;
    }
};

struct GraphSymbolLookupResponse {
    std::string symbol;
    std::vector<GraphExploreSymbol> matches;
    std::vector<GraphExploreSnippet> snippets;
    std::vector<GraphExploreRelation> trail;
    std::vector<std::string> warnings;
    bool ambiguous{false};
    bool truncated{false};

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << symbol << static_cast<uint32_t>(matches.size());
        for (const auto& match : matches) {
            match.serialize(ser);
        }
        ser << static_cast<uint32_t>(snippets.size());
        for (const auto& snippet : snippets) {
            snippet.serialize(ser);
        }
        ser << static_cast<uint32_t>(trail.size());
        for (const auto& relation : trail) {
            relation.serialize(ser);
        }
        ser << warnings << ambiguous << truncated;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<GraphSymbolLookupResponse> deserialize(Deserializer& deser) {
        GraphSymbolLookupResponse response;
        if (auto r = deser.readString(); r)
            response.symbol = std::move(r.value());
        else
            return r.error();
        if (auto cnt = deser.template read<uint32_t>(); cnt) {
            response.matches.reserve(cnt.value());
            for (uint32_t i = 0; i < cnt.value(); ++i) {
                auto match = GraphExploreSymbol::deserialize(deser);
                if (!match)
                    return match.error();
                response.matches.push_back(std::move(match.value()));
            }
        } else
            return cnt.error();
        if (auto cnt = deser.template read<uint32_t>(); cnt) {
            response.snippets.reserve(cnt.value());
            for (uint32_t i = 0; i < cnt.value(); ++i) {
                auto snippet = GraphExploreSnippet::deserialize(deser);
                if (!snippet)
                    return snippet.error();
                response.snippets.push_back(std::move(snippet.value()));
            }
        } else
            return cnt.error();
        if (auto cnt = deser.template read<uint32_t>(); cnt) {
            response.trail.reserve(cnt.value());
            for (uint32_t i = 0; i < cnt.value(); ++i) {
                auto relation = GraphExploreRelation::deserialize(deser);
                if (!relation)
                    return relation.error();
                response.trail.push_back(std::move(relation.value()));
            }
        } else
            return cnt.error();
        if (auto r = deser.readStringVector(); r)
            response.warnings = std::move(r.value());
        else
            return r.error();
        if (auto r = deser.template read<bool>(); r)
            response.ambiguous = r.value();
        else
            return r.error();
        if (auto r = deser.template read<bool>(); r)
            response.truncated = r.value();
        else
            return r.error();
        return response;
    }
};

struct GraphTraceResponse {
    std::string from;
    std::string to;
    std::vector<GraphExploreRelation> path;
    std::vector<GraphExploreSnippet> snippets;
    std::vector<std::string> warnings;
    bool found{false};
    bool truncated{false};

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << from << to << static_cast<uint32_t>(path.size());
        for (const auto& relation : path) {
            relation.serialize(ser);
        }
        ser << static_cast<uint32_t>(snippets.size());
        for (const auto& snippet : snippets) {
            snippet.serialize(ser);
        }
        ser << warnings << found << truncated;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<GraphTraceResponse> deserialize(Deserializer& deser) {
        GraphTraceResponse response;
        if (auto r = deser.readString(); r)
            response.from = std::move(r.value());
        else
            return r.error();
        if (auto r = deser.readString(); r)
            response.to = std::move(r.value());
        else
            return r.error();
        if (auto cnt = deser.template read<uint32_t>(); cnt) {
            response.path.reserve(cnt.value());
            for (uint32_t i = 0; i < cnt.value(); ++i) {
                auto relation = GraphExploreRelation::deserialize(deser);
                if (!relation)
                    return relation.error();
                response.path.push_back(std::move(relation.value()));
            }
        } else
            return cnt.error();
        if (auto cnt = deser.template read<uint32_t>(); cnt) {
            response.snippets.reserve(cnt.value());
            for (uint32_t i = 0; i < cnt.value(); ++i) {
                auto snippet = GraphExploreSnippet::deserialize(deser);
                if (!snippet)
                    return snippet.error();
                response.snippets.push_back(std::move(snippet.value()));
            }
        } else
            return cnt.error();
        if (auto r = deser.readStringVector(); r)
            response.warnings = std::move(r.value());
        else
            return r.error();
        if (auto r = deser.template read<bool>(); r)
            response.found = r.value();
        else
            return r.error();
        if (auto r = deser.template read<bool>(); r)
            response.truncated = r.value();
        else
            return r.error();
        return response;
    }
};

struct GraphImpactResponse {
    std::string symbol;
    std::vector<GraphExploreSymbol> affectedSymbols;
    std::vector<GraphExploreRelation> relationships;
    std::vector<std::string> warnings;
    bool truncated{false};

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << symbol << static_cast<uint32_t>(affectedSymbols.size());
        for (const auto& affected : affectedSymbols) {
            affected.serialize(ser);
        }
        ser << static_cast<uint32_t>(relationships.size());
        for (const auto& relation : relationships) {
            relation.serialize(ser);
        }
        ser << warnings << truncated;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<GraphImpactResponse> deserialize(Deserializer& deser) {
        GraphImpactResponse response;
        if (auto r = deser.readString(); r)
            response.symbol = std::move(r.value());
        else
            return r.error();
        if (auto cnt = deser.template read<uint32_t>(); cnt) {
            response.affectedSymbols.reserve(cnt.value());
            for (uint32_t i = 0; i < cnt.value(); ++i) {
                auto affected = GraphExploreSymbol::deserialize(deser);
                if (!affected)
                    return affected.error();
                response.affectedSymbols.push_back(std::move(affected.value()));
            }
        } else
            return cnt.error();
        if (auto cnt = deser.template read<uint32_t>(); cnt) {
            response.relationships.reserve(cnt.value());
            for (uint32_t i = 0; i < cnt.value(); ++i) {
                auto relation = GraphExploreRelation::deserialize(deser);
                if (!relation)
                    return relation.error();
                response.relationships.push_back(std::move(relation.value()));
            }
        } else
            return cnt.error();
        if (auto r = deser.readStringVector(); r)
            response.warnings = std::move(r.value());
        else
            return r.error();
        if (auto r = deser.template read<bool>(); r)
            response.truncated = r.value();
        else
            return r.error();
        return response;
    }
};

struct GraphAffectedTestsResponse {
    std::vector<std::string> changedFiles;
    std::vector<std::string> affectedTests;
    std::vector<GraphExploreRelation> relationships;
    std::vector<std::string> warnings;
    bool truncated{false};

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << changedFiles << affectedTests << static_cast<uint32_t>(relationships.size());
        for (const auto& relation : relationships) {
            relation.serialize(ser);
        }
        ser << warnings << truncated;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<GraphAffectedTestsResponse> deserialize(Deserializer& deser) {
        GraphAffectedTestsResponse response;
        if (auto r = deser.readStringVector(); r)
            response.changedFiles = std::move(r.value());
        else
            return r.error();
        if (auto r = deser.readStringVector(); r)
            response.affectedTests = std::move(r.value());
        else
            return r.error();
        if (auto cnt = deser.template read<uint32_t>(); cnt) {
            response.relationships.reserve(cnt.value());
            for (uint32_t i = 0; i < cnt.value(); ++i) {
                auto relation = GraphExploreRelation::deserialize(deser);
                if (!relation)
                    return relation.error();
                response.relationships.push_back(std::move(relation.value()));
            }
        } else
            return cnt.error();
        if (auto r = deser.readStringVector(); r)
            response.warnings = std::move(r.value());
        else
            return r.error();
        if (auto r = deser.template read<bool>(); r)
            response.truncated = r.value();
        else
            return r.error();
        return response;
    }
};

struct PathHistoryEntry {
    std::string path;
    std::string snapshotId;
    std::string blobHash;
    std::string changeType;

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << path << snapshotId << blobHash << changeType;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<PathHistoryEntry> deserialize(Deserializer& deser) {
        PathHistoryEntry entry;

        if (auto r = deser.readString(); r)
            entry.path = std::move(r.value());
        else
            return r.error();

        if (auto r = deser.readString(); r)
            entry.snapshotId = std::move(r.value());
        else
            return r.error();

        if (auto r = deser.readString(); r)
            entry.blobHash = std::move(r.value());
        else
            return r.error();

        if (auto r = deser.readString(); r)
            entry.changeType = std::move(r.value());
        else
            return r.error();

        return entry;
    }
};

struct GraphPathHistoryResponse {
    std::string queryPath;
    std::vector<PathHistoryEntry> history;
    bool hasMore{false};

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << queryPath;
        ser << static_cast<uint32_t>(history.size());
        for (const auto& entry : history) {
            entry.serialize(ser);
        }
        ser << hasMore;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<GraphPathHistoryResponse> deserialize(Deserializer& deser) {
        GraphPathHistoryResponse res;

        if (auto r = deser.readString(); r)
            res.queryPath = std::move(r.value());
        else
            return r.error();

        if (auto cnt = deser.template read<uint32_t>(); cnt) {
            res.history.reserve(cnt.value());
            for (uint32_t i = 0; i < cnt.value(); ++i) {
                auto entry = PathHistoryEntry::deserialize(deser);
                if (!entry)
                    return entry.error();
                res.history.push_back(std::move(entry.value()));
            }
        } else
            return cnt.error();

        if (auto r = deser.template read<bool>(); r)
            res.hasMore = r.value();
        else
            return r.error();

        return res;
    }
};

// ============================================================================
// Graph Maintenance Responses (PBI-009 Phase 4.3)
// ============================================================================

struct GraphRepairResponse {
    uint64_t nodesCreated{0};
    uint64_t nodesUpdated{0};
    uint64_t edgesCreated{0};
    uint64_t errors{0};
    std::vector<std::string> issues;
    bool dryRun{false};

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << nodesCreated << nodesUpdated << edgesCreated << errors;
        ser << static_cast<uint32_t>(issues.size());
        for (const auto& issue : issues) {
            ser << issue;
        }
        ser << dryRun;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<GraphRepairResponse> deserialize(Deserializer& deser) {
        GraphRepairResponse res;

        if (auto r = deser.template read<uint64_t>(); r)
            res.nodesCreated = r.value();
        else
            return r.error();

        if (auto r = deser.template read<uint64_t>(); r)
            res.nodesUpdated = r.value();
        else
            return r.error();

        if (auto r = deser.template read<uint64_t>(); r)
            res.edgesCreated = r.value();
        else
            return r.error();

        if (auto r = deser.template read<uint64_t>(); r)
            res.errors = r.value();
        else
            return r.error();

        if (auto cnt = deser.template read<uint32_t>(); cnt) {
            res.issues.reserve(cnt.value());
            for (uint32_t i = 0; i < cnt.value(); ++i) {
                auto issue = deser.readString();
                if (!issue)
                    return issue.error();
                res.issues.push_back(std::move(issue.value()));
            }
        } else
            return cnt.error();

        if (auto r = deser.template read<bool>(); r)
            res.dryRun = r.value();
        else
            return r.error();

        return res;
    }
};

struct GraphValidateResponse {
    uint64_t totalNodes{0};
    uint64_t totalEdges{0};
    uint64_t orphanedNodes{0};
    uint64_t unreachableNodes{0};
    std::vector<std::string> issues;

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << totalNodes << totalEdges << orphanedNodes << unreachableNodes;
        ser << static_cast<uint32_t>(issues.size());
        for (const auto& issue : issues) {
            ser << issue;
        }
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<GraphValidateResponse> deserialize(Deserializer& deser) {
        GraphValidateResponse res;

        if (auto r = deser.template read<uint64_t>(); r)
            res.totalNodes = r.value();
        else
            return r.error();

        if (auto r = deser.template read<uint64_t>(); r)
            res.totalEdges = r.value();
        else
            return r.error();

        if (auto r = deser.template read<uint64_t>(); r)
            res.orphanedNodes = r.value();
        else
            return r.error();

        if (auto r = deser.template read<uint64_t>(); r)
            res.unreachableNodes = r.value();
        else
            return r.error();

        if (auto cnt = deser.template read<uint32_t>(); cnt) {
            res.issues.reserve(cnt.value());
            for (uint32_t i = 0; i < cnt.value(); ++i) {
                auto issue = deser.readString();
                if (!issue)
                    return issue.error();
                res.issues.push_back(std::move(issue.value()));
            }
        } else
            return cnt.error();

        return res;
    }
};

/**
 * Response for KG ingest request (PBI-093 Phase 2).
 */
struct KgIngestResponse {
    uint64_t nodesInserted{0};
    uint64_t nodesSkipped{0};
    uint64_t edgesInserted{0};
    uint64_t edgesSkipped{0};
    uint64_t aliasesInserted{0};
    uint64_t aliasesSkipped{0};
    std::vector<std::string> errors; // Non-fatal errors/warnings
    bool success{true};

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << nodesInserted << nodesSkipped << edgesInserted << edgesSkipped << aliasesInserted
            << aliasesSkipped;
        ser << static_cast<uint32_t>(errors.size());
        for (const auto& err : errors) {
            ser << err;
        }
        ser << success;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<KgIngestResponse> deserialize(Deserializer& deser) {
        KgIngestResponse res;

        if (auto r = deser.template read<uint64_t>(); r)
            res.nodesInserted = r.value();
        else
            return r.error();

        if (auto r = deser.template read<uint64_t>(); r)
            res.nodesSkipped = r.value();
        else
            return r.error();

        if (auto r = deser.template read<uint64_t>(); r)
            res.edgesInserted = r.value();
        else
            return r.error();

        if (auto r = deser.template read<uint64_t>(); r)
            res.edgesSkipped = r.value();
        else
            return r.error();

        if (auto r = deser.template read<uint64_t>(); r)
            res.aliasesInserted = r.value();
        else
            return r.error();

        if (auto r = deser.template read<uint64_t>(); r)
            res.aliasesSkipped = r.value();
        else
            return r.error();

        if (auto cnt = deser.template read<uint32_t>(); cnt) {
            res.errors.reserve(cnt.value());
            for (uint32_t i = 0; i < cnt.value(); ++i) {
                auto err = deser.readString();
                if (!err)
                    return err.error();
                res.errors.push_back(std::move(err.value()));
            }
        } else
            return cnt.error();

        if (auto r = deser.template read<bool>(); r)
            res.success = r.value();
        else
            return r.error();

        return res;
    }
};

// ============================================================================
// Metadata Value Counts Response (generic metadata query for MCP client mode)
// ============================================================================

struct MetadataValueCountsResponse {
    // Map: key -> list of {value, count}
    std::unordered_map<std::string, std::vector<std::pair<std::string, size_t>>> valueCounts;

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << static_cast<uint32_t>(valueCounts.size());
        for (const auto& [key, values] : valueCounts) {
            ser << key;
            ser << static_cast<uint32_t>(values.size());
            for (const auto& [value, count] : values) {
                ser << value << static_cast<uint64_t>(count);
            }
        }
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<MetadataValueCountsResponse> deserialize(Deserializer& deser) {
        MetadataValueCountsResponse res;

        auto mapSize = deser.template read<uint32_t>();
        if (!mapSize)
            return mapSize.error();

        for (uint32_t i = 0; i < mapSize.value(); ++i) {
            auto keyResult = deser.readString();
            if (!keyResult)
                return keyResult.error();

            auto valuesSize = deser.template read<uint32_t>();
            if (!valuesSize)
                return valuesSize.error();

            std::vector<std::pair<std::string, size_t>> values;
            values.reserve(valuesSize.value());

            for (uint32_t j = 0; j < valuesSize.value(); ++j) {
                auto value = deser.readString();
                if (!value)
                    return value.error();

                auto count = deser.template read<uint64_t>();
                if (!count)
                    return count.error();

                values.emplace_back(std::move(value.value()), static_cast<size_t>(count.value()));
            }

            res.valueCounts[std::move(keyResult.value())] = std::move(values);
        }

        return res;
    }
};

// Plugin responses
struct PluginRecord {
    std::string name;
    std::string version;
    uint32_t abiVersion{0};
    std::string path;
    std::string manifestJson;
    std::vector<std::string> interfaces;

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << name << version << abiVersion << path << manifestJson << interfaces;
    }
    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<PluginRecord> deserialize(Deserializer& d) {
        PluginRecord r;
        if (auto a = d.readString(); a)
            r.name = std::move(a.value());
        else
            return a.error();
        if (auto b = d.readString(); b)
            r.version = std::move(b.value());
        else
            return b.error();
        if (auto c = d.template read<uint32_t>(); c)
            r.abiVersion = c.value();
        else
            return c.error();
        if (auto p = d.readString(); p)
            r.path = std::move(p.value());
        else
            return p.error();
        if (auto m = d.readString(); m)
            r.manifestJson = std::move(m.value());
        else
            return m.error();
        if (auto iv = d.readStringVector(); iv)
            r.interfaces = std::move(iv.value());
        else
            return iv.error();
        return r;
    }
};

struct PluginScanResponse {
    std::vector<PluginRecord> plugins;
    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << static_cast<uint32_t>(plugins.size());
        for (const auto& pr : plugins) {
            pr.serialize(ser);
        }
    }
    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<PluginScanResponse> deserialize(Deserializer& d) {
        PluginScanResponse r;
        auto cnt = d.template read<uint32_t>();
        if (!cnt)
            return cnt.error();
        r.plugins.reserve(cnt.value());
        for (uint32_t i = 0; i < cnt.value(); ++i) {
            auto rec = PluginRecord::deserialize(d);
            if (!rec)
                return rec.error();
            r.plugins.push_back(std::move(rec.value()));
        }
        return r;
    }
};

struct PluginLoadResponse {
    bool loaded{false};
    std::string message;
    PluginRecord record;
    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << loaded << message;
        record.serialize(ser);
    }
    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<PluginLoadResponse> deserialize(Deserializer& d) {
        PluginLoadResponse r;
        if (auto ok = d.template read<bool>(); ok)
            r.loaded = ok.value();
        else
            return ok.error();
        if (auto msg = d.readString(); msg)
            r.message = std::move(msg.value());
        else
            return msg.error();
        auto rec = PluginRecord::deserialize(d);
        if (!rec)
            return rec.error();
        r.record = std::move(rec.value());
        return r;
    }
};

struct PluginTrustListResponse {
    std::vector<std::string> paths;
    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << paths;
    }
    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<PluginTrustListResponse> deserialize(Deserializer& d) {
        PluginTrustListResponse r;
        if (auto v = d.readStringVector(); v)
            r.paths = std::move(v.value());
        else
            return v.error();
        return r;
    }
};

struct DownloadResponse {
    std::string hash;      // Content hash of downloaded file
    std::string localPath; // Local path where file was stored
    std::string url;       // Original URL
    std::string jobId;     // Daemon-assigned job id
    std::string state;     // queued|running|completed|failed
    uint64_t createdAtMs{0};
    uint64_t updatedAtMs{0};
    size_t size = 0;      // File size in bytes
    bool success = false; // Download success status
    std::string error;    // Error message if failed

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << hash << localPath << url << jobId << state << createdAtMs << updatedAtMs
            << static_cast<uint64_t>(size) << success << error;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<DownloadResponse> deserialize(Deserializer& deser) {
        DownloadResponse res;
        auto h = deser.readString();
        if (!h)
            return h.error();
        res.hash = std::move(h.value());
        auto lp = deser.readString();
        if (!lp)
            return lp.error();
        res.localPath = std::move(lp.value());
        auto u = deser.readString();
        if (!u)
            return u.error();
        res.url = std::move(u.value());
        auto jid = deser.readString();
        if (!jid)
            return jid.error();
        res.jobId = std::move(jid.value());
        auto st = deser.readString();
        if (!st)
            return st.error();
        res.state = std::move(st.value());
        auto created = deser.template read<uint64_t>();
        if (!created)
            return created.error();
        res.createdAtMs = created.value();
        auto updated = deser.template read<uint64_t>();
        if (!updated)
            return updated.error();
        res.updatedAtMs = updated.value();
        auto s = deser.template read<uint64_t>();
        if (!s)
            return s.error();
        res.size = s.value();
        auto sc = deser.template read<bool>();
        if (!sc)
            return sc.error();
        res.success = sc.value();
        auto e = deser.readString();
        if (!e)
            return e.error();
        res.error = std::move(e.value());
        return res;
    }
};

struct ListDownloadJobsResponse {
    std::vector<DownloadResponse> jobs;

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << jobs;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<ListDownloadJobsResponse> deserialize(Deserializer& deser) {
        ListDownloadJobsResponse res;
        auto items = deser.template readVector<DownloadResponse>();
        if (!items)
            return items.error();
        res.jobs = std::move(items.value());
        return res;
    }
};

struct PrepareSessionResponse {
    uint64_t warmedCount{0};
    std::string message;

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << warmedCount << message;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<PrepareSessionResponse> deserialize(Deserializer& deser) {
        PrepareSessionResponse res;
        auto wc = deser.template read<uint64_t>();
        if (!wc)
            return wc.error();
        res.warmedCount = wc.value();
        auto msg = deser.readString();
        if (!msg)
            return msg.error();
        res.message = std::move(msg.value());
        return res;
    }
};

struct CatResponse {
    std::string hash;
    std::string name;
    std::string content;
    uint64_t size = 0;
    bool hasContent = false;

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << hash << name << content << size << hasContent;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<CatResponse> deserialize(Deserializer& deser) {
        CatResponse r{};
        auto h = deser.readString();
        if (!h)
            return h.error();
        r.hash = std::move(h.value());
        auto n = deser.readString();
        if (!n)
            return n.error();
        r.name = std::move(n.value());
        auto c = deser.readString();
        if (!c)
            return c.error();
        r.content = std::move(c.value());
        auto s = deser.template read<uint64_t>();
        if (!s)
            return s.error();
        r.size = s.value();
        auto hc = deser.template read<bool>();
        if (hc)
            r.hasContent = hc.value();
        return r;
    }
};

struct ListSessionsResponse {
    std::vector<std::string> session_names;
    std::string current_session;

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << session_names << current_session;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<ListSessionsResponse> deserialize(Deserializer& deser) {
        ListSessionsResponse res;
        auto sn = deser.readStringVector();
        if (!sn)
            return sn.error();
        res.session_names = std::move(sn.value());
        auto cs = deser.readString();
        if (!cs)
            return cs.error();
        res.current_session = std::move(cs.value());
        return res;
    }
};

struct TreeChangeEntry {
    std::string changeType;
    std::string path;
    std::string oldPath;
    std::string hash;
    std::string oldHash;
    uint64_t size = 0;
    uint64_t oldSize = 0;
    std::string contentDeltaHash;

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << changeType << path << oldPath << hash << oldHash << size << oldSize
            << contentDeltaHash;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<TreeChangeEntry> deserialize(Deserializer& deser) {
        TreeChangeEntry entry;
        auto ct = deser.readString();
        if (!ct)
            return ct.error();
        entry.changeType = std::move(ct.value());

        auto p = deser.readString();
        if (!p)
            return p.error();
        entry.path = std::move(p.value());

        auto op = deser.readString();
        if (!op)
            return op.error();
        entry.oldPath = std::move(op.value());

        auto h = deser.readString();
        if (!h)
            return h.error();
        entry.hash = std::move(h.value());

        auto oh = deser.readString();
        if (!oh)
            return oh.error();
        entry.oldHash = std::move(oh.value());

        auto sz = deser.template read<uint64_t>();
        if (!sz)
            return sz.error();
        entry.size = sz.value();

        auto osz = deser.template read<uint64_t>();
        if (!osz)
            return osz.error();
        entry.oldSize = osz.value();

        auto cdh = deser.readString();
        if (!cdh)
            return cdh.error();
        entry.contentDeltaHash = std::move(cdh.value());

        return entry;
    }
};

struct ListTreeDiffResponse {
    std::vector<TreeChangeEntry> changes;
    uint64_t totalCount = 0;

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << static_cast<uint32_t>(changes.size());
        for (const auto& entry : changes) {
            entry.serialize(ser);
        }
        ser << totalCount;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<ListTreeDiffResponse> deserialize(Deserializer& deser) {
        ListTreeDiffResponse resp;
        auto countRes = deser.template read<uint32_t>();
        if (!countRes)
            return countRes.error();
        auto count = countRes.value();

        resp.changes.reserve(count);
        for (uint32_t i = 0; i < count; ++i) {
            auto entry = TreeChangeEntry::deserialize(deser);
            if (!entry)
                return entry.error();
            resp.changes.push_back(std::move(entry.value()));
        }

        auto total = deser.template read<uint64_t>();
        if (!total)
            return total.error();
        resp.totalCount = total.value();

        return resp;
    }
};

/**
 * @brief Per-operation result embedded in the final RepairResponse.
 */
struct RepairOperationResult {
    std::string operation;
    uint64_t processed{0};
    uint64_t succeeded{0};
    uint64_t failed{0};
    uint64_t skipped{0};
    std::string message;

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << operation << processed << succeeded << failed << skipped << message;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<RepairOperationResult> deserialize(Deserializer& deser) {
        RepairOperationResult r;
        if (auto v = deser.readString(); v)
            r.operation = std::move(v.value());
        else
            return v.error();
        if (auto v = deser.template read<uint64_t>(); v)
            r.processed = v.value();
        else
            return v.error();
        if (auto v = deser.template read<uint64_t>(); v)
            r.succeeded = v.value();
        else
            return v.error();
        if (auto v = deser.template read<uint64_t>(); v)
            r.failed = v.value();
        else
            return v.error();
        if (auto v = deser.template read<uint64_t>(); v)
            r.skipped = v.value();
        else
            return v.error();
        if (auto v = deser.readString(); v)
            r.message = std::move(v.value());
        else
            return v.error();
        return r;
    }
};

/**
 * @brief Streaming progress event emitted during repair.
 */
struct RepairEvent {
    std::string phase;     // scanning | repairing | completed | error
    std::string operation; // orphans | mime | fts5 | embeddings | stuck_docs | ...
    uint64_t processed{0};
    uint64_t total{0};
    uint64_t succeeded{0};
    uint64_t failed{0};
    uint64_t skipped{0};
    std::string message;

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << phase << operation << processed << total << succeeded << failed << skipped
            << message;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<RepairEvent> deserialize(Deserializer& deser) {
        RepairEvent ev;
        if (auto v = deser.readString(); v)
            ev.phase = std::move(v.value());
        else
            return v.error();
        if (auto v = deser.readString(); v)
            ev.operation = std::move(v.value());
        else
            return v.error();
        if (auto v = deser.template read<uint64_t>(); v)
            ev.processed = v.value();
        else
            return v.error();
        if (auto v = deser.template read<uint64_t>(); v)
            ev.total = v.value();
        else
            return v.error();
        if (auto v = deser.template read<uint64_t>(); v)
            ev.succeeded = v.value();
        else
            return v.error();
        if (auto v = deser.template read<uint64_t>(); v)
            ev.failed = v.value();
        else
            return v.error();
        if (auto v = deser.template read<uint64_t>(); v)
            ev.skipped = v.value();
        else
            return v.error();
        if (auto v = deser.readString(); v)
            ev.message = std::move(v.value());
        else
            return v.error();
        return ev;
    }
};

/**
 * @brief Final response after all requested repair operations complete.
 */
struct RepairResponse {
    bool success{false};
    uint64_t totalOperations{0};
    uint64_t totalSucceeded{0};
    uint64_t totalFailed{0};
    uint64_t totalSkipped{0};
    std::vector<std::string> errors;
    std::vector<RepairOperationResult> operationResults;

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << success << totalOperations << totalSucceeded << totalFailed << totalSkipped;
        ser << static_cast<uint32_t>(errors.size());
        for (const auto& e : errors)
            ser << e;
        ser << static_cast<uint32_t>(operationResults.size());
        for (const auto& r : operationResults)
            r.serialize(ser);
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<RepairResponse> deserialize(Deserializer& deser) {
        RepairResponse resp;
        if (auto v = deser.template read<bool>(); v)
            resp.success = v.value();
        else
            return v.error();
        if (auto v = deser.template read<uint64_t>(); v)
            resp.totalOperations = v.value();
        else
            return v.error();
        if (auto v = deser.template read<uint64_t>(); v)
            resp.totalSucceeded = v.value();
        else
            return v.error();
        if (auto v = deser.template read<uint64_t>(); v)
            resp.totalFailed = v.value();
        else
            return v.error();
        if (auto v = deser.template read<uint64_t>(); v)
            resp.totalSkipped = v.value();
        else
            return v.error();

        if (auto n = deser.template read<uint32_t>(); n) {
            for (uint32_t i = 0; i < n.value(); ++i) {
                if (auto s = deser.readString(); s)
                    resp.errors.push_back(std::move(s.value()));
                else
                    return s.error();
            }
        } else {
            return n.error();
        }

        if (auto n = deser.template read<uint32_t>(); n) {
            for (uint32_t i = 0; i < n.value(); ++i) {
                auto r = RepairOperationResult::deserialize(deser);
                if (!r)
                    return r.error();
                resp.operationResults.push_back(std::move(r.value()));
            }
        } else {
            return n.error();
        }

        return resp;
    }
};

// Forward declaration for batch response type
struct BatchResponse;

// Variant type for all responses
using Response =
    std::variant<SearchResponse, AddResponse, GetResponse, GetInitResponse, GetChunkResponse,
                 StatusResponse, SuccessResponse, ErrorResponse, PongResponse, EmbeddingResponse,
                 BatchEmbeddingResponse, ModelLoadResponse, ModelStatusResponse, ListResponse,
                 AddDocumentResponse, GrepResponse, UpdateDocumentResponse, GetStatsResponse,
                 DownloadResponse, ListDownloadJobsResponse, DeleteResponse, PrepareSessionResponse,
                 EmbedDocumentsResponse, PluginScanResponse, PluginLoadResponse,
                 PluginTrustListResponse, CatResponse, ListSessionsResponse, ListTreeDiffResponse,
                 FileHistoryResponse, PruneResponse, ListSnapshotsResponse,
                 RestoreCollectionResponse, RestoreSnapshotResponse, GraphQueryResponse,
                 GraphExploreResponse, GraphSymbolLookupResponse, GraphTraceResponse,
                 GraphImpactResponse, GraphAffectedTestsResponse, GraphPathHistoryResponse,
                 GraphRepairResponse, GraphValidateResponse, KgIngestResponse,
                 MetadataValueCountsResponse,
                 // Batch response (Track B)
                 BatchResponse,
                 // Streaming events (progress/heartbeats)
                 EmbeddingEvent, ModelLoadEvent,
                 // Repair service responses
                 RepairResponse, RepairEvent>;

} // namespace yams::daemon
