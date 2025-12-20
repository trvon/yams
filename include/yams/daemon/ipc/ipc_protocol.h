#pragma once

#include <yams/core/types.h>

#include <chrono>
#include <concepts>
#include <cstring>
#include <filesystem>
#include <map>
#include <optional>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <variant>
#include <vector>

namespace yams::daemon {

// Forward declarations for serialization
class BinarySerializer;
class BinaryDeserializer;

// Concepts to constrain serialize/deserialize templates
template <typename T>
concept IsSerializer = requires(T& t, const std::string& str, uint32_t val) {
    t << str;
    t << val;
};

template <typename T>
concept IsDeserializer = requires(T& t) {
    { t.readString() } -> std::same_as<Result<std::string>>;
    { t.template read<uint32_t>() } -> std::same_as<Result<uint32_t>>;
};

// ============================================================================
// Request Types
// ============================================================================

struct SearchRequest {
    std::string query;
    size_t limit = 20;
    bool fuzzy = false;
    bool literalText = false;
    double similarity = 0.7;
    std::chrono::milliseconds timeout{5000};

    // Additional fields for feature parity
    std::string searchType = "keyword"; // Search type (keyword/semantic/hybrid)
    bool pathsOnly = false;             // Output only file paths
    bool showHash = false;              // Show document hashes
    bool verbose = false;               // Show detailed info
    bool jsonOutput = false;            // Output as JSON
    bool showLineNumbers = false;       // Show line numbers
    int afterContext = 0;               // Lines after match
    int beforeContext = 0;              // Lines before match
    int context = 0;                    // Lines before and after
    std::string hashQuery;              // Search by file hash

    // Engine-level filtering (parity with app::services::SearchRequest)
    std::string pathPattern; // Glob-like filename/path filter (legacy, single pattern)
    std::vector<std::string> pathPatterns; // Multiple glob patterns (preferred over pathPattern)
    std::vector<std::string> tags;         // Filter by tags (presence-based)
    bool matchAllTags = false;             // Require all specified tags
    std::string extension;                 // File extension filter
    std::string mimeType;                  // MIME type filter
    std::string fileType;                  // High-level file type
    bool textOnly{false};                  // Text-only filter
    bool binaryOnly{false};                // Binary-only filter
    // Time filters
    std::string createdAfter;
    std::string createdBefore;
    std::string modifiedAfter;
    std::string modifiedBefore;
    std::string indexedAfter;
    std::string indexedBefore;

    int vectorStageTimeoutMs{0};
    int keywordStageTimeoutMs{0};
    int snippetHydrationTimeoutMs{0};

    // Session scoping (controls hot/cold path behavior)
    bool useSession = false;
    std::string sessionName;
    bool globalSearch = false; // Session-isolated memory (PBI-082): bypass session isolation

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << query << static_cast<uint32_t>(limit) << fuzzy << literalText << similarity
            << timeout << searchType << pathsOnly << showHash << verbose << jsonOutput
            << showLineNumbers << static_cast<int32_t>(afterContext)
            << static_cast<int32_t>(beforeContext) << static_cast<int32_t>(context) << hashQuery
            << pathPattern << pathPatterns << tags << matchAllTags << extension << mimeType
            << fileType << textOnly << binaryOnly << createdAfter << createdBefore << modifiedAfter
            << modifiedBefore << indexedAfter << indexedBefore
            << static_cast<int32_t>(vectorStageTimeoutMs)
            << static_cast<int32_t>(keywordStageTimeoutMs)
            << static_cast<int32_t>(snippetHydrationTimeoutMs) << useSession << sessionName
            << globalSearch;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<SearchRequest> deserialize(Deserializer& deser) {
        SearchRequest req;

        auto queryResult = deser.readString();
        if (!queryResult)
            return queryResult.error();
        req.query = std::move(queryResult.value());

        auto limitResult = deser.template read<uint32_t>();
        if (!limitResult)
            return limitResult.error();
        req.limit = limitResult.value();

        auto fuzzyResult = deser.template read<bool>();
        if (!fuzzyResult)
            return fuzzyResult.error();
        req.fuzzy = fuzzyResult.value();

        auto literalResult = deser.template read<bool>();
        if (!literalResult)
            return literalResult.error();
        req.literalText = literalResult.value();

        auto similarityResult = deser.template read<double>();
        if (!similarityResult)
            return similarityResult.error();
        req.similarity = similarityResult.value();

        auto timeoutResult = deser.template readDuration<std::chrono::milliseconds>();
        if (!timeoutResult)
            return timeoutResult.error();
        req.timeout = timeoutResult.value();

        // Deserialize new fields
        auto st = deser.readString();
        if (!st)
            return st.error();
        req.searchType = std::move(st.value());

        auto po = deser.template read<bool>();
        if (!po)
            return po.error();
        req.pathsOnly = po.value();

        auto sh = deser.template read<bool>();
        if (!sh)
            return sh.error();
        req.showHash = sh.value();

        auto v = deser.template read<bool>();
        if (!v)
            return v.error();
        req.verbose = v.value();

        auto jo = deser.template read<bool>();
        if (!jo)
            return jo.error();
        req.jsonOutput = jo.value();

        auto sln = deser.template read<bool>();
        if (!sln)
            return sln.error();
        req.showLineNumbers = sln.value();

        auto ac = deser.template read<int32_t>();
        if (!ac)
            return ac.error();
        req.afterContext = ac.value();

        auto bc = deser.template read<int32_t>();
        if (!bc)
            return bc.error();
        req.beforeContext = bc.value();

        auto c = deser.template read<int32_t>();
        if (!c)
            return c.error();
        req.context = c.value();

        auto hq = deser.readString();
        if (!hq)
            return hq.error();
        req.hashQuery = std::move(hq.value());

        // New fields (optional in older peers; keep order consistent with serialize)
        if (auto pp = deser.readString(); pp) {
            req.pathPattern = std::move(pp.value());
        } else {
            return pp.error();
        }
        if (auto pps = deser.readStringVector(); pps) {
            req.pathPatterns = std::move(pps.value());
        } else {
            return pps.error();
        }
        if (auto tg = deser.readStringVector(); tg) {
            req.tags = std::move(tg.value());
        } else {
            return tg.error();
        }
        if (auto mat = deser.template read<bool>(); mat) {
            req.matchAllTags = mat.value();
        } else {
            return mat.error();
        }
        if (auto ext = deser.readString(); ext) {
            req.extension = std::move(ext.value());
        } else {
            return ext.error();
        }
        if (auto mt = deser.readString(); mt) {
            req.mimeType = std::move(mt.value());
        } else {
            return mt.error();
        }
        if (auto ft = deser.readString(); ft) {
            req.fileType = std::move(ft.value());
        } else {
            return ft.error();
        }
        if (auto to = deser.template read<bool>(); to) {
            req.textOnly = to.value();
        } else {
            return to.error();
        }
        if (auto bo = deser.template read<bool>(); bo) {
            req.binaryOnly = bo.value();
        } else {
            return bo.error();
        }
        if (auto ca = deser.readString(); ca) {
            req.createdAfter = std::move(ca.value());
        } else {
            return ca.error();
        }
        if (auto cb = deser.readString(); cb) {
            req.createdBefore = std::move(cb.value());
        } else {
            return cb.error();
        }
        if (auto ma = deser.readString(); ma) {
            req.modifiedAfter = std::move(ma.value());
        } else {
            return ma.error();
        }
        if (auto mb = deser.readString(); mb) {
            req.modifiedBefore = std::move(mb.value());
        } else {
            return mb.error();
        }
        if (auto ia = deser.readString(); ia) {
            req.indexedAfter = std::move(ia.value());
        } else {
            return ia.error();
        }
        if (auto ib = deser.readString(); ib) {
            req.indexedBefore = std::move(ib.value());
        } else {
            return ib.error();
        }

        if (auto vst = deser.template read<int32_t>(); vst) {
            req.vectorStageTimeoutMs = vst.value();
        } else {
            return vst.error();
        }
        if (auto kst = deser.template read<int32_t>(); kst) {
            req.keywordStageTimeoutMs = kst.value();
        } else {
            return kst.error();
        }
        if (auto sht = deser.template read<int32_t>(); sht) {
            req.snippetHydrationTimeoutMs = sht.value();
        } else {
            return sht.error();
        }

        // Deserialize session fields
        if (auto us = deser.template read<bool>(); us) {
            req.useSession = us.value();
        }
        if (auto sn = deser.readString(); sn) {
            req.sessionName = std::move(sn.value());
        }
        // Session-isolated memory (PBI-082)
        if (auto gs = deser.template read<bool>(); gs) {
            req.globalSearch = gs.value();
        }

        return req;
    }
};

struct GetRequest {
    // Target selection (basic)
    std::string hash;
    std::string name;
    bool byName = false;

    // File type filters
    std::string fileType;    // "image", "document", "archive", etc.
    std::string mimeType;    // MIME type filter
    std::string extension;   // file extension filter
    bool binaryOnly = false; // get only binary files
    bool textOnly = false;   // get only text files

    // Time filters
    std::string createdAfter; // ISO 8601, relative, or natural language
    std::string createdBefore;
    std::string modifiedAfter;
    std::string modifiedBefore;
    std::string indexedAfter;
    std::string indexedBefore;

    // Selection options
    bool latest = false; // get most recent matching document
    bool oldest = false; // get oldest matching document

    // Output options
    std::string outputPath;      // output file path (empty = stdout)
    bool metadataOnly = false;   // return only metadata, no content
    uint64_t maxBytes = 0;       // max bytes to transfer (0 = unlimited)
    uint32_t chunkSize = 524288; // streaming chunk size in bytes (increased default)

    // Content options
    bool raw = false;     // output raw content without text extraction
    bool extract = false; // force text extraction even when piping

    // Knowledge graph options
    bool showGraph = false; // show related documents from knowledge graph
    int graphDepth = 1;     // depth of graph traversal (1-5)

    // Display options
    bool verbose = false;          // enable verbose output
    bool acceptCompressed = false; // prefer compressed payloads when supported

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        // Basic target selection
        ser << hash << name << byName;

        // File type filters
        ser << fileType << mimeType << extension << binaryOnly << textOnly;

        // Time filters
        ser << createdAfter << createdBefore << modifiedAfter << modifiedBefore << indexedAfter
            << indexedBefore;

        // Selection options
        ser << latest << oldest;

        // Output options
        ser << outputPath << metadataOnly << static_cast<uint64_t>(maxBytes)
            << static_cast<uint32_t>(chunkSize);

        // Content options
        ser << raw << extract;

        // Knowledge graph options
        ser << showGraph << static_cast<int32_t>(graphDepth);

        // Display options
        ser << verbose << acceptCompressed;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<GetRequest> deserialize(Deserializer& deser) {
        GetRequest req;

        // Basic target selection
        auto hashResult = deser.readString();
        if (!hashResult)
            return hashResult.error();
        req.hash = std::move(hashResult.value());

        auto nameResult = deser.readString();
        if (!nameResult)
            return nameResult.error();
        req.name = std::move(nameResult.value());

        auto byNameResult = deser.template read<bool>();
        if (!byNameResult)
            return byNameResult.error();
        req.byName = byNameResult.value();

        // File type filters
        auto fileTypeResult = deser.readString();
        if (!fileTypeResult)
            return fileTypeResult.error();
        req.fileType = std::move(fileTypeResult.value());

        auto mimeTypeResult = deser.readString();
        if (!mimeTypeResult)
            return mimeTypeResult.error();
        req.mimeType = std::move(mimeTypeResult.value());

        auto extensionResult = deser.readString();
        if (!extensionResult)
            return extensionResult.error();
        req.extension = std::move(extensionResult.value());

        auto binaryOnlyResult = deser.template read<bool>();
        if (!binaryOnlyResult)
            return binaryOnlyResult.error();
        req.binaryOnly = binaryOnlyResult.value();

        auto textOnlyResult = deser.template read<bool>();
        if (!textOnlyResult)
            return textOnlyResult.error();
        req.textOnly = textOnlyResult.value();

        // Time filters
        auto createdAfterResult = deser.readString();
        if (!createdAfterResult)
            return createdAfterResult.error();
        req.createdAfter = std::move(createdAfterResult.value());

        auto createdBeforeResult = deser.readString();
        if (!createdBeforeResult)
            return createdBeforeResult.error();
        req.createdBefore = std::move(createdBeforeResult.value());

        auto modifiedAfterResult = deser.readString();
        if (!modifiedAfterResult)
            return modifiedAfterResult.error();
        req.modifiedAfter = std::move(modifiedAfterResult.value());

        auto modifiedBeforeResult = deser.readString();
        if (!modifiedBeforeResult)
            return modifiedBeforeResult.error();
        req.modifiedBefore = std::move(modifiedBeforeResult.value());

        auto indexedAfterResult = deser.readString();
        if (!indexedAfterResult)
            return indexedAfterResult.error();
        req.indexedAfter = std::move(indexedAfterResult.value());

        auto indexedBeforeResult = deser.readString();
        if (!indexedBeforeResult)
            return indexedBeforeResult.error();
        req.indexedBefore = std::move(indexedBeforeResult.value());

        // Selection options
        auto latestResult = deser.template read<bool>();
        if (!latestResult)
            return latestResult.error();
        req.latest = latestResult.value();

        auto oldestResult = deser.template read<bool>();
        if (!oldestResult)
            return oldestResult.error();
        req.oldest = oldestResult.value();

        // Output options
        auto outputPathResult = deser.readString();
        if (!outputPathResult)
            return outputPathResult.error();
        req.outputPath = std::move(outputPathResult.value());

        auto metadataOnlyResult = deser.template read<bool>();
        if (!metadataOnlyResult)
            return metadataOnlyResult.error();
        req.metadataOnly = metadataOnlyResult.value();

        auto maxBytesResult = deser.template read<uint64_t>();
        if (!maxBytesResult)
            return maxBytesResult.error();
        req.maxBytes = maxBytesResult.value();

        auto chunkSizeResult = deser.template read<uint32_t>();
        if (!chunkSizeResult)
            return chunkSizeResult.error();
        req.chunkSize = chunkSizeResult.value();

        // Content options
        auto rawResult = deser.template read<bool>();
        if (!rawResult)
            return rawResult.error();
        req.raw = rawResult.value();

        auto extractResult = deser.template read<bool>();
        if (!extractResult)
            return extractResult.error();
        req.extract = extractResult.value();

        // Knowledge graph options
        auto showGraphResult = deser.template read<bool>();
        if (!showGraphResult)
            return showGraphResult.error();
        req.showGraph = showGraphResult.value();

        auto graphDepthResult = deser.template read<int32_t>();
        if (!graphDepthResult)
            return graphDepthResult.error();
        req.graphDepth = graphDepthResult.value();

        // Display options
        auto verboseResult = deser.template read<bool>();
        if (!verboseResult)
            return verboseResult.error();
        req.verbose = verboseResult.value();

        auto acceptCompressedResult = deser.template read<bool>();
        if (!acceptCompressedResult)
            return acceptCompressedResult.error();
        req.acceptCompressed = acceptCompressedResult.value();

        return req;
    }
};

// Chunked Get protocol (for large content)
struct GetInitRequest {
    std::string hash;                // if provided, used directly
    std::string name;                // optional path/name when byName = true
    bool byName = false;             // resolve by path/name when true
    bool metadataOnly = false;       // return only metadata (no content transfer)
    uint64_t maxBytes = 0;           // 0 = unlimited; otherwise cap bytes served
    uint32_t chunkSize = 512 * 1024; // hint for preferred chunk size (increased)

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << hash << name << byName << metadataOnly << static_cast<uint64_t>(maxBytes)
            << static_cast<uint32_t>(chunkSize);
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<GetInitRequest> deserialize(Deserializer& deser) {
        GetInitRequest req;
        auto h = deser.readString();
        if (!h)
            return h.error();
        req.hash = std::move(h.value());
        auto n = deser.readString();
        if (!n)
            return n.error();
        req.name = std::move(n.value());
        auto bn = deser.template read<bool>();
        if (!bn)
            return bn.error();
        req.byName = bn.value();
        auto mo = deser.template read<bool>();
        if (!mo)
            return mo.error();
        req.metadataOnly = mo.value();
        auto mb = deser.template read<uint64_t>();
        if (!mb)
            return mb.error();
        req.maxBytes = mb.value();
        auto cs = deser.template read<uint32_t>();
        if (!cs)
            return cs.error();
        req.chunkSize = cs.value();
        return req;
    }
};

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

struct GetChunkRequest {
    uint64_t transferId = 0;
    uint64_t offset = 0;
    uint32_t length = 0;

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << static_cast<uint64_t>(transferId) << static_cast<uint64_t>(offset)
            << static_cast<uint32_t>(length);
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<GetChunkRequest> deserialize(Deserializer& deser) {
        GetChunkRequest req;
        auto id = deser.template read<uint64_t>();
        if (!id)
            return id.error();
        req.transferId = id.value();
        auto off = deser.template read<uint64_t>();
        if (!off)
            return off.error();
        req.offset = off.value();
        auto len = deser.template read<uint32_t>();
        if (!len)
            return len.error();
        req.length = len.value();
        return req;
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

        return e;
    }
};

struct ListResponse {
    std::vector<ListEntry> items;
    uint64_t totalCount = 0;

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << static_cast<uint32_t>(items.size());
        for (const auto& it : items)
            it.serialize(ser);
        ser << static_cast<uint64_t>(totalCount);
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
        return r;
    }
};

struct GetEndRequest {
    uint64_t transferId = 0;

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << static_cast<uint64_t>(transferId);
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<GetEndRequest> deserialize(Deserializer& deser) {
        GetEndRequest req;
        auto id = deser.template read<uint64_t>();
        if (!id)
            return id.error();
        req.transferId = id.value();
        return req;
    }
};

struct DeleteRequest {
    // Single deletion options
    std::string hash;
    std::string name;

    // Batch deletion options
    std::vector<std::string> names;
    std::string pattern;   // glob pattern
    std::string directory; // directory path

    // Flags
    bool purge = false;     // backward compat (maps to force)
    bool force = false;     // skip confirmation
    bool dryRun = false;    // preview without deleting
    bool keepRefs = false;  // don't decrement references
    bool recursive = false; // for directory deletion
    bool verbose = false;

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << hash << purge;
        // Extended fields for enhanced protocol
        ser << name << names << pattern << directory;
        ser << force << dryRun << keepRefs << recursive << verbose;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<DeleteRequest> deserialize(Deserializer& deser) {
        DeleteRequest req;

        auto hashResult = deser.readString();
        if (!hashResult)
            return hashResult.error();
        req.hash = std::move(hashResult.value());

        auto purgeResult = deser.template read<bool>();
        if (!purgeResult)
            return purgeResult.error();
        req.purge = purgeResult.value();

        // Try to read extended fields (backward compatibility)
        auto nameResult = deser.readString();
        if (nameResult)
            req.name = std::move(nameResult.value());

        auto namesResult = deser.readStringVector();
        if (namesResult)
            req.names = std::move(namesResult.value());

        auto patternResult = deser.readString();
        if (patternResult)
            req.pattern = std::move(patternResult.value());

        auto directoryResult = deser.readString();
        if (directoryResult)
            req.directory = std::move(directoryResult.value());

        auto forceResult = deser.template read<bool>();
        if (forceResult)
            req.force = forceResult.value();

        auto dryRunResult = deser.template read<bool>();
        if (dryRunResult)
            req.dryRun = dryRunResult.value();

        auto keepRefsResult = deser.template read<bool>();
        if (keepRefsResult)
            req.keepRefs = keepRefsResult.value();

        auto recursiveResult = deser.template read<bool>();
        if (recursiveResult)
            req.recursive = recursiveResult.value();

        auto verboseResult = deser.template read<bool>();
        if (verboseResult)
            req.verbose = verboseResult.value();

        return req;
    }
};

struct ListRequest {
    // Order fields to minimize padding (clang-tidy: performance.Padding)
    // Large/aligned and string-like types first, then ints, then bools.

    // Basic pagination and sorting
    size_t limit = 20;

    // Tag filtering
    std::vector<std::string> tags; // requested tags filter

    // Format and display options
    std::string format = "table"; // "table" | "json" | "csv" | "minimal"
    std::string sortBy = "date";  // "name" | "size" | "date" | "hash"

    // File type filters
    std::string fileType;   // "image" | "document" | "archive" | "audio" | "video" | "text" |
                            // "executable" | "binary"
    std::string mimeType;   // MIME type filter
    std::string extensions; // comma-separated extensions

    // Time filters (ISO 8601, relative, or natural language)
    std::string createdAfter;
    std::string createdBefore;
    std::string modifiedAfter;
    std::string modifiedBefore;
    std::string indexedAfter;
    std::string indexedBefore;

    // Change tracking
    std::string sinceTime;
    std::string changeWindow = "24h";

    // Tag filtering (continued)
    std::string filterTags; // comma-separated tag filter

    // Name pattern filtering
    std::string namePattern; // glob pattern for file name/path matching

    // 32-bit integral fields
    int offset = 0;
    int recentCount = 0; // 0 means not set, show all
    int snippetLength = 50;

    // Booleans last
    bool recent = true; // backward compatibility
    bool reverse = false;
    bool verbose = false;
    bool showSnippets = true;
    bool showMetadata = false;
    bool showTags = true;
    bool groupBySession = false;
    bool noSnippets = false;
    bool pathsOnly = false; // Output only file paths
    bool binaryOnly = false;
    bool textOnly = false;
    bool showChanges = false;
    bool showDiffTags = false;
    bool showDeleted = false;
    bool matchAllTags = false; // require all tags vs any tag

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        // Basic options
        ser << static_cast<uint32_t>(limit) << static_cast<int32_t>(offset)
            << static_cast<int32_t>(recentCount) << recent;

        // Format and display
        ser << format << sortBy << reverse << verbose << showSnippets << showMetadata << showTags
            << groupBySession << static_cast<int32_t>(snippetLength) << noSnippets;

        // File type filters
        ser << fileType << mimeType << extensions << binaryOnly << textOnly;

        // Time filters
        ser << createdAfter << createdBefore << modifiedAfter << modifiedBefore << indexedAfter
            << indexedBefore;

        // Change tracking
        ser << showChanges << sinceTime << showDiffTags << showDeleted << changeWindow;

        // Tag filtering
        ser << tags << filterTags << matchAllTags;

        // Name pattern filtering
        ser << namePattern;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<ListRequest> deserialize(Deserializer& deser) {
        ListRequest req;

        // Basic options
        auto limitResult = deser.template read<uint32_t>();
        if (!limitResult)
            return limitResult.error();
        req.limit = limitResult.value();

        auto offsetResult = deser.template read<int32_t>();
        if (!offsetResult)
            return offsetResult.error();
        req.offset = offsetResult.value();

        auto recentCountResult = deser.template read<int32_t>();
        if (!recentCountResult)
            return recentCountResult.error();
        req.recentCount = recentCountResult.value();

        auto recentResult = deser.template read<bool>();
        if (!recentResult)
            return recentResult.error();
        req.recent = recentResult.value();

        // Format and display
        auto formatResult = deser.readString();
        if (!formatResult)
            return formatResult.error();
        req.format = std::move(formatResult.value());

        auto sortByResult = deser.readString();
        if (!sortByResult)
            return sortByResult.error();
        req.sortBy = std::move(sortByResult.value());

        auto reverseResult = deser.template read<bool>();
        if (!reverseResult)
            return reverseResult.error();
        req.reverse = reverseResult.value();

        auto verboseResult = deser.template read<bool>();
        if (!verboseResult)
            return verboseResult.error();
        req.verbose = verboseResult.value();

        auto showSnippetsResult = deser.template read<bool>();
        if (!showSnippetsResult)
            return showSnippetsResult.error();
        req.showSnippets = showSnippetsResult.value();

        auto showMetadataResult = deser.template read<bool>();
        if (!showMetadataResult)
            return showMetadataResult.error();
        req.showMetadata = showMetadataResult.value();

        auto showTagsResult = deser.template read<bool>();
        if (!showTagsResult)
            return showTagsResult.error();
        req.showTags = showTagsResult.value();

        auto groupBySessionResult = deser.template read<bool>();
        if (!groupBySessionResult)
            return groupBySessionResult.error();
        req.groupBySession = groupBySessionResult.value();

        auto snippetLengthResult = deser.template read<int32_t>();
        if (!snippetLengthResult)
            return snippetLengthResult.error();
        req.snippetLength = snippetLengthResult.value();

        auto noSnippetsResult = deser.template read<bool>();
        if (!noSnippetsResult)
            return noSnippetsResult.error();
        req.noSnippets = noSnippetsResult.value();

        // File type filters
        auto fileTypeResult = deser.readString();
        if (!fileTypeResult)
            return fileTypeResult.error();
        req.fileType = std::move(fileTypeResult.value());

        auto mimeTypeResult = deser.readString();
        if (!mimeTypeResult)
            return mimeTypeResult.error();
        req.mimeType = std::move(mimeTypeResult.value());

        auto extensionsResult = deser.readString();
        if (!extensionsResult)
            return extensionsResult.error();
        req.extensions = std::move(extensionsResult.value());

        auto binaryOnlyResult = deser.template read<bool>();
        if (!binaryOnlyResult)
            return binaryOnlyResult.error();
        req.binaryOnly = binaryOnlyResult.value();

        auto textOnlyResult = deser.template read<bool>();
        if (!textOnlyResult)
            return textOnlyResult.error();
        req.textOnly = textOnlyResult.value();

        // Time filters
        auto createdAfterResult = deser.readString();
        if (!createdAfterResult)
            return createdAfterResult.error();
        req.createdAfter = std::move(createdAfterResult.value());

        auto createdBeforeResult = deser.readString();
        if (!createdBeforeResult)
            return createdBeforeResult.error();
        req.createdBefore = std::move(createdBeforeResult.value());

        auto modifiedAfterResult = deser.readString();
        if (!modifiedAfterResult)
            return modifiedAfterResult.error();
        req.modifiedAfter = std::move(modifiedAfterResult.value());

        auto modifiedBeforeResult = deser.readString();
        if (!modifiedBeforeResult)
            return modifiedBeforeResult.error();
        req.modifiedBefore = std::move(modifiedBeforeResult.value());

        auto indexedAfterResult = deser.readString();
        if (!indexedAfterResult)
            return indexedAfterResult.error();
        req.indexedAfter = std::move(indexedAfterResult.value());

        auto indexedBeforeResult = deser.readString();
        if (!indexedBeforeResult)
            return indexedBeforeResult.error();
        req.indexedBefore = std::move(indexedBeforeResult.value());

        // Change tracking
        auto showChangesResult = deser.template read<bool>();
        if (!showChangesResult)
            return showChangesResult.error();
        req.showChanges = showChangesResult.value();

        auto sinceTimeResult = deser.readString();
        if (!sinceTimeResult)
            return sinceTimeResult.error();
        req.sinceTime = std::move(sinceTimeResult.value());

        auto showDiffTagsResult = deser.template read<bool>();
        if (!showDiffTagsResult)
            return showDiffTagsResult.error();
        req.showDiffTags = showDiffTagsResult.value();

        auto showDeletedResult = deser.template read<bool>();
        if (!showDeletedResult)
            return showDeletedResult.error();
        req.showDeleted = showDeletedResult.value();

        auto changeWindowResult = deser.readString();
        if (!changeWindowResult)
            return changeWindowResult.error();
        req.changeWindow = std::move(changeWindowResult.value());

        // Tag filtering
        auto tagsResult = deser.readStringVector();
        if (!tagsResult)
            return tagsResult.error();
        req.tags = std::move(tagsResult.value());

        auto filterTagsResult = deser.readString();
        if (!filterTagsResult)
            return filterTagsResult.error();
        req.filterTags = std::move(filterTagsResult.value());

        auto matchAllTagsResult = deser.template read<bool>();
        if (!matchAllTagsResult)
            return matchAllTagsResult.error();
        req.matchAllTags = matchAllTagsResult.value();

        // Name pattern filtering
        auto namePatternResult = deser.readString();
        if (!namePatternResult)
            return namePatternResult.error();
        req.namePattern = std::move(namePatternResult.value());

        return req;
    }
};

struct ShutdownRequest {
    bool graceful = true;
    std::chrono::seconds timeout{10};

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << graceful << timeout;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<ShutdownRequest> deserialize(Deserializer& deser) {
        ShutdownRequest req;

        auto gracefulResult = deser.template read<bool>();
        if (!gracefulResult)
            return gracefulResult.error();
        req.graceful = gracefulResult.value();

        auto timeoutResult = deser.template readDuration<std::chrono::seconds>();
        if (!timeoutResult)
            return timeoutResult.error();
        req.timeout = timeoutResult.value();

        return req;
    }
};

struct StatusRequest {
    bool detailed = false;

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << detailed;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<StatusRequest> deserialize(Deserializer& deser) {
        StatusRequest req;

        auto detailedResult = deser.template read<bool>();
        if (!detailedResult)
            return detailedResult.error();
        req.detailed = detailedResult.value();

        return req;
    }
};

struct CancelRequest {
    uint64_t targetRequestId{0};
    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << targetRequestId;
    }
    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<CancelRequest> deserialize(Deserializer& deser) {
        CancelRequest r;
        auto v = deser.template read<uint64_t>();
        if (!v)
            return v.error();
        r.targetRequestId = v.value();
        return r;
    }
};

struct PingRequest {
    std::chrono::steady_clock::time_point timestamp;

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << static_cast<uint64_t>(timestamp.time_since_epoch().count());
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<PingRequest> deserialize(Deserializer& deser) {
        PingRequest req;

        auto timestampResult = deser.template read<uint64_t>();
        if (!timestampResult)
            return timestampResult.error();
        req.timestamp = std::chrono::steady_clock::time_point{
            std::chrono::steady_clock::duration{timestampResult.value()}};

        return req;
    }
};

// ============================================================================
// Embedding Request Types
// ============================================================================

struct GenerateEmbeddingRequest {
    std::string text;
    std::string modelName = "all-MiniLM-L6-v2";
    bool normalize = true;

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << text << modelName << normalize;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<GenerateEmbeddingRequest> deserialize(Deserializer& deser) {
        GenerateEmbeddingRequest req;

        auto textResult = deser.readString();
        if (!textResult)
            return textResult.error();
        req.text = std::move(textResult.value());

        auto modelResult = deser.readString();
        if (!modelResult)
            return modelResult.error();
        req.modelName = std::move(modelResult.value());

        auto normalizeResult = deser.template read<bool>();
        if (!normalizeResult)
            return normalizeResult.error();
        req.normalize = normalizeResult.value();

        return req;
    }
};

struct BatchEmbeddingRequest {
    std::vector<std::string> texts;
    std::string modelName = "all-MiniLM-L6-v2";
    bool normalize = true;
    size_t batchSize = 32;

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << texts << modelName << normalize << static_cast<uint32_t>(batchSize);
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<BatchEmbeddingRequest> deserialize(Deserializer& deser) {
        BatchEmbeddingRequest req;

        auto textsResult = deser.readStringVector();
        if (!textsResult)
            return textsResult.error();
        req.texts = std::move(textsResult.value());

        auto modelResult = deser.readString();
        if (!modelResult)
            return modelResult.error();
        req.modelName = std::move(modelResult.value());

        auto normalizeResult = deser.template read<bool>();
        if (!normalizeResult)
            return normalizeResult.error();
        req.normalize = normalizeResult.value();

        auto batchSizeResult = deser.template read<uint32_t>();
        if (!batchSizeResult)
            return batchSizeResult.error();
        req.batchSize = batchSizeResult.value();

        return req;
    }
};

struct EmbedDocumentsRequest {
    std::vector<std::string> documentHashes;
    std::string modelName = "all-MiniLM-L6-v2";
    bool normalize = true;
    size_t batchSize = 32;
    bool skipExisting = true;

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << documentHashes << modelName << normalize << static_cast<uint32_t>(batchSize)
            << skipExisting;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<EmbedDocumentsRequest> deserialize(Deserializer& deser) {
        EmbedDocumentsRequest req;
        auto hashes = deser.readStringVector();
        if (!hashes)
            return hashes.error();
        req.documentHashes = std::move(hashes.value());

        auto model = deser.readString();
        if (!model)
            return model.error();
        req.modelName = std::move(model.value());

        auto norm = deser.template read<bool>();
        if (!norm)
            return norm.error();
        req.normalize = norm.value();

        auto bs = deser.template read<uint32_t>();
        if (!bs)
            return bs.error();
        req.batchSize = bs.value();

        auto se = deser.template read<bool>();
        if (!se)
            return se.error();
        req.skipExisting = se.value();

        return req;
    }
};

struct LoadModelRequest {
    std::string modelName;
    bool preload = true;     // Keep in hot pool
    std::string optionsJson; // Optional plugin options (e.g., hf.revision/offline)

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << modelName << preload << optionsJson;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<LoadModelRequest> deserialize(Deserializer& deser) {
        LoadModelRequest req;

        auto modelResult = deser.readString();
        if (!modelResult)
            return modelResult.error();
        req.modelName = std::move(modelResult.value());

        auto preloadResult = deser.template read<bool>();
        if (!preloadResult)
            return preloadResult.error();
        req.preload = preloadResult.value();

        auto opt = deser.readString();
        if (!opt)
            return opt.error();
        req.optionsJson = std::move(opt.value());

        return req;
    }
};

struct UnloadModelRequest {
    std::string modelName;
    bool force = false; // Unload even if in use

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << modelName << force;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<UnloadModelRequest> deserialize(Deserializer& deser) {
        UnloadModelRequest req;

        auto modelResult = deser.readString();
        if (!modelResult)
            return modelResult.error();
        req.modelName = std::move(modelResult.value());

        auto forceResult = deser.template read<bool>();
        if (!forceResult)
            return forceResult.error();
        req.force = forceResult.value();

        return req;
    }
};

struct ModelStatusRequest {
    std::string modelName; // Empty for all models
    bool detailed = false;

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << modelName << detailed;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<ModelStatusRequest> deserialize(Deserializer& deser) {
        ModelStatusRequest req;

        auto modelResult = deser.readString();
        if (!modelResult)
            return modelResult.error();
        req.modelName = std::move(modelResult.value());

        auto detailedResult = deser.template read<bool>();
        if (!detailedResult)
            return detailedResult.error();
        req.detailed = detailedResult.value();

        return req;
    }
};

// ============================================================================
// Additional Request Types
// ============================================================================

struct AddDocumentRequest {
    std::string path;    // File path to add
    std::string content; // Optional content (if not reading from path)
    std::string name;    // Document name
    std::vector<std::string> tags;
    std::map<std::string, std::string> metadata;
    bool recursive = false; // For directories
    bool includeHidden = false;

    // Pattern matching for recursive operations
    std::vector<std::string> includePatterns; // File patterns to include (e.g., "*.cpp")
    std::vector<std::string> excludePatterns; // File patterns to exclude

    // Collection and snapshot options
    std::string collection;    // Collection name for organizing documents
    std::string snapshotId;    // Unique snapshot identifier
    std::string snapshotLabel; // User-friendly snapshot label
    std::string sessionId;     // Session-isolated memory (PBI-082)

    // Content handling options
    std::string mimeType;         // MIME type of the document
    bool disableAutoMime = false; // Disable automatic MIME type detection
    bool noEmbeddings = false;    // Disable automatic embedding generation

    // Gitignore handling
    bool noGitignore = false;     // Ignore .gitignore patterns when adding files

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << path << content << name << tags << metadata << recursive << includeHidden
            << includePatterns << excludePatterns << collection << snapshotId << snapshotLabel
            << sessionId << mimeType << disableAutoMime << noEmbeddings << noGitignore;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<AddDocumentRequest> deserialize(Deserializer& deser) {
        AddDocumentRequest req;
        auto p = deser.readString();
        if (!p)
            return p.error();
        req.path = std::move(p.value());
        auto c = deser.readString();
        if (!c)
            return c.error();
        req.content = std::move(c.value());
        auto n = deser.readString();
        if (!n)
            return n.error();
        req.name = std::move(n.value());
        auto t = deser.readStringVector();
        if (!t)
            return t.error();
        req.tags = std::move(t.value());
        auto m = deser.readStringMap();
        if (!m)
            return m.error();
        req.metadata = std::move(m.value());
        auto r = deser.template read<bool>();
        if (!r)
            return r.error();
        req.recursive = r.value();
        auto h = deser.template read<bool>();
        if (!h)
            return h.error();
        req.includeHidden = h.value();

        // Read new fields
        auto incPat = deser.readStringVector();
        if (!incPat)
            return incPat.error();
        req.includePatterns = std::move(incPat.value());

        auto excPat = deser.readStringVector();
        if (!excPat)
            return excPat.error();
        req.excludePatterns = std::move(excPat.value());

        auto col = deser.readString();
        if (!col)
            return col.error();
        req.collection = std::move(col.value());

        auto snapId = deser.readString();
        if (!snapId)
            return snapId.error();
        req.snapshotId = std::move(snapId.value());

        auto snapLbl = deser.readString();
        if (!snapLbl)
            return snapLbl.error();
        req.snapshotLabel = std::move(snapLbl.value());

        auto sessId = deser.readString();
        if (!sessId)
            return sessId.error();
        req.sessionId = std::move(sessId.value());

        auto mime = deser.readString();
        if (!mime)
            return mime.error();
        req.mimeType = std::move(mime.value());

        auto disAutoMime = deser.template read<bool>();
        if (!disAutoMime)
            return disAutoMime.error();
        req.disableAutoMime = disAutoMime.value();

        auto noEmb = deser.template read<bool>();
        if (!noEmb)
            return noEmb.error();
        req.noEmbeddings = noEmb.value();

        auto noGit = deser.template read<bool>();
        if (!noGit)
            return noGit.error();
        req.noGitignore = noGit.value();

        return req;
    }
};

struct GrepRequest {
    std::string pattern;            // Regex pattern
    std::string path;               // Optional path filter (deprecated - use paths)
    std::vector<std::string> paths; // Multiple paths to search (NEW)
    bool caseInsensitive = false;
    bool invertMatch = false;
    int contextLines = 0;  // Combined context (overrides before/after if > 0)
    size_t maxMatches = 0; // 0 = unlimited per file

    // Additional fields for feature parity
    std::vector<std::string> includePatterns; // File patterns to include
    bool recursive = true;                    // Recursive directory search (NEW)
    bool wholeWord = false;                   // Match whole words only
    bool showLineNumbers = false;             // Show line numbers
    bool showFilename = false;                // Show filename with matches
    bool noFilename = false;                  // Never show filename
    bool countOnly = false;                   // Show only count of matching lines
    bool filesOnly = false;                   // Show only filenames with matches
    bool filesWithoutMatch = false;           // Show only filenames without matches
    bool pathsOnly = false;                   // Show only file paths
    bool literalText = false;                 // Treat pattern as literal text
    bool regexOnly = false;                   // Disable semantic search
    size_t semanticLimit = 10;                // Number of semantic results
    std::vector<std::string> filterTags;      // Filter by tags
    bool matchAllTags = false;                // Require all tags
    std::string colorMode = "auto";           // Color output mode
    int beforeContext = 0;                    // Lines before match
    int afterContext = 0;                     // Lines after match
    bool showDiff = false;

    // Session scoping (controls hot/cold path behavior)
    bool useSession = false; // if true, allow hot path optimization
    std::string sessionName; // optional explicit session name

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << pattern << path << paths << caseInsensitive << invertMatch
            << static_cast<int32_t>(contextLines) << static_cast<uint64_t>(maxMatches)
            << includePatterns << recursive << wholeWord << showLineNumbers << showFilename
            << noFilename << countOnly << filesOnly << filesWithoutMatch << pathsOnly << literalText
            << regexOnly << static_cast<uint64_t>(semanticLimit) << filterTags << matchAllTags
            << colorMode << static_cast<int32_t>(beforeContext)
            << static_cast<int32_t>(afterContext) << showDiff << useSession << sessionName;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<GrepRequest> deserialize(Deserializer& deser) {
        GrepRequest req;
        auto p = deser.readString();
        if (!p)
            return p.error();
        req.pattern = std::move(p.value());
        auto pt = deser.readString();
        if (!pt)
            return pt.error();
        req.path = std::move(pt.value());

        // Deserialize new paths field
        auto pths = deser.readStringVector();
        if (!pths)
            return pths.error();
        req.paths = std::move(pths.value());

        auto ci = deser.template read<bool>();
        if (!ci)
            return ci.error();
        req.caseInsensitive = ci.value();
        auto im = deser.template read<bool>();
        if (!im)
            return im.error();
        req.invertMatch = im.value();
        auto cl = deser.template read<int32_t>();
        if (!cl)
            return cl.error();
        req.contextLines = cl.value();
        auto mm = deser.template read<uint64_t>();
        if (!mm)
            return mm.error();
        req.maxMatches = mm.value();

        // Deserialize new fields
        auto ip = deser.readStringVector();
        if (!ip)
            return ip.error();
        req.includePatterns = std::move(ip.value());

        // Deserialize new recursive field
        auto rec = deser.template read<bool>();
        if (!rec)
            return rec.error();
        req.recursive = rec.value();
        auto ww = deser.template read<bool>();
        if (!ww)
            return ww.error();
        req.wholeWord = ww.value();
        auto sln = deser.template read<bool>();
        if (!sln)
            return sln.error();
        req.showLineNumbers = sln.value();
        auto sf = deser.template read<bool>();
        if (!sf)
            return sf.error();
        req.showFilename = sf.value();
        auto nf = deser.template read<bool>();
        if (!nf)
            return nf.error();
        req.noFilename = nf.value();
        auto co = deser.template read<bool>();
        if (!co)
            return co.error();
        req.countOnly = co.value();
        auto fo = deser.template read<bool>();
        if (!fo)
            return fo.error();
        req.filesOnly = fo.value();
        auto fwm = deser.template read<bool>();
        if (!fwm)
            return fwm.error();
        req.filesWithoutMatch = fwm.value();
        auto po = deser.template read<bool>();
        if (!po)
            return po.error();
        req.pathsOnly = po.value();
        auto lt = deser.template read<bool>();
        if (!lt)
            return lt.error();
        req.literalText = lt.value();
        auto ro = deser.template read<bool>();
        if (!ro)
            return ro.error();
        req.regexOnly = ro.value();
        auto sl = deser.template read<uint64_t>();
        if (!sl)
            return sl.error();
        req.semanticLimit = sl.value();
        auto ft = deser.readStringVector();
        if (!ft)
            return ft.error();
        req.filterTags = std::move(ft.value());
        auto mat = deser.template read<bool>();
        if (!mat)
            return mat.error();
        req.matchAllTags = mat.value();
        auto cm = deser.readString();
        if (!cm)
            return cm.error();
        req.colorMode = std::move(cm.value());
        auto bc = deser.template read<int32_t>();
        if (!bc)
            return bc.error();
        req.beforeContext = bc.value();
        auto ac = deser.template read<int32_t>();
        if (!ac)
            return ac.error();
        req.afterContext = ac.value();

        auto showDiffResult = deser.template read<bool>();
        if (showDiffResult) {
            req.showDiff = showDiffResult.value();
        }

        // Deserialize session fields
        auto usResult = deser.template read<bool>();
        if (usResult) {
            req.useSession = usResult.value();
        }
        auto snResult = deser.readString();
        if (snResult) {
            req.sessionName = std::move(snResult.value());
        }

        return req;
    }
};

struct UpdateDocumentRequest {
    std::string hash;       // Document hash or name
    std::string name;       // Alternative to hash
    std::string newContent; // New content (optional)
    std::vector<std::string> addTags;
    std::vector<std::string> removeTags;
    std::map<std::string, std::string> metadata;
    bool atomic{true};
    bool createBackup{false};
    bool verbose{false};

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << hash << name << newContent << addTags << removeTags << metadata << atomic
            << createBackup << verbose;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<UpdateDocumentRequest> deserialize(Deserializer& deser) {
        UpdateDocumentRequest req;
        auto h = deser.readString();
        if (!h)
            return h.error();
        req.hash = std::move(h.value());
        auto n = deser.readString();
        if (!n)
            return n.error();
        req.name = std::move(n.value());
        auto nc = deser.readString();
        if (!nc)
            return nc.error();
        req.newContent = std::move(nc.value());
        auto at = deser.readStringVector();
        if (!at)
            return at.error();
        req.addTags = std::move(at.value());
        auto rt = deser.readStringVector();
        if (!rt)
            return rt.error();
        req.removeTags = std::move(rt.value());
        auto m = deser.readStringMap();
        if (!m)
            return m.error();
        req.metadata = std::move(m.value());
        if (auto a = deser.template read<bool>(); a)
            req.atomic = a.value();
        else
            return a.error();
        if (auto cb = deser.template read<bool>(); cb)
            req.createBackup = cb.value();
        else
            return cb.error();
        if (auto vb = deser.template read<bool>(); vb)
            req.verbose = vb.value();
        else
            return vb.error();
        return req;
    }
};

struct DownloadRequest {
    std::string url;                             // URL to download
    std::string outputPath;                      // Optional output path/name
    std::vector<std::string> tags;               // Tags to apply to downloaded file
    std::map<std::string, std::string> metadata; // Metadata to apply
    bool quiet = false;                          // Suppress info logging

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << url << outputPath << tags << metadata << quiet;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<DownloadRequest> deserialize(Deserializer& deser) {
        DownloadRequest req;
        auto u = deser.readString();
        if (!u)
            return u.error();
        req.url = std::move(u.value());
        auto op = deser.readString();
        if (!op)
            return op.error();
        req.outputPath = std::move(op.value());
        auto t = deser.readStringVector();
        if (!t)
            return t.error();
        req.tags = std::move(t.value());
        auto m = deser.readStringMap();
        if (!m)
            return m.error();
        req.metadata = std::move(m.value());
        auto q = deser.template read<bool>();
        if (!q)
            return q.error();
        req.quiet = q.value();
        return req;
    }
};

struct GetStatsRequest {
    bool detailed = false;        // Include detailed breakdown
    bool includeCache = false;    // Include cache statistics
    bool showFileTypes = false;   // Include file type breakdown
    bool showCompression = false; // Include compression statistics
    bool showDuplicates = false;  // Include duplicate analysis
    bool showDedup = false;       // Include block-level deduplication
    bool showPerformance = false; // Include performance metrics
    bool includeHealth = false;   // Include health status

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << detailed << includeCache << showFileTypes << showCompression << showDuplicates
            << showDedup << showPerformance << includeHealth;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<GetStatsRequest> deserialize(Deserializer& deser) {
        GetStatsRequest req;
        auto d = deser.template read<bool>();
        if (!d)
            return d.error();
        req.detailed = d.value();

        auto c = deser.template read<bool>();
        if (!c)
            return c.error();
        req.includeCache = c.value();

        auto ft = deser.template read<bool>();
        if (!ft)
            return ft.error();
        req.showFileTypes = ft.value();

        auto sc = deser.template read<bool>();
        if (!sc)
            return sc.error();
        req.showCompression = sc.value();

        auto sd = deser.template read<bool>();
        if (!sd)
            return sd.error();
        req.showDuplicates = sd.value();

        auto sdd = deser.template read<bool>();
        if (!sdd)
            return sdd.error();
        req.showDedup = sdd.value();

        auto sp = deser.template read<bool>();
        if (!sp)
            return sp.error();
        req.showPerformance = sp.value();

        auto ih = deser.template read<bool>();
        if (!ih)
            return ih.error();
        req.includeHealth = ih.value();

        return req;
    }
};

struct PrepareSessionRequest {
    std::string sessionName; // optional; empty = current session
    int cores{-1};
    int memoryGb{-1};
    long timeMs{-1};
    bool aggressive{false};
    std::size_t limit{200};
    std::size_t snippetLen{160};

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << sessionName << cores << memoryGb << timeMs << aggressive
            << static_cast<uint64_t>(limit) << static_cast<uint32_t>(snippetLen);
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<PrepareSessionRequest> deserialize(Deserializer& deser) {
        PrepareSessionRequest req;
        auto s = deser.readString();
        if (!s)
            return s.error();
        req.sessionName = std::move(s.value());
        auto c = deser.template read<int>();
        if (!c)
            return c.error();
        req.cores = c.value();
        auto m = deser.template read<int>();
        if (!m)
            return m.error();
        req.memoryGb = m.value();
        auto t = deser.template read<long>();
        if (!t)
            return t.error();
        req.timeMs = t.value();
        auto a = deser.template read<bool>();
        if (!a)
            return a.error();
        req.aggressive = a.value();
        auto l = deser.template read<uint64_t>();
        if (!l)
            return l.error();
        req.limit = static_cast<std::size_t>(l.value());
        auto sl = deser.template read<uint32_t>();
        if (!sl)
            return sl.error();
        req.snippetLen = static_cast<std::size_t>(sl.value());
        return req;
    }
};

// File history request (PBI-043 enhancement)
struct FileHistoryRequest {
    std::string filepath; // Absolute path to query history for

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << filepath;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<FileHistoryRequest> deserialize(Deserializer& deser) {
        FileHistoryRequest req;
        auto fp = deser.readString();
        if (!fp)
            return fp.error();
        req.filepath = std::move(fp.value());
        return req;
    }
};

// Prune request (PBI-062)
struct PruneRequest {
    std::vector<std::string> categories;      // build-artifacts, logs, etc.
    std::vector<std::string> extensions;      // Specific extensions
    std::string olderThan;                    // "30d", "2w", etc.
    std::string largerThan;                   // "10MB", etc.
    std::string smallerThan;                  // "1KB", etc.
    std::vector<std::string> excludePatterns; // Paths to exclude
    bool dryRun{true};                        // Preview mode
    bool verbose{false};                      // Detailed output

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << categories << extensions << olderThan << largerThan << smallerThan << excludePatterns
            << dryRun << verbose;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<PruneRequest> deserialize(Deserializer& deser) {
        PruneRequest req;

        if (auto r = deser.readStringVector(); r)
            req.categories = std::move(r.value());
        else
            return r.error();

        if (auto r = deser.readStringVector(); r)
            req.extensions = std::move(r.value());
        else
            return r.error();

        if (auto r = deser.readString(); r)
            req.olderThan = std::move(r.value());
        else
            return r.error();

        if (auto r = deser.readString(); r)
            req.largerThan = std::move(r.value());
        else
            return r.error();

        if (auto r = deser.readString(); r)
            req.smallerThan = std::move(r.value());
        else
            return r.error();

        if (auto r = deser.readStringVector(); r)
            req.excludePatterns = std::move(r.value());
        else
            return r.error();

        if (auto r = deser.readBool(); r)
            req.dryRun = r.value();
        else
            return r.error();

        if (auto r = deser.readBool(); r)
            req.verbose = r.value();
        else
            return r.error();

        return req;
    }
};

// Collection and snapshot requests
struct ListCollectionsRequest {
    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize([[maybe_unused]] Serializer& ser) const {
        // No fields to serialize
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<ListCollectionsRequest> deserialize([[maybe_unused]] Deserializer& deser) {
        return ListCollectionsRequest{};
    }
};

struct ListSnapshotsRequest {
    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize([[maybe_unused]] Serializer& ser) const {
        // No fields to serialize
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<ListSnapshotsRequest> deserialize([[maybe_unused]] Deserializer& deser) {
        return ListSnapshotsRequest{};
    }
};

struct RestoreCollectionRequest {
    std::string collection;
    std::string outputDirectory;
    std::string layoutTemplate;
    std::vector<std::string> includePatterns;
    std::vector<std::string> excludePatterns;
    bool overwrite{false};
    bool createDirs{true};
    bool dryRun{false};

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << collection << outputDirectory << layoutTemplate << includePatterns << excludePatterns
            << overwrite << createDirs << dryRun;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<RestoreCollectionRequest> deserialize(Deserializer& deser) {
        RestoreCollectionRequest req;

        if (auto r = deser.readString(); r)
            req.collection = std::move(r.value());
        else
            return r.error();

        if (auto r = deser.readString(); r)
            req.outputDirectory = std::move(r.value());
        else
            return r.error();

        if (auto r = deser.readString(); r)
            req.layoutTemplate = std::move(r.value());
        else
            return r.error();

        if (auto r = deser.readStringVector(); r)
            req.includePatterns = std::move(r.value());
        else
            return r.error();

        if (auto r = deser.readStringVector(); r)
            req.excludePatterns = std::move(r.value());
        else
            return r.error();

        if (auto r = deser.readBool(); r)
            req.overwrite = r.value();
        else
            return r.error();

        if (auto r = deser.readBool(); r)
            req.createDirs = r.value();
        else
            return r.error();

        if (auto r = deser.readBool(); r)
            req.dryRun = r.value();
        else
            return r.error();

        return req;
    }
};

struct RestoreSnapshotRequest {
    std::string snapshotId;
    std::string outputDirectory;
    std::string layoutTemplate;
    std::vector<std::string> includePatterns;
    std::vector<std::string> excludePatterns;
    bool overwrite{false};
    bool createDirs{true};
    bool dryRun{false};

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << snapshotId << outputDirectory << layoutTemplate << includePatterns << excludePatterns
            << overwrite << createDirs << dryRun;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<RestoreSnapshotRequest> deserialize(Deserializer& deser) {
        RestoreSnapshotRequest req;

        if (auto r = deser.readString(); r)
            req.snapshotId = std::move(r.value());
        else
            return r.error();

        if (auto r = deser.readString(); r)
            req.outputDirectory = std::move(r.value());
        else
            return r.error();

        if (auto r = deser.readString(); r)
            req.layoutTemplate = std::move(r.value());
        else
            return r.error();

        if (auto r = deser.readStringVector(); r)
            req.includePatterns = std::move(r.value());
        else
            return r.error();

        if (auto r = deser.readStringVector(); r)
            req.excludePatterns = std::move(r.value());
        else
            return r.error();

        if (auto r = deser.readBool(); r)
            req.overwrite = r.value();
        else
            return r.error();

        if (auto r = deser.readBool(); r)
            req.createDirs = r.value();
        else
            return r.error();

        if (auto r = deser.readBool(); r)
            req.dryRun = r.value();
        else
            return r.error();

        return req;
    }
};

// Plugin management requests
struct PluginScanRequest {
    std::string dir;    // optional
    std::string target; // optional file path

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << dir << target;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<PluginScanRequest> deserialize(Deserializer& d) {
        PluginScanRequest r;
        if (auto s = d.readString(); s)
            r.dir = std::move(s.value());
        else
            return s.error();
        if (auto t = d.readString(); t)
            r.target = std::move(t.value());
        else
            return t.error();
        return r;
    }
};

struct PluginLoadRequest {
    std::string pathOrName;
    std::string configJson;
    bool dryRun{false};

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << pathOrName << configJson << dryRun;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<PluginLoadRequest> deserialize(Deserializer& d) {
        PluginLoadRequest r;
        if (auto a = d.readString(); a)
            r.pathOrName = std::move(a.value());
        else
            return a.error();
        if (auto c = d.readString(); c)
            r.configJson = std::move(c.value());
        else
            return c.error();
        if (auto dr = d.template read<bool>(); dr)
            r.dryRun = dr.value();
        else
            return dr.error();
        return r;
    }
};

struct PluginUnloadRequest {
    std::string name;
    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << name;
    }
    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<PluginUnloadRequest> deserialize(Deserializer& d) {
        PluginUnloadRequest r;
        if (auto n = d.readString(); n)
            r.name = std::move(n.value());
        else
            return n.error();
        return r;
    }
};

struct PluginTrustListRequest {
    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer&) const {}
    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<PluginTrustListRequest> deserialize(Deserializer&) {
        return PluginTrustListRequest{};
    }
};

struct PluginTrustAddRequest {
    std::string path;
    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << path;
    }
    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<PluginTrustAddRequest> deserialize(Deserializer& d) {
        PluginTrustAddRequest r;
        if (auto p = d.readString(); p)
            r.path = std::move(p.value());
        else
            return p.error();
        return r;
    }
};

struct PluginTrustRemoveRequest {
    std::string path;
    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << path;
    }
    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<PluginTrustRemoveRequest> deserialize(Deserializer& d) {
        PluginTrustRemoveRequest r;
        if (auto p = d.readString(); p)
            r.path = std::move(p.value());
        else
            return p.error();
        return r;
    }
};

// Graph query requests (PBI-009)
struct GraphQueryRequest {
    // Origin selection (exactly one should be set, unless listByType mode)
    std::string documentHash;
    std::string documentName;
    std::string snapshotId;
    int64_t nodeId{-1}; // -1 means not set

    // List-by-type mode (PBI-093): when true, ignore origin and list nodes by type
    bool listByType{false};
    std::string nodeType; // Node type to filter (e.g., "binary.function", "binary.import")
    std::string nodeKey;  // Direct node key lookup (e.g., "fn:abc123:0x1000")

    // List-types mode (yams-66h): when true, return available node types with counts
    bool listTypes{false};

    // Traversal options
    std::vector<std::string> relationFilters; // Empty = all relations
    int32_t maxDepth{1};                      // BFS depth limit (1-4)
    uint32_t maxResults{200};                 // Total result cap
    uint32_t maxResultsPerDepth{100};         // Per-depth cap
    bool reverseTraversal{false};             // Traverse incoming edges instead of outgoing
    bool isolatedMode{false};                 // Find isolated nodes (no incoming edges of relationFilters[0])
    std::string isolatedRelation;             // Relation to check for isolation (default: "calls")

    // Snapshot context
    std::string scopeToSnapshot;

    // Pagination
    uint32_t offset{0};
    uint32_t limit{100};

    // Output control
    bool includeEdgeProperties{false};
    bool includeNodeProperties{false};
    bool hydrateFully{true};

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << documentHash << documentName << snapshotId << nodeId << listByType << nodeType
            << nodeKey << listTypes << relationFilters << maxDepth << maxResults << maxResultsPerDepth << reverseTraversal
            << isolatedMode << isolatedRelation
            << scopeToSnapshot << offset << limit << includeEdgeProperties << includeNodeProperties
            << hydrateFully;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<GraphQueryRequest> deserialize(Deserializer& deser) {
        GraphQueryRequest req;

        if (auto r = deser.readString(); r)
            req.documentHash = std::move(r.value());
        else
            return r.error();

        if (auto r = deser.readString(); r)
            req.documentName = std::move(r.value());
        else
            return r.error();

        if (auto r = deser.readString(); r)
            req.snapshotId = std::move(r.value());
        else
            return r.error();

        if (auto r = deser.template read<int64_t>(); r)
            req.nodeId = r.value();
        else
            return r.error();

        // PBI-093: New fields for listByType mode
        if (auto r = deser.template read<bool>(); r)
            req.listByType = r.value();
        else
            return r.error();

        if (auto r = deser.readString(); r)
            req.nodeType = std::move(r.value());
        else
            return r.error();

        if (auto r = deser.readString(); r)
            req.nodeKey = std::move(r.value());
        else
            return r.error();

        // yams-66h: listTypes mode
        if (auto r = deser.template read<bool>(); r)
            req.listTypes = r.value();
        else
            return r.error();

        if (auto r = deser.readStringVector(); r)
            req.relationFilters = std::move(r.value());
        else
            return r.error();

        if (auto r = deser.template read<int32_t>(); r)
            req.maxDepth = r.value();
        else
            return r.error();

        if (auto r = deser.template read<uint32_t>(); r)
            req.maxResults = r.value();
        else
            return r.error();

        if (auto r = deser.template read<uint32_t>(); r)
            req.maxResultsPerDepth = r.value();
        else
            return r.error();
        if (auto r = deser.template read<bool>(); r)
            req.reverseTraversal = r.value();
        else
            return r.error();

        if (auto r = deser.template read<bool>(); r)
            req.isolatedMode = r.value();
        else
            return r.error();

        if (auto r = deser.readString(); r)
            req.isolatedRelation = std::move(r.value());
        else
            return r.error();

        if (auto r = deser.readString(); r)
            req.scopeToSnapshot = std::move(r.value());
        else
            return r.error();

        if (auto r = deser.template read<uint32_t>(); r)
            req.offset = r.value();
        else
            return r.error();

        if (auto r = deser.template read<uint32_t>(); r)
            req.limit = r.value();
        else
            return r.error();

        if (auto r = deser.template read<bool>(); r)
            req.includeEdgeProperties = r.value();
        else
            return r.error();

        if (auto r = deser.template read<bool>(); r)
            req.includeNodeProperties = r.value();
        else
            return r.error();

        if (auto r = deser.template read<bool>(); r)
            req.hydrateFully = r.value();
        else
            return r.error();

        return req;
    }
};

struct GraphPathHistoryRequest {
    std::string path;
    uint32_t limit{100};

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << path << limit;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<GraphPathHistoryRequest> deserialize(Deserializer& deser) {
        GraphPathHistoryRequest req;

        if (auto r = deser.readString(); r)
            req.path = std::move(r.value());
        else
            return r.error();

        if (auto r = deser.template read<uint32_t>(); r)
            req.limit = r.value();
        else
            return r.error();

        return req;
    }
};

// ============================================================================
// Graph Maintenance Requests (PBI-009 Phase 4.3)
// ============================================================================

struct GraphRepairRequest {
    bool dryRun{false};

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << dryRun;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<GraphRepairRequest> deserialize(Deserializer& deser) {
        GraphRepairRequest req;

        if (auto r = deser.template read<bool>(); r)
            req.dryRun = r.value();
        else
            return r.error();

        return req;
    }
};

struct GraphValidateRequest {
    // No parameters needed for now
    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& /*ser*/) const {
        // Empty for now
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<GraphValidateRequest> deserialize(Deserializer& /*deser*/) {
        return GraphValidateRequest{};
    }
};

// ============================================================================
// KG Ingest Request/Response (PBI-093 Phase 2)
// ============================================================================

/**
 * Node for bulk KG ingestion. Matches KGNode structure but uses IPC serialization.
 */
struct KgIngestNode {
    std::string nodeKey;    // Unique logical key (required)
    std::string label;      // Human-readable name
    std::string type;       // Node type (e.g., "binary.function")
    std::string properties; // JSON properties blob

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << nodeKey << label << type << properties;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<KgIngestNode> deserialize(Deserializer& deser) {
        KgIngestNode node;
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
            node.properties = std::move(r.value());
        else
            return r.error();
        return node;
    }
};

/**
 * Edge for bulk KG ingestion. Uses node keys for referencing (resolved by daemon).
 */
struct KgIngestEdge {
    std::string srcNodeKey; // Source node key (resolved to ID by daemon)
    std::string dstNodeKey; // Destination node key (resolved to ID by daemon)
    std::string relation;   // Relation/predicate (e.g., "CALLS", "CONTAINS")
    float weight{1.0f};     // Optional weight
    std::string properties; // JSON properties blob

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << srcNodeKey << dstNodeKey << relation << weight << properties;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<KgIngestEdge> deserialize(Deserializer& deser) {
        KgIngestEdge edge;
        if (auto r = deser.readString(); r)
            edge.srcNodeKey = std::move(r.value());
        else
            return r.error();
        if (auto r = deser.readString(); r)
            edge.dstNodeKey = std::move(r.value());
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

/**
 * Alias for bulk KG ingestion.
 */
struct KgIngestAlias {
    std::string nodeKey;    // Node key to associate alias with
    std::string alias;      // Surface form
    std::string source;     // Origin/system of alias
    float confidence{1.0f}; // [0,1]

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << nodeKey << alias << source << confidence;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<KgIngestAlias> deserialize(Deserializer& deser) {
        KgIngestAlias a;
        if (auto r = deser.readString(); r)
            a.nodeKey = std::move(r.value());
        else
            return r.error();
        if (auto r = deser.readString(); r)
            a.alias = std::move(r.value());
        else
            return r.error();
        if (auto r = deser.readString(); r)
            a.source = std::move(r.value());
        else
            return r.error();
        if (auto r = deser.template read<float>(); r)
            a.confidence = r.value();
        else
            return r.error();
        return a;
    }
};

/**
 * Bulk KG ingest request for ingesting nodes, edges, and aliases from external sources
 * (e.g., Ghidra plugin binary analysis results).
 */
struct KgIngestRequest {
    std::vector<KgIngestNode> nodes;
    std::vector<KgIngestEdge> edges;
    std::vector<KgIngestAlias> aliases;

    // Optional: associate all entities with a document
    std::string documentHash; // If set, links entities to this document

    // Options
    bool skipExistingNodes{true}; // Skip nodes that already exist (by nodeKey)
    bool skipExistingEdges{true}; // Skip edges that already exist (by src/dst/relation)

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        // Nodes
        ser << static_cast<uint32_t>(nodes.size());
        for (const auto& node : nodes) {
            node.serialize(ser);
        }
        // Edges
        ser << static_cast<uint32_t>(edges.size());
        for (const auto& edge : edges) {
            edge.serialize(ser);
        }
        // Aliases
        ser << static_cast<uint32_t>(aliases.size());
        for (const auto& alias : aliases) {
            alias.serialize(ser);
        }
        ser << documentHash << skipExistingNodes << skipExistingEdges;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<KgIngestRequest> deserialize(Deserializer& deser) {
        KgIngestRequest req;

        // Nodes
        if (auto cnt = deser.template read<uint32_t>(); cnt) {
            req.nodes.reserve(cnt.value());
            for (uint32_t i = 0; i < cnt.value(); ++i) {
                auto node = KgIngestNode::deserialize(deser);
                if (!node)
                    return node.error();
                req.nodes.push_back(std::move(node.value()));
            }
        } else
            return cnt.error();

        // Edges
        if (auto cnt = deser.template read<uint32_t>(); cnt) {
            req.edges.reserve(cnt.value());
            for (uint32_t i = 0; i < cnt.value(); ++i) {
                auto edge = KgIngestEdge::deserialize(deser);
                if (!edge)
                    return edge.error();
                req.edges.push_back(std::move(edge.value()));
            }
        } else
            return cnt.error();

        // Aliases
        if (auto cnt = deser.template read<uint32_t>(); cnt) {
            req.aliases.reserve(cnt.value());
            for (uint32_t i = 0; i < cnt.value(); ++i) {
                auto alias = KgIngestAlias::deserialize(deser);
                if (!alias)
                    return alias.error();
                req.aliases.push_back(std::move(alias.value()));
            }
        } else
            return cnt.error();

        if (auto r = deser.readString(); r)
            req.documentHash = std::move(r.value());
        else
            return r.error();

        if (auto r = deser.template read<bool>(); r)
            req.skipExistingNodes = r.value();
        else
            return r.error();

        if (auto r = deser.template read<bool>(); r)
            req.skipExistingEdges = r.value();
        else
            return r.error();

        return req;
    }
};

// Forward declarations for late-defined request types used in the Request variant
struct CatRequest;
struct ListSessionsRequest;
struct UseSessionRequest;
struct AddPathSelectorRequest;
struct RemovePathSelectorRequest;
struct ListTreeDiffRequest;

// Variant type for all requests
using Request = std::variant<
    SearchRequest, GetRequest, GetInitRequest, GetChunkRequest, GetEndRequest, DeleteRequest,
    ListRequest, ShutdownRequest, StatusRequest, PingRequest, GenerateEmbeddingRequest,
    BatchEmbeddingRequest, LoadModelRequest, UnloadModelRequest, ModelStatusRequest,
    AddDocumentRequest, GrepRequest, UpdateDocumentRequest, DownloadRequest, GetStatsRequest,
    PrepareSessionRequest, EmbedDocumentsRequest, PluginScanRequest, PluginLoadRequest,
    PluginUnloadRequest, PluginTrustListRequest, PluginTrustAddRequest, PluginTrustRemoveRequest,
    CancelRequest, CatRequest, ListSessionsRequest, UseSessionRequest, AddPathSelectorRequest,
    RemovePathSelectorRequest, ListTreeDiffRequest, FileHistoryRequest, PruneRequest,
    ListCollectionsRequest, ListSnapshotsRequest, RestoreCollectionRequest, RestoreSnapshotRequest,
    GraphQueryRequest, GraphPathHistoryRequest, GraphRepairRequest, GraphValidateRequest,
    KgIngestRequest>;

// ============================================================================
// Response Types
// ============================================================================

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

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << static_cast<uint32_t>(results.size());
        for (const auto& result : results) {
            result.serialize(ser);
        }
        ser << static_cast<uint64_t>(totalCount) << elapsed;
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
    double memoryUsageMb;
    double cpuUsagePercent;
    std::string version;
    // Vector DB health metrics
    bool vectorDbInitAttempted{false};
    bool vectorDbReady{false};
    uint32_t vectorDbDim{0};
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
    std::string contentStoreRoot;  // absolute path to storage root (daemon-resolved)
    std::string contentStoreError; // last initialization error (if any)

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
        bool isProvider{false}; // true if this plugin is the adopted model provider
        std::vector<std::string> interfaces; // plugin interfaces (e.g., content_extractor_v1)
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
        ser << vectorDbInitAttempted << vectorDbReady << static_cast<uint32_t>(vectorDbDim);

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

        // Serialize content store diagnostics (as strings)
        ser << contentStoreRoot << contentStoreError;

        // Serialize embedding runtime details
        ser << embeddingAvailable << embeddingBackend << embeddingModel << embeddingModelPath
            << static_cast<uint32_t>(embeddingDim) << static_cast<int32_t>(embeddingThreadsIntra)
            << static_cast<int32_t>(embeddingThreadsInter);

        // Serialize centralized tuning pool sizes
        ser << static_cast<uint32_t>(ipcPoolSize) << static_cast<uint32_t>(ioPoolSize);
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

        // Deserialize content store diagnostics
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
    ErrorCode code;
    std::string message;

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << static_cast<uint32_t>(code) << message;
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

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << hash << path << documentsAdded << documentsUpdated << documentsSkipped << message
            << static_cast<uint64_t>(size) << snapshotId << snapshotLabel;
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

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << hash << contentUpdated << metadataUpdated << tagsUpdated;
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

// Collection and snapshot responses
struct ListCollectionsResponse {
    std::vector<std::string> collections;
    uint64_t totalCount{0};

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << collections << totalCount;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<ListCollectionsResponse> deserialize(Deserializer& deser) {
        ListCollectionsResponse res;

        if (auto r = deser.readStringVector(); r)
            res.collections = std::move(r.value());
        else
            return r.error();

        if (auto r = deser.template read<uint64_t>(); r)
            res.totalCount = r.value();
        else
            return r.error();

        return res;
    }
};

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

struct GraphQueryResponse {
    GraphNode originNode;
    std::vector<GraphNode> connectedNodes;
    uint64_t totalNodesFound{0};
    uint64_t totalEdgesTraversed{0};
    bool truncated{false};
    int32_t maxDepthReached{0};
    int64_t queryTimeMs{0};
    bool kgAvailable{true};
    std::string warning;

    // yams-66h: Node type counts for --list-types mode
    std::vector<std::pair<std::string, uint64_t>> nodeTypeCounts;

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        originNode.serialize(ser);
        ser << static_cast<uint32_t>(connectedNodes.size());
        for (const auto& node : connectedNodes) {
            node.serialize(ser);
        }
        ser << totalNodesFound << totalEdgesTraversed << truncated << maxDepthReached << queryTimeMs
            << kgAvailable << warning;
        // yams-66h: Serialize node type counts
        ser << static_cast<uint32_t>(nodeTypeCounts.size());
        for (const auto& [type, count] : nodeTypeCounts) {
            ser << type << count;
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

        return res;
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
    size_t size = 0;       // File size in bytes
    bool success = false;  // Download success status
    std::string error;     // Error message if failed

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << hash << localPath << url << static_cast<uint64_t>(size) << success << error;
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

struct CatRequest {
    std::string hash;
    std::string name;

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << hash << name;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<CatRequest> deserialize(Deserializer& deser) {
        CatRequest req;
        auto h = deser.readString();
        if (!h)
            return h.error();
        req.hash = std::move(h.value());
        auto n = deser.readString();
        if (!n)
            return n.error();
        req.name = std::move(n.value());
        return req;
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

struct ListSessionsRequest {
    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer&) const {}

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<ListSessionsRequest> deserialize(Deserializer&) {
        return ListSessionsRequest{};
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

struct UseSessionRequest {
    std::string session_name;

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << session_name;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<UseSessionRequest> deserialize(Deserializer& deser) {
        UseSessionRequest req;
        auto sn = deser.readString();
        if (!sn)
            return sn.error();
        req.session_name = std::move(sn.value());
        return req;
    }
};

struct AddPathSelectorRequest {
    std::string session_name;
    std::string path;
    std::vector<std::string> tags;
    std::map<std::string, std::string> metadata;

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << session_name << path << tags << metadata;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<AddPathSelectorRequest> deserialize(Deserializer& deser) {
        AddPathSelectorRequest req;
        auto sn = deser.readString();
        if (!sn)
            return sn.error();
        req.session_name = std::move(sn.value());
        auto p = deser.readString();
        if (!p)
            return p.error();
        req.path = std::move(p.value());
        auto t = deser.readStringVector();
        if (!t)
            return t.error();
        req.tags = std::move(t.value());
        auto m = deser.readStringMap();
        if (!m)
            return m.error();
        req.metadata = std::move(m.value());
        return req;
    }
};

struct RemovePathSelectorRequest {
    std::string session_name;
    std::string path;

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << session_name << path;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<RemovePathSelectorRequest> deserialize(Deserializer& deser) {
        RemovePathSelectorRequest req;
        auto sn = deser.readString();
        if (!sn)
            return sn.error();
        req.session_name = std::move(sn.value());
        auto p = deser.readString();
        if (!p)
            return p.error();
        req.path = std::move(p.value());
        return req;
    }
};

// ============================================================================
// Tree Diff Request/Response (PBI-043)
// ============================================================================

struct ListTreeDiffRequest {
    std::string baseSnapshotId;
    std::string targetSnapshotId;
    std::string pathPrefix;
    std::string typeFilter;
    uint64_t limit = 1000;
    uint64_t offset = 0;

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << baseSnapshotId << targetSnapshotId << pathPrefix << typeFilter << limit << offset;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<ListTreeDiffRequest> deserialize(Deserializer& deser) {
        ListTreeDiffRequest req;
        auto base = deser.readString();
        if (!base)
            return base.error();
        req.baseSnapshotId = std::move(base.value());

        auto target = deser.readString();
        if (!target)
            return target.error();
        req.targetSnapshotId = std::move(target.value());

        auto prefix = deser.readString();
        if (!prefix)
            return prefix.error();
        req.pathPrefix = std::move(prefix.value());

        auto filter = deser.readString();
        if (!filter)
            return filter.error();
        req.typeFilter = std::move(filter.value());

        auto lim = deser.template read<uint64_t>();
        if (!lim)
            return lim.error();
        req.limit = lim.value();

        auto off = deser.template read<uint64_t>();
        if (!off)
            return off.error();
        req.offset = off.value();

        return req;
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

// Variant type for all responses
using Response =
    std::variant<SearchResponse, AddResponse, GetResponse, GetInitResponse, GetChunkResponse,
                 StatusResponse, SuccessResponse, ErrorResponse, PongResponse, EmbeddingResponse,
                 BatchEmbeddingResponse, ModelLoadResponse, ModelStatusResponse, ListResponse,
                 AddDocumentResponse, GrepResponse, UpdateDocumentResponse, GetStatsResponse,
                 DownloadResponse, DeleteResponse, PrepareSessionResponse, EmbedDocumentsResponse,
                 PluginScanResponse, PluginLoadResponse, PluginTrustListResponse, CatResponse,
                 ListSessionsResponse, ListTreeDiffResponse, FileHistoryResponse, PruneResponse,
                 ListCollectionsResponse, ListSnapshotsResponse, RestoreCollectionResponse,
                 RestoreSnapshotResponse, GraphQueryResponse, GraphPathHistoryResponse,
                 GraphRepairResponse, GraphValidateResponse, KgIngestResponse,
                 // Streaming events (progress/heartbeats)
                 EmbeddingEvent, ModelLoadEvent>;

// ============================================================================
// Message Envelope
// ============================================================================

struct Message {
    uint32_t version = 1;
    uint64_t requestId;
    std::chrono::steady_clock::time_point timestamp;

    // Payload
    std::variant<Request, Response> payload;

    // Optional fields
    std::optional<std::string> sessionId;
    std::optional<std::string> clientVersion;

    // Streaming preference - client indicates if it expects chunked/streaming response
    bool expectsStreamingResponse = false;
};

// ============================================================================
// Protocol Constants
// ============================================================================

constexpr uint32_t PROTOCOL_VERSION = 2;
constexpr size_t MAX_MESSAGE_SIZE =
    static_cast<size_t>(16) * static_cast<size_t>(1024) * static_cast<size_t>(1024); // 16MB
constexpr size_t HEADER_SIZE = 16; // version(4) + size(4) + requestId(8)

// Message type tags for serialization
enum class MessageType : uint8_t {
    // Requests
    SearchRequest = 1,
    AddRequest = 2,
    GetRequest = 3,
    GetInitRequest = 14,
    GetChunkRequest = 15,
    GetEndRequest = 16,
    DeleteRequest = 4,
    ListRequest = 5,
    ShutdownRequest = 6,
    StatusRequest = 7,
    PingRequest = 8,
    GenerateEmbeddingRequest = 9,
    BatchEmbeddingRequest = 10,
    LoadModelRequest = 11,
    UnloadModelRequest = 12,
    ModelStatusRequest = 13,
    DownloadRequest = 17,
    AddDocumentRequest = 18,
    GrepRequest = 19,
    UpdateDocumentRequest = 20,
    GetStatsRequest = 21,
    CancelRequest = 22,
    PrepareSessionRequest = 23,
    EmbedDocumentsRequest = 24,
    // Session and utility requests
    CatRequest = 25,
    ListSessionsRequest = 26,
    UseSessionRequest = 27,
    AddPathSelectorRequest = 28,
    RemovePathSelectorRequest = 29,
    // Tree diff requests (PBI-043)
    ListTreeDiffRequest = 30,
    // File history request (PBI-043 enhancement)
    FileHistoryRequest = 31,
    // Prune request (PBI-062)
    PruneRequest = 32,
    // Collection and snapshot requests (PBI-066)
    ListCollectionsRequest = 33,
    ListSnapshotsRequest = 34,
    RestoreCollectionRequest = 35,
    RestoreSnapshotRequest = 36,
    // Plugin requests
    PluginScanRequest = 37,
    PluginLoadRequest = 38,
    PluginUnloadRequest = 39,
    PluginTrustListRequest = 40,
    PluginTrustAddRequest = 41,
    PluginTrustRemoveRequest = 42,
    // Graph query requests (PBI-009)
    GraphQueryRequest = 43,
    GraphPathHistoryRequest = 44,
    // Graph maintenance requests (PBI-009 Phase 4.3)
    GraphRepairRequest = 45,
    GraphValidateRequest = 46,

    // Responses
    SearchResponse = 128,
    AddResponse = 129,
    GetResponse = 130,
    GetInitResponse = 139,
    GetChunkResponse = 140,
    StatusResponse = 131,
    SuccessResponse = 132,
    ErrorResponse = 133,
    PongResponse = 134,
    EmbeddingResponse = 135,
    BatchEmbeddingResponse = 136,
    ModelLoadResponse = 137,
    ModelStatusResponse = 138,
    ListResponse = 141,
    DownloadResponse = 142,
    AddDocumentResponse = 143,
    GrepResponse = 144,
    UpdateDocumentResponse = 145,
    GetStatsResponse = 146,
    DeleteResponse = 147,
    PrepareSessionResponse = 148,
    CatResponse = 152,
    ListSessionsResponse = 153,
    // Tree diff responses (PBI-043)
    ListTreeDiffResponse = 154,
    // File history response (PBI-043 enhancement)
    FileHistoryResponse = 155,
    // Prune response (PBI-062)
    PruneResponse = 156,
    // Collection and snapshot responses (PBI-066)
    ListCollectionsResponse = 157,
    ListSnapshotsResponse = 158,
    RestoreCollectionResponse = 159,
    RestoreSnapshotResponse = 160,
    // Plugin responses
    PluginScanResponse = 161,
    PluginLoadResponse = 162,
    PluginTrustListResponse = 163,
    // Graph query responses (PBI-009)
    GraphQueryResponse = 164,
    GraphPathHistoryResponse = 165,
    // Graph maintenance responses (PBI-009 Phase 4.3)
    GraphRepairResponse = 166,
    GraphValidateResponse = 167,
    // KG ingest (PBI-093 Phase 2)
    KgIngestRequest = 70,
    KgIngestResponse = 168,
    // Events
    EmbeddingEvent = 149,
    ModelLoadEvent = 150,
    EmbedDocumentsResponse = 151
};

// ============================================================================
// Helper Functions
// ============================================================================

// Get message type from variant
MessageType getMessageType(const Request& req);
MessageType getMessageType(const Response& res);

// Request name for logging
std::string getRequestName(const Request& req);

} // namespace yams::daemon
