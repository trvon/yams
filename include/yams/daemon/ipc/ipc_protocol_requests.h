#pragma once

#include <yams/daemon/ipc/ipc_protocol_common.h>

namespace yams::daemon {

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
    std::string hashQuery = {};         // Search by file hash

    // Engine-level filtering (parity with app::services::SearchRequest)
    std::string pathPattern = {}; // Glob-like filename/path filter (legacy, single pattern)
    std::vector<std::string> pathPatterns =
        {};                             // Multiple glob patterns (preferred over pathPattern)
    std::vector<std::string> tags = {}; // Filter by tags (presence-based)
    bool matchAllTags = false;          // Require all specified tags
    std::string extension = {};         // File extension filter
    std::string mimeType = {};          // MIME type filter
    std::string fileType = {};          // High-level file type
    bool textOnly{false};               // Text-only filter
    bool binaryOnly{false};             // Binary-only filter
    // Time filters
    std::string createdAfter = {};
    std::string createdBefore = {};
    std::string modifiedAfter = {};
    std::string modifiedBefore = {};
    std::string indexedAfter = {};
    std::string indexedBefore = {};

    int vectorStageTimeoutMs{0};
    int keywordStageTimeoutMs{0};
    int snippetHydrationTimeoutMs{0};

    // Session scoping (controls hot/cold path behavior)
    bool useSession = false;
    std::string sessionName = {};
    bool globalSearch = false;   // Session-isolated memory (PBI-082): bypass session isolation
    bool symbolRank = true;      // Enable automatic symbol ranking boost for code-like queries
    std::string instanceId = {}; // Instance-level isolation (UUID of MCP connection)

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
            << globalSearch << symbolRank << instanceId;
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
        if (auto sr = deser.template read<bool>(); sr) {
            req.symbolRank = sr.value();
        }
        if (auto ii = deser.readString(); ii) {
            req.instanceId = std::move(ii.value());
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
    bool includeContent = true;  // include document content when not metadataOnly
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
        ser << outputPath << metadataOnly << includeContent << static_cast<uint64_t>(maxBytes)
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

        auto includeContentResult = deser.template read<bool>();
        if (!includeContentResult)
            return includeContentResult.error();
        req.includeContent = includeContentResult.value();

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

    // Session scoping
    std::string sessionId;
    std::string instanceId; // Instance-level isolation

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << hash << purge;
        // Extended fields for enhanced protocol
        ser << name << names << pattern << directory;
        ser << force << dryRun << keepRefs << recursive << verbose;
        ser << sessionId << instanceId;
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

        // Session scoping (optional, backward compatible)
        auto sessionIdResult = deser.readString();
        if (sessionIdResult)
            req.sessionId = std::move(sessionIdResult.value());
        if (auto ii = deser.readString(); ii)
            req.instanceId = std::move(ii.value());

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

    // Metadata key-value filters (e.g., {{"pbi", "PBI-080"}, {"task", "hook-export"}})
    std::map<std::string, std::string> metadataFilters;

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

    // Session filtering
    std::string sessionId;  // filter by session ID
    std::string instanceId; // Instance-level isolation

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
    bool matchAllTags = false;    // require all tags vs any tag
    bool matchAllMetadata = true; // AND vs OR for metadata filters (default AND)

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

        // Name pattern and session filtering
        ser << namePattern << sessionId << instanceId;
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

        // Session filtering
        auto sessionIdResult = deser.readString();
        if (!sessionIdResult)
            return sessionIdResult.error();
        req.sessionId = std::move(sessionIdResult.value());

        if (auto ii = deser.readString(); ii)
            req.instanceId = std::move(ii.value());

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
        YAMS_TRY(ipc_detail::readField(deser, req.graceful));
        YAMS_TRY(ipc_detail::readDurationField(deser, req.timeout));
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
        YAMS_TRY(ipc_detail::readField(deser, req.detailed));
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
        YAMS_TRY(ipc_detail::readField(deser, r.targetRequestId));
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
        uint64_t timestamp{};
        YAMS_TRY(ipc_detail::readField(deser, timestamp));
        req.timestamp =
            std::chrono::steady_clock::time_point{std::chrono::steady_clock::duration{timestamp}};
        return req;
    }
};

// ============================================================================
// Embedding Request Types
// ============================================================================

struct GenerateEmbeddingRequest {
    std::string text;
    std::string modelName;
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
        YAMS_TRY(ipc_detail::readFields(deser, req.text, req.modelName, req.normalize));
        return req;
    }
};

struct BatchEmbeddingRequest {
    std::vector<std::string> texts;
    std::string modelName;
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
    std::string modelName;
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
    bool preload = true;          // Keep in hot pool
    std::string optionsJson = {}; // Optional plugin options (e.g., hf.revision/offline)

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
    std::string instanceId;    // Instance-level isolation

    // Content handling options
    std::string mimeType;         // MIME type of the document
    bool disableAutoMime = false; // Disable automatic MIME type detection
    bool noEmbeddings = false;    // Disable automatic embedding generation

    // Gitignore handling
    bool noGitignore = false; // Ignore .gitignore patterns when adding files

    // Sync extraction wait options
    bool waitForProcessing = false; // Wait for text extraction to complete before returning
    int waitTimeoutSeconds = 30;    // Max seconds to wait for extraction (0 = no timeout)

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << path << content << name << tags << metadata << recursive << includeHidden
            << includePatterns << excludePatterns << collection << snapshotId << snapshotLabel
            << sessionId << mimeType << disableAutoMime << noEmbeddings << noGitignore
            << waitForProcessing << waitTimeoutSeconds << instanceId;
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

        // Read sync wait options (backward compatible - default if not present)
        auto waitProc = deser.template read<bool>();
        if (waitProc) {
            req.waitForProcessing = waitProc.value();
        }

        auto waitTimeout = deser.template read<int>();
        if (waitTimeout) {
            req.waitTimeoutSeconds = waitTimeout.value();
        }

        if (auto ii = deser.readString(); ii)
            req.instanceId = std::move(ii.value());

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
    std::string instanceId;  // Instance-level isolation

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << pattern << path << paths << caseInsensitive << invertMatch
            << static_cast<int32_t>(contextLines) << static_cast<uint64_t>(maxMatches)
            << includePatterns << recursive << wholeWord << showLineNumbers << showFilename
            << noFilename << countOnly << filesOnly << filesWithoutMatch << pathsOnly << literalText
            << regexOnly << static_cast<uint64_t>(semanticLimit) << filterTags << matchAllTags
            << colorMode << static_cast<int32_t>(beforeContext)
            << static_cast<int32_t>(afterContext) << showDiff << useSession << sessionName
            << instanceId;
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
        if (auto ii = deser.readString(); ii) {
            req.instanceId = std::move(ii.value());
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
    std::string checksum;                        // Optional checksum "<algo>:<hex>"
    std::vector<std::string> tags;               // Tags to apply to downloaded file
    std::map<std::string, std::string> metadata; // Metadata to apply
    bool quiet = false;                          // Suppress info logging

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << url << outputPath << checksum << tags << metadata << quiet;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<DownloadRequest> deserialize(Deserializer& deser) {
        DownloadRequest req;
        YAMS_TRY(ipc_detail::readFields(deser, req.url, req.outputPath, req.checksum, req.tags,
                                        req.metadata, req.quiet));
        return req;
    }
};

struct DownloadStatusRequest {
    std::string jobId;

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << jobId;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<DownloadStatusRequest> deserialize(Deserializer& deser) {
        DownloadStatusRequest req;
        YAMS_TRY(ipc_detail::readField(deser, req.jobId));
        return req;
    }
};

struct CancelDownloadJobRequest {
    std::string jobId;

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << jobId;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<CancelDownloadJobRequest> deserialize(Deserializer& deser) {
        CancelDownloadJobRequest req;
        YAMS_TRY(ipc_detail::readField(deser, req.jobId));
        return req;
    }
};

struct ListDownloadJobsRequest {
    uint32_t limit{20};

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << limit;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<ListDownloadJobsRequest> deserialize(Deserializer& deser) {
        ListDownloadJobsRequest req;
        auto lim = deser.template read<uint32_t>();
        if (!lim)
            return lim.error();
        req.limit = lim.value();
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

// Snapshot requests (collections use generic metadata query via getMetadataValueCounts)
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

    // List-relations mode: when true, return relation type statistics
    bool listRelations{false};

    // Search mode: search nodes by label pattern (fuzzy/prefix match)
    bool searchMode{false};
    std::string searchPattern; // Search pattern for node labels

    // Traversal options
    std::vector<std::string> relationFilters; // Empty = all relations
    int32_t maxDepth{1};                      // BFS depth limit (1-4)
    uint32_t maxResults{200};                 // Total result cap
    uint32_t maxResultsPerDepth{100};         // Per-depth cap
    bool reverseTraversal{false};             // Traverse incoming edges instead of outgoing
    bool isolatedMode{false};     // Find isolated nodes (no incoming edges of relationFilters[0])
    std::string isolatedRelation; // Relation to check for isolation (default: "calls")

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
            << nodeKey << listTypes << listRelations << searchMode << searchPattern
            << relationFilters << maxDepth << maxResults << maxResultsPerDepth << reverseTraversal
            << isolatedMode << isolatedRelation << scopeToSnapshot << offset << limit
            << includeEdgeProperties << includeNodeProperties << hydrateFully;
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

        // listRelations mode
        if (auto r = deser.template read<bool>(); r)
            req.listRelations = r.value();
        else
            return r.error();

        // searchMode and searchPattern
        if (auto r = deser.template read<bool>(); r)
            req.searchMode = r.value();
        else
            return r.error();

        if (auto r = deser.readString(); r)
            req.searchPattern = std::move(r.value());
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

struct GraphExploreRequest {
    std::string query;
    uint64_t maxFiles{8};
    uint64_t maxSymbols{32};
    uint64_t maxTotalChars{24000};
    uint64_t maxCharsPerFile{7000};
    uint64_t maxSnippetLines{160};
    bool includeLineNumbers{true};
    bool includeRelationships{true};
    bool includeCode{true};
    bool includeTests{false};

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << query << maxFiles << maxSymbols << maxTotalChars << maxCharsPerFile
            << maxSnippetLines << includeLineNumbers << includeRelationships << includeCode
            << includeTests;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<GraphExploreRequest> deserialize(Deserializer& deser) {
        GraphExploreRequest req;
        if (auto r = deser.readString(); r)
            req.query = std::move(r.value());
        else
            return r.error();
        if (auto r = deser.template read<uint64_t>(); r)
            req.maxFiles = r.value();
        else
            return r.error();
        if (auto r = deser.template read<uint64_t>(); r)
            req.maxSymbols = r.value();
        else
            return r.error();
        if (auto r = deser.template read<uint64_t>(); r)
            req.maxTotalChars = r.value();
        else
            return r.error();
        if (auto r = deser.template read<uint64_t>(); r)
            req.maxCharsPerFile = r.value();
        else
            return r.error();
        if (auto r = deser.template read<uint64_t>(); r)
            req.maxSnippetLines = r.value();
        else
            return r.error();
        if (auto r = deser.template read<bool>(); r)
            req.includeLineNumbers = r.value();
        else
            return r.error();
        if (auto r = deser.template read<bool>(); r)
            req.includeRelationships = r.value();
        else
            return r.error();
        if (auto r = deser.template read<bool>(); r)
            req.includeCode = r.value();
        else
            return r.error();
        if (auto r = deser.template read<bool>(); r)
            req.includeTests = r.value();
        else
            return r.error();
        return req;
    }
};

struct GraphSymbolLookupRequest {
    std::string symbol;
    bool hasFile{false};
    std::string file;
    bool hasLine{false};
    int32_t line{0};
    uint64_t maxFiles{8};
    uint64_t maxSymbols{32};
    uint64_t maxTotalChars{24000};
    uint64_t maxCharsPerFile{7000};
    uint64_t maxSnippetLines{160};
    bool includeLineNumbers{true};
    bool includeCode{false};

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << symbol << hasFile << file << hasLine << line << maxFiles << maxSymbols
            << maxTotalChars << maxCharsPerFile << maxSnippetLines << includeLineNumbers
            << includeCode;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<GraphSymbolLookupRequest> deserialize(Deserializer& deser) {
        GraphSymbolLookupRequest req;
        if (auto r = deser.readString(); r)
            req.symbol = std::move(r.value());
        else
            return r.error();
        if (auto r = deser.template read<bool>(); r)
            req.hasFile = r.value();
        else
            return r.error();
        if (auto r = deser.readString(); r)
            req.file = std::move(r.value());
        else
            return r.error();
        if (auto r = deser.template read<bool>(); r)
            req.hasLine = r.value();
        else
            return r.error();
        if (auto r = deser.template read<int32_t>(); r)
            req.line = r.value();
        else
            return r.error();
        if (auto r = deser.template read<uint64_t>(); r)
            req.maxFiles = r.value();
        else
            return r.error();
        if (auto r = deser.template read<uint64_t>(); r)
            req.maxSymbols = r.value();
        else
            return r.error();
        if (auto r = deser.template read<uint64_t>(); r)
            req.maxTotalChars = r.value();
        else
            return r.error();
        if (auto r = deser.template read<uint64_t>(); r)
            req.maxCharsPerFile = r.value();
        else
            return r.error();
        if (auto r = deser.template read<uint64_t>(); r)
            req.maxSnippetLines = r.value();
        else
            return r.error();
        if (auto r = deser.template read<bool>(); r)
            req.includeLineNumbers = r.value();
        else
            return r.error();
        if (auto r = deser.template read<bool>(); r)
            req.includeCode = r.value();
        else
            return r.error();
        return req;
    }
};

struct GraphTraceRequest {
    std::string from;
    std::string to;
    uint64_t maxDepth{6};
    uint64_t maxFiles{8};
    uint64_t maxSymbols{32};
    uint64_t maxTotalChars{24000};
    uint64_t maxCharsPerFile{7000};
    uint64_t maxSnippetLines{160};
    bool includeLineNumbers{true};

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << from << to << maxDepth << maxFiles << maxSymbols << maxTotalChars << maxCharsPerFile
            << maxSnippetLines << includeLineNumbers;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<GraphTraceRequest> deserialize(Deserializer& deser) {
        GraphTraceRequest req;
        if (auto r = deser.readString(); r)
            req.from = std::move(r.value());
        else
            return r.error();
        if (auto r = deser.readString(); r)
            req.to = std::move(r.value());
        else
            return r.error();
        if (auto r = deser.template read<uint64_t>(); r)
            req.maxDepth = r.value();
        else
            return r.error();
        if (auto r = deser.template read<uint64_t>(); r)
            req.maxFiles = r.value();
        else
            return r.error();
        if (auto r = deser.template read<uint64_t>(); r)
            req.maxSymbols = r.value();
        else
            return r.error();
        if (auto r = deser.template read<uint64_t>(); r)
            req.maxTotalChars = r.value();
        else
            return r.error();
        if (auto r = deser.template read<uint64_t>(); r)
            req.maxCharsPerFile = r.value();
        else
            return r.error();
        if (auto r = deser.template read<uint64_t>(); r)
            req.maxSnippetLines = r.value();
        else
            return r.error();
        if (auto r = deser.template read<bool>(); r)
            req.includeLineNumbers = r.value();
        else
            return r.error();
        return req;
    }
};

struct GraphImpactRequest {
    std::string symbol;
    uint64_t depth{2};
    uint64_t maxSymbols{32};

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << symbol << depth << maxSymbols;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<GraphImpactRequest> deserialize(Deserializer& deser) {
        GraphImpactRequest req;
        if (auto r = deser.readString(); r)
            req.symbol = std::move(r.value());
        else
            return r.error();
        if (auto r = deser.template read<uint64_t>(); r)
            req.depth = r.value();
        else
            return r.error();
        if (auto r = deser.template read<uint64_t>(); r)
            req.maxSymbols = r.value();
        else
            return r.error();
        return req;
    }
};

struct GraphAffectedTestsRequest {
    std::vector<std::string> changedFiles;
    uint64_t depth{5};
    std::string testPathPattern;

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << changedFiles << depth << testPathPattern;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<GraphAffectedTestsRequest> deserialize(Deserializer& deser) {
        GraphAffectedTestsRequest req;
        if (auto r = deser.readStringVector(); r)
            req.changedFiles = std::move(r.value());
        else
            return r.error();
        if (auto r = deser.template read<uint64_t>(); r)
            req.depth = r.value();
        else
            return r.error();
        if (auto r = deser.readString(); r)
            req.testPathPattern = std::move(r.value());
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

// ============================================================================
// Metadata Value Counts Request (generic metadata query for MCP client mode)
// ============================================================================

struct MetadataValueCountsRequest {
    std::vector<std::string> keys; // e.g., ["collection"]

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << keys;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<MetadataValueCountsRequest> deserialize(Deserializer& deser) {
        MetadataValueCountsRequest req;

        if (auto k = deser.readStringVector(); k)
            req.keys = std::move(k.value());
        else
            return k.error();

        return req;
    }
};

// ============================================================================
// Repair Service Request/Response/Event Types
// ============================================================================

/**
 * @brief Request to run one or more repair operations via the daemon.
 *
 * The daemon's RepairService owns all repair logic.  The CLI builds a
 * RepairRequest from its flags and streams RepairEvent progress back.
 */
struct RepairRequest {
    // Operation flags
    bool repairOrphans{false};
    bool repairMime{false};
    bool repairDownloads{false};
    bool repairPathTree{false};
    bool repairChunks{false};
    bool repairBlockRefs{false};
    bool repairFts5{false};
    bool repairEmbeddings{false};
    bool repairStuckDocs{false};
    bool repairGraph{false};
    bool repairTopology{false};
    bool repairDedupe{false};
    bool optimizeDb{false};
    bool repairAll{false};

    // Options
    bool dryRun{false};
    bool verbose{false};
    bool force{false};
    bool removeCorrupt{false};
    bool foreground{false};
    std::string embeddingModel;
    std::vector<std::string> includeMime;
    int32_t maxRetries{3};

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << repairOrphans << repairMime << repairDownloads << repairPathTree << repairChunks
            << repairBlockRefs << repairFts5 << repairEmbeddings << repairStuckDocs << repairGraph
            << repairTopology << repairDedupe << optimizeDb << repairAll << dryRun << verbose
            << force << removeCorrupt << foreground << embeddingModel << includeMime << maxRetries;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<RepairRequest> deserialize(Deserializer& deser) {
        RepairRequest req;
        auto readBool = [&](bool& v) -> bool {
            if (auto r = deser.template read<bool>(); r) {
                v = r.value();
                return true;
            }
            return false;
        };
        if (!readBool(req.repairOrphans))
            return Error{ErrorCode::SerializationError, "RepairRequest"};
        if (!readBool(req.repairMime))
            return Error{ErrorCode::SerializationError, "RepairRequest"};
        if (!readBool(req.repairDownloads))
            return Error{ErrorCode::SerializationError, "RepairRequest"};
        if (!readBool(req.repairPathTree))
            return Error{ErrorCode::SerializationError, "RepairRequest"};
        if (!readBool(req.repairChunks))
            return Error{ErrorCode::SerializationError, "RepairRequest"};
        if (!readBool(req.repairBlockRefs))
            return Error{ErrorCode::SerializationError, "RepairRequest"};
        if (!readBool(req.repairFts5))
            return Error{ErrorCode::SerializationError, "RepairRequest"};
        if (!readBool(req.repairEmbeddings))
            return Error{ErrorCode::SerializationError, "RepairRequest"};
        if (!readBool(req.repairStuckDocs))
            return Error{ErrorCode::SerializationError, "RepairRequest"};
        if (!readBool(req.repairGraph))
            return Error{ErrorCode::SerializationError, "RepairRequest"};
        if (!readBool(req.repairTopology))
            return Error{ErrorCode::SerializationError, "RepairRequest"};
        if (!readBool(req.repairDedupe))
            return Error{ErrorCode::SerializationError, "RepairRequest"};
        if (!readBool(req.optimizeDb))
            return Error{ErrorCode::SerializationError, "RepairRequest"};
        if (!readBool(req.repairAll))
            return Error{ErrorCode::SerializationError, "RepairRequest"};
        if (!readBool(req.dryRun))
            return Error{ErrorCode::SerializationError, "RepairRequest"};
        if (!readBool(req.verbose))
            return Error{ErrorCode::SerializationError, "RepairRequest"};
        if (!readBool(req.force))
            return Error{ErrorCode::SerializationError, "RepairRequest"};
        if (!readBool(req.removeCorrupt))
            return Error{ErrorCode::SerializationError, "RepairRequest"};
        if (!readBool(req.foreground))
            return Error{ErrorCode::SerializationError, "RepairRequest"};
        if (auto r = deser.readString(); r)
            req.embeddingModel = std::move(r.value());
        else
            return r.error();
        if (auto r = deser.readStringVector(); r)
            req.includeMime = std::move(r.value());
        else
            return r.error();
        if (auto r = deser.template read<int32_t>(); r)
            req.maxRetries = r.value();
        else
            return r.error();
        return req;
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
    std::vector<std::string> tags = {};
    std::map<std::string, std::string> metadata = {};

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

// Forward declaration for batch request type (defined with the batch envelope types)
struct BatchRequest;

// Variant type for all requests
using Request = std::variant<
    SearchRequest, GetRequest, GetInitRequest, GetChunkRequest, GetEndRequest, DeleteRequest,
    ListRequest, ShutdownRequest, StatusRequest, PingRequest, GenerateEmbeddingRequest,
    BatchEmbeddingRequest, LoadModelRequest, UnloadModelRequest, ModelStatusRequest,
    AddDocumentRequest, GrepRequest, UpdateDocumentRequest, DownloadRequest, DownloadStatusRequest,
    CancelDownloadJobRequest, ListDownloadJobsRequest, GetStatsRequest, PrepareSessionRequest,
    EmbedDocumentsRequest, PluginScanRequest, PluginLoadRequest, PluginUnloadRequest,
    PluginTrustListRequest, PluginTrustAddRequest, PluginTrustRemoveRequest, CancelRequest,
    CatRequest, ListSessionsRequest, UseSessionRequest, AddPathSelectorRequest,
    RemovePathSelectorRequest, ListTreeDiffRequest, FileHistoryRequest, PruneRequest,
    ListSnapshotsRequest, RestoreCollectionRequest, RestoreSnapshotRequest, GraphQueryRequest,
    GraphExploreRequest, GraphSymbolLookupRequest, GraphTraceRequest, GraphImpactRequest,
    GraphAffectedTestsRequest, GraphPathHistoryRequest, GraphRepairRequest, GraphValidateRequest,
    KgIngestRequest, MetadataValueCountsRequest, BatchRequest, RepairRequest>;

} // namespace yams::daemon
