#pragma once

#include <yams/core/types.h>

#include <chrono>
#include <filesystem>
#include <map>
#include <string>
#include <type_traits>
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
    size_t limit = 10;
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

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << query << static_cast<uint32_t>(limit) << fuzzy << literalText << similarity
            << timeout << searchType << pathsOnly << showHash << verbose << jsonOutput
            << showLineNumbers << static_cast<int32_t>(afterContext)
            << static_cast<int32_t>(beforeContext) << static_cast<int32_t>(context) << hashQuery;
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

        return req;
    }
};

struct AddRequest {
    std::filesystem::path path;
    std::vector<std::string> tags;
    std::map<std::string, std::string> metadata;
    bool recursive = false;
    std::string includePattern;

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << path.string() << tags << metadata << recursive << includePattern;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<AddRequest> deserialize(Deserializer& deser) {
        AddRequest req;

        auto pathResult = deser.readString();
        if (!pathResult)
            return pathResult.error();
        req.path = pathResult.value();

        auto tagsResult = deser.readStringVector();
        if (!tagsResult)
            return tagsResult.error();
        req.tags = std::move(tagsResult.value());

        auto metadataResult = deser.readStringMap();
        if (!metadataResult)
            return metadataResult.error();
        req.metadata = std::move(metadataResult.value());

        auto recursiveResult = deser.template read<bool>();
        if (!recursiveResult)
            return recursiveResult.error();
        req.recursive = recursiveResult.value();

        auto patternResult = deser.readString();
        if (!patternResult)
            return patternResult.error();
        req.includePattern = std::move(patternResult.value());

        return req;
    }
};

struct GetRequest {
    std::string hash;
    std::string name;
    bool byName = false;

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << hash << name << byName;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<GetRequest> deserialize(Deserializer& deser) {
        GetRequest req;

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
    uint32_t chunkSize = 256 * 1024; // hint for preferred chunk size

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

// Simple list response for daemon-first list command
struct ListEntry {
    std::string hash;
    std::string path;
    std::string name;
    uint64_t size = 0;

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << hash << path << name << static_cast<uint64_t>(size);
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<ListEntry> deserialize(Deserializer& deser) {
        ListEntry e;
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
        auto s = deser.template read<uint64_t>();
        if (!s)
            return s.error();
        e.size = s.value();
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
    std::string hash;
    bool purge = false;

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << hash << purge;
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

        return req;
    }
};

struct ListRequest {
    size_t limit = 20;
    bool recent = true;
    std::vector<std::string> tags;

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << static_cast<uint32_t>(limit) << recent << tags;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<ListRequest> deserialize(Deserializer& deser) {
        ListRequest req;

        auto limitResult = deser.template read<uint32_t>();
        if (!limitResult)
            return limitResult.error();
        req.limit = limitResult.value();

        auto recentResult = deser.template read<bool>();
        if (!recentResult)
            return recentResult.error();
        req.recent = recentResult.value();

        auto tagsResult = deser.readStringVector();
        if (!tagsResult)
            return tagsResult.error();
        req.tags = std::move(tagsResult.value());

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

struct LoadModelRequest {
    std::string modelName;
    bool preload = true; // Keep in hot pool

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << modelName << preload;
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

    // Content handling options
    std::string mimeType;         // MIME type of the document
    bool disableAutoMime = false; // Disable automatic MIME type detection
    bool noEmbeddings = false;    // Disable automatic embedding generation

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << path << content << name << tags << metadata << recursive << includeHidden
            << includePatterns << excludePatterns << collection << snapshotId << snapshotLabel
            << mimeType << disableAutoMime << noEmbeddings;
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

        return req;
    }
};

struct GrepRequest {
    std::string pattern; // Regex pattern
    std::string path;    // Optional path filter
    bool caseInsensitive = false;
    bool invertMatch = false;
    int contextLines = 0;
    size_t maxMatches = 0; // 0 = unlimited

    // Additional fields for feature parity
    std::vector<std::string> includePatterns; // File patterns to include
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
    size_t semanticLimit = 3;                 // Number of semantic results
    std::vector<std::string> filterTags;      // Filter by tags
    bool matchAllTags = false;                // Require all tags
    std::string colorMode = "auto";           // Color output mode
    int beforeContext = 0;                    // Lines before match
    int afterContext = 0;                     // Lines after match

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << pattern << path << caseInsensitive << invertMatch
            << static_cast<int32_t>(contextLines) << static_cast<uint64_t>(maxMatches)
            << includePatterns << wholeWord << showLineNumbers << showFilename << noFilename
            << countOnly << filesOnly << filesWithoutMatch << pathsOnly << literalText << regexOnly
            << static_cast<uint64_t>(semanticLimit) << filterTags << matchAllTags << colorMode
            << static_cast<int32_t>(beforeContext) << static_cast<int32_t>(afterContext);
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

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << hash << name << newContent << addTags << removeTags << metadata;
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
    bool detailed = false;     // Include detailed breakdown
    bool includeCache = false; // Include cache statistics

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << detailed << includeCache;
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
        return req;
    }
};

// Variant type for all requests
using Request =
    std::variant<SearchRequest, AddRequest, GetRequest, GetInitRequest, GetChunkRequest,
                 GetEndRequest, DeleteRequest, ListRequest, ShutdownRequest, StatusRequest,
                 PingRequest, GenerateEmbeddingRequest, BatchEmbeddingRequest, LoadModelRequest,
                 UnloadModelRequest, ModelStatusRequest, AddDocumentRequest, GrepRequest,
                 UpdateDocumentRequest, DownloadRequest, GetStatsRequest>;

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
    size_t totalCount;
    std::chrono::milliseconds elapsed;

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

struct GetResponse {
    std::string content;
    std::string hash;
    std::map<std::string, std::string> metadata;

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << content << hash << metadata;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<GetResponse> deserialize(Deserializer& deser) {
        GetResponse res;

        auto contentResult = deser.readString();
        if (!contentResult)
            return contentResult.error();
        res.content = std::move(contentResult.value());

        auto hashResult = deser.readString();
        if (!hashResult)
            return hashResult.error();
        res.hash = std::move(hashResult.value());

        auto metadataResult = deser.readStringMap();
        if (!metadataResult)
            return metadataResult.error();
        res.metadata = std::move(metadataResult.value());

        return res;
    }
};

struct StatusResponse {
    bool running = true;
    bool ready;
    size_t uptimeSeconds;
    size_t requestsProcessed;
    size_t activeConnections;
    double memoryUsageMb;
    double cpuUsagePercent;
    std::string version;
    std::map<std::string, size_t> requestCounts;

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

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << running << ready << static_cast<uint64_t>(uptimeSeconds)
            << static_cast<uint64_t>(requestsProcessed) << static_cast<uint64_t>(activeConnections)
            << memoryUsageMb << cpuUsagePercent << version;

        // Serialize request counts map
        ser << static_cast<uint32_t>(requestCounts.size());
        for (const auto& [key, value] : requestCounts) {
            ser << key << static_cast<uint64_t>(value);
        }

        // Serialize models
        ser << static_cast<uint32_t>(models.size());
        for (const auto& model : models) {
            model.serialize(ser);
        }
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

        return res;
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
    std::string hash;          // Document hash
    std::string path;          // Stored path
    size_t size = 0;           // Document size
    size_t documentsAdded = 0; // For recursive adds

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << hash << path << static_cast<uint64_t>(size) << static_cast<uint64_t>(documentsAdded);
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
        auto s = deser.template read<uint64_t>();
        if (!s)
            return s.error();
        res.size = s.value();
        auto d = deser.template read<uint64_t>();
        if (!d)
            return d.error();
        res.documentsAdded = d.value();
        return res;
    }
};

struct GrepMatch {
    std::string file;
    size_t lineNumber = 0;
    std::string line;
    std::vector<std::string> contextBefore;
    std::vector<std::string> contextAfter;

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << file << static_cast<uint64_t>(lineNumber) << line << contextBefore << contextAfter;
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
        return m;
    }
};

struct GrepResponse {
    std::vector<GrepMatch> matches;
    size_t totalMatches = 0;
    size_t filesSearched = 0;

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << static_cast<uint32_t>(matches.size());
        for (const auto& m : matches)
            m.serialize(ser);
        ser << static_cast<uint64_t>(totalMatches) << static_cast<uint64_t>(filesSearched);
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<GrepResponse> deserialize(Deserializer& deser) {
        GrepResponse res;
        auto cnt = deser.template read<uint32_t>();
        if (!cnt)
            return cnt.error();
        res.matches.reserve(cnt.value());
        for (uint32_t i = 0; i < cnt.value(); ++i) {
            auto m = GrepMatch::deserialize(deser);
            if (!m)
                return m.error();
            res.matches.push_back(std::move(m.value()));
        }
        auto tm = deser.template read<uint64_t>();
        if (!tm)
            return tm.error();
        res.totalMatches = tm.value();
        auto fs = deser.template read<uint64_t>();
        if (!fs)
            return fs.error();
        res.filesSearched = fs.value();
        return res;
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

// Variant type for all responses
using Response =
    std::variant<SearchResponse, AddResponse, GetResponse, GetInitResponse, GetChunkResponse,
                 StatusResponse, SuccessResponse, ErrorResponse, PongResponse, EmbeddingResponse,
                 BatchEmbeddingResponse, ModelLoadResponse, ModelStatusResponse, ListResponse,
                 AddDocumentResponse, GrepResponse, UpdateDocumentResponse, DownloadResponse,
                 GetStatsResponse>;

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
};

// ============================================================================
// Protocol Constants
// ============================================================================

constexpr uint32_t PROTOCOL_VERSION = 1;
constexpr size_t MAX_MESSAGE_SIZE = 16 * 1024 * 1024; // 16MB
constexpr size_t HEADER_SIZE = 16;                    // version(4) + size(4) + requestId(8)

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
    GetStatsResponse = 146
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
