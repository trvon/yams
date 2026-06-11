#include <yams/app/services/services.hpp>
#include <yams/common/fs_utils.h>
#include <yams/common/time_utils.h>
#include <yams/core/assert.hpp>
#include <yams/core/uuid.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/WriteBatchCoalescer.h>
#include <yams/daemon/components/WriteCoordinator.h>
#include <yams/metadata/versioning_util.h>
// Hot/Cold mode helpers (env-driven)
#include "../../cli/hot_cold_utils.h"

#include <spdlog/spdlog.h>
#include <yams/api/content_store.h>
#include <yams/app/services/graph_query_service.hpp>
#include <yams/app/services/path_projection.hpp>
#include <yams/app/services/service_utils.hpp>
#include <yams/compression/compression_header.h>
#include <yams/compression/compression_utils.h>
#include <yams/compression/compressor_interface.h>
#include <yams/detection/file_type_detector.h>
#include <yams/extraction/extraction_util.h>
#include <yams/extraction/format_handlers/format_handler.hpp>
#include <yams/extraction/format_handlers/text_basic_handler.hpp>
#include <yams/extraction/text_extractor.h>

#include <yams/core/cpp23_features.hpp>
#include <yams/ingest/ingest_helpers.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/path_utils.h>
#include <yams/metadata/query_helpers.h>
#include <yams/vector/vector_database.h>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstddef>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <optional>
#include <span>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace yams::app::services {

namespace {

daemon::WriteBatchCoalescer& getVersioningCoalescer() {
    static daemon::WriteBatchCoalescer instance("doc_svc/versioning");
    return instance;
}

daemon::WriteBatchCoalescer& getKgSyncCoalescer() {
    static daemon::WriteBatchCoalescer instance("doc_svc/kg_sync");
    return instance;
}

} // namespace

namespace {

constexpr std::size_t kCentroidPreviewLimit = 16;

// Use project compatibility helpers from cpp23_features.hpp
using yams::features::string_starts_with;

// Extract searchable terms from a document for fuzzy search indexing.
// Extracts: filename stem (without extension) and file extension.
// Terms shorter than 2 characters are skipped to reduce noise.
inline std::vector<std::string>
collectDocumentTermsForFuzzySearch(const metadata::DocumentInfo& info) {
    constexpr size_t kMinTermLength = 2;

    std::vector<std::string> terms;
    auto addTerm = [&](std::string_view term) {
        if (term.length() >= kMinTermLength) {
            terms.emplace_back(term);
        }
    };

    // Add filename without extension
    if (!info.fileName.empty()) {
        std::filesystem::path fp(info.fileName);
        auto stem = fp.stem().string();
        addTerm(stem);

        // Also add extension without the dot (e.g., "cpp", "py")
        auto ext = fp.extension().string();
        if (ext.length() > 1) {     // Has extension beyond just "."
            addTerm(ext.substr(1)); // Skip the leading dot
        }
    }

    return terms;
}

inline void enqueueDocumentTermsForFuzzySearch(daemon::WriteCoordinator* writeCoordinator,
                                               const metadata::DocumentInfo& info) {
    if (!writeCoordinator) {
        spdlog::debug("WriteCoordinator unavailable; skipping SymSpell terms for {}",
                      info.filePath);
        return;
    }

    auto terms = collectDocumentTermsForFuzzySearch(info);
    if (terms.empty()) {
        return;
    }

    auto batch = std::make_unique<daemon::WriteBatch>();
    batch->source = "doc_svc/symspell";
    batch->ops.emplace_back(daemon::AddSymSpellTermsOp{std::move(terms)});
    writeCoordinator->enqueue(std::move(batch));
}

inline void addTagPairsToMap(const std::vector<std::string>& tags,
                             std::unordered_map<std::string, std::string>& out) {
    for (const auto& t : tags) {
        if (t.empty())
            continue;
        // Store tags under a normalized key with value equal to the tag itself.
        // Using key "tag:<name>" enables efficient tag queries and consistent extraction,
        // while setting the value to the tag ensures UI/reporting shows plain names.
        out[std::string("tag:") + t] = t;
    }
}

inline void addMetadataToMap(const std::unordered_map<std::string, std::string>& kv,
                             std::unordered_map<std::string, std::string>& out) {
    for (const auto& [k, v] : kv) {
        out[k] = v;
    }
}

// Returns true if s consists only of hex digits
inline bool isHex(const std::string& s) {
    return std::all_of(s.begin(), s.end(), [](unsigned char c) { return std::isxdigit(c) != 0; });
}

// Heuristic: treat as hash when it looks like a hex string of reasonable length (6-64)
inline bool looksLikePartialHash(const std::string& s) {
    if (s.size() < 6 || s.size() >= 64)
        return false;
    return isHex(s);
}

// ============================================================================
// DocumentResolver: Unified document resolution with strategy chain
// ============================================================================
// Resolution strategies are tried in priority order:
//   1. FileName - Exact match on file_name column (for --name added documents)
//   2. TreeLookup - Path tree index lookup (PBI-053)
//   3. ExactPath - Exact match on file_path column
//   4. PathSuffix - Pattern matching for relative paths
//   5. HashPrefix - Partial hash matching (only for explicit hash queries)
//
// This class provides two main methods:
//   - resolveToHash(): Returns single hash (errors on ambiguous results)
//   - resolveAll(): Returns all matching documents (for batch operations)
// ============================================================================

template <typename Repo> class DocumentResolver {
public:
    struct ResolveOptions {
        bool oldest = false;        // Return oldest match instead of newest
        bool tryHashPrefix = false; // Whether to attempt hash prefix resolution
    };

    explicit DocumentResolver(Repo& repo) : repo_(repo) {}

    // Single document resolution - returns first/best match or error
    Result<std::string> resolveToHash(const std::string& query, const ResolveOptions& opts = {}) {
        spdlog::info("[RESOLVE] Starting resolution for: '{}'", query);

        // Strategy 1: FileName exact match (highest priority for --name documents)
        if (auto hash = tryFileName(query)) {
            spdlog::info("[RESOLVE] ✓ Resolved via fileName exact match");
            return *hash;
        }

        // Strategy 2: Tree-based lookup (PBI-053)
        if (auto hash = tryTreeLookup(query)) {
            spdlog::info("[RESOLVE] ✓ Resolved via path tree");
            return *hash;
        }

        // Strategy 3: Exact path match
        if (auto hash = tryExactPath(query)) {
            spdlog::info("[RESOLVE] ✓ Resolved via exact path");
            return *hash;
        }

        // Strategy 4: Path pattern matching (suffix/relative)
        auto patternMatches = tryPathPatterns(query);
        if (!patternMatches.empty()) {
            if (patternMatches.size() == 1) {
                spdlog::info("[RESOLVE] ✓ Resolved via path pattern (single match)");
                return patternMatches[0].sha256Hash;
            }
            // Multiple matches - sort and return based on opts
            return selectFromMultiple(patternMatches, opts);
        }

        // Strategy 5: Hash prefix (only if explicitly requested)
        if (opts.tryHashPrefix && looksLikePartialHash(query)) {
            auto hashMatches = tryHashPrefix(query);
            if (!hashMatches.empty()) {
                if (hashMatches.size() == 1) {
                    spdlog::info("[RESOLVE] ✓ Resolved via hash prefix (single match)");
                    return hashMatches[0].sha256Hash;
                }
                return Error{ErrorCode::InvalidOperation,
                             "Ambiguous hash prefix: " + std::to_string(hashMatches.size()) +
                                 " matches for '" + query + "'"};
            }
        }

        spdlog::warn("[RESOLVE] ❌ No matches found for: '{}'", query);
        return Error{ErrorCode::NotFound, "Document not found: " + query};
    }

    // Multi-document resolution - returns all matches (for delete, batch ops)
    Result<std::vector<metadata::DocumentInfo>> resolveAll(const std::string& query,
                                                           const ResolveOptions& opts = {}) {
        std::vector<metadata::DocumentInfo> results;
        std::unordered_set<std::string> seenHashes;

        auto addUnique = [&](const std::vector<metadata::DocumentInfo>& docs) {
            for (const auto& doc : docs) {
                if (seenHashes.insert(doc.sha256Hash).second) {
                    results.push_back(doc);
                }
            }
        };

        // Try fileName match
        if (auto docs = tryFileNameAll(query); !docs.empty()) {
            addUnique(docs);
        }

        // Try exact path
        auto exactRes = repo_.findDocumentByExactPath(query);
        if (exactRes && exactRes.value().has_value()) {
            std::vector<metadata::DocumentInfo> v = {exactRes.value().value()};
            addUnique(v);
        }

        // Try path patterns
        addUnique(tryPathPatterns(query));

        // Try hash prefix if requested
        if (opts.tryHashPrefix && looksLikePartialHash(query)) {
            addUnique(tryHashPrefix(query));
        }

        if (results.empty()) {
            return Error{ErrorCode::NotFound, "No documents found matching: " + query};
        }

        return results;
    }

private:
    // Strategy implementations

    std::optional<std::string> tryFileName(const std::string& name) {
        metadata::DocumentQueryOptions opts;
        opts.fileName = name;
        opts.limit = 1;
        auto res = repo_.queryDocuments(opts);
        if (res && !res.value().empty()) {
            return res.value()[0].sha256Hash;
        }
        return std::nullopt;
    }

    std::vector<metadata::DocumentInfo> tryFileNameAll(const std::string& name) {
        metadata::DocumentQueryOptions opts;
        opts.fileName = name;
        auto res = repo_.queryDocuments(opts);
        if (res) {
            return res.value();
        }
        return {};
    }

    std::optional<std::string> tryTreeLookup(const std::string& path) {
        std::string normalized = path;
        std::replace(normalized.begin(), normalized.end(), '\\', '/');

        auto treeNodeRes = repo_.findPathTreeNodeByFullPath(normalized);
        if (!treeNodeRes || !treeNodeRes.value().has_value()) {
            return std::nullopt;
        }

        const auto& node = treeNodeRes.value().value();
        auto docRes = repo_.findDocumentByExactPath(node.fullPath);
        if (docRes && docRes.value().has_value()) {
            return docRes.value().value().sha256Hash;
        }
        return std::nullopt;
    }

    std::optional<std::string> tryExactPath(const std::string& path) {
        auto res = repo_.findDocumentByExactPath(path);
        if (res && res.value().has_value()) {
            return res.value().value().sha256Hash;
        }
        return std::nullopt;
    }

    std::vector<metadata::DocumentInfo> tryPathPatterns(const std::string& name) {
        // Glob shortcut: if name contains glob chars, use glob-to-SQL conversion
        if (name.find('*') != std::string::npos || name.find('?') != std::string::npos) {
            std::vector<std::string> patterns = {name};
            // If the glob is relative, also try matching as a suffix of absolute paths
            if (!name.empty() && name[0] != '/') {
                patterns.push_back("**/" + name);
            }
            auto res = metadata::queryDocumentsByGlobPatterns(repo_, patterns);
            if (res)
                return res.value();
            return {};
        }

        std::vector<std::string> patterns;
        std::filesystem::path inputPath(name);
        std::string basename = inputPath.filename().string();

        // Basename with path prefix (for filename-only queries)
        if (!basename.empty() && name.find('/') == std::string::npos &&
            name.find('\\') == std::string::npos) {
            patterns.push_back("%/" + basename);
        }

        // Just basename if different from input
        if (!basename.empty() && basename != name) {
            patterns.push_back(basename);
        }

        // Full path as suffix
        if (name.find('/') != std::string::npos || name.find('\\') != std::string::npos) {
            patterns.push_back("%" + name);
        }

        // For absolute paths, try progressive suffix patterns
        if (!name.empty() && (name[0] == '/' || name.find(":\\") != std::string::npos)) {
            try {
                std::string pathStr = name;
                std::replace(pathStr.begin(), pathStr.end(), '\\', '/');
                size_t pos = 0;
                while ((pos = pathStr.find('/', pos + 1)) != std::string::npos) {
                    std::string suffix = pathStr.substr(pos + 1);
                    if (!suffix.empty() && suffix != basename) {
                        patterns.push_back("%" + suffix);
                    }
                }
            } catch (...) {
                // Ignore path manipulation errors
            }
        }

        // Execute patterns and collect results
        std::vector<metadata::DocumentInfo> allMatches;
        std::unordered_set<std::string> seenHashes;

        for (const auto& pattern : patterns) {
            auto res = metadata::queryDocumentsByPattern(repo_, pattern);
            if (res) {
                for (auto& doc : res.value()) {
                    if (seenHashes.insert(doc.sha256Hash).second) {
                        allMatches.push_back(std::move(doc));
                    }
                }
            }
        }

        return allMatches;
    }

    std::vector<metadata::DocumentInfo> tryHashPrefix(const std::string& prefix) {
        // Use efficient hash prefix search instead of loading all documents
        auto matchResult = repo_.findDocumentsByHashPrefix(prefix, 100);
        if (matchResult) {
            return matchResult.value();
        }
        return {};
    }

    Result<std::string> selectFromMultiple(std::vector<metadata::DocumentInfo>& matches,
                                           const ResolveOptions& opts) {
        const char* strategy = opts.oldest ? "oldest" : "most recent";
        spdlog::warn("[RESOLVE] ⚠️ Ambiguous: {} matches - returning {}", matches.size(), strategy);

        std::sort(matches.begin(), matches.end(),
                  [&opts](const metadata::DocumentInfo& a, const metadata::DocumentInfo& b) {
                      return opts.oldest ? (a.indexedTime < b.indexedTime)
                                         : (a.indexedTime > b.indexedTime);
                  });

        spdlog::info("[RESOLVE] ✓ Selected {}: {} (indexed: {})", strategy, matches[0].fileName,
                     matches[0].indexedTime.time_since_epoch().count());

        return matches[0].sha256Hash;
    }

    Repo& repo_;
};

struct CompressedPayload {
    std::string blob;
    compression::CompressionHeader header;
};

Result<CompressedPayload> makeCompressedPayload(std::span<const std::byte> data) {
    auto& registry = compression::CompressionRegistry::instance();
    auto compressor = registry.createCompressor(compression::CompressionAlgorithm::Zstandard);
    if (!compressor) {
        return Error{ErrorCode::InvalidState, "Zstandard compressor unavailable"};
    }

    constexpr uint8_t kDefaultLevel = 3;
    auto compressedResult = compressor->compress(data, kDefaultLevel);
    if (!compressedResult) {
        return compressedResult.error();
    }

    const auto& compressedVal = compressedResult.value();

    compression::CompressionHeader header{};
    header.magic = compression::CompressionHeader::MAGIC;
    header.version = compression::CompressionHeader::VERSION;
    // Use the actual algorithm from compression result - may be None if compression was ineffective
    header.algorithm = static_cast<uint8_t>(compressedVal.algorithm);
    header.level = compressedVal.level;
    header.uncompressedSize = static_cast<uint64_t>(compressedVal.originalSize);
    header.compressedSize = static_cast<uint64_t>(compressedVal.compressedSize);
    header.uncompressedCRC32 = compression::calculateCRC32(data);
    auto compressedSpan =
        std::span<const std::byte>(compressedVal.data.data(), compressedVal.data.size());
    header.compressedCRC32 = compression::calculateCRC32(compressedSpan);
    const auto nowNs = std::chrono::duration_cast<std::chrono::nanoseconds>(
                           std::chrono::system_clock::now().time_since_epoch())
                           .count();
    header.timestamp = nowNs < 0 ? 0ULL : static_cast<uint64_t>(nowNs);
    header.flags = 0;
    header.reserved1 = 0;
    std::memset(header.reserved2, 0, sizeof(header.reserved2));

    CompressedPayload payload;
    payload.blob.resize(sizeof(header) + compressedVal.data.size());
    std::memcpy(payload.blob.data(), &header, sizeof(header));
    std::memcpy(payload.blob.data() + sizeof(header), compressedVal.data.data(),
                compressedVal.data.size());
    payload.header = header;
    return payload;
}

inline void resetCompressionMetadata(RetrievedDocument& doc) {
    doc.compressed = false;
    doc.compressionAlgorithm.reset();
    doc.compressionLevel.reset();
    doc.uncompressedSize.reset();
    doc.compressedCrc32.reset();
    doc.uncompressedCrc32.reset();
    doc.compressionHeader.clear();
}

inline void applyCompressionMetadata(RetrievedDocument& doc,
                                     const compression::CompressionHeader& header) {
    const auto* headerBytes = reinterpret_cast<const std::byte*>(&header);

    uint64_t uncompressedSize = 0;
    uint32_t compressedCrc32 = 0;
    uint32_t uncompressedCrc32 = 0;

    std::memcpy(&uncompressedSize,
                headerBytes + offsetof(compression::CompressionHeader, uncompressedSize),
                sizeof(uncompressedSize));
    std::memcpy(&compressedCrc32,
                headerBytes + offsetof(compression::CompressionHeader, compressedCRC32),
                sizeof(compressedCrc32));
    std::memcpy(&uncompressedCrc32,
                headerBytes + offsetof(compression::CompressionHeader, uncompressedCRC32),
                sizeof(uncompressedCrc32));

    doc.compressed = true;
    doc.compressionAlgorithm = header.algorithm;
    doc.compressionLevel = header.level;
    doc.uncompressedSize = uncompressedSize;
    doc.compressedCrc32 = compressedCrc32;
    doc.uncompressedCrc32 = uncompressedCrc32;
    const auto* bytes = reinterpret_cast<const uint8_t*>(headerBytes);
    doc.compressionHeader.assign(bytes, bytes + sizeof(header));
    doc.size = uncompressedSize;
}

inline void markUncompressed(RetrievedDocument& doc, uint64_t size) {
    doc.compressed = false;
    doc.compressionAlgorithm.reset();
    doc.compressionLevel.reset();
    doc.compressedCrc32.reset();
    doc.uncompressedCrc32.reset();
    doc.compressionHeader.clear();
    doc.uncompressedSize = size;
    doc.size = size;
}

inline std::string makeTempFilePathFor(const std::string& name) {
    namespace fs = std::filesystem;
    auto tmpDir = fs::temp_directory_path();
    // sanitize name
    std::string safe = name;
#ifdef _WIN32
    std::replace(safe.begin(), safe.end(), '\\', '_');
#endif
    std::replace(safe.begin(), safe.end(), '/', '_');
    auto now = std::chrono::high_resolution_clock::now().time_since_epoch().count();
    fs::path tmp = tmpDir / (safe + "." + std::to_string(now) + ".yams.tmp");
    return tmp.string();
}

// Basic "tags" extraction heuristic from metadata rows
inline std::vector<std::string>
extractTags(const std::unordered_map<std::string, yams::metadata::MetadataValue>& all) {
    std::vector<std::string> tags;
    for (const auto& [key, value] : all) {
        if (key == "tag" || string_starts_with(key, "tag:")) {
            const auto s = value.asString();
            if (!s.empty())
                tags.push_back(s);
            else
                tags.push_back(key);
        }
    }
    return tags;
}

} // namespace

class DocumentServiceImpl final : public IDocumentService {
private:
    struct DeleteTarget {
        std::optional<metadata::DocumentInfo> doc;
        std::string hash;
        std::string name;
    };

    using MetadataMap = std::unordered_map<std::string, metadata::MetadataValue>;
    using MetadataCache = std::unordered_map<int64_t, MetadataMap>;
    using SnippetPreviewCache = std::unordered_map<int64_t, std::string>;

    static void addUniqueDeleteTargets(std::vector<DeleteTarget>& targets,
                                       std::unordered_set<std::string>& seenHashes,
                                       const std::vector<metadata::DocumentInfo>& docs) {
        for (const auto& doc : docs) {
            if (seenHashes.insert(doc.sha256Hash).second) {
                DeleteTarget target;
                target.doc = doc;
                target.hash = doc.sha256Hash;
                target.name = doc.fileName.empty() ? doc.filePath : doc.fileName;
                targets.push_back(std::move(target));
            }
        }
    }

    static Result<void>
    addResolvedDeleteTargets(std::vector<DeleteTarget>& targets,
                             std::unordered_set<std::string>& seenHashes,
                             const DeleteByNameRequest& req, const std::string& selector,
                             const Result<std::vector<metadata::DocumentInfo>>& resolved) {
        if (!resolved) {
            return Result<void>();
        }
        if (!req.force && resolved.value().size() > 1) {
            return Error{ErrorCode::InvalidOperation, "Multiple documents match '" + selector +
                                                          "'; use --force to delete all matches "
                                                          "or specify the exact hash"};
        }
        addUniqueDeleteTargets(targets, seenHashes, resolved.value());
        return Result<void>();
    }

    Result<std::vector<DeleteTarget>> resolveDeleteTargets(const DeleteByNameRequest& req) {
        DocumentResolver<metadata::IMetadataRepository> resolver(*ctx_.metadataRepo);
        typename DocumentResolver<metadata::IMetadataRepository>::ResolveOptions resolveOpts;
        resolveOpts.tryHashPrefix = true;

        std::vector<DeleteTarget> targets;
        std::unordered_set<std::string> seenHashes;
        bool rawFullHashWithoutMetadata = false;

        if (!req.hash.empty()) {
            if (req.hash.size() == 64) {
                auto docRes = ctx_.metadataRepo->getDocumentByHash(req.hash);
                if (docRes && docRes.value().has_value()) {
                    addUniqueDeleteTargets(targets, seenHashes, {docRes.value().value()});
                } else if (docRes && isHex(req.hash)) {
                    rawFullHashWithoutMetadata = true;
                }
            } else {
                auto matchResult = ctx_.metadataRepo->findDocumentsByHashPrefix(req.hash, 100);
                if (matchResult) {
                    addUniqueDeleteTargets(targets, seenHashes, matchResult.value());
                }
            }
        }

        if (!req.pattern.empty()) {
            auto pat = globToSqlLike(req.pattern);
            auto res = metadata::queryDocumentsByPattern(*ctx_.metadataRepo, pat);
            if (res && !res.value().empty()) {
                addUniqueDeleteTargets(targets, seenHashes, res.value());
            }
        }

        for (const auto& n : req.names) {
            auto added = addResolvedDeleteTargets(targets, seenHashes, req, n,
                                                  resolver.resolveAll(n, resolveOpts));
            if (!added) {
                return added.error();
            }
        }

        if (!req.name.empty()) {
            auto added = addResolvedDeleteTargets(targets, seenHashes, req, req.name,
                                                  resolver.resolveAll(req.name, resolveOpts));
            if (!added) {
                return added.error();
            }
        }

        if (rawFullHashWithoutMetadata && !seenHashes.contains(req.hash)) {
            DeleteTarget target;
            target.hash = req.hash;
            target.name = "hash:" + req.hash.substr(0, 8) + "...";
            targets.push_back(std::move(target));
        }

        return targets;
    }

    std::optional<MetadataMap> captureMetadataForDelete(const metadata::DocumentInfo& doc) {
        auto metadata = ctx_.metadataRepo->getAllMetadata(doc.id);
        if (!metadata) {
            spdlog::warn("Failed to capture metadata for delete rollback on {}: {}", doc.sha256Hash,
                         metadata.error().message);
            return std::nullopt;
        }
        return std::move(metadata.value());
    }

    void restoreMetadataAfterContentDeleteFailure(const metadata::DocumentInfo& doc,
                                                  const std::optional<MetadataMap>& metadata) {
        auto restoredId = ctx_.metadataRepo->insertDocument(doc);
        if (!restoredId) {
            spdlog::error("Failed to restore metadata for {} after content delete failure: {}",
                          doc.sha256Hash, restoredId.error().message);
            return;
        }
        if (metadata && !metadata->empty()) {
            std::vector<std::tuple<int64_t, std::string, metadata::MetadataValue>> entries;
            entries.reserve(metadata->size());
            for (const auto& [key, value] : *metadata) {
                entries.emplace_back(restoredId.value(), key, value);
            }
            auto restoredMetadata = ctx_.metadataRepo->setMetadataBatch(entries);
            if (!restoredMetadata) {
                spdlog::warn(
                    "Failed to restore metadata fields for {} after content delete failure: {}",
                    doc.sha256Hash, restoredMetadata.error().message);
            }
        }
    }

    bool cleanupMetadataForDelete(const metadata::DocumentInfo& doc, DeleteByNameResult& r) {
        if (ctx_.vectorDatabase && ctx_.vectorDatabase->isInitialized()) {
            if (!ctx_.vectorDatabase->deleteVectorsByDocument(doc.sha256Hash)) {
                spdlog::warn("Failed to delete vectors for document {}: {}", doc.sha256Hash,
                             ctx_.vectorDatabase->getLastError());
            }
        }

        auto metaResult = ctx_.metadataRepo->deleteDocument(doc.id);
        if (!metaResult) {
            r.error = "Failed to delete metadata: " + metaResult.error().message;
            return false;
        }

        auto lingering = ctx_.metadataRepo->getDocumentByHash(doc.sha256Hash);
        if (!lingering) {
            r.error = "Failed to verify metadata deletion: " + lingering.error().message;
            return false;
        }
        if (lingering.value().has_value()) {
            auto retryDelete = ctx_.metadataRepo->deleteDocument(lingering.value()->id);
            if (!retryDelete) {
                r.error = "Failed to delete lingering metadata: " + retryDelete.error().message;
                return false;
            }
        }

        if (ctx_.kgStore) {
            auto kgResult = ctx_.kgStore->deleteNodesForDocumentHash(doc.sha256Hash);
            if (!kgResult) {
                spdlog::warn("Failed to clean up KG nodes for document {}: {}", doc.sha256Hash,
                             kgResult.error().message);
            }
        }
        return true;
    }

    void executeDeleteTarget(const DeleteByNameRequest& req, const DeleteTarget& target,
                             DeleteByNameResponse& resp) {
        DeleteByNameResult r;
        r.name = target.name;
        r.hash = target.hash;

        if (req.dryRun) {
            r.deleted = false;
            resp.deleted.push_back(r);
            return;
        }

        if (target.doc.has_value()) {
            const auto metadataBeforeDelete = captureMetadataForDelete(*target.doc);
            if (!cleanupMetadataForDelete(*target.doc, r)) {
                // Content deletion is intentionally ordered after metadata cleanup so a metadata
                // failure cannot leave the index pointing at missing content.
                YAMS_DCHECK(!r.contentRemoved, "content removed before metadata cleanup failed");
                resp.errors.push_back(r);
                return;
            }

            auto storeResult = ctx_.store->remove(target.hash);
            if (!storeResult) {
                const bool canForceMetadataCleanup =
                    req.force &&
                    storeResult.error().message.find("Corrupted data") != std::string::npos;
                if (!canForceMetadataCleanup) {
                    restoreMetadataAfterContentDeleteFailure(*target.doc, metadataBeforeDelete);
                    r.deleted = false;
                    r.error = storeResult.error().message;
                    resp.errors.push_back(r);
                    return;
                }
                spdlog::warn("Storage data is corrupted for {}. Metadata deletion was forced.",
                             target.name);
            } else if (storeResult.value()) {
                r.contentRemoved = true;
            } else {
                spdlog::debug("Cleaned orphaned metadata for {}; content was absent", target.name);
            }

            r.deleted = true;
            resp.deleted.push_back(r);
            return;
        }

        auto storeResult = ctx_.store->remove(target.hash);
        if (!storeResult) {
            r.deleted = false;
            r.error = storeResult.error().message;
            resp.errors.push_back(r);
            return;
        }
        if (!storeResult.value()) {
            r.error = "Document not found in store";
            resp.errors.push_back(r);
            return;
        }

        r.contentRemoved = true;
        r.deleted = true;
        resp.deleted.push_back(r);
    }

    static metadata::DocumentInfo
    documentFromProjection(metadata::ListDocumentProjection&& projection) {
        metadata::DocumentInfo doc;
        doc.id = projection.id;
        doc.filePath = std::move(projection.filePath);
        doc.fileName = std::move(projection.fileName);
        doc.fileExtension = std::move(projection.fileExtension);
        doc.fileSize = projection.fileSize;
        doc.sha256Hash = std::move(projection.sha256Hash);
        doc.mimeType = std::move(projection.mimeType);
        doc.createdTime = projection.createdTime;
        doc.modifiedTime = projection.modifiedTime;
        doc.indexedTime = projection.indexedTime;
        doc.extractionStatus = projection.extractionStatus;
        return doc;
    }

    static std::vector<metadata::DocumentInfo>
    documentsFromProjections(std::vector<metadata::ListDocumentProjection>&& projected) {
        std::vector<metadata::DocumentInfo> docs;
        docs.reserve(projected.size());
        for (auto& projection : projected) {
            docs.push_back(documentFromProjection(std::move(projection)));
        }
        return docs;
    }

    static std::optional<std::string> normalizeSingleListExtension(const std::string& ext) {
        if (ext.empty() || ext.find(',') != std::string::npos) {
            return std::nullopt;
        }
        return normalizeExtension(ext);
    }

    static std::string canonicalizeListPattern(const std::string& pattern) {
        if (pattern.empty()) {
            return {};
        }

        std::string canonicalPattern = pattern;
        auto wildcardPos = pattern.find_first_of("*?");
        if (wildcardPos != std::string::npos) {
            std::string prefix = pattern.substr(0, wildcardPos);
            std::string suffix = pattern.substr(wildcardPos);
            while (!prefix.empty() && (prefix.back() == '/' || prefix.back() == '\\')) {
                prefix.pop_back();
                suffix = "/" + suffix;
            }
            try {
                if (!prefix.empty() && std::filesystem::exists(prefix)) {
                    canonicalPattern = std::filesystem::canonical(prefix).string() + suffix;
                }
            } catch (...) {
                // Ignore errors; use original pattern.
            }
            return canonicalPattern;
        }

        try {
            if (std::filesystem::exists(pattern)) {
                canonicalPattern = std::filesystem::canonical(pattern).string();
            }
        } catch (...) {
            // Ignore errors; use original pattern.
        }
        return canonicalPattern;
    }

    static void configureListPatternQuery(const std::string& pattern,
                                          metadata::DocumentQueryOptions& queryOpts,
                                          bool& useFallback, bool& useTree,
                                          std::string& treePrefix) {
        if (pattern.empty()) {
            return;
        }

        auto wildcardPos = pattern.find_first_of("*?");
        const bool hasWildcard = wildcardPos != std::string::npos;
        if (!hasWildcard) {
            std::error_code ec;
            std::filesystem::path candidate{pattern};
            if (std::filesystem::exists(candidate, ec) &&
                std::filesystem::is_directory(candidate, ec)) {
                queryOpts.pathPrefix = pattern;
                queryOpts.prefixIsDirectory = true;
                queryOpts.includeSubdirectories = true;
            } else {
                queryOpts.exactPath = pattern;
            }
            return;
        }

        if (pattern.back() == '*') {
            std::string prefix = pattern;
            while (!prefix.empty() && (prefix.back() == '*' || prefix.back() == '?')) {
                prefix.pop_back();
            }
            while (!prefix.empty() && (prefix.back() == '/' || prefix.back() == '\\')) {
                prefix.pop_back();
            }
            if (!prefix.empty()) {
                useTree = true;
                treePrefix = prefix;
            }
            queryOpts.pathPrefix = prefix;
            queryOpts.prefixIsDirectory = true;
            return;
        }

        if (pattern.front() == '*' &&
            pattern.find_first_of("*?", wildcardPos + 1) == std::string::npos) {
            queryOpts.containsFragment = pattern.substr(1);
            queryOpts.containsUsesFts = true;
            return;
        }

        useFallback = true;
    }

    static bool listDocumentMatchesTypeFilters(const metadata::DocumentInfo& doc,
                                               const ListDocumentsRequest& req,
                                               const std::string& requestedType) {
        if (req.text && !isTextMime(doc.mimeType)) {
            return false;
        }
        if (req.binary && isTextMime(doc.mimeType)) {
            return false;
        }
        if (requestedType.empty()) {
            return true;
        }
        if (requestedType == "text") {
            return isTextMime(doc.mimeType);
        }
        if (requestedType == "binary") {
            return !isTextMime(doc.mimeType);
        }
        return utils::classifyFileType(doc.mimeType, doc.fileExtension) == requestedType;
    }

    static std::optional<size_t> listConcurrencyOverride() {
        const char* env = std::getenv("YAMS_LIST_CONCURRENCY"); // NOLINT(concurrency-mt-unsafe)
        if (!env || !*env) {
            return std::nullopt;
        }
        try {
            auto value = static_cast<size_t>(std::stoul(env));
            if (value > 0) {
                return value;
            }
        } catch (const std::exception& e) {
            spdlog::debug("Ignoring invalid YAMS_LIST_CONCURRENCY='{}': {}", env, e.what());
        } catch (...) {
            spdlog::debug("Ignoring invalid YAMS_LIST_CONCURRENCY='{}'", env);
        }
        return std::nullopt;
    }

    static DocumentEntry makeBaseListEntry(const metadata::DocumentInfo& doc) {
        DocumentEntry entry;
        entry.name = doc.fileName;
        entry.fileName = doc.fileName;
        entry.hash = doc.sha256Hash;
        entry.path = doc.filePath;
        entry.extension = doc.fileExtension;
        entry.size = static_cast<uint64_t>(doc.fileSize);
        entry.mimeType = doc.mimeType;
        entry.fileType = utils::classifyFileType(doc.mimeType, doc.fileExtension);
        entry.created = toEpochSeconds(doc.createdTime);
        entry.modified = toEpochSeconds(doc.modifiedTime);
        entry.indexed = toEpochSeconds(doc.indexedTime);
        entry.extractionStatus = metadata::ExtractionStatusUtils::toString(doc.extractionStatus);
        return entry;
    }

    static std::vector<int64_t>
    collectDocumentIds(const std::vector<metadata::DocumentInfo>& docs) {
        std::vector<int64_t> ids;
        ids.reserve(docs.size());
        for (const auto& doc : docs) {
            ids.push_back(doc.id);
        }
        return ids;
    }

    void applyFallbackListFilters(std::vector<metadata::DocumentInfo>& docs,
                                  const ListDocumentsRequest& req) {
        if (!req.extension.empty()) {
            const std::string wanted = normalizeExtension(req.extension);
            docs.erase(std::remove_if(docs.begin(), docs.end(),
                                      [&](const auto& doc) {
                                          return normalizeExtension(doc.fileExtension) != wanted;
                                      }),
                       docs.end());
        }

        if (!req.tags.empty()) {
            std::vector<metadata::DocumentInfo> filtered;
            for (const auto& doc : docs) {
                auto md = ctx_.metadataRepo->getAllMetadata(doc.id);
                if (!md) {
                    continue;
                }
                auto tags = extractTags(md.value());
                const bool hasTag =
                    std::any_of(req.tags.begin(), req.tags.end(), [&](const auto& tag) {
                        return std::find(tags.begin(), tags.end(), tag) != tags.end();
                    });
                if (hasTag) {
                    filtered.push_back(doc);
                }
            }
            docs.swap(filtered);
        }

        if (!req.metadataFilters.empty()) {
            std::vector<metadata::DocumentInfo> filtered;
            for (const auto& doc : docs) {
                auto md = ctx_.metadataRepo->getAllMetadata(doc.id);
                if (!md) {
                    continue;
                }
                const auto& mdMap = md.value();
                bool match = req.matchAllMetadata;
                for (const auto& [key, expectedValue] : req.metadataFilters) {
                    auto it = mdMap.find(key);
                    const bool keyMatches =
                        (it != mdMap.end() && it->second.asString() == expectedValue);
                    match = req.matchAllMetadata ? (match && keyMatches) : (match || keyMatches);
                }
                if (match) {
                    filtered.push_back(doc);
                }
            }
            docs.swap(filtered);
        }

        if (req.recent && *req.recent > 0) {
            std::sort(docs.begin(), docs.end(),
                      [](const auto& a, const auto& b) { return a.indexedTime > b.indexedTime; });
            if (static_cast<int>(docs.size()) > *req.recent) {
                docs.resize(static_cast<size_t>(*req.recent));
            }
        }

        if (req.sortBy == "name") {
            std::sort(docs.begin(), docs.end(),
                      [](const auto& a, const auto& b) { return a.fileName < b.fileName; });
            if (req.sortOrder == "desc") {
                std::reverse(docs.begin(), docs.end());
            }
        }
    }

    MetadataCache hydrateListMetadata(const std::vector<int64_t>& ids) {
        if (ids.empty()) {
            return {};
        }
        auto metaRes = ctx_.metadataRepo->getMetadataForDocuments(std::span<const int64_t>(ids));
        if (!metaRes) {
            return {};
        }
        return std::move(metaRes.value());
    }

    std::pair<SnippetPreviewCache, bool> hydrateListSnippets(const std::vector<int64_t>& ids,
                                                             int snippetLength) {
        if (ids.empty()) {
            return {SnippetPreviewCache{}, false};
        }
        const int previewChars = std::clamp(snippetLength * 8, 256, 8192);
        auto previewRes = ctx_.metadataRepo->batchGetContentPreview(ids, previewChars, 0);
        if (previewRes) {
            return {std::move(previewRes.value()), false};
        }
        return {SnippetPreviewCache{}, true};
    }

    static DocumentEntry buildHydratedListEntry(const ListDocumentsRequest& req,
                                                const metadata::DocumentInfo& doc,
                                                const MetadataCache& metadataCache,
                                                const SnippetPreviewCache& snippetPreviewCache,
                                                bool snippetFetchFailed) {
        DocumentEntry entry = makeBaseListEntry(doc);
        const MetadataMap* cachedMetadata = nullptr;
        if (auto it = metadataCache.find(doc.id); it != metadataCache.end()) {
            cachedMetadata = &it->second;
        }

        if (req.showSnippets && req.snippetLength > 0) {
            auto sit = snippetPreviewCache.find(doc.id);
            if (sit != snippetPreviewCache.end()) {
                auto snippet =
                    utils::createSnippet(sit->second, static_cast<size_t>(req.snippetLength), true);
                entry.snippet = snippet.empty() ? "[No text content]" : std::move(snippet);
            } else if (snippetFetchFailed) {
                entry.snippet = "[Content extraction failed]";
            } else {
                entry.snippet = "[Content not available]";
            }
        }

        if (cachedMetadata) {
            if (req.showTags) {
                entry.tags = extractTags(*cachedMetadata);
            }
            if (req.showMetadata) {
                for (const auto& [key, value] : *cachedMetadata) {
                    entry.metadata[key] = value.value;
                }
            }
        }
        return entry;
    }

public:
    explicit DocumentServiceImpl(const AppContext& ctx) : ctx_(ctx) {}

    // Store: accept path or (content + name); apply tags/metadata; update repo entry
    Result<StoreDocumentResponse> store(const StoreDocumentRequest& req) override {
        if (!ctx_.store) {
            return Error{ErrorCode::NotInitialized, "Content store not available"};
        }
        api::ContentMetadata md;
        if (!req.name.empty())
            md.name = req.name;
        // Attach mime_type as a tag (for consistency with other parts of the system)
        if (!req.mimeType.empty()) {
            md.tags["mime_type"] = req.mimeType;
        }
        addTagPairsToMap(req.tags, md.tags);
        addMetadataToMap(req.metadata, md.tags);

        // Add collection and snapshot metadata if provided
        if (!req.collection.empty()) {
            md.tags["collection"] = req.collection;
        }
        const auto snapshotId =
            req.snapshotId.empty() ? yams::core::generateSnapshotId() : req.snapshotId;
        const auto snapshotTime =
            std::to_string(std::chrono::duration_cast<std::chrono::microseconds>(
                               std::chrono::system_clock::now().time_since_epoch())
                               .count());
        md.tags["snapshot_id"] = snapshotId;
        md.tags["snapshot_time"] = snapshotTime;
        md.tags["snapshot_id:" + snapshotId] = snapshotId;
        md.tags["snapshot_time:" + snapshotId] = snapshotTime;
        if (!req.snapshotLabel.empty()) {
            md.tags["snapshot_label"] = req.snapshotLabel;
        }
        if (req.noEmbeddings) {
            // Persist the opt-out as both:
            // - a tag key (tag:no_embeddings) so tag queries and getDocumentTags() see it
            // - a legacy metadata key (no_embeddings=true) for any existing consumers
            md.tags["tag:no_embeddings"] = "no_embeddings";
            md.tags["no_embeddings"] = "true";
        }
        if (!req.sessionId.empty() && !req.bypassSession) {
            md.tags["session_id"] = req.sessionId;
        }
        if (!req.precomputedHash.empty()) {
            md.contentHash = req.precomputedHash;
            if (req.precomputedFileSize) {
                md.size = *req.precomputedFileSize;
            }
            md.tags["__yams_trusted_hash_hint"] = "1";
            if (req.precomputedLastWriteTimeNs) {
                md.tags["__yams_hash_hint_mtime_ns"] =
                    std::to_string(*req.precomputedLastWriteTimeNs);
            }
        }

        std::string usePath;
        std::optional<std::filesystem::path> tmpToRemove;

        if (!req.path.empty()) {
            // Defensive: reject directory paths early; callers should use addDirectory
            if (std::error_code __ec; std::filesystem::is_directory(req.path, __ec)) {
                return Error{ErrorCode::InvalidArgument,
                             "Path is a directory; use addDirectory/recursive ingestion"};
            }
            usePath = req.path;
        } else if (!req.content.empty() && !req.name.empty()) {
            // Write content to a temp file
            usePath = makeTempFilePathFor(req.name);
            tmpToRemove = std::filesystem::path(usePath);
            std::error_code ec;
            if (!yams::common::ensureDirectories(tmpToRemove->parent_path())) {
                return Error{ErrorCode::WriteError,
                             "Failed to create directory for temporary file"};
            }
            std::ofstream ofs(usePath, std::ios::binary);
            if (!ofs.good()) {
                return Error{ErrorCode::WriteError, "Failed to create temporary file for content"};
            }
            ofs.write(req.content.data(), static_cast<std::streamsize>(req.content.size()));
            ofs.flush();
            if (!ofs.good()) {
                ofs.close();
                std::filesystem::remove(*tmpToRemove, ec);
                return Error{ErrorCode::WriteError, "Failed to write content to temporary file"};
            }
        } else {
            return Error{ErrorCode::InvalidArgument, "Provide either 'path' or 'content' + 'name'"};
        }

        auto storeRes = ctx_.store->store(usePath, md);
        if (!storeRes) {
            // Preserve upstream error code when meaningful; enrich FileNotFound with guidance.
            auto err = storeRes.error();
            if (err.code == ErrorCode::FileNotFound || err.code == ErrorCode::NotFound) {
                std::string hint;
                try {
                    if (usePath.find(',') != std::string::npos) {
                        hint = " (hint: pass multiple files individually or use addDirectory; "
                               "commas are not path separators)";
                    }
                } catch (...) {
                }
                return Error{ErrorCode::FileNotFound,
                             std::string("File not found: ") + usePath + hint};
            }
            return err;
        }

        StoreDocumentResponse out;
        out.hash = storeRes.value().contentHash;
        out.bytesStored = storeRes.value().bytesStored;
        out.bytesDeduped = storeRes.value().bytesDeduped;

        // Best-effort: insert/update metadata repository entry
        if (ctx_.metadataRepo) {
            metadata::DocumentInfo info;
            std::filesystem::path p = usePath;
            // When content is provided directly, use the logical name as the canonical path so
            // tree indexing and session diffs work for raw-text documents too.
            if (!req.path.empty()) {
                info.filePath = p.string();
                // Use explicit name if provided, otherwise derive from path
                if (!req.name.empty()) {
                    info.fileName = req.name;
                    std::filesystem::path np = req.name;
                    info.fileExtension = np.extension().string();
                } else {
                    info.fileName = p.filename().string();
                    info.fileExtension = p.extension().string();
                }
            } else {
                // Prefer the provided document name as both filePath and fileName
                info.filePath = req.name;
                info.fileName = req.name;
                std::filesystem::path np = req.name;
                info.fileExtension = np.extension().string();
            }
            std::error_code ec;
            info.fileSize = static_cast<int64_t>(std::filesystem::exists(p, ec)
                                                     ? std::filesystem::file_size(p, ec)
                                                     : req.content.size());
            info.sha256Hash = out.hash;
            // Detect MIME when not provided or generic
            info.mimeType = !req.mimeType.empty() ? req.mimeType : "";
            if (info.mimeType.empty() || info.mimeType == "application/octet-stream") {
                bool triedDetection = false;
                if (!info.fileExtension.empty()) {
                    info.mimeType = yams::detection::FileTypeDetector::getMimeTypeFromExtension(
                        info.fileExtension);
                    if (!info.mimeType.empty() && info.mimeType != "application/octet-stream") {
                        triedDetection = true;
                    }
                }
                if (!triedDetection) {
                    try {
                        (void)yams::detection::FileTypeDetector::initializeWithMagicNumbers();
                        auto& det = yams::detection::FileTypeDetector::instance();
                        if (auto sig = det.detectFromFile(p)) {
                            if (!sig.value().mimeType.empty())
                                info.mimeType = sig.value().mimeType;
                        }
                        if (info.mimeType.empty()) {
                            const auto& extForMime = info.fileExtension.empty()
                                                         ? p.extension().string()
                                                         : info.fileExtension;
                            info.mimeType =
                                yams::detection::FileTypeDetector::getMimeTypeFromExtension(
                                    extForMime);
                        }
                    } catch (...) {
                        // fallback below
                    }
                }
            }
            if (info.mimeType.empty() || info.mimeType == "application/octet-stream") {
                // For extensionless inline content, sniff the provided bytes instead of
                // assuming everything piped on stdin is text.
                if (!req.content.empty() && info.fileExtension.empty()) {
                    try {
                        (void)yams::detection::FileTypeDetector::initializeWithMagicNumbers();
                        auto& det = yams::detection::FileTypeDetector::instance();
                        auto bytes =
                            std::as_bytes(std::span(req.content.data(), req.content.size()));
                        if (auto sig = det.detectFromBuffer(bytes)) {
                            info.mimeType = sig.value().mimeType;
                        }
                    } catch (...) {
                    }
                } else if (info.mimeType.empty())
                    info.mimeType = "application/octet-stream";
            }

            // Clean up temp file after MIME detection (needs the file to exist)
            if (tmpToRemove) {
                std::error_code ec;
                std::filesystem::remove(*tmpToRemove, ec);
            }

            using std::chrono::floor;
            using namespace std::chrono;
            auto now = std::chrono::system_clock::now();
            auto now_s = floor<seconds>(now);
            info.createdTime = now_s;
            info.modifiedTime = now_s;
            info.indexedTime = now_s;
            info.contentExtracted = isTextMime(info.mimeType);
            info.extractionStatus = info.contentExtracted ? metadata::ExtractionStatus::Success
                                                          : metadata::ExtractionStatus::Pending;

            metadata::populatePathDerivedFields(info);

            // Build metadata tags as key-value pairs for the combined insert
            std::vector<std::pair<std::string, metadata::MetadataValue>> tagPairs;
            tagPairs.reserve(md.tags.size());
            for (const auto& [k, v] : md.tags) {
                tagPairs.emplace_back(k, metadata::MetadataValue(v));
            }

            // Build snapshot record for the combined insert
            metadata::TreeSnapshotRecord snapshotRecord;
            snapshotRecord.snapshotId = snapshotId;
            // ingestDocumentId is set by insertDocumentWithMetadata
            snapshotRecord.createdTime = yams::common::timePointToEpochSeconds(now);
            snapshotRecord.fileCount = 1;
            snapshotRecord.totalBytes = info.fileSize;
            snapshotRecord.metadata["directory_path"] = info.filePath;
            if (!req.snapshotLabel.empty()) {
                snapshotRecord.metadata["snapshot_label"] = req.snapshotLabel;
            }
            if (!req.collection.empty()) {
                snapshotRecord.metadata["collection"] = req.collection;
            }

            // Single-transaction insert: document + metadata + snapshot in one
            // BEGIN IMMEDIATE, reducing 15-20 lock acquisitions to 1.
            metadata::MetadataOpScope metadataScope("app_store_document");
            auto ins =
                ctx_.metadataRepo->insertDocumentWithMetadata(info, tagPairs, &snapshotRecord);
            if (ins) {
                int64_t docId = ins.value();
                out.documentId = docId;

                // Path-series versioning (best-effort)
                try {
                    auto* writeCoord = (ctx_.service_manager)
                                           ? ctx_.service_manager->getWriteCoordinator()
                                           : nullptr;
                    if (writeCoord) {
                        int64_t maxVersion = 0;
                        std::optional<int64_t> prevLatestId;
                        auto priorDoc = ctx_.metadataRepo->findDocumentByExactPath(info.filePath);
                        if (priorDoc && priorDoc.value().has_value()) {
                            auto& prior = priorDoc.value().value();
                            if (prior.sha256Hash != info.sha256Hash) {
                                prevLatestId = prior.id;
                                auto verRes = ctx_.metadataRepo->getMetadata(prior.id, "version");
                                if (verRes && verRes.value().has_value()) {
                                    try {
                                        maxVersion = verRes.value().value().asInteger();
                                    } catch (...) {
                                    }
                                }
                            }
                        }
                        if (!prevLatestId.has_value()) {
                            auto prevListRes = metadata::queryDocumentsByPattern(*ctx_.metadataRepo,
                                                                                 info.filePath);
                            if (prevListRes) {
                                for (const auto& d : prevListRes.value()) {
                                    if (d.id == docId)
                                        continue;
                                    auto latestRes =
                                        ctx_.metadataRepo->getMetadata(d.id, "is_latest");
                                    if (latestRes && latestRes.value().has_value() &&
                                        latestRes.value().value().asBoolean()) {
                                        prevLatestId = d.id;
                                    }
                                    auto verRes = ctx_.metadataRepo->getMetadata(d.id, "version");
                                    if (verRes && verRes.value().has_value()) {
                                        try {
                                            int64_t v = verRes.value().value().asInteger();
                                            if (v > maxVersion)
                                                maxVersion = v;
                                        } catch (...) {
                                        }
                                    }
                                    if (prevLatestId.has_value())
                                        break;
                                }
                            }
                        }

                        int64_t newVersion = prevLatestId.has_value() ? maxVersion + 1 : 1;

                        auto& vc = getVersioningCoalescer();
                        if (prevLatestId.has_value()) {
                            vc.addOp(daemon::SetMetadataBatchOp{{{*prevLatestId, "is_latest",
                                                                  metadata::MetadataValue(false)}}},
                                     writeCoord);

                            metadata::DocumentRelationship rel;
                            rel.parentId = *prevLatestId;
                            rel.childId = docId;
                            rel.relationshipType = metadata::RelationshipType::VersionOf;
                            rel.createdTime = std::chrono::floor<std::chrono::seconds>(
                                std::chrono::system_clock::now());
                            vc.addOp(daemon::InsertRelationshipOp{std::move(rel)}, writeCoord);
                        }

                        vc.addOp(
                            daemon::SetMetadataBatchOp{
                                {{docId, "version", metadata::MetadataValue(newVersion)},
                                 {docId, "is_latest", metadata::MetadataValue(true)},
                                 {docId, "series_key", metadata::MetadataValue(info.filePath)}}},
                            writeCoord);
                    } else {
                        auto priorDoc = ctx_.metadataRepo->findDocumentByExactPath(info.filePath);
                        if (priorDoc && priorDoc.value().has_value()) {
                            auto& prior = priorDoc.value().value();
                            if (prior.sha256Hash != info.sha256Hash) {
                                metadata::applyPathSeriesVersioning(*ctx_.metadataRepo,
                                                                    info.filePath, docId, prior);
                            }
                        } else {
                            metadata::applyPathSeriesVersioning(*ctx_.metadataRepo, info.filePath,
                                                                docId, std::nullopt);
                        }
                    }
                } catch (...) {
                }

                // Update path tree for this document (best-effort, separate txn)
                try {
                    auto treeRes = ctx_.metadataRepo->upsertPathTreeForDocument(
                        info, docId, true /* isNewDocument */, std::span<const float>());
                    if (!treeRes) {
                        spdlog::debug("Failed to update path tree for {}: {}", info.filePath,
                                      treeRes.error().message);
                    }
                } catch (...) {
                    // Non-fatal: path tree update is opportunistic
                }

                // Keep the lightweight corpus graph in sync for single-document stores so
                // embedded/mobile graph queries can resolve the stored document immediately.
                if (ctx_.kgStore) {
                    try {
                        auto* writeCoord = (ctx_.service_manager)
                                               ? ctx_.service_manager->getWriteCoordinator()
                                               : nullptr;
                        if (writeCoord) {
                            std::string blobNodeKey = std::string("blob:") + info.sha256Hash;
                            std::string docNodeKey = std::string("doc:") + info.sha256Hash;
                            std::string snapshotPathKey =
                                std::string("path:") + snapshotId + ":" + info.filePath;
                            std::string logicalPathKey =
                                std::string("path:logical:") + info.filePath;

                            metadata::KGNode blobNode;
                            blobNode.nodeKey = blobNodeKey;
                            blobNode.label = std::string(info.sha256Hash).substr(0, 16) + "...";
                            blobNode.type = "blob";

                            metadata::KGNode docNode;
                            docNode.nodeKey = docNodeKey;
                            docNode.label = info.filePath;
                            docNode.type = "document";

                            metadata::KGNode snapshotPathNode;
                            snapshotPathNode.nodeKey = snapshotPathKey;
                            snapshotPathNode.label = info.filePath;
                            snapshotPathNode.type = "path";
                            snapshotPathNode.properties =
                                std::string("{\"snapshot_id\":\"") + snapshotId + "\",\"path\":\"" +
                                info.filePath + "\",\"is_directory\":false}";

                            metadata::KGNode logicalPathNode;
                            logicalPathNode.nodeKey = logicalPathKey;
                            logicalPathNode.label = info.filePath;
                            logicalPathNode.type = "path";
                            logicalPathNode.properties =
                                std::string("{\"path\":\"") + info.filePath +
                                "\",\"is_directory\":false,\"logical\":true}";

                            auto& kc = getKgSyncCoalescer();
                            kc.addOp(daemon::UpsertNodesOp{{std::move(blobNode), std::move(docNode),
                                                            std::move(snapshotPathNode),
                                                            std::move(logicalPathNode)}},
                                     writeCoord);

                            daemon::DeferredEdgeOp pathVerEdge;
                            pathVerEdge.srcNodeKey = logicalPathKey;
                            pathVerEdge.dstNodeKey = snapshotPathKey;
                            pathVerEdge.relation = "path_version";

                            daemon::DeferredEdgeOp hasVerEdge;
                            hasVerEdge.srcNodeKey = snapshotPathKey;
                            hasVerEdge.dstNodeKey = blobNodeKey;
                            hasVerEdge.relation = "has_version";
                            hasVerEdge.properties = "{\"diff_id\":0}";

                            kc.addOp(daemon::AddDeferredEdgesOp{{std::move(pathVerEdge),
                                                                 std::move(hasVerEdge)}},
                                     writeCoord);
                        } else {
                            auto blobNodeResult = ctx_.kgStore->ensureBlobNode(info.sha256Hash);
                            if (!blobNodeResult) {
                                spdlog::debug("Failed to ensure KG blob node for {}: {}",
                                              info.sha256Hash.substr(0, 8),
                                              blobNodeResult.error().message);
                            } else {
                                metadata::KGNode docNode;
                                docNode.nodeKey = "doc:" + info.sha256Hash;
                                docNode.label = info.filePath;
                                docNode.type = "document";
                                auto docNodeIds = ctx_.kgStore->upsertNodes({std::move(docNode)});
                                if (!docNodeIds) {
                                    spdlog::debug("Failed to upsert KG doc node for {}: {}",
                                                  info.sha256Hash.substr(0, 8),
                                                  docNodeIds.error().message);
                                }

                                metadata::PathNodeDescriptor pathDesc;
                                pathDesc.snapshotId = snapshotId;
                                pathDesc.path = info.filePath;
                                pathDesc.isDirectory = false;

                                auto pathNodeResult = ctx_.kgStore->ensurePathNode(pathDesc);
                                if (!pathNodeResult) {
                                    spdlog::debug("Failed to ensure KG path node for {}: {}",
                                                  info.filePath, pathNodeResult.error().message);
                                } else {
                                    auto linkResult = ctx_.kgStore->linkPathVersion(
                                        pathNodeResult.value(), blobNodeResult.value(), 0);
                                    if (!linkResult) {
                                        spdlog::debug("Failed to link KG path version for "
                                                      "{}: {}",
                                                      info.filePath, linkResult.error().message);
                                    }
                                }
                            }
                        }
                    } catch (const std::exception& ex) {
                        spdlog::warn("Exception syncing KG for {}: {}", info.filePath, ex.what());
                    }
                }

                // Index document terms for fuzzy search through the centralized writer lane
                // (best-effort; lookup remains read-only).
                try {
                    auto* writeCoord = (ctx_.service_manager)
                                           ? ctx_.service_manager->getWriteCoordinator()
                                           : nullptr;
                    enqueueDocumentTermsForFuzzySearch(writeCoord, info);
                } catch (const std::exception& ex) {
                    spdlog::debug("Failed to enqueue SymSpell terms for {}: {}", info.filePath,
                                  ex.what());
                }

                // Synchronously persist extracted text for direct text adds so sync callers can
                // search immediately without waiting for post-ingest indexing.
                if (isTextMime(info.mimeType)) {
                    std::string extractedText;
                    std::string extractionMethod;
                    if (!req.content.empty()) {
                        extractedText = req.content;
                        extractionMethod = "inline_store_sync";
                    } else {
                        std::ifstream ifs(usePath, std::ios::binary);
                        if (ifs.good()) {
                            std::ostringstream oss;
                            oss << ifs.rdbuf();
                            extractedText = oss.str();
                            extractionMethod = "text_store_sync";
                        }
                    }
                    if (!extractedText.empty()) {
                        (void)ingest::persist_content_and_index(*ctx_.metadataRepo, docId,
                                                                info.fileName, extractedText,
                                                                info.mimeType, extractionMethod);
                    }
                }
            }
        }

        return out;
    }

    // Normalize common prefixed hash forms like "sha256:<hex>"
    static std::string normalizeHashInput(const std::string& in) {
        if (in.size() > 7) {
            // Accept case-insensitive prefix
            auto lower = in;
            std::transform(lower.begin(), lower.end(), lower.begin(),
                           [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
            const std::string prefix = "sha256:";
            if (lower.rfind(prefix, 0) == 0) {
                return in.substr(prefix.size());
            }
        }
        return in;
    }

    // Retrieve by hash or name (+ optional outputPath) with optional content and simple graph
    Result<RetrieveDocumentResponse> retrieve(const RetrieveDocumentRequest& req) override {
        if (!ctx_.store) {
            return Error{ErrorCode::NotInitialized, "Content store not available"};
        }
        if (req.hash.empty() && req.name.empty()) {
            return Error{ErrorCode::InvalidArgument, "Either hash or name is required"};
        }

        metadata::MetadataOpScope metadataScope("client_get");

        RetrieveDocumentResponse resp;
        resp.graphEnabled = false;

        // Validate hash format if provided (must be hex string, at least 8 chars for partial)
        if (!req.hash.empty()) {
            std::string normalized = normalizeHashInput(req.hash);
            // Check if it's a valid hex string (allows partial hashes >= 8 chars)
            if (normalized.size() < 8 ||
                !std::all_of(normalized.begin(), normalized.end(),
                             [](char c) { return std::isxdigit(static_cast<unsigned char>(c)); })) {
                return Error{ErrorCode::NotFound,
                             "Document not found for hash: '" + req.hash + "'"};
            }
        }

        // If name is provided but no hash, resolve name to hash first
        std::string resolvedHash = normalizeHashInput(req.hash);
        if (!req.name.empty() && req.hash.empty()) {
            auto resolveResult = resolveNameToHash(req.name, req.oldest);
            if (!resolveResult) {
                return Error{ErrorCode::NotFound, "Document not found with name: " + req.name};
            }
            resolvedHash = resolveResult.value();
            resolvedHash = normalizeHashInput(resolvedHash);
        }

        // Resolve hash (handle partial hashes)
        if (!resolvedHash.empty() && resolvedHash.size() != 64 &&
            looksLikePartialHash(resolvedHash)) {
            if (!ctx_.metadataRepo) {
                return Error{ErrorCode::NotInitialized,
                             "Metadata repository not available for partial hash resolution"};
            }

            auto matchResult = ctx_.metadataRepo->findDocumentsByHashPrefix(resolvedHash, 100);
            if (!matchResult) {
                return Error{ErrorCode::InternalError,
                             "Failed to resolve partial hash: " + matchResult.error().message};
            }

            std::vector<std::string> matches;
            for (const auto& d : matchResult.value()) {
                matches.push_back(d.sha256Hash);
            }

            if (matches.empty()) {
                return Error{ErrorCode::NotFound,
                             "No documents found matching hash prefix: " + resolvedHash};
            }
            if (matches.size() > 1) {
                return Error{ErrorCode::InvalidOperation,
                             "Ambiguous hash prefix: " + std::to_string(matches.size()) +
                                 " matches for '" + resolvedHash + "'"};
            }
            resolvedHash = matches[0];
        }

        std::optional<metadata::DocumentInfo> foundDoc;
        if (ctx_.metadataRepo) {
            auto docResult = ctx_.metadataRepo->getDocumentByHash(resolvedHash);
            if (docResult && docResult.value()) {
                foundDoc = docResult.value();
            }
            if (!foundDoc) {
                return Error{ErrorCode::NotFound, "Document not found"};
            }
        }

        RetrievedDocument doc;
        doc.hash = resolvedHash;

        if (foundDoc) {
            doc.path = foundDoc->filePath;
            doc.name = foundDoc->fileName;
            doc.size = static_cast<uint64_t>(foundDoc->fileSize);
            doc.mimeType = foundDoc->mimeType;
        } else {
            // Fallback: path unknown; mime default
            doc.path = req.outputPath.empty() ? resolvedHash : req.outputPath;
            doc.name.clear();
            doc.size = 0;
            doc.mimeType = "application/octet-stream";
        }

        if (ctx_.metadataRepo && !doc.path.empty()) {
            auto nodeRes = ctx_.metadataRepo->findPathTreeNodeByFullPath(doc.path);
            if (!nodeRes) {
                spdlog::warn("DocumentService: path tree lookup failed for {}: {}", doc.path,
                             nodeRes.error().message);
            } else if (nodeRes.value()) {
                const auto& node = *nodeRes.value();
                if (node.centroidWeight > 0)
                    doc.centroidWeight = static_cast<uint32_t>(node.centroidWeight);
                if (!node.centroid.empty()) {
                    doc.centroidDims = static_cast<uint32_t>(node.centroid.size());
                    const std::size_t previewCount =
                        std::min<std::size_t>(node.centroid.size(), kCentroidPreviewLimit);
                    doc.centroidPreview.assign(node.centroid.begin(),
                                               node.centroid.begin() + previewCount);
                    std::ostringstream oss;
                    oss.setf(std::ios::fixed);
                    oss << std::setprecision(4);
                    for (std::size_t i = 0; i < previewCount; ++i) {
                        if (i != 0)
                            oss << ' ';
                        oss << doc.centroidPreview[i];
                    }
                    if (node.centroid.size() > previewCount) {
                        oss << " ... (dims=" << node.centroid.size() << ")";
                    }
                    doc.metadata.emplace("yams.centroid.preview", oss.str());
                }
                doc.metadata.emplace("yams.centroid.weight", std::to_string(node.centroidWeight));
                doc.metadata.emplace("yams.centroid.dims", std::to_string(node.centroid.size()));
            }
        }

        // If outputPath provided, retrieve to file
        if (!req.outputPath.empty()) {
            auto r = ctx_.store->retrieve(resolvedHash, req.outputPath);
            if (!r) {
                return Error{ErrorCode::InternalError, "Retrieve failed: " + r.error().message};
            }
            // update doc.path/size best-effort
            std::error_code ec;
            if (std::filesystem::exists(req.outputPath, ec)) {
                doc.path = req.outputPath;
                doc.size = static_cast<uint64_t>(std::filesystem::file_size(req.outputPath, ec));
            }
        }

        if (req.outputPath.empty() && !req.includeContent) {
            auto existsResult = ctx_.store->exists(resolvedHash);
            if (!existsResult) {
                return existsResult.error();
            }
            if (!existsResult.value()) {
                return Error{ErrorCode::NotFound, "Document not found"};
            }
        }

        resetCompressionMetadata(doc);
        if (doc.size > 0) {
            doc.uncompressedSize = doc.size;
        }

        // Retrieve actual content from CAS (compressed storage) - the source of truth
        if (req.includeContent) {
            if (req.acceptCompressed) {
                auto bytesResult = ctx_.store->retrieveBytes(resolvedHash);
                if (!bytesResult) {
                    if (doc.size == 0) {
                        doc.content.emplace();
                        markUncompressed(doc, 0);
                    } else {
                        return Error{bytesResult.error().code, "Document content not found"};
                    }
                } else {
                    auto data = std::move(bytesResult.value());
                    auto payloadResult =
                        makeCompressedPayload(std::span<const std::byte>(data.data(), data.size()));
                    if (!payloadResult) {
                        return payloadResult.error();
                    }
                    auto payload = std::move(payloadResult.value());
                    doc.content.emplace(std::move(payload.blob));
                    applyCompressionMetadata(doc, payload.header);
                }
            } else {
                auto bytesResult = ctx_.store->retrieveBytes(resolvedHash);
                if (!bytesResult) {
                    if (doc.size == 0) {
                        doc.content.emplace();
                        markUncompressed(doc, 0);
                    } else {
                        return Error{bytesResult.error().code, "Document content not found"};
                    }
                } else {
                    auto data = std::move(bytesResult.value());
                    doc.content.emplace(reinterpret_cast<const char*>(data.data()), data.size());
                    markUncompressed(doc, static_cast<uint64_t>(data.size()));
                }
            }
        }

        // Optionally attach extracted text as metadata (from DB, for search/grep context)
        if (req.extract && ctx_.metadataRepo && foundDoc) {
            auto contentResult = ctx_.metadataRepo->getContent(foundDoc->id);
            if (contentResult && contentResult.value().has_value()) {
                const auto& content = contentResult.value().value();
                if (!content.contentText.empty()) {
                    doc.extractedText = content.contentText;
                }
            }
        }
        resp.document = doc;

        // Build knowledge graph relationships via GraphQueryService
        if (req.graph && foundDoc) {
            std::unordered_set<std::string> seenHashes;
            seenHashes.insert(foundDoc->sha256Hash);

            int maxDepth = std::clamp(req.depth, 1, 5);

            if (ctx_.graphQueryService) {
                GraphQueryRequest graphReq;
                graphReq.documentHash = foundDoc->sha256Hash;
                graphReq.maxDepth = maxDepth;
                graphReq.limit = 100;
                graphReq.hydrateFully = true;
                graphReq.relationFilters = {
                    GraphRelationType::SameContent, GraphRelationType::RenamedFrom,
                    GraphRelationType::RenamedTo, GraphRelationType::PathVersion};

                auto graphResult = ctx_.graphQueryService->query(graphReq);
                if (graphResult && !graphResult.value().allConnectedNodes.empty()) {
                    for (const auto& connNode : graphResult.value().allConnectedNodes) {
                        if (!connNode.nodeMetadata.documentHash.has_value() ||
                            seenHashes.count(connNode.nodeMetadata.documentHash.value()) > 0) {
                            continue;
                        }

                        auto docRes = ctx_.metadataRepo->getDocumentByHash(
                            connNode.nodeMetadata.documentHash.value());
                        if (docRes && docRes.value()) {
                            const auto& doc = docRes.value().value();
                            RelatedDocument rd;
                            rd.hash = doc.sha256Hash;
                            rd.path = doc.filePath;
                            rd.name = doc.fileName;
                            rd.distance = connNode.distance;

                            if (!connNode.connectingEdges.empty()) {
                                const auto& edge = connNode.connectingEdges.front();
                                if (edge.relation == "has_version" ||
                                    edge.relation == "same_content") {
                                    rd.relationship = "same_content";
                                } else if (edge.relation == "renamed_from" ||
                                           edge.relation == "renamed_to") {
                                    rd.relationship = "renamed";
                                } else {
                                    rd.relationship = edge.relation;
                                }
                            } else {
                                rd.relationship = "related";
                            }

                            resp.related.push_back(std::move(rd));
                            seenHashes.insert(doc.sha256Hash);
                        }
                    }
                }

                if (maxDepth >= 1 && ctx_.kgStore) {
                    auto historyRes = ctx_.kgStore->fetchPathHistory(foundDoc->filePath, 100);
                    if (historyRes) {
                        for (const auto& record : historyRes.value()) {
                            if (record.path != foundDoc->filePath &&
                                seenHashes.count(record.blobHash) == 0) {
                                auto docRes = ctx_.metadataRepo->getDocumentByHash(record.blobHash);
                                if (docRes && docRes.value()) {
                                    const auto& doc = docRes.value().value();
                                    RelatedDocument rd;
                                    rd.hash = doc.sha256Hash;
                                    rd.path = doc.filePath;
                                    rd.name = doc.fileName;
                                    rd.relationship = record.changeType.value_or("renamed");
                                    rd.distance = 1;
                                    resp.related.push_back(std::move(rd));
                                    seenHashes.insert(doc.sha256Hash);
                                }
                            }
                        }
                    }
                }
            }

            if (resp.related.empty() && ctx_.metadataRepo) {
                std::filesystem::path baseDir =
                    std::filesystem::path(foundDoc->filePath).parent_path();
                // Query only documents in the same directory using path prefix
                std::string dirPattern = baseDir.string() + "/%";
                auto docsRes =
                    metadata::queryDocumentsByPattern(*ctx_.metadataRepo, dirPattern, 21);
                if (docsRes) {
                    int count = 0;
                    for (const auto& other : docsRes.value()) {
                        if (other.sha256Hash == foundDoc->sha256Hash)
                            continue;
                        if (seenHashes.count(other.sha256Hash) == 0 && count < 20) {
                            RelatedDocument rd;
                            rd.hash = other.sha256Hash;
                            rd.path = other.filePath;
                            rd.name = other.fileName;
                            rd.relationship = "same_directory";
                            rd.distance = 1;
                            resp.related.push_back(std::move(rd));
                            seenHashes.insert(other.sha256Hash);
                            count++;
                        }
                    }
                }
            }
            resp.graphEnabled = !resp.related.empty();
        }

        return resp;
    }

    // Cat: by hash or by name -> content + size
    Result<CatDocumentResponse> cat(const CatDocumentRequest& req) override {
        if (!ctx_.store) {
            return Error{ErrorCode::NotInitialized, "Content store not available"};
        }

        metadata::MetadataOpScope metadataScope("client_cat");

        std::string hash;
        std::string name = req.name;

        if (!req.hash.empty()) {
            hash = normalizeHashInput(req.hash);
        } else if (!req.name.empty()) {
            auto h = resolveNameToHash(req.name);
            if (!h)
                return Error{h.error().code, h.error().message};
            hash = normalizeHashInput(h.value());
        } else {
            return Error{ErrorCode::InvalidArgument, "Provide 'hash' or 'name'"};
        }

        // If an extraction query is present, route through the format handler registry
        if (req.extractionQuery.has_value()) {
            // Retrieve raw bytes
            std::vector<std::byte> contentBytes;
            {
                auto rb = ctx_.store->retrieveBytes(hash);
                if (!rb) {
                    return Error{ErrorCode::InternalError,
                                 "Failed to retrieve content bytes: " + rb.error().message};
                }
                contentBytes = std::move(rb.value());
            }

            // Determine mime and extension (best-effort)
            std::string mime;
            std::string ext;
            if (ctx_.metadataRepo) {
                auto docRes = ctx_.metadataRepo->getDocumentByHash(hash);
                if (docRes && docRes.value().has_value()) {
                    mime = docRes.value()->mimeType;
                    ext = docRes.value()->fileExtension;
                }
            }
            if (ext.empty() && !name.empty()) {
                ext = std::filesystem::path(name).extension().string();
            }

            // Build handler registry with basic text handler for Phase 1
            yams::extraction::format::HandlerRegistry registry;
            yams::extraction::format::registerTextBasicHandler(registry);

            // Map services::ExtractionQuery -> format::ExtractionQuery
            const auto& q = *req.extractionQuery;
            yams::extraction::format::ExtractionQuery fq;
            auto toLower = [](std::string s) {
                std::transform(s.begin(), s.end(), s.begin(),
                               [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
                return s;
            };
            const std::string scope = toLower(q.scope);
            if (scope == "range") {
                fq.scope = yams::extraction::format::Scope::Range;
            } else if (scope == "section") {
                fq.scope = yams::extraction::format::Scope::Section;
            } else if (scope == "selector") {
                fq.scope = yams::extraction::format::Scope::Selector;
            } else {
                fq.scope = yams::extraction::format::Scope::All;
            }
            fq.range = q.range;
            fq.sectionPath = q.sectionPath;
            fq.selector = q.selector;
            fq.search = q.search;
            fq.maxMatches = q.maxMatches;
            fq.includeBBoxes = q.includeBBoxes;
            fq.format = q.format;
            fq.formatOptions = q.formatOptions;

            auto span = std::span<const std::byte>(contentBytes.data(), contentBytes.size());
            auto er = registry.extract(mime, ext, span, fq); // best handler based on mime/ext
            if (!er) {
                return Error{er.error().code, "Extraction failed: " + er.error().message};
            }

            const auto& r = er.value();
            CatDocumentResponse out;
            out.hash = hash;
            out.name = name;

            // Prefer JSON when requested, otherwise text
            if (fq.format == "json" && r.json.has_value()) {
                out.content = *r.json;
            } else if (r.text.has_value()) {
                out.content = *r.text;
            } else {
                // Fallback: raw bytes to string
                out.content.assign(reinterpret_cast<const char*>(contentBytes.data()),
                                   reinterpret_cast<const char*>(contentBytes.data()) +
                                       contentBytes.size());
            }
            out.size = out.content.size();
            return out;
        }

        // Tiered retrieval: hot (metadata text) vs cold (CAS reconstruct)
        // Mode: YAMS_RETRIEVAL_MODE = hot_only|cold_only|auto (default: auto)
        yams::cli::HotColdMode mode = yams::cli::getRetrievalMode();

        // Honor per-document force_cold tag/metadata
        bool forceCold = false;
        std::optional<yams::metadata::DocumentInfo> targetDoc;
        if (ctx_.metadataRepo) {
            // PERFORMANCE: Use direct hash lookup instead of scanning all documents
            auto docRes = ctx_.metadataRepo->getDocumentByHash(hash);
            if (docRes && docRes.value().has_value()) {
                targetDoc = docRes.value();
            }
            if (targetDoc) {
                auto md = ctx_.metadataRepo->getAllMetadata(targetDoc->id);
                if (md) {
                    auto& all = md.value();
                    auto it = all.find("force_cold");
                    if (it != all.end()) {
                        auto v = it->second.asString();
                        std::string lv = v;
                        std::transform(lv.begin(), lv.end(), lv.begin(), ::tolower);
                        forceCold = (lv == "1" || lv == "true" || lv == "yes");
                    }
                    if (!forceCold && all.find("tag:force_cold") != all.end())
                        forceCold = true;
                }
            }
        }

        if (mode == yams::cli::HotColdMode::HotOnly && !forceCold && ctx_.metadataRepo &&
            targetDoc) {
            auto c = ctx_.metadataRepo->getContent(targetDoc->id);
            if (c && c.value().has_value()) {
                CatDocumentResponse out;
                out.hash = hash;
                out.name = name;
                out.content = c.value()->contentText;
                out.size = out.content.size();
                return out;
            }
            // if no hot content available, fall through to cold path only when in Auto mode
            if (mode == yams::cli::HotColdMode::HotOnly) {
                return Error{ErrorCode::NotFound, "Hot content not available for document"};
            }
        }

        // Auto: prefer hot content when available; otherwise cold
        if (mode == yams::cli::HotColdMode::Auto && !forceCold && ctx_.metadataRepo && targetDoc) {
            auto c = ctx_.metadataRepo->getContent(targetDoc->id);
            if (c && c.value().has_value() && !c.value()->contentText.empty()) {
                CatDocumentResponse out;
                out.hash = hash;
                out.name = name;
                out.content = c.value()->contentText;
                out.size = out.content.size();
                return out;
            }
        }

        // Default path (backward compatible): raw content stream
        std::ostringstream oss;
        auto rs = ctx_.store->retrieveStream(hash, oss, nullptr);
        if (!rs) {
            return Error{ErrorCode::InternalError,
                         "Failed to retrieve content: " + rs.error().message};
        }

        CatDocumentResponse out;
        out.hash = hash;
        out.name = name;
        out.content = oss.str();
        out.size = out.content.size();
        return out;
    }

    // List with filters and sorting (minimal parity: pattern, extension, type, tags (presence),
    // recent, sort_by name)
    Result<ListDocumentsResponse> list(const ListDocumentsRequest& req) override {
        if (!ctx_.metadataRepo) {
            return Error{ErrorCode::NotInitialized, "Metadata repository not available"};
        }

        metadata::MetadataOpScope metadataScope("client_list");

        std::vector<metadata::DocumentInfo> docs;
        bool usedQuery = false;
        std::size_t totalFoundApprox = 0;

        metadata::DocumentQueryOptions queryOpts;
        queryOpts.limit = req.limit > 0 ? req.limit : 0;
        queryOpts.offset = std::max(0, req.offset);
        queryOpts.pathsOnly = req.pathsOnly;

        if (auto normExt = normalizeSingleListExtension(req.extension))
            queryOpts.extension = *normExt;

        if (!req.mime.empty())
            queryOpts.mimeType = req.mime;

        if (req.text || req.type == "text") {
            queryOpts.textOnly = true;
        } else if (req.binary || req.type == "binary") {
            queryOpts.binaryOnly = true;
        }

        if (!req.tags.empty())
            queryOpts.tags = req.tags;

        // Session filtering via metadata
        if (!req.sessionId.empty()) {
            queryOpts.metadataFilters.emplace_back("session_id", req.sessionId);
        }

        if (req.sortBy == "name") {
            queryOpts.orderByNameAsc = (req.sortOrder != "desc");
        } else {
            queryOpts.orderByIndexedDesc = (req.sortOrder != "asc");
        }

        bool useFallback = false;
        bool useTree = false;
        std::string treePrefix;
        std::string requestedType = req.type;
        std::transform(requestedType.begin(), requestedType.end(), requestedType.begin(),
                       [](unsigned char c) { return static_cast<char>(std::tolower(c)); });

        const std::string canonicalPattern = canonicalizeListPattern(req.pattern);
        configureListPatternQuery(canonicalPattern, queryOpts, useFallback, useTree, treePrefix);

        // Try tree-based query for path prefix patterns with full filter support
        if (useTree && !treePrefix.empty()) {
            // Pass full queryOpts to support tags, mime, extension, etc.
            auto treeDocsRes = ctx_.metadataRepo->queryDocumentsForListProjection(queryOpts);
            if (treeDocsRes) {
                docs = documentsFromProjections(std::move(treeDocsRes.value()));
                usedQuery = true;
                totalFoundApprox = docs.size();
            } else {
                // Tree query failed, fall back to SQL glob matching
                spdlog::warn("[LIST] Tree-based query failed: {}, falling back to SQL glob",
                             treeDocsRes.error().message);
                useTree = false;
            }
        }

        if (!useFallback && !useTree) {
            auto docsRes = ctx_.metadataRepo->queryDocumentsForListProjection(queryOpts);
            if (!docsRes) {
                return Error{ErrorCode::InternalError,
                             "Failed to query documents: " + docsRes.error().message};
            }
            docs = documentsFromProjections(std::move(docsRes.value()));
            usedQuery = true;
            totalFoundApprox = static_cast<std::size_t>(queryOpts.offset) + docs.size();
        }

        if (!usedQuery) {
            std::string sqlPattern =
                canonicalPattern.empty() ? "%" : globToSqlLike(canonicalPattern);
            auto docsRes = metadata::queryDocumentsByPattern(*ctx_.metadataRepo, sqlPattern);
            if (!docsRes) {
                return Error{ErrorCode::InternalError,
                             "Failed to query documents: " + docsRes.error().message};
            }

            docs = std::move(docsRes.value());
            applyFallbackListFilters(docs, req);
        }

        if (req.text || req.binary || !requestedType.empty()) {
            docs.erase(std::remove_if(docs.begin(), docs.end(),
                                      [&](const metadata::DocumentInfo& doc) {
                                          return !listDocumentMatchesTypeFilters(doc, req,
                                                                                 requestedType);
                                      }),
                       docs.end());
        }

        // Pagination: offset, limit
        int start = std::max(0, req.offset);
        int lim = std::max(0, req.limit);
        std::vector<metadata::DocumentInfo> page;
        if (usedQuery) {
            page = std::move(docs);
        } else if (start < static_cast<int>(docs.size())) {
            auto itStart = docs.begin() + start;
            auto itEnd = (lim > 0) ? std::min(docs.end(), itStart + lim) : docs.end();
            page.assign(itStart, itEnd);
        }

        ListDocumentsResponse out;
        out.totalFound = usedQuery ? totalFoundApprox : docs.size();
        out.count = page.size();
        out.sortBy = req.sortBy;
        out.sortOrder = req.sortOrder;
        if (!req.pattern.empty())
            out.pattern = req.pattern;
        if (!req.tags.empty())
            out.filteredByTags = req.tags;

        // Hot path: minimal listing avoids metadata/snippet hydration entirely.
        // When environment forces hot mode, still hydrate when the request explicitly asks.
        yams::cli::HotColdMode listMode = yams::cli::getListMode();
        bool forceHot = yams::cli::isForceHot(listMode);
        const bool wantsSnippets = req.showSnippets && req.snippetLength > 0;
        const bool wantsMetadata = req.showMetadata || req.showTags;
        const bool wantsHydration = wantsSnippets || wantsMetadata;
        if (req.pathsOnly || (forceHot && !wantsHydration)) {
            out.documents.reserve(page.size());
            std::unordered_set<std::string> seenPaths;
            for (const auto& d : page) {
                DocumentEntry e = makeBaseListEntry(d);
                if (req.pathsOnly) {
                    path_projection::appendUniquePath(
                        out.paths, seenPaths, path_projection::displayPath(e.path, e.fileName),
                        req.limit > 0 ? static_cast<std::size_t>(req.limit) : 0);
                }
                out.documents.push_back(std::move(e));
            }
            if (req.pathsOnly) {
                out.count = out.paths.size();
            }
            return out;
        }

        const std::vector<int64_t> docIds = collectDocumentIds(page);
        MetadataCache metadataCache;
        if (wantsMetadata) {
            metadataCache = hydrateListMetadata(docIds);
        }

        SnippetPreviewCache snippetPreviewCache;
        bool snippetFetchFailed = false;
        if (wantsSnippets) {
            auto hydrated = hydrateListSnippets(docIds, req.snippetLength);
            snippetPreviewCache = std::move(hydrated.first);
            snippetFetchFailed = hydrated.second;
        }

        auto buildEntryForDoc = [&](const metadata::DocumentInfo& doc) -> DocumentEntry {
            return buildHydratedListEntry(req, doc, metadataCache, snippetPreviewCache,
                                          snippetFetchFailed);
        };

        // Build entries in parallel when large pages, else sequential.
        const auto concurrencyOverride = listConcurrencyOverride();
        const bool useParallel = page.size() >= 200 || concurrencyOverride.has_value();
        if (useParallel) {
            std::vector<DocumentEntry> tmp(page.size());
            std::atomic<size_t> nextIdx{0};
            size_t workers = concurrencyOverride.value_or(
                std::max<size_t>(1, std::thread::hardware_concurrency()));
            workers = std::min(workers, page.size() > 0 ? page.size() : size_t{1});
            auto buildOne = [&](size_t i) { tmp[i] = buildEntryForDoc(page[i]); };
            std::vector<std::thread> ths;
            ths.reserve(workers);
            for (size_t t = 0; t < workers; ++t) {
                ths.emplace_back([&]() {
                    while (true) {
                        size_t i = nextIdx.fetch_add(1);
                        if (i >= page.size())
                            break;
                        buildOne(i);
                    }
                });
            }
            for (auto& th : ths)
                th.join();
            for (size_t i = 0; i < page.size(); ++i) {
                out.documents.push_back(std::move(tmp[i]));
            }
        } else {
            for (const auto& d : page) {
                out.documents.push_back(buildEntryForDoc(d));
            }
        }

        return out;
    }

    // Update metadata: accept pairs array, and/or keyValues map; return updates_applied and
    // document_id
    Result<UpdateMetadataResponse> updateMetadata(const UpdateMetadataRequest& req) override {
        if (!ctx_.metadataRepo) {
            return Error{ErrorCode::NotInitialized, "Metadata repository not available"};
        }

        std::string hash = req.hash;
        if (hash.empty() && !req.name.empty()) {
            auto r = resolveNameToHash(req.name);
            if (!r)
                return Error{r.error().code, r.error().message};
            hash = r.value();
        }
        if (hash.empty()) {
            return Error{ErrorCode::InvalidArgument, "Provide 'hash' or 'name' to update metadata"};
        }

        // Find target document by hash - use direct lookup instead of scanning all
        auto docRes = ctx_.metadataRepo->getDocumentByHash(hash);
        if (!docRes) {
            return Error{ErrorCode::InternalError,
                         "Failed to lookup document: " + docRes.error().message};
        }
        if (!docRes.value().has_value()) {
            return Error{ErrorCode::NotFound, "Document not found"};
        }
        auto target = docRes.value().value();

        UpdateMetadataResponse resp;
        resp.hash = hash;
        resp.documentId = target.id;

        // Create backup if requested
        if (req.createBackup && ctx_.store) {
            std::ostringstream contentStream;
            auto retrieveResult = ctx_.store->retrieveStream(hash, contentStream);
            if (retrieveResult) {
                // Store backup with timestamp suffix
                auto backupContent = contentStream.str();
                std::istringstream backupStream(backupContent);

                api::ContentMetadata backupMeta;
                backupMeta.name =
                    target.fileName + ".backup_" +
                    std::to_string(std::chrono::system_clock::now().time_since_epoch().count());
                backupMeta.mimeType = target.mimeType;

                auto stored = ctx_.store->storeStream(backupStream, backupMeta);
                if (stored) {
                    resp.backupHash = stored.value().contentHash;
                }
            }
        }

        YAMS_DCHECK(target.id > 0,
                    "document metadata updates should target a persisted document id");

        std::optional<std::string> pendingContentHash;

        // Handle content update if requested
        if (!req.newContent.empty() && ctx_.store) {
            std::istringstream contentStream(req.newContent);

            api::ContentMetadata contentMeta;
            contentMeta.name = target.fileName;
            contentMeta.mimeType = target.mimeType;

            auto stored = ctx_.store->storeStream(contentStream, contentMeta);
            if (!stored) {
                if (req.atomic) {
                    return Error{ErrorCode::InternalError,
                                 "Failed to update content: " + stored.error().message};
                }
            } else if (req.atomic) {
                pendingContentHash = stored.value().contentHash;
            } else {
                auto updateHash = ctx_.metadataRepo->setMetadata(
                    target.id, "content_hash", metadata::MetadataValue(stored.value().contentHash));
                if (updateHash) {
                    resp.contentUpdated = true;
                    resp.hash = stored.value().contentHash;
                }
            }
        }

        std::size_t count = 0;
        std::vector<std::string> errors;

        std::unordered_set<std::string> currentTagSet;
        if (!req.removeTags.empty()) {
            auto currentTags = ctx_.metadataRepo->getDocumentTags(target.id);
            if (currentTags) {
                currentTagSet.insert(currentTags.value().begin(), currentTags.value().end());
            }
        }

        if (req.atomic) {
            std::vector<std::tuple<int64_t, std::string, metadata::MetadataValue>> metadataEntries;
            metadataEntries.reserve(static_cast<std::size_t>(
                req.keyValues.size() + req.pairs.size() + req.addTags.size() +
                req.removeTags.size() + (pendingContentHash ? 1 : 0)));

            if (pendingContentHash.has_value()) {
                metadataEntries.emplace_back(target.id, "content_hash",
                                             metadata::MetadataValue(*pendingContentHash));
            }
            for (const auto& [k, v] : req.keyValues) {
                metadataEntries.emplace_back(target.id, k, metadata::MetadataValue(v));
            }
            for (const auto& p : req.pairs) {
                auto pos = p.find('=');
                if (pos == std::string::npos) {
                    continue;
                }
                metadataEntries.emplace_back(target.id, p.substr(0, pos),
                                             metadata::MetadataValue(p.substr(pos + 1)));
                count++;
            }
            count += req.keyValues.size();
            for (const auto& tag : req.addTags) {
                metadataEntries.emplace_back(target.id, "tag:" + tag, metadata::MetadataValue(tag));
            }
            for (const auto& tag : req.removeTags) {
                if (!currentTagSet.contains(tag)) {
                    continue;
                }
                metadataEntries.emplace_back(target.id, "tag:" + tag, metadata::MetadataValue(""));
                resp.tagsRemoved++;
            }

            if (!metadataEntries.empty()) {
                metadata::MetadataOpScope metadataScope("app_document_update_metadata");
                auto metadataResult = ctx_.metadataRepo->setMetadataBatch(metadataEntries);
                if (!metadataResult) {
                    return Error{ErrorCode::InternalError, "Failed to apply metadata batch: " +
                                                               metadataResult.error().message};
                }
            }

            resp.tagsAdded = req.addTags.size();
            if (pendingContentHash.has_value()) {
                resp.contentUpdated = true;
                resp.hash = *pendingContentHash;
            }
        } else {
            for (const auto& [k, v] : req.keyValues) {
                auto u = ctx_.metadataRepo->setMetadata(target.id, k, metadata::MetadataValue(v));
                if (!u) {
                    errors.push_back("Failed to update metadata: " + k);
                } else {
                    count++;
                }
            }

            for (const auto& p : req.pairs) {
                auto pos = p.find('=');
                if (pos == std::string::npos)
                    continue;
                std::string k = p.substr(0, pos);
                std::string v = p.substr(pos + 1);
                auto u = ctx_.metadataRepo->setMetadata(target.id, k, metadata::MetadataValue(v));
                if (!u) {
                    errors.push_back("Failed to update metadata: " + k);
                } else {
                    count++;
                }
            }

            for (const auto& tag : req.addTags) {
                auto u = ctx_.metadataRepo->setMetadata(target.id, "tag:" + tag,
                                                        metadata::MetadataValue(tag));
                if (!u) {
                    errors.push_back("Failed to add tag: " + tag);
                } else {
                    resp.tagsAdded++;
                }
            }

            for (const auto& tag : req.removeTags) {
                if (!currentTagSet.contains(tag)) {
                    continue;
                }
                auto u = ctx_.metadataRepo->setMetadata(target.id, "tag:" + tag,
                                                        metadata::MetadataValue(""));
                if (!u) {
                    errors.push_back("Failed to remove tag: " + tag);
                } else {
                    resp.tagsRemoved++;
                }
            }
        }

        resp.success = (errors.empty() || !req.atomic);
        resp.updatesApplied = count;
        return resp;
    }

    // Resolve name to hash using unified DocumentResolver
    Result<std::string> resolveNameToHash(const std::string& name, bool oldest = false) override {
        if (!ctx_.metadataRepo) {
            return Error{ErrorCode::NotInitialized, "Metadata repository not available"};
        }

        DocumentResolver<metadata::IMetadataRepository> resolver(*ctx_.metadataRepo);
        typename DocumentResolver<metadata::IMetadataRepository>::ResolveOptions opts;
        opts.oldest = oldest;
        opts.tryHashPrefix = false; // Name resolution shouldn't try hash prefix

        return resolver.resolveToHash(name, opts);
    }

    // Delete by name(s) or pattern (dry-run supported)
    // Uses DocumentResolver for unified resolution strategy
    Result<DeleteByNameResponse> deleteByName(const DeleteByNameRequest& req) override {
        if (!ctx_.store) {
            return Error{ErrorCode::NotInitialized, "Content store not available"};
        }
        if (!ctx_.metadataRepo) {
            return Error{ErrorCode::NotInitialized, "Metadata repository not available"};
        }

        DeleteByNameResponse resp;
        resp.dryRun = req.dryRun;

        auto targets = resolveDeleteTargets(req);
        if (!targets) {
            return targets.error();
        }

        for (const auto& target : targets.value()) {
            executeDeleteTarget(req, target, resp);
        }

        resp.count = req.dryRun ? targets.value().size() : resp.deleted.size();
        return resp;
    }

private:
    AppContext ctx_;
};

// Factory function for document service
std::shared_ptr<IDocumentService> makeDocumentService(const AppContext& ctx) {
    return std::make_shared<DocumentServiceImpl>(ctx);
}

} // namespace yams::app::services
