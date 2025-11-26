#include <yams/app/services/services.hpp>
// Hot/Cold mode helpers (env-driven)
#include "../../cli/hot_cold_utils.h"

#include <spdlog/spdlog.h>
#include <yams/api/content_store.h>
#include <yams/app/services/graph_query_service.hpp>
#include <yams/compression/compression_header.h>
#include <yams/compression/compression_utils.h>
#include <yams/compression/compressor_interface.h>
#include <yams/detection/file_type_detector.h>
#include <yams/extraction/extraction_util.h>
#include <yams/extraction/format_handlers/format_handler.hpp>
#include <yams/extraction/format_handlers/text_basic_handler.hpp>
#include <yams/extraction/text_extractor.h>

#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/path_utils.h>
#include <yams/metadata/query_helpers.h>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <optional>
#include <span>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace yams::app::services {

namespace {

constexpr std::size_t kCentroidPreviewLimit = 16;

inline bool startsWith(const std::string& s, const std::string& prefix) {
    return s.rfind(prefix, 0) == 0;
}

inline std::string normalizeExtension(const std::string& ext) {
    if (ext.empty())
        return ext;
    if (ext[0] == '.')
        return ext;
    return "." + ext;
}

inline bool isTextMime(const std::string& mime) {
    return !mime.empty() && startsWith(mime, "text/");
}

inline std::string toFileType(const std::string& mime) {
    return isTextMime(mime) ? "text" : "binary";
}

// Convert glob to SQL LIKE pattern. This is updated to better handle subpath matching
// by prepending a wildcard if the pattern appears to be a relative path.
inline std::string globToSqlLike(const std::string& glob) {
    if (glob.empty())
        return "%";
    std::string sql = glob;
    std::replace(sql.begin(), sql.end(), '*', '%');
    std::replace(sql.begin(), sql.end(), '?', '_');

    // Check if the path is absolute before modifying it.
    const bool is_absolute = !glob.empty() && std::filesystem::path(glob).is_absolute();

    const bool hasSlash =
        (sql.find('/') != std::string::npos) || (sql.find('\\') != std::string::npos);
    if (hasSlash && sql.front() != '%' && !is_absolute) {
        // Path segment, treat as suffix match
        sql = "%" + sql;
    } else if (!sql.empty() && sql.front() != '%' && !hasSlash) {
        // Heuristic to match filename at end of path when no directory component was provided
        sql = "%/" + sql;
    }
    return sql;
}

inline int64_t toEpochSeconds(const std::chrono::system_clock::time_point& tp) {
    return std::chrono::duration_cast<std::chrono::seconds>(tp.time_since_epoch()).count();
}

inline void populatePathDerivedFields(metadata::DocumentInfo& info) {
    auto derived = metadata::computePathDerivedValues(info.filePath);
    info.filePath = derived.normalizedPath;
    info.pathPrefix = derived.pathPrefix;
    info.reversePath = derived.reversePath;
    info.pathHash = derived.pathHash;
    info.parentHash = derived.parentHash;
    info.pathDepth = derived.pathDepth;
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
    header.algorithm = static_cast<uint8_t>(compression::CompressionAlgorithm::Zstandard);
    header.level = kDefaultLevel;
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
    doc.compressed = true;
    doc.compressionAlgorithm = header.algorithm;
    doc.compressionLevel = header.level;
    doc.uncompressedSize = header.uncompressedSize;
    doc.compressedCrc32 = header.compressedCRC32;
    doc.uncompressedCrc32 = header.uncompressedCRC32;
    const auto* bytes = reinterpret_cast<const uint8_t*>(&header);
    doc.compressionHeader.assign(bytes, bytes + sizeof(header));
    doc.size = header.uncompressedSize;
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

// Removed unused function trimLeadingDot

// Basic "tags" extraction heuristic from metadata rows
inline std::vector<std::string>
extractTags(const std::unordered_map<std::string, yams::metadata::MetadataValue>& all) {
    std::vector<std::string> tags;
    for (const auto& [key, value] : all) {
        if (key == "tag" || startsWith(key, "tag:")) {
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
        if (!req.snapshotId.empty()) {
            md.tags["snapshot_id"] = req.snapshotId;
        }
        if (!req.snapshotLabel.empty()) {
            md.tags["snapshot_label"] = req.snapshotLabel;
        }
        if (req.noEmbeddings) {
            md.tags["no_embeddings"] = "true";
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
            std::filesystem::create_directories(tmpToRemove->parent_path(), ec);
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
        // Clean up temp file if used
        if (tmpToRemove) {
            std::error_code ec;
            std::filesystem::remove(*tmpToRemove, ec);
        }
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
                info.fileName = p.filename().string();
                info.fileExtension = p.extension().string();
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
                try {
                    (void)yams::detection::FileTypeDetector::initializeWithMagicNumbers();
                    auto& det = yams::detection::FileTypeDetector::instance();
                    if (auto sig = det.detectFromFile(p)) {
                        if (!sig.value().mimeType.empty())
                            info.mimeType = sig.value().mimeType;
                    }
                    if (info.mimeType.empty()) {
                        info.mimeType = yams::detection::FileTypeDetector::getMimeTypeFromExtension(
                            p.extension().string());
                    }
                } catch (...) {
                    // fallback below
                }
            }
            if (info.mimeType.empty())
                info.mimeType = "application/octet-stream";
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

            populatePathDerivedFields(info);

            auto ins = ctx_.metadataRepo->insertDocument(info);
            if (ins) {
                int64_t docId = ins.value();
                for (const auto& [k, v] : md.tags) {
                    (void)ctx_.metadataRepo->setMetadata(docId, k, metadata::MetadataValue(v));
                }
                // Best-effort: index content into FTS5 using robust extraction (plugins +
                // built-ins)
                try {
                    auto extractedOpt = yams::extraction::util::extractDocumentText(
                        ctx_.store, out.hash, info.mimeType, info.fileExtension,
                        ctx_.contentExtractors);
                    if (extractedOpt && !extractedOpt->empty()) {
                        const std::string& extracted = *extractedOpt;

                        if (docId > 0) {
                            metadata::DocumentContent contentRow;
                            contentRow.documentId = docId;
                            contentRow.contentText = extracted;
                            contentRow.contentLength =
                                static_cast<int64_t>(contentRow.contentText.size());
                            contentRow.extractionMethod = "inline";
                            double langConfidence = 0.0;
                            contentRow.language =
                                yams::extraction::LanguageDetector::detectLanguage(
                                    contentRow.contentText, &langConfidence);
                            auto contentUpsert = ctx_.metadataRepo->insertContent(contentRow);
                            if (!contentUpsert) {
                                spdlog::warn("Failed to upsert extracted content for {}: {}",
                                             out.hash, contentUpsert.error().message);
                            }
                        }

                        (void)ctx_.metadataRepo->indexDocumentContent(docId, info.fileName,
                                                                      extracted, info.mimeType);
                        (void)ctx_.metadataRepo->updateFuzzyIndex(docId);
                        // Try to update extraction flags on the document row
                        auto d = ctx_.metadataRepo->getDocument(docId);
                        if (d && d.value().has_value()) {
                            auto updated = d.value().value();
                            updated.contentExtracted = true;
                            updated.extractionStatus = metadata::ExtractionStatus::Success;
                            (void)ctx_.metadataRepo->updateDocument(updated);
                        }
                    } else {
                        // Mark as attempted but possibly skipped/failed for non-text types
                        auto d = ctx_.metadataRepo->getDocument(docId);
                        if (d && d.value().has_value()) {
                            auto updated = d.value().value();
                            updated.contentExtracted = false;
                            updated.extractionStatus = metadata::ExtractionStatus::Skipped;
                            (void)ctx_.metadataRepo->updateDocument(updated);
                        }
                    }
                } catch (...) {
                    // Non-fatal: indexing is opportunistic here
                }

                // Update path tree for this document (best-effort)
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

        RetrieveDocumentResponse resp;
        resp.graphEnabled = req.graph;

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
                    return Error{bytesResult.error().code, "Document content not found"};
                }
                auto data = std::move(bytesResult.value());
                auto payloadResult =
                    makeCompressedPayload(std::span<const std::byte>(data.data(), data.size()));
                if (!payloadResult) {
                    return payloadResult.error();
                }
                auto payload = std::move(payloadResult.value());
                doc.content.emplace(std::move(payload.blob));
                applyCompressionMetadata(doc, payload.header);
            } else {
                auto bytesResult = ctx_.store->retrieveBytes(resolvedHash);
                if (!bytesResult) {
                    return Error{bytesResult.error().code, "Document content not found"};
                }
                auto data = std::move(bytesResult.value());
                doc.content.emplace(reinterpret_cast<const char*>(data.data()), data.size());
                markUncompressed(doc, static_cast<uint64_t>(data.size()));
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
                auto docsRes = metadata::queryDocumentsByPattern(*ctx_.metadataRepo, "%");
                if (docsRes) {
                    int count = 0;
                    for (const auto& other : docsRes.value()) {
                        if (other.sha256Hash == foundDoc->sha256Hash)
                            continue;
                        if (std::filesystem::path(other.filePath).parent_path() == baseDir) {
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
            }
        }

        return resp;
    }

    // Cat: by hash or by name -> content + size
    Result<CatDocumentResponse> cat(const CatDocumentRequest& req) override {
        if (!ctx_.store) {
            return Error{ErrorCode::NotInitialized, "Content store not available"};
        }
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

        std::vector<metadata::DocumentInfo> docs;
        bool usedQuery = false;
        std::size_t totalFoundApprox = 0;

        metadata::DocumentQueryOptions queryOpts;
        queryOpts.limit = req.limit > 0 ? req.limit : 0;
        queryOpts.offset = std::max(0, req.offset);
        queryOpts.pathsOnly = req.pathsOnly;

        auto normalizeSingleExtension = [&](const std::string& ext) -> std::optional<std::string> {
            if (ext.empty())
                return std::nullopt;
            if (ext.find(',') != std::string::npos)
                return std::nullopt; // delegate to fallback for multi-extension pattern
            return normalizeExtension(ext);
        };

        if (auto normExt = normalizeSingleExtension(req.extension))
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

        if (req.sortBy == "name") {
            queryOpts.orderByNameAsc = (req.sortOrder != "desc");
        } else {
            queryOpts.orderByIndexedDesc = (req.sortOrder != "asc");
        }

        bool useFallback = false;
        bool useTree = false;
        std::string treePrefix;

        if (!req.pattern.empty()) {
            const auto& pattern = req.pattern;
            auto wildcardPos = pattern.find_first_of("*?");
            bool hasWildcard = wildcardPos != std::string::npos;

            if (!hasWildcard) {
                queryOpts.exactPath = pattern;
            } else if (pattern.back() == '*') {
                // Handle patterns ending with wildcards: /path/* or /path/**
                // Strip trailing wildcards to get the prefix
                std::string prefix = pattern;
                while (!prefix.empty() && (prefix.back() == '*' || prefix.back() == '?'))
                    prefix.pop_back();

                // Also strip trailing slashes
                while (!prefix.empty() && (prefix.back() == '/' || prefix.back() == '\\'))
                    prefix.pop_back();

                // Use tree-based query for path prefixes (PBI-043)
                // Tree query now supports all filters via queryDocuments
                if (!prefix.empty()) {
                    useTree = true;
                    treePrefix = prefix;
                    spdlog::info("[LIST] Using tree-based query for prefix: '{}' (with filters: "
                                 "tags={} mime={} ext={})",
                                 treePrefix, !req.tags.empty(), !req.mime.empty(),
                                 !req.extension.empty());
                }

                // Also set queryOpts for the tree path to use
                queryOpts.pathPrefix = prefix;
                queryOpts.prefixIsDirectory = true; // Patterns with * imply directory recursion
            } else if (pattern.front() == '*' &&
                       pattern.find_first_of("*?", wildcardPos + 1) == std::string::npos) {
                queryOpts.containsFragment = pattern.substr(1);
                queryOpts.containsUsesFts = true;
            } else {
                useFallback = true;
            }
        }

        // Try tree-based query for path prefix patterns with full filter support
        if (useTree && !treePrefix.empty()) {
            // Pass full queryOpts to support tags, mime, extension, etc.
            auto treeDocsRes = ctx_.metadataRepo->queryDocuments(queryOpts);
            if (treeDocsRes) {
                docs = std::move(treeDocsRes.value());
                usedQuery = true;
                totalFoundApprox = docs.size();
                spdlog::info("[LIST] Tree-based query (via queryDocuments) returned {} documents",
                             docs.size());
            } else {
                // Tree query failed, fall back to SQL glob matching
                spdlog::warn("[LIST] Tree-based query failed: {}, falling back to SQL glob",
                             treeDocsRes.error().message);
                useTree = false;
            }
        }

        if (!useFallback && !useTree) {
            auto docsRes = ctx_.metadataRepo->queryDocuments(queryOpts);
            if (!docsRes) {
                return Error{ErrorCode::InternalError,
                             "Failed to query documents: " + docsRes.error().message};
            }
            docs = std::move(docsRes.value());
            usedQuery = true;
            totalFoundApprox = static_cast<std::size_t>(queryOpts.offset) + docs.size();
        }

        if (!usedQuery) {
            std::string sqlPattern = req.pattern.empty() ? "%" : globToSqlLike(req.pattern);
            auto docsRes = metadata::queryDocumentsByPattern(*ctx_.metadataRepo, sqlPattern);
            if (!docsRes) {
                return Error{ErrorCode::InternalError,
                             "Failed to query documents: " + docsRes.error().message};
            }

            docs = std::move(docsRes.value());

            // Filter by extension (accept ".md" or "md")
            if (!req.extension.empty()) {
                std::string wanted = normalizeExtension(req.extension);
                docs.erase(std::remove_if(docs.begin(), docs.end(),
                                          [&](const metadata::DocumentInfo& d) {
                                              std::string ext = d.fileExtension;
                                              return normalizeExtension(ext) != wanted;
                                          }),
                           docs.end());
            }
        }

        if (!usedQuery) {
            // Filter by type (text/binary) or explicit flags
            if (req.text || req.type == "text") {
                docs.erase(std::remove_if(docs.begin(), docs.end(),
                                          [](const metadata::DocumentInfo& d) {
                                              return !isTextMime(d.mimeType);
                                          }),
                           docs.end());
            } else if (req.binary || req.type == "binary") {
                docs.erase(std::remove_if(docs.begin(), docs.end(),
                                          [](const metadata::DocumentInfo& d) {
                                              return isTextMime(d.mimeType);
                                          }),
                           docs.end());
            }

            // Filter by tags (presence-based)
            if (!req.tags.empty()) {
                std::vector<metadata::DocumentInfo> filtered;
                for (const auto& d : docs) {
                    auto md = ctx_.metadataRepo->getAllMetadata(d.id);
                    if (!md)
                        continue;
                    auto tags = extractTags(md.value());
                    bool has = false;
                    for (const auto& t : req.tags) {
                        if (std::find(tags.begin(), tags.end(), t) != tags.end()) {
                            has = true;
                            break;
                        }
                    }
                    if (has)
                        filtered.push_back(d);
                }
                docs.swap(filtered);
            }

            // Recent: keep N most recently indexed (desc)
            if (req.recent && *req.recent > 0) {
                std::sort(docs.begin(), docs.end(), [](const auto& a, const auto& b) {
                    return a.indexedTime > b.indexedTime;
                });
                if (static_cast<int>(docs.size()) > *req.recent) {
                    docs.resize(static_cast<size_t>(*req.recent));
                }
            }

            // Sorting (minimal): name, asc|desc; default to req.sortBy if present
            if (req.sortBy == "name") {
                std::sort(docs.begin(), docs.end(),
                          [](const auto& a, const auto& b) { return a.fileName < b.fileName; });
                if (req.sortOrder == "desc") {
                    std::reverse(docs.begin(), docs.end());
                }
            }
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

        std::unordered_map<int64_t, std::unordered_map<std::string, metadata::MetadataValue>>
            metadataCache;
        if (!page.empty() &&
            (req.showMetadata || req.showTags || (!req.tags.empty() && !usedQuery))) {
            std::vector<int64_t> docIds;
            docIds.reserve(page.size());
            for (const auto& doc : page)
                docIds.push_back(doc.id);
            auto metaRes =
                ctx_.metadataRepo->getMetadataForDocuments(std::span<const int64_t>(docIds));
            if (metaRes)
                metadataCache = std::move(metaRes.value());
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

        // Hot path: paths-only/minimal listing avoids metadata/snippet hydration entirely.
        // Also engage when environment forces hot mode.
        yams::cli::HotColdMode listMode = yams::cli::getListMode();
        bool forceHot = yams::cli::isForceHot(listMode);
        if (req.pathsOnly || forceHot) {
            out.documents.reserve(page.size());
            for (const auto& d : page) {
                DocumentEntry e;
                e.name = d.fileName;
                e.fileName = d.fileName;
                e.hash = d.sha256Hash;
                e.path = d.filePath;
                e.extension = d.fileExtension;
                e.size = static_cast<uint64_t>(d.fileSize);
                e.mimeType = d.mimeType;
                e.fileType = toFileType(d.mimeType);
                e.created = toEpochSeconds(d.createdTime);
                e.modified = toEpochSeconds(d.modifiedTime);
                e.indexed = toEpochSeconds(d.indexedTime);
                out.documents.push_back(std::move(e));
            }
            return out;
        }

        // Build entries in parallel when large pages, else sequential
        const bool useParallel = page.size() >= 200 || std::getenv("YAMS_LIST_CONCURRENCY");
        if (useParallel) {
            std::vector<DocumentEntry> tmp(page.size());
            std::atomic<size_t> nextIdx{0};
            size_t hw = std::max<size_t>(1, std::thread::hardware_concurrency());
            size_t workers = hw;
            if (const char* env = std::getenv("YAMS_LIST_CONCURRENCY"); env && *env) {
                try {
                    auto v = static_cast<size_t>(std::stoul(env));
                    if (v > 0)
                        workers = v;
                } catch (...) {
                }
            }
            workers = std::min(workers, page.size() > 0 ? page.size() : size_t{1});
            auto buildOne = [&](size_t i) {
                const auto& d = page[i];
                DocumentEntry e;
                e.name = d.fileName;
                e.fileName = d.fileName;
                e.hash = d.sha256Hash;
                e.path = d.filePath;
                e.extension = d.fileExtension;
                e.size = static_cast<uint64_t>(d.fileSize);
                e.mimeType = d.mimeType;
                e.fileType = toFileType(d.mimeType);
                e.created = toEpochSeconds(d.createdTime);
                e.modified = toEpochSeconds(d.modifiedTime);
                e.indexed = toEpochSeconds(d.indexedTime);
                const auto* cachedMetadata =
                    [&]() -> const std::unordered_map<std::string, metadata::MetadataValue>* {
                    auto it = metadataCache.find(d.id);
                    return it == metadataCache.end() ? nullptr : &it->second;
                }();
                if (req.showSnippets && req.snippetLength > 0) {
                    auto contentResult = ctx_.metadataRepo->getContent(d.id);
                    if (contentResult) {
                        const auto& optionalContent = contentResult.value();
                        if (optionalContent.has_value()) {
                            const auto& content = optionalContent.value();
                            if (!content.contentText.empty() && content.contentText.length() > 3) {
                                std::string snippet = content.contentText;
                                if (snippet.length() > static_cast<size_t>(req.snippetLength)) {
                                    snippet = snippet.substr(
                                        0, static_cast<size_t>(req.snippetLength - 3));
                                    size_t lastSpace = snippet.find_last_of(' ');
                                    if (lastSpace != std::string::npos &&
                                        lastSpace > static_cast<size_t>(req.snippetLength / 2)) {
                                        snippet = snippet.substr(0, lastSpace);
                                    }
                                    snippet += "...";
                                }
                                std::string cleaned;
                                cleaned.reserve(snippet.size());
                                bool lastWasSpace = false;
                                for (char c : snippet) {
                                    if (std::isspace(static_cast<unsigned char>(c))) {
                                        if (!lastWasSpace) {
                                            cleaned += ' ';
                                            lastWasSpace = true;
                                        }
                                    } else {
                                        cleaned += c;
                                        lastWasSpace = false;
                                    }
                                }
                                e.snippet = std::move(cleaned);
                            } else {
                                e.snippet = "[No text content]";
                            }
                        } else {
                            e.snippet = "[Content not available]";
                        }
                    } else {
                        e.snippet = "[Content extraction failed]";
                    }
                }
                if (req.showTags || req.showMetadata) {
                    if (cachedMetadata) {
                        if (req.showTags) {
                            e.tags = extractTags(*cachedMetadata);
                        }
                        if (req.showMetadata) {
                            for (const auto& [key, value] : *cachedMetadata) {
                                e.metadata[key] = value.value;
                            }
                        }
                    }
                }
                tmp[i] = std::move(e);
            };
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
                DocumentEntry e;
                e.name = d.fileName;
                e.fileName = d.fileName;
                e.hash = d.sha256Hash;
                e.path = d.filePath;
                e.extension = d.fileExtension;
                e.size = static_cast<uint64_t>(d.fileSize);
                e.mimeType = d.mimeType;
                e.fileType = toFileType(d.mimeType);
                e.created = toEpochSeconds(d.createdTime);
                e.modified = toEpochSeconds(d.modifiedTime);
                e.indexed = toEpochSeconds(d.indexedTime);
                const auto* cachedMetadata =
                    [&]() -> const std::unordered_map<std::string, metadata::MetadataValue>* {
                    auto it = metadataCache.find(d.id);
                    return it == metadataCache.end() ? nullptr : &it->second;
                }();
                if (req.showSnippets && req.snippetLength > 0) {
                    auto contentResult = ctx_.metadataRepo->getContent(d.id);
                    if (contentResult) {
                        const auto& optionalContent = contentResult.value();
                        if (optionalContent.has_value()) {
                            const auto& content = optionalContent.value();
                            if (!content.contentText.empty() && content.contentText.length() > 3) {
                                std::string snippet = content.contentText;
                                if (snippet.length() > static_cast<size_t>(req.snippetLength)) {
                                    snippet = snippet.substr(
                                        0, static_cast<size_t>(req.snippetLength - 3));
                                    size_t lastSpace = snippet.find_last_of(' ');
                                    if (lastSpace != std::string::npos &&
                                        lastSpace > static_cast<size_t>(req.snippetLength / 2)) {
                                        snippet = snippet.substr(0, lastSpace);
                                    }
                                    snippet += "...";
                                }
                                std::string cleaned;
                                cleaned.reserve(snippet.size());
                                bool lastWasSpace = false;
                                for (char c : snippet) {
                                    if (std::isspace(static_cast<unsigned char>(c))) {
                                        if (!lastWasSpace) {
                                            cleaned += ' ';
                                            lastWasSpace = true;
                                        }
                                    } else {
                                        cleaned += c;
                                        lastWasSpace = false;
                                    }
                                }
                                e.snippet = std::move(cleaned);
                            } else {
                                e.snippet = "[No text content]";
                            }
                        } else {
                            e.snippet = "[Content not available]";
                        }
                    } else {
                        e.snippet = "[Content extraction failed]";
                    }
                }
                if (req.showTags || req.showMetadata) {
                    if (cachedMetadata) {
                        if (req.showTags) {
                            e.tags = extractTags(*cachedMetadata);
                        }
                        if (req.showMetadata) {
                            for (const auto& [key, value] : *cachedMetadata) {
                                e.metadata[key] = value.value;
                            }
                        }
                    }
                }
                out.documents.push_back(std::move(e));
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

        // Find target document by hash
        auto docsRes = metadata::queryDocumentsByPattern(*ctx_.metadataRepo, "%");
        if (!docsRes) {
            return Error{ErrorCode::InternalError,
                         "Failed to enumerate documents: " + docsRes.error().message};
        }

        std::optional<metadata::DocumentInfo> target;
        for (const auto& d : docsRes.value()) {
            if (d.sha256Hash == hash) {
                target = d;
                break;
            }
        }
        if (!target) {
            return Error{ErrorCode::NotFound, "Document not found"};
        }

        UpdateMetadataResponse resp;
        resp.hash = hash;
        resp.documentId = target->id;

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
                    target->fileName + ".backup_" +
                    std::to_string(std::chrono::system_clock::now().time_since_epoch().count());
                backupMeta.mimeType = target->mimeType;

                auto stored = ctx_.store->storeStream(backupStream, backupMeta);
                if (stored) {
                    resp.backupHash = stored.value().contentHash;
                }
            }
        }

        // Handle content update if requested
        if (!req.newContent.empty() && ctx_.store) {
            std::istringstream contentStream(req.newContent);

            api::ContentMetadata contentMeta;
            contentMeta.name = target->fileName;
            contentMeta.mimeType = target->mimeType;

            auto stored = ctx_.store->storeStream(contentStream, contentMeta);
            if (!stored) {
                if (req.atomic) {
                    return Error{ErrorCode::InternalError,
                                 "Failed to update content: " + stored.error().message};
                }
            } else {
                // Update the document's hash in metadata
                auto updateHash = ctx_.metadataRepo->setMetadata(
                    target->id, "content_hash",
                    metadata::MetadataValue(stored.value().contentHash));
                if (updateHash) {
                    resp.contentUpdated = true;
                    resp.hash = stored.value().contentHash; // Update to new hash
                }
            }
        }

        std::size_t count = 0;
        std::vector<std::string> errors;

        // Apply metadata updates
        for (const auto& [k, v] : req.keyValues) {
            auto u = ctx_.metadataRepo->setMetadata(target->id, k, metadata::MetadataValue(v));
            if (!u) {
                errors.push_back("Failed to update metadata: " + k);
                if (req.atomic) {
                    return Error{ErrorCode::InternalError, errors.back()};
                }
            } else {
                count++;
            }
        }

        // Apply pairs "k=v"
        for (const auto& p : req.pairs) {
            auto pos = p.find('=');
            if (pos == std::string::npos)
                continue;
            std::string k = p.substr(0, pos);
            std::string v = p.substr(pos + 1);
            auto u = ctx_.metadataRepo->setMetadata(target->id, k, metadata::MetadataValue(v));
            if (!u) {
                errors.push_back("Failed to update metadata: " + k);
                if (req.atomic) {
                    return Error{ErrorCode::InternalError, errors.back()};
                }
            } else {
                count++;
            }
        }

        // Handle tag additions (tags are stored as metadata with "tag:" prefix)
        for (const auto& tag : req.addTags) {
            auto u = ctx_.metadataRepo->setMetadata(target->id, "tag:" + tag,
                                                    metadata::MetadataValue("true"));
            if (!u) {
                errors.push_back("Failed to add tag: " + tag);
                if (req.atomic) {
                    return Error{ErrorCode::InternalError, errors.back()};
                }
            } else {
                resp.tagsAdded++;
            }
        }

        // Handle tag removals (remove metadata entries with "tag:" prefix)
        for (const auto& tag : req.removeTags) {
            // Get current tags to verify it exists
            auto currentTags = ctx_.metadataRepo->getDocumentTags(target->id);
            bool tagExists = false;
            if (currentTags) {
                for (const auto& existingTag : currentTags.value()) {
                    if (existingTag == tag) {
                        tagExists = true;
                        break;
                    }
                }
            }

            if (tagExists) {
                // Remove by setting to empty/null value (this typically removes the metadata entry)
                auto u = ctx_.metadataRepo->setMetadata(target->id, "tag:" + tag,
                                                        metadata::MetadataValue(""));
                if (!u) {
                    errors.push_back("Failed to remove tag: " + tag);
                    if (req.atomic) {
                        return Error{ErrorCode::InternalError, errors.back()};
                    }
                } else {
                    resp.tagsRemoved++;
                }
            }
        }

        // Update fuzzy index
        ctx_.metadataRepo->updateFuzzyIndex(target->id);

        resp.success = (errors.empty() || !req.atomic);
        resp.updatesApplied = count;
        return resp;
    }

    // Tree-aware path resolution using PathTreeNode infrastructure (PBI-053)
    std::optional<std::string> resolvePathViaTree(const std::string& path) {
        if (!ctx_.metadataRepo) {
            return std::nullopt;
        }

        // Normalize path separators
        std::string normalizedPath = path;
        std::replace(normalizedPath.begin(), normalizedPath.end(), '\\', '/');

        // Try exact path lookup in tree
        auto treeNodeRes = ctx_.metadataRepo->findPathTreeNodeByFullPath(normalizedPath);
        if (!treeNodeRes) {
            spdlog::debug("[RESOLVE-TRACE] Tree lookup failed: {}", treeNodeRes.error().message);
            return std::nullopt;
        }

        if (!treeNodeRes.value().has_value()) {
            spdlog::debug("[RESOLVE-TRACE] Path not found in tree: '{}'", normalizedPath);
            return std::nullopt;
        }

        const auto& node = treeNodeRes.value().value();
        spdlog::info("[RESOLVE-TRACE] ✓ Found in tree: node_id={}, fullPath='{}'", node.id,
                     node.fullPath);

        // Get the document associated with this path node
        auto docRes = ctx_.metadataRepo->findDocumentByExactPath(node.fullPath);
        if (!docRes || !docRes.value().has_value()) {
            spdlog::warn("[RESOLVE-TRACE] Tree node exists but no document found for path: '{}'",
                         node.fullPath);
            return std::nullopt;
        }

        return docRes.value().value().sha256Hash;
    }

    // Resolve name to hash (disambiguate or error)
    Result<std::string> resolveNameToHash(const std::string& name, bool oldest = false) override {
        spdlog::info("[RESOLVE-TRACE] === Starting resolveNameToHash for: '{}' (oldest={})", name,
                     oldest);

        if (!ctx_.metadataRepo) {
            return Error{ErrorCode::NotInitialized, "Metadata repository not available"};
        }

        // Strategy 1: Tree-based lookup (highest priority, PBI-053)
        auto treeResult = resolvePathViaTree(name);
        if (treeResult.has_value()) {
            spdlog::info("[RESOLVE-TRACE] ✓ Resolved via tree");
            return treeResult.value();
        }

        // Strategy 2: Exact path match
        auto exactRes = ctx_.metadataRepo->findDocumentByExactPath(name);
        if (exactRes && exactRes.value().has_value()) {
            spdlog::info("[RESOLVE-TRACE] ✓ Resolved via exact path");
            return exactRes.value().value().sha256Hash;
        }

        // Strategy 3: Path suffix or filename match
        std::vector<std::string> candidatePatterns;

        // Always try basename (filename only) match
        std::filesystem::path inputPath(name);
        std::string basename = inputPath.filename().string();

        // If input is just a basename (no path separators), try suffix match
        if (!basename.empty() && name.find('/') == std::string::npos &&
            name.find('\\') == std::string::npos) {
            candidatePatterns.push_back("%/" + basename);
        }

        // If input is a path, add basename separately
        if (!basename.empty() && basename != name) {
            candidatePatterns.push_back(basename);
        }

        // Try full path suffix match
        if (name.find('/') != std::string::npos || name.find('\\') != std::string::npos) {
            candidatePatterns.push_back("%" + name);
        }

        // Strategy 4: For absolute paths, try without the directory component mismatch
        // e.g., if indexed from /path/a/file.txt but queried from /path/b/file.txt
        if (!name.empty() && (name[0] == '/' || name.find(":\\") != std::string::npos)) {
            try {
                std::filesystem::path absPath(name);
                // Try suffix patterns of increasing specificity
                // E.g., for /home/user/Downloads/docs/file.txt try:
                //   - docs/file.txt
                //   - Downloads/docs/file.txt
                std::string pathStr = absPath.string();
                size_t pos = 0;
                while ((pos = pathStr.find('/', pos + 1)) != std::string::npos) {
                    std::string suffix = pathStr.substr(pos + 1);
                    if (!suffix.empty() && suffix != basename) {
                        candidatePatterns.push_back("%" + suffix);
                    }
                }
            } catch (...) {
                // Ignore path manipulation errors
            }
        }

        // Try suffix/relative patterns
        std::vector<metadata::DocumentInfo> allMatches;
        for (const auto& pattern : candidatePatterns) {
            auto res = metadata::queryDocumentsByPattern(*ctx_.metadataRepo, pattern);
            if (res && !res.value().empty()) {
                allMatches.insert(allMatches.end(), res.value().begin(), res.value().end());
            }
        }

        // Deduplicate by hash
        std::unordered_set<std::string> seenHashes;
        std::vector<metadata::DocumentInfo> uniqueMatches;
        for (const auto& doc : allMatches) {
            if (seenHashes.insert(doc.sha256Hash).second) {
                uniqueMatches.push_back(doc);
            }
        }

        if (uniqueMatches.empty()) {
            spdlog::warn("[RESOLVE-TRACE] ❌ No matches found for: '{}'", name);
            return Error{ErrorCode::NotFound, "Document not found: " + name};
        }
        if (uniqueMatches.size() > 1) {
            const char* strategy = oldest ? "oldest" : "most recent";
            spdlog::warn("[RESOLVE-TRACE] ⚠️ Ambiguous: {} matches - returning {}",
                         uniqueMatches.size(), strategy);

            // Sort by indexedTime (descending for latest, ascending for oldest)
            std::sort(uniqueMatches.begin(), uniqueMatches.end(),
                      [oldest](const metadata::DocumentInfo& a, const metadata::DocumentInfo& b) {
                          return oldest ? (a.indexedTime < b.indexedTime)
                                        : (a.indexedTime > b.indexedTime);
                      });

            // Log the selection for transparency
            spdlog::info("[RESOLVE-TRACE] ✓ Selected {}: {} (indexed: {})", strategy,
                         uniqueMatches[0].fileName,
                         uniqueMatches[0].indexedTime.time_since_epoch().count());

            // Return the selected match
            return uniqueMatches[0].sha256Hash;
        }

        spdlog::info("[RESOLVE-TRACE] ✓ Resolved via pattern matching");
        return uniqueMatches[0].sha256Hash;
    }

    // Delete by name(s) or pattern (dry-run supported)
    Result<DeleteByNameResponse> deleteByName(const DeleteByNameRequest& req) override {
        if (!ctx_.store) {
            return Error{ErrorCode::NotInitialized, "Content store not available"};
        }
        if (!ctx_.metadataRepo) {
            return Error{ErrorCode::NotInitialized, "Metadata repository not available"};
        }

        DeleteByNameResponse resp;
        resp.dryRun = req.dryRun;

        std::vector<std::pair<std::string, std::string>> targets; // (name, hash)

        auto addDocVec = [&](const std::vector<metadata::DocumentInfo>& v) {
            for (const auto& d : v) {
                targets.emplace_back(d.fileName, d.sha256Hash);
            }
        };

        // If a hash is provided (full or prefix), resolve it first
        if (!req.hash.empty()) {
            auto all = metadata::queryDocumentsByPattern(*ctx_.metadataRepo, "%");
            if (!all) {
                return Error{ErrorCode::InternalError,
                             "Failed to enumerate documents: " + all.error().message};
            }
            const bool isPrefix = req.hash.size() < 64; // treat shorter as prefix
            std::vector<metadata::DocumentInfo> matched;
            for (const auto& d : all.value()) {
                if ((isPrefix && d.sha256Hash.rfind(req.hash, 0) == 0) ||
                    (!isPrefix && d.sha256Hash == req.hash)) {
                    matched.push_back(d);
                }
            }
            if (!matched.empty()) {
                addDocVec(matched);
            }
        }

        if (!req.pattern.empty()) {
            auto pat = globToSqlLike(req.pattern);
            auto res = metadata::queryDocumentsByPattern(*ctx_.metadataRepo, pat);
            if (res && !res.value().empty()) {
                addDocVec(res.value());
            }
        }
        for (const auto& n : req.names) {
            auto res = metadata::queryDocumentsByPattern(*ctx_.metadataRepo, "%/" + n);
            if (!res || res.value().empty()) {
                // Try direct path match
                auto res2 = metadata::queryDocumentsByPattern(*ctx_.metadataRepo, n);
                if (res2 && !res2.value().empty())
                    addDocVec(res2.value());
                continue;
            }
            addDocVec(res.value());
        }
        if (!req.name.empty()) {
            auto res = metadata::queryDocumentsByPattern(*ctx_.metadataRepo, "%/" + req.name);
            if (!res || res.value().empty()) {
                auto res2 = metadata::queryDocumentsByPattern(*ctx_.metadataRepo, req.name);
                if (res2 && !res2.value().empty())
                    addDocVec(res2.value());
            } else {
                addDocVec(res.value());
            }
        }

        // De-duplicate targets by hash
        std::sort(targets.begin(), targets.end(),
                  [](const auto& a, const auto& b) { return a.second < b.second; });
        targets.erase(
            std::unique(targets.begin(), targets.end(),
                        [](const auto& a, const auto& b) { return a.second == b.second; }),
            targets.end());

        for (const auto& [name, hash] : targets) {
            DeleteByNameResult r;
            r.name = name;
            r.hash = hash;
            if (!req.dryRun) {
                bool storeDeleted = false;
                if (auto del = ctx_.store->remove(hash)) {
                    storeDeleted = del.value();
                    if (!storeDeleted) {
                        r.error = "Document not found";
                        resp.errors.push_back(r);
                        continue;
                    }
                } else {
                    r.deleted = false;
                    r.error = del.error().message;
                    resp.errors.push_back(r);
                    continue;
                }

                bool metadataOk = true;
                if (ctx_.metadataRepo) {
                    auto docInfo = ctx_.metadataRepo->getDocumentByHash(hash);
                    if (docInfo && docInfo.value().has_value()) {
                        auto deleteResult = ctx_.metadataRepo->deleteDocument(docInfo.value()->id);
                        if (!deleteResult) {
                            metadataOk = false;
                            r.error = "Failed to delete metadata: " + deleteResult.error().message;
                            resp.errors.push_back(r);
                        }
                    }
                }

                if (metadataOk) {
                    r.deleted = storeDeleted;
                    resp.deleted.push_back(r);
                }
            } else {
                r.deleted = false;
                resp.deleted.push_back(r);
            }
        }

        resp.count = req.dryRun ? targets.size() : resp.deleted.size();
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
