#include <yams/app/services/services.hpp>
// Hot/Cold mode helpers (env-driven)
#include "../../cli/hot_cold_utils.h"

#include <spdlog/spdlog.h>
#include <yams/api/content_store.h>
#include <yams/detection/file_type_detector.h>
#include <yams/extraction/extraction_util.h>
#include <yams/extraction/format_handlers/format_handler.hpp>
#include <yams/extraction/format_handlers/text_basic_handler.hpp>
#include <yams/extraction/text_extractor.h>

#include <yams/metadata/metadata_repository.h>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <optional>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace yams::app::services {

namespace {

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
            info.filePath = p.string();
            // If we created a temporary file from content+name, prefer the logical name
            if (!req.path.empty()) {
                info.fileName = p.filename().string();
                info.fileExtension = p.extension().string();
            } else {
                // Use the provided document name for metadata visibility and queries
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
            auto now = std::chrono::system_clock::now();
            info.createdTime = now;
            info.modifiedTime = now;
            info.indexedTime = now;
            info.contentExtracted = isTextMime(info.mimeType);
            info.extractionStatus = info.contentExtracted ? metadata::ExtractionStatus::Success
                                                          : metadata::ExtractionStatus::Pending;

            auto ins = ctx_.metadataRepo->insertDocument(info);
            if (ins) {
                int64_t docId = ins.value();
                for (const auto& [k, v] : md.tags) {
                    (void)ctx_.metadataRepo->setMetadata(docId, k, metadata::MetadataValue(v));
                }
                if (!req.deferExtraction) {
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
                return Error{ErrorCode::InvalidArgument,
                             "Invalid hash format: expected hex string (min 8 chars), got '" +
                                 req.hash + "'. Use --name for file paths."};
            }
        }

        // If name is provided but no hash, resolve name to hash first
        std::string resolvedHash = normalizeHashInput(req.hash);
        if (!req.name.empty() && req.hash.empty()) {
            auto resolveResult = resolveNameToHash(req.name);
            if (!resolveResult) {
                return Error{ErrorCode::NotFound, "Document not found with name: " + req.name};
            }
            resolvedHash = resolveResult.value();
            resolvedHash = normalizeHashInput(resolvedHash);
        }

        // Resolve hash (handle partial hashes)
        if (!resolvedHash.empty() && resolvedHash.size() != 64 &&
            looksLikePartialHash(resolvedHash)) {
            // Partial hash resolution
            if (!ctx_.metadataRepo) {
                return Error{ErrorCode::NotInitialized,
                             "Metadata repository not available for partial hash resolution"};
            }
            auto docsRes = ctx_.metadataRepo->findDocumentsByPath("%");
            if (!docsRes) {
                return Error{ErrorCode::InternalError,
                             "Failed to enumerate documents for partial hash resolution: " +
                                 docsRes.error().message};
            }

            std::vector<std::string> matches;
            for (const auto& d : docsRes.value()) {
                if (d.sha256Hash.size() >= resolvedHash.size() &&
                    d.sha256Hash.compare(0, resolvedHash.size(), resolvedHash) == 0) {
                    matches.push_back(d.sha256Hash);
                }
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

        // Find document in metadata repository to populate metadata/path/name/size
        std::optional<metadata::DocumentInfo> foundDoc;
        if (ctx_.metadataRepo) {
            auto docsRes = ctx_.metadataRepo->findDocumentsByPath("%");
            if (docsRes) {
                for (const auto& d : docsRes.value()) {
                    if (d.sha256Hash == resolvedHash) {
                        foundDoc = d;
                        break;
                    }
                }
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

        // Include content if requested (prefer hot extracted text when available)
        if (req.includeContent) {
            bool filled = false;
            if (ctx_.metadataRepo && foundDoc) {
                auto contentResult = ctx_.metadataRepo->getContent(foundDoc->id);
                if (contentResult) {
                    const auto& optionalContent = contentResult.value();
                    if (optionalContent.has_value() && !optionalContent->contentText.empty()) {
                        doc.content = optionalContent->contentText;
                        doc.size = static_cast<uint64_t>(doc.content->size());
                        filled = true;
                    }
                }
            }
            if (!filled) {
                std::ostringstream oss;
                auto rs = ctx_.store->retrieveStream(resolvedHash, oss, nullptr);
                if (!rs) {
                    return Error{ErrorCode::NotFound, "Document content not found"};
                }
                doc.content = oss.str();
                doc.size = static_cast<uint64_t>(doc.content->size());
            }
        }

        // PBI-006 Phase 1: attach extracted text via MetadataRepository when requested
        if (req.extract && ctx_.metadataRepo && foundDoc) {
            auto contentResult = ctx_.metadataRepo->getContent(foundDoc->id);
            if (contentResult) {
                const auto& optionalContent = contentResult.value();
                if (optionalContent.has_value()) {
                    const auto& content = optionalContent.value();
                    if (!content.contentText.empty()) {
                        doc.extractedText = content.contentText;
                    }
                }
            }
        }
        resp.document = doc;

        // Build simple related graph (same directory = distance 1)
        if (req.graph && ctx_.metadataRepo && foundDoc) {
            std::filesystem::path baseDir = std::filesystem::path(foundDoc->filePath).parent_path();
            auto docsRes = ctx_.metadataRepo->findDocumentsByPath("%");
            if (docsRes) {
                for (const auto& other : docsRes.value()) {
                    if (other.sha256Hash == foundDoc->sha256Hash)
                        continue;
                    if (std::filesystem::path(other.filePath).parent_path() == baseDir) {
                        RelatedDocument rd;
                        rd.hash = other.sha256Hash;
                        rd.path = other.filePath;
                        rd.distance = 1;
                        resp.related.push_back(std::move(rd));
                    }
                }
            }
            // depth>1 could be implemented later; we cap at distance 1
            (void)req.depth;
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
                auto docsRes = ctx_.metadataRepo->findDocumentsByPath("%");
                if (docsRes) {
                    for (const auto& d : docsRes.value()) {
                        if (d.sha256Hash == hash) {
                            mime = d.mimeType;
                            ext = d.fileExtension;
                            break;
                        }
                    }
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
            auto docsRes = ctx_.metadataRepo->findDocumentsByPath("%");
            if (docsRes) {
                for (const auto& d : docsRes.value()) {
                    if (d.sha256Hash == hash) {
                        targetDoc = d;
                        break;
                    }
                }
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

        std::string sqlPattern = req.pattern.empty() ? "%" : globToSqlLike(req.pattern);
        auto docsRes = ctx_.metadataRepo->findDocumentsByPath(sqlPattern);
        if (!docsRes) {
            return Error{ErrorCode::InternalError,
                         "Failed to query documents: " + docsRes.error().message};
        }

        std::vector<metadata::DocumentInfo> docs = docsRes.value();

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

        // Filter by type (text/binary) or explicit flags
        if (req.text || req.type == "text") {
            docs.erase(std::remove_if(
                           docs.begin(), docs.end(),
                           [](const metadata::DocumentInfo& d) { return !isTextMime(d.mimeType); }),
                       docs.end());
        } else if (req.binary || req.type == "binary") {
            docs.erase(std::remove_if(
                           docs.begin(), docs.end(),
                           [](const metadata::DocumentInfo& d) { return isTextMime(d.mimeType); }),
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
            std::sort(docs.begin(), docs.end(),
                      [](const auto& a, const auto& b) { return a.indexedTime > b.indexedTime; });
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

        // Pagination: offset, limit
        int start = std::max(0, req.offset);
        int lim = std::max(0, req.limit);
        std::vector<metadata::DocumentInfo> page;
        if (start < static_cast<int>(docs.size())) {
            auto itStart = docs.begin() + start;
            auto itEnd = (lim > 0) ? std::min(docs.end(), itStart + lim) : docs.end();
            page.assign(itStart, itEnd);
        }

        ListDocumentsResponse out;
        out.totalFound = docs.size();
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
                std::optional<std::unordered_map<std::string, metadata::MetadataValue>>
                    cachedMetadata;
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
                    if (!cachedMetadata) {
                        auto metadataResult = ctx_.metadataRepo->getAllMetadata(d.id);
                        if (metadataResult)
                            cachedMetadata = metadataResult.value();
                    }
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
                std::optional<std::unordered_map<std::string, metadata::MetadataValue>>
                    cachedMetadata;
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
                    if (!cachedMetadata) {
                        auto metadataResult = ctx_.metadataRepo->getAllMetadata(d.id);
                        if (metadataResult)
                            cachedMetadata = metadataResult.value();
                    }
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
        auto docsRes = ctx_.metadataRepo->findDocumentsByPath("%");
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

    // Resolve name to hash (disambiguate or error)
    Result<std::string> resolveNameToHash(const std::string& name) override {
        if (!ctx_.metadataRepo) {
            return Error{ErrorCode::NotInitialized, "Metadata repository not available"};
        }
        // Match filename anywhere at end of path; fallback to direct LIKE
        auto byName = ctx_.metadataRepo->findDocumentsByPath("%/" + name);
        if (!byName) {
            auto direct = ctx_.metadataRepo->findDocumentsByPath(name);
            if (!direct) {
                return Error{ErrorCode::NotFound, "Document not found: " + name};
            }
            if (direct.value().empty()) {
                return Error{ErrorCode::NotFound, "Document not found: " + name};
            }
            if (direct.value().size() > 1) {
                return Error{ErrorCode::InvalidOperation,
                             "Ambiguous name: multiple matches for '" + name + "'"};
            }
            return direct.value()[0].sha256Hash;
        }
        if (byName.value().empty()) {
            return Error{ErrorCode::NotFound, "Document not found: " + name};
        }
        if (byName.value().size() > 1) {
            return Error{ErrorCode::InvalidOperation,
                         "Ambiguous name: multiple matches for '" + name + "'"};
        }
        return byName.value()[0].sha256Hash;
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
            auto all = ctx_.metadataRepo->findDocumentsByPath("%");
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
            auto res = ctx_.metadataRepo->findDocumentsByPath(pat);
            if (res && !res.value().empty()) {
                addDocVec(res.value());
            }
        }
        for (const auto& n : req.names) {
            auto res = ctx_.metadataRepo->findDocumentsByPath("%/" + n);
            if (!res || res.value().empty()) {
                // Try direct path match
                auto res2 = ctx_.metadataRepo->findDocumentsByPath(n);
                if (res2 && !res2.value().empty())
                    addDocVec(res2.value());
                continue;
            }
            addDocVec(res.value());
        }
        if (!req.name.empty()) {
            auto res = ctx_.metadataRepo->findDocumentsByPath("%/" + req.name);
            if (!res || res.value().empty()) {
                auto res2 = ctx_.metadataRepo->findDocumentsByPath(req.name);
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
