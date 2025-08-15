#include <yams/app/services/services.hpp>

#include <yams/api/content_store.h>
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

// Convert glob to SQL LIKE pattern: * -> %, ? -> _
// If not starting with %, prepend "%/" to match filename at end of path
inline std::string globToSqlLike(const std::string& glob) {
    if (glob.empty())
        return "%";
    std::string sql = glob;
    std::replace(sql.begin(), sql.end(), '*', '%');
    std::replace(sql.begin(), sql.end(), '?', '_');
    if (!sql.empty() && sql.front() != '%') {
        // heuristic to match filename at end of path
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
        // Store tag as key with empty value (consistent with existing usage)
        out[t] = "";
    }
}

inline void addMetadataToMap(const std::unordered_map<std::string, std::string>& kv,
                             std::unordered_map<std::string, std::string>& out) {
    for (const auto& [k, v] : kv) {
        out[k] = v;
    }
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

inline std::string trimLeadingDot(const std::string& ext) {
    if (!ext.empty() && ext[0] == '.')
        return ext.substr(1);
    return ext;
}

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

        std::string usePath;
        std::optional<std::filesystem::path> tmpToRemove;

        if (!req.path.empty()) {
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
            return Error{ErrorCode::InternalError, storeRes.error().message};
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
            info.fileName = p.filename().string();
            info.fileExtension = p.extension().string();
            std::error_code ec;
            info.fileSize = static_cast<int64_t>(std::filesystem::exists(p, ec)
                                                     ? std::filesystem::file_size(p, ec)
                                                     : req.content.size());
            info.sha256Hash = out.hash;
            info.mimeType = !req.mimeType.empty() ? req.mimeType : "application/octet-stream";
            auto now = std::chrono::system_clock::now();
            info.createdTime = now;
            info.modifiedTime = now;
            info.indexedTime = now;
            info.contentExtracted = isTextMime(info.mimeType);
            info.extractionStatus = info.contentExtracted ? metadata::ExtractionStatus::Success
                                                          : metadata::ExtractionStatus::Skipped;

            auto ins = ctx_.metadataRepo->insertDocument(info);
            if (ins) {
                int64_t docId = ins.value();
                for (const auto& [k, v] : md.tags) {
                    (void)ctx_.metadataRepo->setMetadata(docId, k, metadata::MetadataValue(v));
                }
            }
        }

        return out;
    }

    // Retrieve by hash (+ optional outputPath) with optional content and simple graph
    Result<RetrieveDocumentResponse> retrieve(const RetrieveDocumentRequest& req) override {
        if (!ctx_.store) {
            return Error{ErrorCode::NotInitialized, "Content store not available"};
        }
        if (req.hash.empty()) {
            return Error{ErrorCode::InvalidArgument, "Hash is required"};
        }

        RetrieveDocumentResponse resp;
        resp.graphEnabled = req.graph;

        // Find document in metadata repository to populate metadata/path/name/size
        std::optional<metadata::DocumentInfo> foundDoc;
        if (ctx_.metadataRepo) {
            auto docsRes = ctx_.metadataRepo->findDocumentsByPath("%");
            if (docsRes) {
                for (const auto& d : docsRes.value()) {
                    if (d.sha256Hash == req.hash) {
                        foundDoc = d;
                        break;
                    }
                }
            }
        }

        RetrievedDocument doc;
        doc.hash = req.hash;

        if (foundDoc) {
            doc.path = foundDoc->filePath;
            doc.name = foundDoc->fileName;
            doc.size = static_cast<uint64_t>(foundDoc->fileSize);
            doc.mimeType = foundDoc->mimeType;
        } else {
            // Fallback: path unknown; mime default
            doc.path = req.outputPath.empty() ? req.hash : req.outputPath;
            doc.name.clear();
            doc.size = 0;
            doc.mimeType = "application/octet-stream";
        }

        // If outputPath provided, retrieve to file
        if (!req.outputPath.empty()) {
            auto r = ctx_.store->retrieve(req.hash, req.outputPath);
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

        // Include content if requested
        if (req.includeContent) {
            std::ostringstream oss;
            auto rs = ctx_.store->retrieveStream(req.hash, oss, nullptr);
            if (rs) {
                doc.content = oss.str();
                doc.size = static_cast<uint64_t>(doc.content->size());
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
            hash = req.hash;
        } else if (!req.name.empty()) {
            auto h = resolveNameToHash(req.name);
            if (!h)
                return Error{h.error().code, h.error().message};
            hash = h.value();
        } else {
            return Error{ErrorCode::InvalidArgument, "Provide 'hash' or 'name'"};
        }

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

        for (const auto& d : page) {
            DocumentEntry e;
            e.name = d.fileName;
            e.hash = d.sha256Hash;
            e.path = d.filePath;
            e.extension = d.fileExtension;
            e.size = static_cast<uint64_t>(d.fileSize);
            e.mimeType = d.mimeType;
            e.fileType = toFileType(d.mimeType);
            e.created = toEpochSeconds(d.createdTime);
            e.modified = toEpochSeconds(d.modifiedTime);
            e.indexed = toEpochSeconds(d.indexedTime);
            // add tags when filtering requested
            if (!req.tags.empty()) {
                auto md = ctx_.metadataRepo->getAllMetadata(d.id);
                if (md)
                    e.tags = extractTags(md.value());
            }
            out.documents.push_back(std::move(e));
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

        std::size_t count = 0;
        // Apply map keyValues
        for (const auto& [k, v] : req.keyValues) {
            auto u = ctx_.metadataRepo->setMetadata(target->id, k, metadata::MetadataValue(v));
            if (!u)
                return Error{ErrorCode::InternalError, "Failed to update metadata: " + k};
            count++;
        }
        // Apply pairs "k=v"
        for (const auto& p : req.pairs) {
            auto pos = p.find('=');
            if (pos == std::string::npos)
                continue;
            std::string k = p.substr(0, pos);
            std::string v = p.substr(pos + 1);
            auto u = ctx_.metadataRepo->setMetadata(target->id, k, metadata::MetadataValue(v));
            if (!u)
                return Error{ErrorCode::InternalError, "Failed to update metadata: " + k};
            count++;
        }

        UpdateMetadataResponse resp;
        resp.success = true;
        resp.hash = hash;
        resp.updatesApplied = count;
        resp.documentId = target->id;
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
                auto del = ctx_.store->remove(hash);
                if (!del) {
                    r.deleted = false;
                    r.error = del.error().message;
                    resp.errors.push_back(r);
                    continue;
                }
                r.deleted = del.value();
                if (!r.deleted) {
                    r.error = "Document not found";
                    resp.errors.push_back(r);
                    continue;
                }
                resp.deleted.push_back(r);
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