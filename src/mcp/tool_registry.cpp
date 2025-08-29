#include <yams/mcp/tool_registry.h>

namespace yams::mcp {

// MCPSearchRequest implementation
MCPSearchRequest MCPSearchRequest::fromJson(const json& j) {
    MCPSearchRequest req;
    req.query = j.value("query", std::string{});
    req.limit = static_cast<size_t>(j.value("limit", 10));
    req.fuzzy = j.value("fuzzy", false);
    req.similarity = j.value("similarity", 0.7f);
    req.hash = j.value("hash", std::string{});
    req.type = j.value("type", std::string{"hybrid"});
    req.verbose = j.value("verbose", false);
    req.pathsOnly = j.value("paths_only", false);
    req.lineNumbers = j.value("line_numbers", false);

    const int context = j.value("context", 0);
    req.beforeContext = (context > 0) ? context : j.value("before_context", 0);
    req.afterContext = (context > 0) ? context : j.value("after_context", 0);
    req.context = context;

    req.colorMode = j.value("color", std::string{"never"});
    req.pathPattern = j.value("path_pattern", j.value("path", std::string{}));

    if (j.contains("tags") && j["tags"].is_array()) {
        for (const auto& tag : j["tags"]) {
            if (tag.is_string()) {
                req.tags.push_back(tag.get<std::string>());
            }
        }
    }
    req.matchAllTags = j.value("match_all_tags", false);

    return req;
}

json MCPSearchRequest::toJson() const {
    return json{{"query", query},
                {"limit", limit},
                {"fuzzy", fuzzy},
                {"similarity", similarity},
                {"hash", hash},
                {"type", type},
                {"verbose", verbose},
                {"paths_only", pathsOnly},
                {"line_numbers", lineNumbers},
                {"before_context", beforeContext},
                {"after_context", afterContext},
                {"context", context},
                {"color", colorMode},
                {"path_pattern", pathPattern},
                {"tags", tags},
                {"match_all_tags", matchAllTags}};
}

// MCPSearchResponse implementation
MCPSearchResponse MCPSearchResponse::fromJson(const json& j) {
    MCPSearchResponse resp;
    resp.total = j.value("total", size_t{0});
    resp.type = j.value("type", std::string{});
    resp.executionTimeMs = j.value("execution_time_ms", uint64_t{0});

    if (j.contains("paths") && j["paths"].is_array()) {
        for (const auto& path : j["paths"]) {
            if (path.is_string()) {
                resp.paths.push_back(path.get<std::string>());
            }
        }
    }

    if (j.contains("results") && j["results"].is_array()) {
        for (const auto& result : j["results"]) {
            MCPSearchResponse::Result r;
            r.id = result.value("id", std::string{});
            r.hash = result.value("hash", std::string{});
            r.title = result.value("title", std::string{});
            r.path = result.value("path", std::string{});
            r.score = result.value("score", 0.0f);
            r.snippet = result.value("snippet", std::string{});

            if (result.contains("score_breakdown")) {
                const auto& breakdown = result["score_breakdown"];
                if (breakdown.contains("vector_score")) {
                    r.vectorScore = breakdown["vector_score"].get<float>();
                }
                if (breakdown.contains("keyword_score")) {
                    r.keywordScore = breakdown["keyword_score"].get<float>();
                }
                if (breakdown.contains("kg_entity_score")) {
                    r.kgEntityScore = breakdown["kg_entity_score"].get<float>();
                }
                if (breakdown.contains("structural_score")) {
                    r.structuralScore = breakdown["structural_score"].get<float>();
                }
            }

            resp.results.push_back(std::move(r));
        }
    }

    return resp;
}

json MCPSearchResponse::toJson() const {
    json j;
    j["total"] = total;
    j["type"] = type;
    j["execution_time_ms"] = executionTimeMs;

    if (!paths.empty()) {
        j["paths"] = paths;
        return j; // paths_only mode
    }

    json results_array = json::array();
    for (const auto& result : results) {
        json r;
        r["id"] = result.id;
        if (!result.hash.empty())
            r["hash"] = result.hash;
        if (!result.title.empty())
            r["title"] = result.title;
        if (!result.path.empty())
            r["path"] = result.path;
        r["score"] = result.score;
        if (!result.snippet.empty())
            r["snippet"] = result.snippet;

        if (result.vectorScore || result.keywordScore || result.kgEntityScore ||
            result.structuralScore) {
            json breakdown;
            if (result.vectorScore)
                breakdown["vector_score"] = *result.vectorScore;
            if (result.keywordScore)
                breakdown["keyword_score"] = *result.keywordScore;
            if (result.kgEntityScore)
                breakdown["kg_entity_score"] = *result.kgEntityScore;
            if (result.structuralScore)
                breakdown["structural_score"] = *result.structuralScore;
            r["score_breakdown"] = std::move(breakdown);
        }

        results_array.push_back(std::move(r));
    }
    j["results"] = std::move(results_array);

    return j;
}

// MCPGrepRequest implementation
MCPGrepRequest MCPGrepRequest::fromJson(const json& j) {
    MCPGrepRequest req;
    req.pattern = j.value("pattern", std::string{});

    if (j.contains("paths") && j["paths"].is_array()) {
        for (const auto& path : j["paths"]) {
            if (path.is_string()) {
                req.paths.push_back(path.get<std::string>());
            }
        }
    }

    req.ignoreCase = j.value("ignore_case", false);
    req.word = j.value("word", false);
    req.invert = j.value("invert", false);
    req.lineNumbers = j.value("line_numbers", false);
    req.withFilename = j.value("with_filename", true);
    req.count = j.value("count", false);
    req.filesWithMatches = j.value("files_with_matches", false);
    req.filesWithoutMatch = j.value("files_without_match", false);
    req.afterContext = j.value("after_context", 0);
    req.beforeContext = j.value("before_context", 0);
    req.context = j.value("context", 0);
    req.color = j.value("color", std::string{"auto"});

    if (j.contains("max_count") && j["max_count"].is_number_integer()) {
        req.maxCount = j["max_count"].get<int>();
    }

    return req;
}

json MCPGrepRequest::toJson() const {
    json j;
    j["pattern"] = pattern;
    j["paths"] = paths;
    j["ignore_case"] = ignoreCase;
    j["word"] = word;
    j["invert"] = invert;
    j["line_numbers"] = lineNumbers;
    j["with_filename"] = withFilename;
    j["count"] = count;
    j["files_with_matches"] = filesWithMatches;
    j["files_without_match"] = filesWithoutMatch;
    j["after_context"] = afterContext;
    j["before_context"] = beforeContext;
    j["context"] = context;
    j["color"] = color;
    if (maxCount)
        j["max_count"] = *maxCount;
    return j;
}

// MCPGrepResponse implementation
MCPGrepResponse MCPGrepResponse::fromJson(const json& j) {
    MCPGrepResponse resp;
    resp.output = j.value("output", std::string{});
    resp.matchCount = j.value("match_count", size_t{0});
    resp.fileCount = j.value("file_count", size_t{0});
    return resp;
}

json MCPGrepResponse::toJson() const {
    return json{{"output", output}, {"match_count", matchCount}, {"file_count", fileCount}};
}

// MCPDownloadRequest implementation
MCPDownloadRequest MCPDownloadRequest::fromJson(const json& j) {
    MCPDownloadRequest req;
    req.url = j.value("url", std::string{});

    if (j.contains("headers") && j["headers"].is_array()) {
        for (const auto& h : j["headers"]) {
            if (h.is_string()) {
                req.headers.push_back(h.get<std::string>());
            }
        }
    }

    req.checksum = j.value("checksum", std::string{});
    req.concurrency = j.value("concurrency", 4);
    req.chunkSizeBytes = static_cast<size_t>(j.value("chunk_size_bytes", 8'388'608));
    req.timeoutMs = j.value("timeout_ms", 60'000);
    req.resume = j.value("resume", true);
    req.proxy = j.value("proxy", std::string{});
    req.followRedirects = j.value("follow_redirects", true);
    req.storeOnly = j.value("store_only", true);
    req.exportPath = j.value("export_path", std::string{});
    req.overwrite = j.value("overwrite", std::string{"never"});

    // Post-index fields
    req.postIndex = j.value("post_index", false);

    if (j.contains("tags") && j["tags"].is_array()) {
        for (const auto& t : j["tags"]) {
            if (t.is_string()) {
                req.tags.push_back(t.get<std::string>());
            }
        }
    }

    if (j.contains("metadata") && j["metadata"].is_object()) {
        for (auto it = j["metadata"].begin(); it != j["metadata"].end(); ++it) {
            const std::string key = it.key();
            if (it.value().is_string()) {
                req.metadata[key] = it.value().get<std::string>();
            } else if (it.value().is_number_integer()) {
                req.metadata[key] = std::to_string(it.value().get<long long>());
            } else if (it.value().is_number_unsigned()) {
                req.metadata[key] = std::to_string(it.value().get<unsigned long long>());
            } else if (it.value().is_number_float()) {
                req.metadata[key] = std::to_string(it.value().get<double>());
            } else if (it.value().is_boolean()) {
                req.metadata[key] = it.value().get<bool>() ? "true" : "false";
            } else {
                // Fallback: dump JSON value to string
                req.metadata[key] = it.value().dump();
            }
        }
    }

    req.collection = j.value("collection", std::string{});
    req.snapshotId = j.value("snapshot_id", std::string{});
    req.snapshotLabel = j.value("snapshot_label", std::string{});

    return req;
}

json MCPDownloadRequest::toJson() const {
    json j{{"url", url},
           {"headers", headers},
           {"checksum", checksum},
           {"concurrency", concurrency},
           {"chunk_size_bytes", chunkSizeBytes},
           {"timeout_ms", timeoutMs},
           {"resume", resume},
           {"proxy", proxy},
           {"follow_redirects", followRedirects},
           {"store_only", storeOnly},
           {"export_path", exportPath},
           {"overwrite", overwrite},
           {"post_index", postIndex},
           {"collection", collection},
           {"snapshot_id", snapshotId},
           {"snapshot_label", snapshotLabel}};
    // tags
    j["tags"] = tags;
    // metadata
    j["metadata"] = json::object();
    for (const auto& [k, v] : metadata) {
        j["metadata"][k] = v;
    }
    return j;
}

// MCPDownloadResponse implementation
MCPDownloadResponse MCPDownloadResponse::fromJson(const json& j) {
    MCPDownloadResponse resp;
    resp.url = j.value("url", std::string{});
    resp.hash = j.value("hash", std::string{});
    resp.storedPath = j.value("stored_path", std::string{});
    resp.sizeBytes = j.value("size_bytes", uint64_t{0});
    resp.success = j.value("success", false);
    if (j.contains("http_status")) {
        resp.httpStatus = j["http_status"].get<int>();
    }
    if (j.contains("etag")) {
        resp.etag = j["etag"].get<std::string>();
    }
    if (j.contains("last_modified")) {
        resp.lastModified = j["last_modified"].get<std::string>();
    }
    if (j.contains("checksum_ok")) {
        resp.checksumOk = j["checksum_ok"].get<bool>();
    }
    return resp;
}

json MCPDownloadResponse::toJson() const {
    json j{{"url", url},
           {"hash", hash},
           {"stored_path", storedPath},
           {"size_bytes", sizeBytes},
           {"success", success}};
    if (httpStatus)
        j["http_status"] = *httpStatus;
    if (etag)
        j["etag"] = *etag;
    if (lastModified)
        j["last_modified"] = *lastModified;
    if (checksumOk)
        j["checksum_ok"] = *checksumOk;
    return j;
}

// MCPStoreDocumentRequest implementation
MCPStoreDocumentRequest MCPStoreDocumentRequest::fromJson(const json& j) {
    MCPStoreDocumentRequest req;
    req.path = j.value("path", std::string{});
    req.content = j.value("content", std::string{});
    req.name = j.value("name", std::string{});
    req.mimeType = j.value("mime_type", std::string{});

    if (j.contains("tags") && j["tags"].is_array()) {
        for (const auto& tag : j["tags"]) {
            if (tag.is_string()) {
                req.tags.push_back(tag.get<std::string>());
            }
        }
    }

    if (j.contains("metadata")) {
        req.metadata = j["metadata"];
    }

    return req;
}

json MCPStoreDocumentRequest::toJson() const {
    return json{{"path", path},          {"content", content}, {"name", name},
                {"mime_type", mimeType}, {"tags", tags},       {"metadata", metadata}};
}

// MCPStoreDocumentResponse implementation
MCPStoreDocumentResponse MCPStoreDocumentResponse::fromJson(const json& j) {
    MCPStoreDocumentResponse resp;
    resp.hash = j.value("hash", std::string{});
    resp.bytesStored = j.value("bytes_stored", uint64_t{0});
    resp.bytesDeduped = j.value("bytes_deduped", uint64_t{0});
    return resp;
}

json MCPStoreDocumentResponse::toJson() const {
    return json{{"hash", hash}, {"bytes_stored", bytesStored}, {"bytes_deduped", bytesDeduped}};
}

// MCPRetrieveDocumentRequest implementation
MCPRetrieveDocumentRequest MCPRetrieveDocumentRequest::fromJson(const json& j) {
    MCPRetrieveDocumentRequest req;
    req.hash = j.value("hash", std::string{});
    req.outputPath = j.value("output_path", std::string{});
    req.graph = j.value("graph", false);
    req.depth = j.value("depth", 1);
    req.includeContent = j.value("include_content", false);
    return req;
}

json MCPRetrieveDocumentRequest::toJson() const {
    return json{{"hash", hash},
                {"output_path", outputPath},
                {"graph", graph},
                {"depth", depth},
                {"include_content", includeContent}};
}

// MCPRetrieveDocumentResponse implementation
MCPRetrieveDocumentResponse MCPRetrieveDocumentResponse::fromJson(const json& j) {
    MCPRetrieveDocumentResponse resp;
    resp.hash = j.value("hash", std::string{});
    resp.path = j.value("path", std::string{});
    resp.name = j.value("name", std::string{});
    resp.size = j.value("size", uint64_t{0});
    resp.mimeType = j.value("mime_type", std::string{});
    if (j.contains("content")) {
        resp.content = j["content"].get<std::string>();
    }
    resp.graphEnabled = j.value("graph_enabled", false);
    if (j.contains("related") && j["related"].is_array()) {
        resp.related = j["related"];
    }
    return resp;
}

json MCPRetrieveDocumentResponse::toJson() const {
    json j{{"hash", hash}, {"path", path},          {"name", name},
           {"size", size}, {"mime_type", mimeType}, {"graph_enabled", graphEnabled}};
    if (content)
        j["content"] = *content;
    if (!related.empty())
        j["related"] = related;
    return j;
}

// MCPListDocumentsRequest implementation
MCPListDocumentsRequest MCPListDocumentsRequest::fromJson(const json& j) {
    MCPListDocumentsRequest req;
    req.pattern = j.value("pattern", std::string{});

    if (j.contains("tags") && j["tags"].is_array()) {
        for (const auto& tag : j["tags"]) {
            if (tag.is_string()) {
                req.tags.push_back(tag.get<std::string>());
            }
        }
    }

    req.type = j.value("type", std::string{});
    req.mime = j.value("mime", std::string{});
    req.extension = j.value("extension", std::string{});
    req.binary = j.value("binary", false);
    req.text = j.value("text", false);
    req.recent = j.value("recent", 0);
    req.limit = j.value("limit", 100);
    req.offset = j.value("offset", 0);
    req.sortBy = j.value("sort_by", std::string{"modified"});
    req.sortOrder = j.value("sort_order", std::string{"desc"});
    return req;
}

json MCPListDocumentsRequest::toJson() const {
    return json{{"pattern", pattern}, {"tags", tags},           {"type", type},
                {"mime", mime},       {"extension", extension}, {"binary", binary},
                {"text", text},       {"recent", recent},       {"limit", limit},
                {"offset", offset},   {"sort_by", sortBy},      {"sort_order", sortOrder}};
}

// MCPListDocumentsResponse implementation
MCPListDocumentsResponse MCPListDocumentsResponse::fromJson(const json& j) {
    MCPListDocumentsResponse resp;
    if (j.contains("documents") && j["documents"].is_array()) {
        resp.documents = j["documents"];
    }
    resp.total = j.value("total", size_t{0});
    return resp;
}

json MCPListDocumentsResponse::toJson() const {
    return json{{"documents", documents}, {"total", total}};
}

// MCPStatsRequest implementation
MCPStatsRequest MCPStatsRequest::fromJson(const json& j) {
    MCPStatsRequest req;
    req.fileTypes = j.value("file_types", false);
    req.verbose = j.value("verbose", false);
    return req;
}

json MCPStatsRequest::toJson() const {
    return json{{"file_types", fileTypes}, {"verbose", verbose}};
}

// MCPStatsResponse implementation
MCPStatsResponse MCPStatsResponse::fromJson(const json& j) {
    MCPStatsResponse resp;
    resp.totalObjects = j.value("total_objects", uint64_t{0});
    resp.totalBytes = j.value("total_bytes", uint64_t{0});
    resp.uniqueHashes = j.value("unique_hashes", uint64_t{0});
    resp.deduplicationSavings = j.value("deduplication_savings", uint64_t{0});
    if (j.contains("file_types") && j["file_types"].is_array()) {
        resp.fileTypes = j["file_types"];
    }
    if (j.contains("additional_stats")) {
        resp.additionalStats = j["additional_stats"];
    }
    return resp;
}

json MCPStatsResponse::toJson() const {
    json j{{"total_objects", totalObjects},
           {"total_bytes", totalBytes},
           {"unique_hashes", uniqueHashes},
           {"deduplication_savings", deduplicationSavings}};
    if (!fileTypes.empty())
        j["file_types"] = fileTypes;
    if (!additionalStats.empty())
        j["additional_stats"] = additionalStats;
    return j;
}

// MCPAddDirectoryRequest implementation
MCPAddDirectoryRequest MCPAddDirectoryRequest::fromJson(const json& j) {
    MCPAddDirectoryRequest req;
    req.directoryPath = j.value("directory_path", std::string{});
    req.collection = j.value("collection", std::string{});

    if (j.contains("include_patterns") && j["include_patterns"].is_array()) {
        for (const auto& pattern : j["include_patterns"]) {
            if (pattern.is_string()) {
                req.includePatterns.push_back(pattern.get<std::string>());
            }
        }
    }

    if (j.contains("exclude_patterns") && j["exclude_patterns"].is_array()) {
        for (const auto& pattern : j["exclude_patterns"]) {
            if (pattern.is_string()) {
                req.excludePatterns.push_back(pattern.get<std::string>());
            }
        }
    }

    if (j.contains("metadata")) {
        req.metadata = j["metadata"];
    }

    req.recursive = j.value("recursive", true);
    req.followSymlinks = j.value("follow_symlinks", false);
    return req;
}

json MCPAddDirectoryRequest::toJson() const {
    return json{{"directory_path", directoryPath},
                {"collection", collection},
                {"include_patterns", includePatterns},
                {"exclude_patterns", excludePatterns},
                {"metadata", metadata},
                {"recursive", recursive},
                {"follow_symlinks", followSymlinks}};
}

// MCPAddDirectoryResponse implementation
MCPAddDirectoryResponse MCPAddDirectoryResponse::fromJson(const json& j) {
    MCPAddDirectoryResponse resp;
    resp.directoryPath = j.value("directory_path", std::string{});
    resp.collection = j.value("collection", std::string{});
    resp.filesProcessed = j.value("files_processed", size_t{0});
    resp.filesIndexed = j.value("files_indexed", size_t{0});
    resp.filesSkipped = j.value("files_skipped", size_t{0});
    resp.filesFailed = j.value("files_failed", size_t{0});
    if (j.contains("results") && j["results"].is_array()) {
        resp.results = j["results"];
    }
    return resp;
}

json MCPAddDirectoryResponse::toJson() const {
    return json{{"directory_path", directoryPath},
                {"collection", collection},
                {"files_processed", filesProcessed},
                {"files_indexed", filesIndexed},
                {"files_skipped", filesSkipped},
                {"files_failed", filesFailed},
                {"results", results}};
}

// MCPGetByNameRequest implementation
MCPGetByNameRequest MCPGetByNameRequest::fromJson(const json& j) {
    MCPGetByNameRequest req;
    req.name = j.value("name", std::string{});
    req.rawContent = j.value("raw_content", false);
    req.extractText = j.value("extract_text", true);
    return req;
}

json MCPGetByNameRequest::toJson() const {
    return json{{"name", name}, {"raw_content", rawContent}, {"extract_text", extractText}};
}

// MCPGetByNameResponse implementation
MCPGetByNameResponse MCPGetByNameResponse::fromJson(const json& j) {
    MCPGetByNameResponse resp;
    resp.hash = j.value("hash", std::string{});
    resp.name = j.value("name", std::string{});
    resp.path = j.value("path", std::string{});
    resp.size = j.value("size", uint64_t{0});
    resp.mimeType = j.value("mime_type", std::string{});
    resp.content = j.value("content", std::string{});
    return resp;
}

json MCPGetByNameResponse::toJson() const {
    return json{{"hash", hash}, {"name", name},          {"path", path},
                {"size", size}, {"mime_type", mimeType}, {"content", content}};
}

// MCPDeleteByNameRequest implementation
MCPDeleteByNameRequest MCPDeleteByNameRequest::fromJson(const json& j) {
    MCPDeleteByNameRequest req;
    req.name = j.value("name", std::string{});
    req.pattern = j.value("pattern", std::string{});
    req.dryRun = j.value("dry_run", false);

    if (j.contains("names") && j["names"].is_array()) {
        for (const auto& n : j["names"]) {
            if (n.is_string()) {
                req.names.push_back(n.get<std::string>());
            }
        }
    }

    return req;
}

json MCPDeleteByNameRequest::toJson() const {
    return json{{"name", name}, {"names", names}, {"pattern", pattern}, {"dry_run", dryRun}};
}

// MCPDeleteByNameResponse implementation
MCPDeleteByNameResponse MCPDeleteByNameResponse::fromJson(const json& j) {
    MCPDeleteByNameResponse resp;
    resp.count = j.value("count", size_t{0});
    resp.dryRun = j.value("dry_run", false);

    if (j.contains("deleted") && j["deleted"].is_array()) {
        for (const auto& d : j["deleted"]) {
            if (d.is_string()) {
                resp.deleted.push_back(d.get<std::string>());
            }
        }
    }

    return resp;
}

json MCPDeleteByNameResponse::toJson() const {
    return json{{"deleted", deleted}, {"count", count}, {"dry_run", dryRun}};
}

// MCPCatDocumentRequest implementation
MCPCatDocumentRequest MCPCatDocumentRequest::fromJson(const json& j) {
    MCPCatDocumentRequest req;
    req.hash = j.value("hash", std::string{});
    req.name = j.value("name", std::string{});
    req.rawContent = j.value("raw_content", false);
    req.extractText = j.value("extract_text", true);
    return req;
}

json MCPCatDocumentRequest::toJson() const {
    return json{
        {"hash", hash}, {"name", name}, {"raw_content", rawContent}, {"extract_text", extractText}};
}

// MCPCatDocumentResponse implementation
MCPCatDocumentResponse MCPCatDocumentResponse::fromJson(const json& j) {
    MCPCatDocumentResponse resp;
    resp.content = j.value("content", std::string{});
    resp.hash = j.value("hash", std::string{});
    resp.name = j.value("name", std::string{});
    resp.size = j.value("size", uint64_t{0});
    return resp;
}

json MCPCatDocumentResponse::toJson() const {
    return json{{"content", content}, {"hash", hash}, {"name", name}, {"size", size}};
}

// MCPUpdateMetadataRequest implementation
MCPUpdateMetadataRequest MCPUpdateMetadataRequest::fromJson(const json& j) {
    MCPUpdateMetadataRequest req;
    req.hash = j.value("hash", std::string{});
    req.name = j.value("name", std::string{});

    if (j.contains("metadata")) {
        req.metadata = j["metadata"];
    }

    if (j.contains("tags") && j["tags"].is_array()) {
        for (const auto& tag : j["tags"]) {
            if (tag.is_string()) {
                req.tags.push_back(tag.get<std::string>());
            }
        }
    }

    return req;
}

json MCPUpdateMetadataRequest::toJson() const {
    return json{{"hash", hash}, {"name", name}, {"metadata", metadata}, {"tags", tags}};
}

// MCPUpdateMetadataResponse implementation
MCPUpdateMetadataResponse MCPUpdateMetadataResponse::fromJson(const json& j) {
    MCPUpdateMetadataResponse resp;
    resp.success = j.value("success", false);
    resp.message = j.value("message", std::string{});
    return resp;
}

json MCPUpdateMetadataResponse::toJson() const {
    return json{{"success", success}, {"message", message}};
}

// MCPRestoreCollectionRequest implementation
MCPRestoreCollectionRequest MCPRestoreCollectionRequest::fromJson(const json& j) {
    MCPRestoreCollectionRequest req;
    req.collection = j.value("collection", std::string{});
    req.outputDirectory = j.value("output_directory", std::string{});
    req.layoutTemplate = j.value("layout_template", std::string{"{path}"});
    req.overwrite = j.value("overwrite", false);
    req.createDirs = j.value("create_dirs", true);
    req.dryRun = j.value("dry_run", false);

    if (j.contains("include_patterns") && j["include_patterns"].is_array()) {
        for (const auto& p : j["include_patterns"]) {
            if (p.is_string()) {
                req.includePatterns.push_back(p.get<std::string>());
            }
        }
    }

    if (j.contains("exclude_patterns") && j["exclude_patterns"].is_array()) {
        for (const auto& p : j["exclude_patterns"]) {
            if (p.is_string()) {
                req.excludePatterns.push_back(p.get<std::string>());
            }
        }
    }

    return req;
}

json MCPRestoreCollectionRequest::toJson() const {
    return json{{"collection", collection},
                {"output_directory", outputDirectory},
                {"layout_template", layoutTemplate},
                {"include_patterns", includePatterns},
                {"exclude_patterns", excludePatterns},
                {"overwrite", overwrite},
                {"create_dirs", createDirs},
                {"dry_run", dryRun}};
}

// MCPRestoreCollectionResponse implementation
MCPRestoreCollectionResponse MCPRestoreCollectionResponse::fromJson(const json& j) {
    MCPRestoreCollectionResponse resp;
    resp.filesRestored = j.value("files_restored", size_t{0});
    resp.dryRun = j.value("dry_run", false);

    if (j.contains("restored_paths") && j["restored_paths"].is_array()) {
        for (const auto& p : j["restored_paths"]) {
            if (p.is_string()) {
                resp.restoredPaths.push_back(p.get<std::string>());
            }
        }
    }

    return resp;
}

json MCPRestoreCollectionResponse::toJson() const {
    return json{
        {"files_restored", filesRestored}, {"restored_paths", restoredPaths}, {"dry_run", dryRun}};
}

// MCPRestoreSnapshotRequest implementation
MCPRestoreSnapshotRequest MCPRestoreSnapshotRequest::fromJson(const json& j) {
    MCPRestoreSnapshotRequest req;
    req.snapshotId = j.value("snapshot_id", std::string{});
    req.snapshotLabel = j.value("snapshot_label", std::string{});
    req.outputDirectory = j.value("output_directory", std::string{});
    req.layoutTemplate = j.value("layout_template", std::string{"{path}"});
    req.overwrite = j.value("overwrite", false);
    req.createDirs = j.value("create_dirs", true);
    req.dryRun = j.value("dry_run", false);

    if (j.contains("include_patterns") && j["include_patterns"].is_array()) {
        for (const auto& p : j["include_patterns"]) {
            if (p.is_string()) {
                req.includePatterns.push_back(p.get<std::string>());
            }
        }
    }

    if (j.contains("exclude_patterns") && j["exclude_patterns"].is_array()) {
        for (const auto& p : j["exclude_patterns"]) {
            if (p.is_string()) {
                req.excludePatterns.push_back(p.get<std::string>());
            }
        }
    }

    return req;
}

json MCPRestoreSnapshotRequest::toJson() const {
    return json{{"snapshot_id", snapshotId},
                {"snapshot_label", snapshotLabel},
                {"output_directory", outputDirectory},
                {"layout_template", layoutTemplate},
                {"include_patterns", includePatterns},
                {"exclude_patterns", excludePatterns},
                {"overwrite", overwrite},
                {"create_dirs", createDirs},
                {"dry_run", dryRun}};
}

// MCPRestoreSnapshotResponse implementation
MCPRestoreSnapshotResponse MCPRestoreSnapshotResponse::fromJson(const json& j) {
    MCPRestoreSnapshotResponse resp;
    resp.filesRestored = j.value("files_restored", size_t{0});
    resp.dryRun = j.value("dry_run", false);

    if (j.contains("restored_paths") && j["restored_paths"].is_array()) {
        for (const auto& p : j["restored_paths"]) {
            if (p.is_string()) {
                resp.restoredPaths.push_back(p.get<std::string>());
            }
        }
    }

    return resp;
}

json MCPRestoreSnapshotResponse::toJson() const {
    return json{
        {"files_restored", filesRestored}, {"restored_paths", restoredPaths}, {"dry_run", dryRun}};
}

// MCPListCollectionsRequest implementation
MCPListCollectionsRequest MCPListCollectionsRequest::fromJson([[maybe_unused]] const json& j) {
    MCPListCollectionsRequest req;
    // No fields to parse
    return req;
}

json MCPListCollectionsRequest::toJson() const {
    return json::object();
}

// MCPListCollectionsResponse implementation
MCPListCollectionsResponse MCPListCollectionsResponse::fromJson(const json& j) {
    MCPListCollectionsResponse resp;

    if (j.contains("collections") && j["collections"].is_array()) {
        for (const auto& c : j["collections"]) {
            if (c.is_string()) {
                resp.collections.push_back(c.get<std::string>());
            }
        }
    }

    return resp;
}

json MCPListCollectionsResponse::toJson() const {
    return json{{"collections", collections}};
}

// MCPListSnapshotsRequest implementation
MCPListSnapshotsRequest MCPListSnapshotsRequest::fromJson(const json& j) {
    MCPListSnapshotsRequest req;
    req.collection = j.value("collection", std::string{});
    req.withLabels = j.value("with_labels", true);
    return req;
}

json MCPListSnapshotsRequest::toJson() const {
    return json{{"collection", collection}, {"with_labels", withLabels}};
}

// MCPListSnapshotsResponse implementation
MCPListSnapshotsResponse MCPListSnapshotsResponse::fromJson(const json& j) {
    MCPListSnapshotsResponse resp;

    if (j.contains("snapshots") && j["snapshots"].is_array()) {
        resp.snapshots = j["snapshots"];
    }

    return resp;
}

json MCPListSnapshotsResponse::toJson() const {
    return json{{"snapshots", snapshots}};
}

} // namespace yams::mcp