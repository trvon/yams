#include <yams/mcp/tool_registry.h>

namespace yams::mcp {

namespace {
// Tolerant numeric parsing: accept number, numeric-like string; ignore empty string
static int parse_int_tolerant(const json& j, const char* key, int def) {
    if (!j.contains(key) || j[key].is_null())
        return def;
    if (j[key].is_number_integer())
        return j[key].get<int>();
    if (j[key].is_string()) {
        auto s = j[key].get<std::string>();
        if (s.empty())
            return def;
        try {
            return std::stoi(s);
        } catch (...) {
            return def;
        }
    }
    return def;
}
static size_t parse_size_tolerant(const json& j, const char* key, size_t def) {
    if (!j.contains(key) || j[key].is_null())
        return def;
    if (j[key].is_number_unsigned())
        return j[key].get<size_t>();
    if (j[key].is_number_integer())
        return static_cast<size_t>(j[key].get<long long>());
    if (j[key].is_string()) {
        auto s = j[key].get<std::string>();
        if (s.empty())
            return def;
        try {
            return static_cast<size_t>(std::stoll(s));
        } catch (...) {
            return def;
        }
    }
    return def;
}
static double parse_double_tolerant(const json& j, const char* key, double def) {
    if (!j.contains(key) || j[key].is_null())
        return def;
    if (j[key].is_number_float())
        return j[key].get<double>();
    if (j[key].is_number_integer())
        return static_cast<double>(j[key].get<long long>());
    if (j[key].is_string()) {
        auto s = j[key].get<std::string>();
        if (s.empty())
            return def;
        try {
            return std::stod(s);
        } catch (...) {
            return def;
        }
    }
    return def;
}
static std::string normalize_query(std::string q) {
    for (char& c : q) {
        if (c == '\n' || c == '\r')
            c = ' ';
    }
    // collapse runs of spaces (simple pass)
    std::string out;
    out.reserve(q.size());
    bool prev_space = false;
    for (char c : q) {
        bool is_space = (c == ' ' || c == '\t');
        if (is_space) {
            if (!prev_space)
                out.push_back(' ');
        } else {
            out.push_back(c);
        }
        prev_space = is_space;
    }
    // trim
    while (!out.empty() && (out.front() == ' ' || out.front() == '\t'))
        out.erase(out.begin());
    while (!out.empty() && (out.back() == ' ' || out.back() == '\t'))
        out.pop_back();
    return out;
}
} // namespace

// MCPSearchRequest implementation
MCPSearchRequest MCPSearchRequest::fromJson(const json& j) {
    MCPSearchRequest req;
    req.query = normalize_query(j.value("query", std::string{}));
    req.limit = parse_size_tolerant(j, "limit", 10);
    req.fuzzy = detail::jsonValueOr(j, "fuzzy", false);
    req.similarity = static_cast<float>(parse_double_tolerant(j, "similarity", 0.7));
    req.hash = j.value("hash", std::string{});
    req.type = j.value("type", std::string{"hybrid"});
    req.verbose = detail::jsonValueOr(j, "verbose", false);
    req.pathsOnly = detail::jsonValueOr(j, "paths_only", false);
    req.lineNumbers = detail::jsonValueOr(j, "line_numbers", false);

    const int context = parse_int_tolerant(j, "context", 0);
    req.beforeContext = (context > 0) ? context : parse_int_tolerant(j, "before_context", 0);
    req.afterContext = (context > 0) ? context : parse_int_tolerant(j, "after_context", 0);
    req.context = context;

    req.colorMode = j.value("color", std::string{"never"});
    req.pathPattern = j.value("path_pattern", j.value("path", std::string{}));

    detail::readStringArray(j, "include_patterns", req.includePatterns);
    detail::readStringArray(j, "tags", req.tags);
    req.matchAllTags = detail::jsonValueOr(j, "match_all_tags", false);
    req.includeDiff = detail::jsonValueOr(j, "include_diff", false);
    req.useSession = detail::jsonValueOr(j, "use_session", true);
    req.sessionName = j.value("session", std::string{});
    req.cwd = j.value("cwd", std::string{});

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
                {"include_patterns", includePatterns},
                {"tags", tags},
                {"match_all_tags", matchAllTags},
                {"include_diff", includeDiff},
                {"cwd", cwd}};
}

// MCPSearchResponse implementation
MCPSearchResponse MCPSearchResponse::fromJson(const json& j) {
    MCPSearchResponse resp;
    resp.total = j.value("total", size_t{0});
    resp.type = j.value("type", std::string{});
    resp.executionTimeMs = j.value("execution_time_ms", uint64_t{0});
    resp.traceId = j.value("trace_id", std::string{});

    detail::readStringArray(j, "paths", resp.paths);

    if (j.contains("results") && j["results"].is_array()) {
        for (const auto& result : j["results"]) {
            MCPSearchResponse::Result r;
            r.id = result.value("id", std::string{});
            r.hash = result.value("hash", std::string{});
            r.title = result.value("title", std::string{});
            r.path = result.value("path", std::string{});
            r.score = result.value("score", 0.0f);
            r.snippet = result.value("snippet", std::string{});
            if (auto it = result.find("line_start"); it != result.end() && !it->is_null()) {
                r.lineStart = it->get<uint64_t>();
            }
            if (auto it = result.find("line_end"); it != result.end() && !it->is_null()) {
                r.lineEnd = it->get<uint64_t>();
            }
            if (auto it = result.find("char_start"); it != result.end() && !it->is_null()) {
                r.charStart = it->get<uint64_t>();
            }
            if (auto it = result.find("char_end"); it != result.end() && !it->is_null()) {
                r.charEnd = it->get<uint64_t>();
            }
            r.snippetTruncated = result.value("snippet_truncated", false);

            if (result.contains("diff") && !result["diff"].is_null()) {
                r.diff = result["diff"];
            }
            if (result.contains("local_input_file") && result["local_input_file"].is_string()) {
                r.localInputFile = result["local_input_file"].get<std::string>();
            }

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
    if (!traceId.empty())
        j["trace_id"] = traceId;

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
        if (result.lineStart)
            r["line_start"] = *result.lineStart;
        if (result.lineEnd)
            r["line_end"] = *result.lineEnd;
        if (result.charStart)
            r["char_start"] = *result.charStart;
        if (result.charEnd)
            r["char_end"] = *result.charEnd;
        if (result.snippetTruncated)
            r["snippet_truncated"] = true;
        if (result.diff)
            r["diff"] = *result.diff;
        if (result.localInputFile)
            r["local_input_file"] = *result.localInputFile;

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

    // Optional name/subpath helpers for path scoping
    req.name = j.value("name", std::string{});
    req.subpath = j.value("subpath", true);

    detail::readStringArray(j, "paths", req.paths);
    detail::readStringArray(j, "include_patterns", req.includePatterns);

    req.ignoreCase = j.value("ignore_case", false);
    req.word = j.value("word", false);
    req.invert = j.value("invert", false);
    req.lineNumbers = j.value("line_numbers", false);
    req.withFilename = j.value("with_filename", true);
    req.count = j.value("count", false);
    req.filesWithMatches = j.value("files_with_matches", false);
    req.fastFirst = j.value("fast_first", false);
    req.filesWithoutMatch = j.value("files_without_match", false);
    req.afterContext = parse_int_tolerant(j, "after_context", 0);
    req.beforeContext = parse_int_tolerant(j, "before_context", 0);
    req.context = parse_int_tolerant(j, "context", 0);
    req.color = j.value("color", std::string{"auto"});
    req.useSession = j.value("use_session", true);
    req.sessionName = j.value("session", std::string{});
    req.cwd = j.value("cwd", std::string{});

    detail::readStringArray(j, "tags", req.tags);
    req.matchAllTags = j.value("match_all_tags", false);

    if (j.contains("max_count")) {
        int mc = parse_int_tolerant(j, "max_count", -1);
        if (mc >= 0)
            req.maxCount = mc;
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
    j["fast_first"] = fastFirst;
    j["files_without_match"] = filesWithoutMatch;
    j["after_context"] = afterContext;
    j["before_context"] = beforeContext;
    j["context"] = context;
    j["color"] = color;
    if (maxCount)
        j["max_count"] = *maxCount;
    if (!tags.empty())
        j["tags"] = tags;
    if (matchAllTags)
        j["match_all_tags"] = matchAllTags;
    if (!cwd.empty())
        j["cwd"] = cwd;
    return j;
}

// MCPGrepResponse implementation
MCPGrepResponse MCPGrepResponse::fromJson(const json& j) {
    MCPGrepResponse resp;
    resp.output = j.value("output", std::string{});
    resp.matchCount = j.value("match_count", size_t{0});
    resp.fileCount = j.value("file_count", size_t{0});
    resp.outputTruncated = j.value("output_truncated", false);
    resp.outputMaxBytes = j.value("output_max_bytes", size_t{0});
    if (j.contains("matches") && j["matches"].is_array()) {
        for (const auto& m : j["matches"]) {
            MCPGrepResponse::Match gm;
            gm.file = m.value("file", std::string{});
            gm.lineNumber = m.value("line_number", size_t{0});
            gm.lineText = m.value("line_text", std::string{});
            detail::readStringArray(m, "context_before", gm.contextBefore);
            detail::readStringArray(m, "context_after", gm.contextAfter);
            gm.matchType = m.value("match_type", std::string{"regex"});
            gm.confidence = m.value("confidence", 1.0);
            gm.matchId = m.value("match_id", std::string{});
            gm.fileMatches = m.value("file_matches", size_t{0});
            resp.matches.push_back(std::move(gm));
        }
    }
    return resp;
}

json MCPGrepResponse::toJson() const {
    json j{{"output", output}, {"match_count", matchCount}, {"file_count", fileCount}};
    if (outputTruncated)
        j["output_truncated"] = true;
    if (outputMaxBytes > 0)
        j["output_max_bytes"] = outputMaxBytes;
    if (!matches.empty()) {
        json arr = json::array();
        for (const auto& m : matches) {
            json jm{{"file", m.file},
                    {"line_number", m.lineNumber},
                    {"line_text", m.lineText},
                    {"context_before", m.contextBefore},
                    {"context_after", m.contextAfter},
                    {"match_type", m.matchType},
                    {"confidence", m.confidence}};
            if (!m.matchId.empty())
                jm["match_id"] = m.matchId;
            if (m.fileMatches > 0)
                jm["file_matches"] = m.fileMatches;
            arr.push_back(std::move(jm));
        }
        j["matches"] = std::move(arr);
    }
    return j;
}

// MCPDownloadRequest implementation
MCPDownloadRequest MCPDownloadRequest::fromJson(const json& j) {
    MCPDownloadRequest req;
    req.url = j.value("url", std::string{});

    detail::readStringArray(j, "headers", req.headers);

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
    // Default to true so that the returned hash is ingest-ready for retrieval
    req.postIndex = j.value("post_index", true);

    detail::readStringArray(j, "tags", req.tags);

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
    resp.indexed = j.value("indexed", false);
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
    if (j.contains("content_type")) {
        resp.contentType = j["content_type"].get<std::string>();
    }
    if (j.contains("suggested_name")) {
        resp.suggestedName = j["suggested_name"].get<std::string>();
    }
    return resp;
}

json MCPDownloadResponse::toJson() const {
    json j{{"url", url},
           {"hash", hash},
           {"stored_path", storedPath},
           {"size_bytes", sizeBytes},
           {"success", success},
           {"indexed", indexed}};
    if (httpStatus)
        j["http_status"] = *httpStatus;
    if (etag)
        j["etag"] = *etag;
    if (lastModified)
        j["last_modified"] = *lastModified;
    if (checksumOk)
        j["checksum_ok"] = *checksumOk;
    if (contentType)
        j["content_type"] = *contentType;
    if (suggestedName)
        j["suggested_name"] = *suggestedName;
    return j;
}

// MCPStoreDocumentRequest implementation
MCPStoreDocumentRequest MCPStoreDocumentRequest::fromJson(const json& j) {
    MCPStoreDocumentRequest req;
    req.path = j.value("path", std::string{});
    req.content = j.value("content", std::string{});
    req.name = j.value("name", std::string{});
    req.mimeType = j.value("mime_type", std::string{});
    req.collection = j.value("collection", std::string{});
    req.snapshotId = j.value("snapshot_id", std::string{});
    req.snapshotLabel = j.value("snapshot_label", std::string{});
    req.recursive = j.value("recursive", false);
    detail::readStringArray(j, "include", req.includePatterns);
    detail::readStringArray(j, "exclude", req.excludePatterns);
    req.disableAutoMime = j.value("disable_auto_mime", false);
    req.noEmbeddings = j.value("no_embeddings", false);

    detail::readStringArray(j, "tags", req.tags);

    if (j.contains("metadata")) {
        req.metadata = j["metadata"];
    }

    return req;
}

json MCPStoreDocumentRequest::toJson() const {
    json j = {{"path", path},
              {"content", content},
              {"name", name},
              {"mime_type", mimeType},
              {"collection", collection},
              {"snapshot_id", snapshotId},
              {"snapshot_label", snapshotLabel},
              {"recursive", recursive},
              {"include", includePatterns},
              {"exclude", excludePatterns},
              {"disable_auto_mime", disableAutoMime},
              {"no_embeddings", noEmbeddings},
              {"tags", tags},
              {"metadata", metadata}};
    return j;
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
    req.name = j.value("name", std::string{});
    req.outputPath = j.value("output_path", std::string{});
    req.graph = j.value("graph", false);
    req.depth = j.value("depth", 1);
    req.includeContent = j.value("include_content", true);
    req.useSession = j.value("use_session", true);
    req.sessionName = j.value("session", std::string{});
    return req;
}

json MCPRetrieveDocumentRequest::toJson() const {
    return json{{"hash", hash},
                {"name", name},
                {"output_path", outputPath},
                {"graph", graph},
                {"depth", depth},
                {"include_content", includeContent},
                {"use_session", useSession},
                {"session", sessionName}};
}

// MCPRetrieveDocumentResponse implementation
MCPRetrieveDocumentResponse MCPRetrieveDocumentResponse::fromJson(const json& j) {
    MCPRetrieveDocumentResponse resp;
    resp.hash = j.value("hash", std::string{});
    resp.path = j.value("path", std::string{});
    resp.name = j.value("name", std::string{});
    resp.size = j.value("size", uint64_t{0});
    resp.mimeType = j.value("mime_type", std::string{});
    resp.compressed = j.value("compressed", false);
    if (auto it = j.find("compression_algorithm"); it != j.end() && !it->is_null()) {
        resp.compressionAlgorithm = static_cast<uint8_t>(it->get<uint32_t>());
    }
    if (auto it = j.find("compression_level"); it != j.end() && !it->is_null()) {
        resp.compressionLevel = static_cast<uint8_t>(it->get<uint32_t>());
    }
    if (auto it = j.find("uncompressed_size"); it != j.end() && !it->is_null()) {
        resp.uncompressedSize = it->get<uint64_t>();
    }
    if (auto it = j.find("compressed_crc32"); it != j.end() && !it->is_null()) {
        resp.compressedCrc32 = it->get<uint32_t>();
    }
    if (auto it = j.find("uncompressed_crc32"); it != j.end() && !it->is_null()) {
        resp.uncompressedCrc32 = it->get<uint32_t>();
    }
    if (auto it = j.find("compression_header"); it != j.end() && !it->is_null()) {
        resp.compressionHeader = it->get<std::string>();
    }
    if (j.contains("content")) {
        resp.content = j["content"].get<std::string>();
    }
    resp.contentTruncated = j.value("content_truncated", false);
    resp.contentBytes = j.value("content_bytes", uint64_t{0});
    resp.contentMaxBytes = j.value("content_max_bytes", uint64_t{0});
    if (j.contains("metadata") && j["metadata"].is_object()) {
        for (auto it = j["metadata"].begin(); it != j["metadata"].end(); ++it) {
            if (it.value().is_string()) {
                resp.metadata[it.key()] = it.value().get<std::string>();
            }
        }
    }
    resp.graphEnabled = j.value("graph_enabled", false);
    if (j.contains("related") && j["related"].is_array()) {
        resp.related = j["related"];
    }
    return resp;
}

json MCPRetrieveDocumentResponse::toJson() const {
    json j{{"hash", hash},
           {"path", path},
           {"name", name},
           {"size", size},
           {"mime_type", mimeType},
           {"compressed", compressed},
           {"graph_enabled", graphEnabled}};
    if (content)
        j["content"] = *content;
    if (contentTruncated)
        j["content_truncated"] = true;
    if (contentBytes > 0)
        j["content_bytes"] = contentBytes;
    if (contentMaxBytes > 0)
        j["content_max_bytes"] = contentMaxBytes;
    if (!metadata.empty())
        j["metadata"] = metadata;
    if (!related.empty())
        j["related"] = related;
    if (compressionAlgorithm.has_value())
        j["compression_algorithm"] = *compressionAlgorithm;
    if (compressionLevel.has_value())
        j["compression_level"] = *compressionLevel;
    if (uncompressedSize.has_value())
        j["uncompressed_size"] = *uncompressedSize;
    if (compressedCrc32.has_value())
        j["compressed_crc32"] = *compressedCrc32;
    if (uncompressedCrc32.has_value())
        j["uncompressed_crc32"] = *uncompressedCrc32;
    if (compressionHeader.has_value())
        j["compression_header"] = *compressionHeader;
    return j;
}

// MCPListDocumentsRequest implementation
MCPListDocumentsRequest MCPListDocumentsRequest::fromJson(const json& j) {
    MCPListDocumentsRequest req;
    req.pattern = j.value("pattern", std::string{});
    req.name = j.value("name", std::string{});

    detail::readStringArray(j, "tags", req.tags);
    req.matchAllTags = j.value("match_all_tags", false);

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
    req.pathsOnly = j.value("paths_only", false);
    req.includeDiff = j.value("include_diff", false);
    req.useSession = j.value("use_session", true);
    req.sessionName = j.value("session", std::string{});
    return req;
}

MCPSessionPinRequest MCPSessionPinRequest::fromJson(const json& j) {
    MCPSessionPinRequest req;
    req.path = j.value("path", std::string{});
    detail::readStringArray(j, "tags", req.tags);
    if (j.contains("metadata") && j["metadata"].is_object()) {
        req.metadata = j["metadata"];
    } else {
        req.metadata = json::object();
    }
    return req;
}

json MCPSessionPinRequest::toJson() const {
    return json{{"path", path}, {"tags", tags}, {"metadata", metadata}};
}

MCPSessionPinResponse MCPSessionPinResponse::fromJson(const json& j) {
    MCPSessionPinResponse r;
    r.updated = j.value("updated", size_t{0});
    return r;
}

json MCPSessionPinResponse::toJson() const {
    return json{{"updated", updated}};
}

MCPSessionUnpinRequest MCPSessionUnpinRequest::fromJson(const json& j) {
    MCPSessionUnpinRequest req;
    req.path = j.value("path", std::string{});
    return req;
}

json MCPSessionUnpinRequest::toJson() const {
    return json{{"path", path}};
}

MCPSessionUnpinResponse MCPSessionUnpinResponse::fromJson(const json& j) {
    MCPSessionUnpinResponse r;
    r.updated = j.value("updated", size_t{0});
    return r;
}

json MCPSessionUnpinResponse::toJson() const {
    return json{{"updated", updated}};
}

json MCPListDocumentsRequest::toJson() const {
    return json{{"pattern", pattern},     {"name", name},
                {"tags", tags},           {"match_all_tags", matchAllTags},
                {"type", type},           {"mime", mime},
                {"extension", extension}, {"binary", binary},
                {"text", text},           {"recent", recent},
                {"limit", limit},         {"offset", offset},
                {"sort_by", sortBy},      {"sort_order", sortOrder},
                {"paths_only", pathsOnly}};
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

    detail::readStringArray(j, "include_patterns", req.includePatterns);
    detail::readStringArray(j, "exclude_patterns", req.excludePatterns);
    detail::readStringArray(j, "tags", req.tags);
    if (j.contains("metadata")) {
        req.metadata = j["metadata"];
    }
    req.recursive = j.value("recursive", true);
    req.followSymlinks = j.value("follow_symlinks", false);
    req.snapshotId = j.value("snapshot_id", std::string{});
    req.snapshotLabel = j.value("snapshot_label", std::string{});
    return req;
}

json MCPAddDirectoryRequest::toJson() const {
    return json{{"directory_path", directoryPath},
                {"collection", collection},
                {"include_patterns", includePatterns},
                {"exclude_patterns", excludePatterns},
                {"metadata", metadata},
                {"recursive", recursive},
                {"follow_symlinks", followSymlinks},
                {"snapshot_id", snapshotId},
                {"snapshot_label", snapshotLabel},
                {"tags", tags}};
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
    req.path = j.value("path", std::string{});
    req.subpath = j.value("subpath", true);
    req.rawContent = j.value("raw_content", false);
    req.extractText = j.value("extract_text", true);
    req.latest = j.value("latest", false);
    req.oldest = j.value("oldest", false);
    return req;
}

json MCPGetByNameRequest::toJson() const {
    return json{{"name", name},
                {"path", path},
                {"subpath", subpath},
                {"raw_content", rawContent},
                {"extract_text", extractText},
                {"latest", latest},
                {"oldest", oldest}};
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

    detail::readStringArray(j, "names", req.names);

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

    detail::readStringArray(j, "deleted", resp.deleted);

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
    req.latest = j.value("latest", false);
    req.oldest = j.value("oldest", false);
    return req;
}

json MCPCatDocumentRequest::toJson() const {
    return json{{"hash", hash},
                {"name", name},
                {"raw_content", rawContent},
                {"extract_text", extractText},
                {"latest", latest},
                {"oldest", oldest}};
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
    req.path = j.value("path", std::string{});
    req.pattern = j.value("pattern", std::string{});
    req.latest = j.value("latest", false);
    req.oldest = j.value("oldest", false);

    if (j.contains("metadata")) {
        req.metadata = j["metadata"];
    }

    detail::readStringArray(j, "tags", req.tags);
    detail::readStringArray(j, "remove_tags", req.removeTags);
    detail::readStringArray(j, "names", req.names);

    req.dryRun = j.value("dry_run", false);
    req.useSession = j.value("use_session", true);
    req.sessionName = j.value("session", std::string{});

    return req;
}

json MCPUpdateMetadataRequest::toJson() const {
    json j{{"hash", hash},       {"name", name},
           {"path", path},       {"names", names},
           {"pattern", pattern}, {"latest", latest},
           {"oldest", oldest},   {"metadata", metadata},
           {"tags", tags},       {"remove_tags", removeTags},
           {"dry_run", dryRun}};
    return j;
}

// MCPUpdateMetadataResponse implementation
MCPUpdateMetadataResponse MCPUpdateMetadataResponse::fromJson(const json& j) {
    MCPUpdateMetadataResponse resp;
    resp.success = j.value("success", false);
    resp.message = j.value("message", std::string{});
    resp.matched = j.value("matched", std::size_t{0});
    resp.updated = j.value("updated", std::size_t{0});
    detail::readStringArray(j, "updated_hashes", resp.updatedHashes);
    return resp;
}

json MCPUpdateMetadataResponse::toJson() const {
    json j{{"success", success}, {"message", message}, {"matched", matched}, {"updated", updated}};
    if (!updatedHashes.empty())
        j["updated_hashes"] = updatedHashes;
    return j;
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

    detail::readStringArray(j, "include_patterns", req.includePatterns);
    detail::readStringArray(j, "exclude_patterns", req.excludePatterns);

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

    detail::readStringArray(j, "restored_paths", resp.restoredPaths);

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

    detail::readStringArray(j, "include_patterns", req.includePatterns);
    detail::readStringArray(j, "exclude_patterns", req.excludePatterns);

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

    detail::readStringArray(j, "restored_paths", resp.restoredPaths);

    return resp;
}

json MCPRestoreSnapshotResponse::toJson() const {
    return json{
        {"files_restored", filesRestored}, {"restored_paths", restoredPaths}, {"dry_run", dryRun}};
}

// MCPRestoreRequest implementation
MCPRestoreRequest MCPRestoreRequest::fromJson(const json& j) {
    MCPRestoreRequest req;
    req.collection = j.value("collection", std::string{});
    req.snapshotId = j.value("snapshot_id", std::string{});
    req.snapshotLabel = j.value("snapshot_label", std::string{});
    req.outputDirectory = j.value("output_directory", std::string{});
    req.layoutTemplate = j.value("layout_template", std::string{"{path}"});
    req.overwrite = j.value("overwrite", false);
    req.createDirs = j.value("create_dirs", true);
    req.dryRun = j.value("dry_run", false);

    detail::readStringArray(j, "include_patterns", req.includePatterns);
    detail::readStringArray(j, "exclude_patterns", req.excludePatterns);

    return req;
}

json MCPRestoreRequest::toJson() const {
    return json{{"collection", collection},
                {"snapshot_id", snapshotId},
                {"snapshot_label", snapshotLabel},
                {"output_directory", outputDirectory},
                {"layout_template", layoutTemplate},
                {"include_patterns", includePatterns},
                {"exclude_patterns", excludePatterns},
                {"overwrite", overwrite},
                {"create_dirs", createDirs},
                {"dry_run", dryRun}};
}

// MCPRestoreResponse implementation
MCPRestoreResponse MCPRestoreResponse::fromJson(const json& j) {
    MCPRestoreResponse resp;
    resp.filesRestored = j.value("files_restored", size_t{0});
    resp.dryRun = j.value("dry_run", false);
    detail::readStringArray(j, "restored_paths", resp.restoredPaths);
    return resp;
}

json MCPRestoreResponse::toJson() const {
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

// MCPGraphRequest implementation
MCPGraphRequest MCPGraphRequest::fromJson(const json& j) {
    MCPGraphRequest req;
    req.action = j.value("action", std::string{"query"});

    // Query fields
    req.hash = j.value("hash", std::string{});
    req.name = j.value("name", std::string{});
    req.nodeKey = j.value("node_key", std::string{});
    req.nodeId = static_cast<int64_t>(parse_int_tolerant(j, "node_id", -1));

    req.listTypes = j.value("list_types", false);
    req.listType = j.value("list_type", std::string{});
    req.isolated = j.value("isolated", false);

    detail::readStringArray(j, "relation_filters", req.relationFilters);
    req.relation = j.value("relation", std::string{});

    req.depth = parse_int_tolerant(j, "depth", 1);
    req.limit = parse_size_tolerant(j, "limit", 100);
    req.offset = parse_size_tolerant(j, "offset", 0);
    req.reverse = j.value("reverse", false);

    const bool includePropsShorthand = j.value("include_properties", false);
    req.includeNodeProperties = j.value("include_node_properties", includePropsShorthand);
    req.includeEdgeProperties = j.value("include_edge_properties", includePropsShorthand);
    req.hydrateFully = j.value("hydrate_fully", true);

    req.scopeSnapshot = j.value("scope_snapshot", std::string{});

    // Ingest fields (only relevant when action == "ingest")
    if (j.contains("nodes") && j["nodes"].is_array()) {
        for (const auto& n : j["nodes"]) {
            MCPKgIngestNodeInput node;
            node.nodeKey = n.value("node_key", std::string{});
            node.label = n.value("label", std::string{});
            node.type = n.value("type", std::string{});
            if (n.contains("properties"))
                node.properties = n["properties"];
            req.nodes.push_back(std::move(node));
        }
    }

    if (j.contains("edges") && j["edges"].is_array()) {
        for (const auto& e : j["edges"]) {
            MCPKgIngestEdgeInput edge;
            edge.srcNodeKey = e.value("src_node_key", std::string{});
            edge.dstNodeKey = e.value("dst_node_key", std::string{});
            edge.relation = e.value("relation", std::string{});
            edge.weight = e.value("weight", 1.0f);
            if (e.contains("properties"))
                edge.properties = e["properties"];
            req.edges.push_back(std::move(edge));
        }
    }

    if (j.contains("aliases") && j["aliases"].is_array()) {
        for (const auto& a : j["aliases"]) {
            MCPKgIngestAliasInput alias;
            alias.nodeKey = a.value("node_key", std::string{});
            alias.alias = a.value("alias", std::string{});
            alias.source = a.value("source", std::string{});
            alias.confidence = a.value("confidence", 1.0f);
            req.aliases.push_back(std::move(alias));
        }
    }

    req.documentHash = j.value("document_hash", std::string{});
    req.skipExistingNodes = j.value("skip_existing_nodes", true);
    req.skipExistingEdges = j.value("skip_existing_edges", true);

    return req;
}

json MCPGraphRequest::toJson() const {
    json j;
    j["action"] = action;

    // Query fields
    j["hash"] = hash;
    j["name"] = name;
    j["node_key"] = nodeKey;
    j["node_id"] = nodeId;
    j["list_types"] = listTypes;
    j["list_type"] = listType;
    j["isolated"] = isolated;
    j["relation_filters"] = relationFilters;
    j["relation"] = relation;
    j["depth"] = depth;
    j["limit"] = limit;
    j["offset"] = offset;
    j["reverse"] = reverse;
    j["include_node_properties"] = includeNodeProperties;
    j["include_edge_properties"] = includeEdgeProperties;
    j["hydrate_fully"] = hydrateFully;
    j["scope_snapshot"] = scopeSnapshot;

    // Ingest fields
    if (!nodes.empty()) {
        json jnodes = json::array();
        for (const auto& n : nodes) {
            json jn;
            jn["node_key"] = n.nodeKey;
            jn["label"] = n.label;
            jn["type"] = n.type;
            if (!n.properties.is_null())
                jn["properties"] = n.properties;
            jnodes.push_back(std::move(jn));
        }
        j["nodes"] = std::move(jnodes);
    }
    if (!edges.empty()) {
        json jedges = json::array();
        for (const auto& e : edges) {
            json je;
            je["src_node_key"] = e.srcNodeKey;
            je["dst_node_key"] = e.dstNodeKey;
            je["relation"] = e.relation;
            je["weight"] = e.weight;
            if (!e.properties.is_null())
                je["properties"] = e.properties;
            jedges.push_back(std::move(je));
        }
        j["edges"] = std::move(jedges);
    }
    if (!aliases.empty()) {
        json jaliases = json::array();
        for (const auto& a : aliases) {
            json ja;
            ja["node_key"] = a.nodeKey;
            ja["alias"] = a.alias;
            ja["source"] = a.source;
            ja["confidence"] = a.confidence;
            jaliases.push_back(std::move(ja));
        }
        j["aliases"] = std::move(jaliases);
    }
    if (!documentHash.empty())
        j["document_hash"] = documentHash;
    j["skip_existing_nodes"] = skipExistingNodes;
    j["skip_existing_edges"] = skipExistingEdges;

    return j;
}

// MCPGraphResponse implementation
MCPGraphResponse MCPGraphResponse::fromJson(const json& j) {
    MCPGraphResponse resp;
    resp.action = j.value("action", std::string{"query"});

    // Query result fields
    if (j.contains("origin"))
        resp.origin = j["origin"];
    if (j.contains("connected_nodes"))
        resp.connectedNodes = j["connected_nodes"];
    if (j.contains("node_type_counts"))
        resp.nodeTypeCounts = j["node_type_counts"];

    resp.totalNodesFound = j.value("total_nodes_found", uint64_t{0});
    resp.totalEdgesTraversed = j.value("total_edges_traversed", uint64_t{0});
    resp.truncated = j.value("truncated", false);
    resp.maxDepthReached = j.value("max_depth_reached", 0);
    resp.queryTimeMs = j.value("query_time_ms", int64_t{0});
    resp.kgAvailable = j.value("kg_available", true);
    resp.warning = j.value("warning", std::string{});

    // Ingest result fields
    resp.nodesInserted = j.value("nodes_inserted", uint64_t{0});
    resp.nodesSkipped = j.value("nodes_skipped", uint64_t{0});
    resp.edgesInserted = j.value("edges_inserted", uint64_t{0});
    resp.edgesSkipped = j.value("edges_skipped", uint64_t{0});
    resp.aliasesInserted = j.value("aliases_inserted", uint64_t{0});
    resp.aliasesSkipped = j.value("aliases_skipped", uint64_t{0});
    resp.success = j.value("success", true);
    detail::readStringArray(j, "errors", resp.errors);

    return resp;
}

json MCPGraphResponse::toJson() const {
    json j;
    j["action"] = action;

    if (action == "ingest") {
        // Ingest results only
        j["nodes_inserted"] = nodesInserted;
        j["nodes_skipped"] = nodesSkipped;
        j["edges_inserted"] = edgesInserted;
        j["edges_skipped"] = edgesSkipped;
        j["aliases_inserted"] = aliasesInserted;
        j["aliases_skipped"] = aliasesSkipped;
        j["success"] = success;
        if (!errors.empty())
            j["errors"] = errors;
    } else {
        // Query results
        j["origin"] = origin;
        j["connected_nodes"] = connectedNodes;
        if (!nodeTypeCounts.is_null() && !nodeTypeCounts.empty())
            j["node_type_counts"] = nodeTypeCounts;
        j["total_nodes_found"] = totalNodesFound;
        j["total_edges_traversed"] = totalEdgesTraversed;
        j["truncated"] = truncated;
        j["max_depth_reached"] = maxDepthReached;
        j["query_time_ms"] = queryTimeMs;
        j["kg_available"] = kgAvailable;
        if (!warning.empty())
            j["warning"] = warning;
    }
    return j;
}

} // namespace yams::mcp
