#include <yams/app/services/grep_mode_tls.h>
#include <yams/app/services/services.hpp>
// Hot/Cold mode helpers (env-driven)
#include "../../cli/hot_cold_utils.h"
#include <yams/metadata/metadata_repository.h>
#include <yams/search/search_engine_builder.h>
#include <yams/vector/vector_index_manager.h>

#include <algorithm>
#include <atomic>
#include <cctype>
#include <filesystem>
#include <future>
#include <mutex>
#include <thread>
#ifndef _WIN32
#include <unistd.h>
#endif
#include <regex>
#include <set>
#include <sstream>
#include <string>
#include <unordered_set>
#include <vector>

namespace yams::app::services {

namespace {

// Helper function to escape regex special characters
static std::string escapeRegex(const std::string& text) {
    static const std::string specialChars = "\\^$.|?*+()[]{}";
    std::string escaped;
    escaped.reserve(text.size() * 2);
    for (char c : text) {
        if (specialChars.find(c) != std::string::npos) {
            escaped += '\\';
        }
        escaped += c;
    }
    return escaped;
}

static bool hasWildcard(const std::string& s) {
    return s.find('*') != std::string::npos || s.find('?') != std::string::npos;
}

// Helper to check if document has required tags
static bool metadataHasTags(metadata::MetadataRepository* repo, int64_t docId,
                            const std::vector<std::string>& tags, bool matchAll) {
    if (!repo || tags.empty())
        return true;
    auto md = repo->getAllMetadata(docId);
    if (!md)
        return false;
    auto& all = md.value();

    auto hasTag = [&](const std::string& t) {
        // Match if key equals tag OR string value equals tag
        auto it = all.find(t);
        if (it != all.end())
            return true;
        for (const auto& [k, v] : all) {
            (void)k;
            if (v.asString() == t)
                return true;
        }
        return false;
    };

    if (matchAll) {
        for (const auto& t : tags) {
            if (!hasTag(t))
                return false;
        }
        return true;
    } else {
        for (const auto& t : tags) {
            if (hasTag(t))
                return true;
        }
        return false;
    }
}

static bool pathFilterMatch(const std::string& filePath, const std::vector<std::string>& filters) {
    if (filters.empty())
        return true;
    for (const auto& f : filters) {
        if (f.empty())
            continue;
        if (hasWildcard(f)) {
            if (yams::app::services::utils::matchGlob(filePath, f))
                return true;
        } else {
            // Treat filter as prefix (dir) or exact path
            if (filePath == f)
                return true;
#ifdef _WIN32
            // Normalize slash for crude prefix match
            std::string fp = filePath;
            std::replace(fp.begin(), fp.end(), '\\', '/');
            std::string ff = f;
            std::replace(ff.begin(), ff.end(), '\\', '/');
            if (fp.rfind(ff, 0) == 0)
                return true;
#else
            if (filePath.rfind(f, 0) == 0)
                return true;
#endif
        }
    }
    return false;
}

} // namespace

class GrepServiceImpl final : public IGrepService {
public:
    explicit GrepServiceImpl(const AppContext& ctx) : ctx_(ctx) {}

    Result<GrepResponse> grep(const GrepRequest& req) override {
        if (!ctx_.metadataRepo) {
            return Error{ErrorCode::NotInitialized, "Metadata repository not available"};
        }
        if (!ctx_.store) {
            return Error{ErrorCode::NotInitialized, "Content store not available"};
        }
        if (req.pattern.empty()) {
            return Error{ErrorCode::InvalidArgument, "Pattern is required"};
        }

        // Build regex
        std::string pat = req.pattern;
        if (req.literalText) {
            // Escape regex special characters for literal matching
            pat = escapeRegex(pat);
        }
        if (req.word) {
            // Wrap with word boundaries; use a non-capturing group to avoid precedence issues
            pat = "\\b(?:" + pat + ")\\b";
        }

        std::regex_constants::syntax_option_type flags = std::regex::ECMAScript;
        if (req.ignoreCase)
            flags |= std::regex::icase;

        std::regex re;
        try {
            re = std::regex(pat, flags);
        } catch (const std::regex_error& e) {
            return Error{ErrorCode::InvalidArgument, std::string("Invalid regex: ") + e.what()};
        }

        // Resolve context windows
        int beforeContext = req.context > 0 ? req.context : req.beforeContext;
        int afterContext = req.context > 0 ? req.context : req.afterContext;
        if (beforeContext < 0)
            beforeContext = 0;
        if (afterContext < 0)
            afterContext = 0;

        // Stage caps and timeouts (env-overridable)
        auto getenv_int = [](const char* k, int def) -> int {
            if (const char* v = std::getenv(k)) {
                try {
                    return std::max(0, std::stoi(v));
                } catch (...) {
                }
            }
            return def;
        };
        const int max_docs_hot = getenv_int("YAMS_GREP_MAX_DOCS_HOT", 2000);
        const int max_docs_cold = getenv_int("YAMS_GREP_MAX_DOCS_COLD", 200);
        const int budget_ms = getenv_int("YAMS_GREP_TIME_BUDGET_MS", 15000);
        auto start_time = std::chrono::steady_clock::now();

        // Build candidate document set: KG(tags) -> metadata -> fallback
        std::vector<metadata::DocumentInfo> docs;
        if (!req.tags.empty()) {
            auto tRes = ctx_.metadataRepo->findDocumentsByTags(req.tags, req.matchAllTags);
            if (!tRes) {
                return Error{ErrorCode::InternalError,
                             "Failed to query documents by tags: " + tRes.error().message};
            }
            docs = std::move(tRes.value());
        }
        if (docs.empty()) {
            bool canUseFTS = req.literalText || req.word;
            if (canUseFTS) {
                auto sRes = ctx_.metadataRepo->search(req.pattern, /*limit*/ 2000, /*offset*/ 0);
                if (sRes && sRes.value().isSuccess() && !sRes.value().results.empty()) {
                    std::unordered_set<int64_t> allow;
                    allow.reserve(sRes.value().results.size());
                    for (const auto& r : sRes.value().results)
                        allow.insert(r.document.id);
                    auto allRes = ctx_.metadataRepo->findDocumentsByPath("%");
                    if (!allRes) {
                        return Error{ErrorCode::InternalError,
                                     "Failed to enumerate documents: " + allRes.error().message};
                    }
                    for (auto& d : allRes.value()) {
                        if (allow.find(d.id) != allow.end())
                            docs.push_back(std::move(d));
                    }
                }
            }
            if (docs.empty()) {
                bool usePreselect = true;
                if (const char* env = std::getenv("YAMS_GREP_PRESELECT")) {
                    std::string v(env);
                    std::transform(v.begin(), v.end(), v.begin(), ::tolower);
                    usePreselect = !(v == "0" || v == "false" || v == "off");
                }
                const bool hasExplicitPaths = !req.paths.empty();
                if (usePreselect && !hasExplicitPaths) {
                    try {
                        std::shared_ptr<yams::search::HybridSearchEngine> eng = ctx_.hybridEngine;
                        if (!eng) {
                            auto vecMgr = std::make_shared<yams::vector::VectorIndexManager>();
                            yams::search::SearchEngineBuilder builder;
                            builder.withVectorIndex(vecMgr).withMetadataRepo(ctx_.metadataRepo);
                            auto opts =
                                yams::search::SearchEngineBuilder::BuildOptions::makeDefault();
                            opts.hybrid.final_top_k = 1000;
                            opts.hybrid.vector_weight = 0.30f;
                            opts.hybrid.keyword_weight = 0.70f;
                            auto engRes = builder.buildEmbedded(opts);
                            if (engRes)
                                eng = engRes.value();
                        }
                        if (eng) {
                            auto hres = eng->search(req.pattern, /*topk*/ 1000);
                            if (hres && !hres.value().empty()) {
                                std::unordered_set<std::string> allowPaths;
                                allowPaths.reserve(hres.value().size());
                                for (const auto& r : hres.value()) {
                                    auto it = r.metadata.find("path");
                                    if (it != r.metadata.end() && !it->second.empty()) {
                                        allowPaths.insert(it->second);
                                    }
                                }
                                auto allRes = ctx_.metadataRepo->findDocumentsByPath("%");
                                if (!allRes) {
                                    return Error{ErrorCode::InternalError,
                                                 "Failed to enumerate documents: " +
                                                     allRes.error().message};
                                }
                                for (auto& d : allRes.value()) {
                                    if (allowPaths.find(d.filePath) != allowPaths.end())
                                        docs.push_back(std::move(d));
                                }
                            }
                        }
                    } catch (...) {
                    }
                }
            }
        }
        if (docs.empty()) {
            auto allRes = ctx_.metadataRepo->findDocumentsByPath("%");
            if (!allRes) {
                return Error{ErrorCode::InternalError,
                             "Failed to enumerate documents: " + allRes.error().message};
            }
            docs = std::move(allRes.value());
        }

        // Apply early path/include filtering
        if (!req.paths.empty() || !req.includePatterns.empty()) {
            std::vector<metadata::DocumentInfo> filtered;
            filtered.reserve(docs.size());
            for (auto& d : docs) {
                if (!req.paths.empty() && !pathFilterMatch(d.filePath, req.paths))
                    continue;
                if (!req.includePatterns.empty() &&
                    !pathFilterMatch(d.filePath, req.includePatterns))
                    continue;
                filtered.push_back(std::move(d));
            }
            docs = std::move(filtered);
        }

        // Prefer hot docs (extracted/text) then cold; cap both sets
        std::vector<metadata::DocumentInfo> hotDocs;
        std::vector<metadata::DocumentInfo> coldDocs;
        hotDocs.reserve(docs.size());
        coldDocs.reserve(docs.size());
        for (auto& d : docs) {
            bool isText = !d.mimeType.empty() && d.mimeType.rfind("text/", 0) == 0;
            if (d.contentExtracted || isText)
                hotDocs.push_back(std::move(d));
            else
                coldDocs.push_back(std::move(d));
        }
        if (max_docs_hot >= 0 && hotDocs.size() > static_cast<size_t>(max_docs_hot))
            hotDocs.resize(static_cast<size_t>(max_docs_hot));
        if (max_docs_cold >= 0 && coldDocs.size() > static_cast<size_t>(max_docs_cold))
            coldDocs.resize(static_cast<size_t>(max_docs_cold));

        docs.clear();
        docs.insert(docs.end(), hotDocs.begin(), hotDocs.end());
        docs.insert(docs.end(), coldDocs.begin(), coldDocs.end());

        GrepResponse response;
        response.totalMatches = 0;
        response.regexMatches = 0;
        response.semanticMatches = 0;

        response.filesSearched = docs.size();

        // Mode selection for tiered grep
        enum class Mode { Auto, HotOnly, ColdOnly };
        Mode mode = Mode::Auto;
        // Thread-local override from streaming fast/full execution, if set
        switch (get_grep_mode_tls()) {
            case GrepExecMode::HotOnly:
                mode = Mode::HotOnly;
                break;
            case GrepExecMode::ColdOnly:
                mode = Mode::ColdOnly;
                break;
            default: {
                yams::cli::HotColdMode grepMode = yams::cli::getGrepMode();
                switch (get_grep_mode_tls()) {
                    case GrepExecMode::HotOnly:
                        mode = Mode::HotOnly;
                        break;
                    case GrepExecMode::ColdOnly:
                        mode = Mode::ColdOnly;
                        break;
                    default:
                        if (grepMode == yams::cli::HotColdMode::HotOnly)
                            mode = Mode::HotOnly;
                        else if (grepMode == yams::cli::HotColdMode::ColdOnly)
                            mode = Mode::ColdOnly;
                        break;
                }
                break;
            }
        }

        // Dynamic, bounded parallelism (config-free): cap to a conservative small number
        size_t hw = std::max<size_t>(1, std::thread::hardware_concurrency());
        size_t rec = hw > 1 ? (hw - 1) : 1;
#if !defined(_WIN32) && !defined(__ANDROID__)
        double loads[3] = {0, 0, 0};
        if (getloadavg(loads, 3) == 3) {
            double load1 = loads[0];
            double capacity = std::max(0.0, static_cast<double>(hw) - load1);
            rec = static_cast<size_t>(std::clamp(capacity, 1.0, static_cast<double>(hw)));
        }
#endif
        // Keep grep background-friendly by default: at most 4 workers (or fewer if docs < 4)
        size_t workers = std::min<size_t>({rec, hw, static_cast<size_t>(4)});
        workers = std::min(workers, docs.size() > 0 ? docs.size() : size_t{1});

        std::atomic<size_t> next{0};
        std::mutex outMutex;
        std::vector<GrepFileResult> outResults;
        outResults.reserve(docs.size());
        std::vector<std::string> filesWith;
        std::vector<std::string> filesWithout;
        std::atomic<size_t> totalMatches{0};
        std::atomic<size_t> regexMatches{0};
        std::atomic<bool> stop{false};

        auto worker = [&]() {
            while (true) {
                if (stop.load(std::memory_order_relaxed))
                    break;
                if (budget_ms > 0) {
                    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now() - start_time);
                    if (elapsed.count() >= budget_ms) {
                        stop.store(true, std::memory_order_relaxed);
                        break;
                    }
                }
                size_t i = next.fetch_add(1);
                if (i >= docs.size())
                    break;
                const auto& doc = docs[i];

                if (!pathFilterMatch(doc.filePath, req.paths))
                    continue;
                if (!req.includePatterns.empty()) {
                    bool ok = false;
                    for (const auto& pattern : req.includePatterns) {
                        if (hasWildcard(pattern)) {
                            if (yams::app::services::utils::matchGlob(doc.filePath, pattern)) {
                                ok = true;
                                break;
                            }
                        } else {
                            if (doc.filePath.find(pattern) != std::string::npos) {
                                ok = true;
                                break;
                            }
                        }
                    }
                    if (!ok)
                        continue;
                }
                if (!metadataHasTags(ctx_.metadataRepo.get(), doc.id, req.tags, req.matchAllTags))
                    continue;

                // Respect per-document force_cold (metadata key or tag)
                bool forceCold = false;
                if (ctx_.metadataRepo) {
                    auto md = ctx_.metadataRepo->getAllMetadata(doc.id);
                    if (md) {
                        auto& all = md.value();
                        auto it = all.find("force_cold");
                        if (it != all.end()) {
                            auto v = it->second.asString();
                            std::string lv = v;
                            std::transform(lv.begin(), lv.end(), lv.begin(), ::tolower);
                            forceCold = (lv == "1" || lv == "true" || lv == "yes");
                        }
                        if (!forceCold && all.find("tag:force_cold") != all.end()) {
                            forceCold = true;
                        }
                    }
                }

                auto literalMatch = [&](const std::string& line,
                                        const std::string& needle) -> bool {
                    return line.find(needle) != std::string::npos;
                };
                auto evalMatch = [&](const std::string& line) -> bool {
                    if (req.literalText && !req.word && !req.ignoreCase) {
                        return literalMatch(line, pat);
                    }
                    return std::regex_search(line, re);
                };

                GrepFileResult fileResult;
                fileResult.file = doc.filePath;
                fileResult.fileName = std::filesystem::path(doc.filePath).filename().string();
                fileResult.matchCount = 0;
                size_t ln_counter = 0;
                auto onLine = [&](const std::string& line) {
                    ++ln_counter;
                    bool matched = evalMatch(line);
                    if (req.invert)
                        matched = !matched;
                    if (!matched)
                        return;
                    fileResult.matchCount++;
                    if (req.count)
                        return;
                    GrepMatch gm;
                    gm.matchType = req.literalText ? std::string("literal") : std::string("regex");
                    gm.confidence = 1.0;
                    if (req.lineNumbers)
                        gm.lineNumber = ln_counter;
                    gm.line = line;
                    if (!req.invert && !req.literalText) {
                        std::smatch sm;
                        if (std::regex_search(line, sm, re)) {
                            gm.columnStart = static_cast<size_t>(sm.position()) + 1;
                            gm.columnEnd = gm.columnStart + static_cast<size_t>(sm.length());
                        }
                    } else if (!req.invert && req.literalText) {
                        auto pos = line.find(pat);
                        if (pos != std::string::npos) {
                            gm.columnStart = pos + 1;
                            gm.columnEnd = gm.columnStart + pat.size();
                        }
                    }
                    fileResult.matches.push_back(std::move(gm));
                };

                // Hot path: process extracted text line-by-line without touching CAS
                if (mode == Mode::HotOnly && !forceCold) {
                    if (ctx_.metadataRepo) {
                        auto c = ctx_.metadataRepo->getContent(doc.id);
                        if (c && c.value().has_value()) {
                            std::istringstream iss(c.value()->contentText);
                            std::string line;
                            while (std::getline(iss, line)) {
                                if (!line.empty() && line.back() == '\r')
                                    line.pop_back();
                                onLine(line);
                                if (req.maxCount > 0 &&
                                    static_cast<int>(fileResult.matchCount) >= req.maxCount)
                                    break;
                                if (budget_ms > 0) {
                                    auto e2 = std::chrono::duration_cast<std::chrono::milliseconds>(
                                        std::chrono::steady_clock::now() - start_time);
                                    if (e2.count() >= budget_ms) {
                                        stop.store(true, std::memory_order_relaxed);
                                        break;
                                    }
                                }
                            }
                        } else {
                            continue;
                        }
                    } else {
                        continue;
                    }
                } else {
                    // Cold path: stream content and scan lines incrementally
                    struct LineScanBuf : public std::streambuf {
                        std::string buffer;
                        std::function<void(const std::string&)> cb;
                        explicit LineScanBuf(std::function<void(const std::string&)> f)
                            : cb(std::move(f)) {}
                        int overflow(int ch) override {
                            if (ch == traits_type::eof())
                                return 0;
                            char c = static_cast<char>(ch);
                            if (c == '\n') {
                                cb(buffer);
                                buffer.clear();
                            } else if (c != '\r') {
                                buffer.push_back(c);
                            }
                            return ch;
                        }
                        std::streamsize xsputn(const char* s, std::streamsize n) override {
                            // Process in bulk: split on '\n' without per-char callbacks
                            const char* p = s;
                            const char* end = s + n;
                            while (p < end) {
                                const void* nl = memchr(p, '\n', static_cast<size_t>(end - p));
                                if (!nl) {
                                    // no newline in remainder
                                    // append non-CR characters
                                    if (memchr(p, '\r', static_cast<size_t>(end - p)) != nullptr) {
                                        // handle CR occurrences by copying segments
                                        const char* q = p;
                                        while (q < end) {
                                            const char* r = static_cast<const char*>(
                                                memchr(q, '\r', static_cast<size_t>(end - q)));
                                            if (!r) {
                                                buffer.append(q, end);
                                                break;
                                            }
                                            buffer.append(q, r);
                                            q = r + 1; // skip CR
                                        }
                                    } else {
                                        buffer.append(p, end);
                                    }
                                    return n;
                                }
                                const char* nlc = static_cast<const char*>(nl);
                                // append up to newline, skipping CRs
                                if (memchr(p, '\r', static_cast<size_t>(nlc - p)) != nullptr) {
                                    const char* q = p;
                                    while (q < nlc) {
                                        const char* r = static_cast<const char*>(
                                            memchr(q, '\r', static_cast<size_t>(nlc - q)));
                                        if (!r) {
                                            buffer.append(q, nlc);
                                            break;
                                        }
                                        buffer.append(q, r);
                                        q = r + 1; // skip CR
                                    }
                                } else {
                                    buffer.append(p, nlc);
                                }
                                // deliver line
                                cb(buffer);
                                buffer.clear();
                                p = nlc + 1; // skip LF
                            }
                            return n;
                        }
                    };
                    LineScanBuf sb(onLine);
                    std::ostream os(&sb);
                    if (budget_ms > 0) {
                        auto e3 = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::steady_clock::now() - start_time);
                        if (e3.count() >= budget_ms) {
                            stop.store(true, std::memory_order_relaxed);
                            break;
                        }
                    }
                    auto rs = ctx_.store->retrieveStream(doc.sha256Hash, os, nullptr);
                    if (!rs)
                        continue;
                }

                // Early exit shaping for files-only/paths-only
                if (req.filesWithMatches || req.pathsOnly || req.filesWithoutMatch) {
                    // Defer formatting to caller; we just track counts and file sets below
                }

                std::lock_guard<std::mutex> lk(outMutex);
                if (fileResult.matchCount > 0) {
                    totalMatches += static_cast<size_t>(fileResult.matchCount);
                    regexMatches += static_cast<size_t>(fileResult.matchCount);
                    filesWith.push_back(fileResult.file);
                    if (!req.filesWithMatches && !req.pathsOnly)
                        outResults.push_back(std::move(fileResult));
                } else {
                    filesWithout.push_back(doc.filePath);
                }
            }
        };

        std::vector<std::thread> ths;
        ths.reserve(workers);
        for (size_t t = 0; t < workers; ++t)
            ths.emplace_back(worker);
        for (auto& th : ths)
            th.join();

        response.results = std::move(outResults);
        response.filesWith = std::move(filesWith);
        response.filesWithout = std::move(filesWithout);
        response.totalMatches = totalMatches.load();
        response.regexMatches = regexMatches.load();
        response.queryInfo = "grep: parallel regex scan";
        response.searchStats["workers"] = std::to_string(workers);
        response.searchStats["files_scanned"] = std::to_string(response.filesSearched);

        // Perform semantic search unless disabled; allow even in count/files-only/paths-only modes
        if (!req.regexOnly && req.semanticLimit > 0) {
            try {
                std::shared_ptr<yams::search::HybridSearchEngine> eng = ctx_.hybridEngine;
                size_t topk = static_cast<size_t>(std::max(1, req.semanticLimit)) * 3;
                if (!eng) {
                    auto vecMgr = std::make_shared<yams::vector::VectorIndexManager>();
                    yams::search::SearchEngineBuilder builder;
                    builder.withVectorIndex(vecMgr).withMetadataRepo(ctx_.metadataRepo);
                    auto opts = yams::search::SearchEngineBuilder::BuildOptions::makeDefault();
                    opts.hybrid.final_top_k = topk;
                    auto engRes = builder.buildEmbedded(opts);
                    if (engRes) {
                        eng = engRes.value();
                    }
                }
                if (eng) {
                    auto hres = eng->search(req.pattern, topk);
                    if (hres) {
                        std::set<std::string> regexFiles;
                        for (const auto& fr : response.results)
                            regexFiles.insert(fr.file);
                        std::vector<yams::search::HybridSearchResult> sem = hres.value();
                        int taken = 0;
                        for (const auto& r : sem) {
                            auto itPath = r.metadata.find("path");
                            if (itPath == r.metadata.end())
                                continue;
                            const std::string& path = itPath->second;
                            if (!pathFilterMatch(path, req.paths))
                                continue;
                            if (!req.includePatterns.empty()) {
                                bool ok = false;
                                for (const auto& p : req.includePatterns) {
                                    if (hasWildcard(p)) {
                                        if (yams::app::services::utils::matchGlob(path, p)) {
                                            ok = true;
                                            break;
                                        }
                                    } else {
                                        if (path.find(p) != std::string::npos) {
                                            ok = true;
                                            break;
                                        }
                                    }
                                }
                                if (!ok)
                                    continue;
                            }
                            if (regexFiles.find(path) != regexFiles.end())
                                continue;
                            GrepFileResult fr;
                            fr.file = path;
                            fr.fileName = std::filesystem::path(path).filename().string();
                            GrepMatch gm;
                            gm.matchType = "semantic";
                            float conf = r.hybrid_score > 0.0f ? r.hybrid_score : r.vector_score;
                            if (conf < 0.0f)
                                conf = 0.0f;
                            if (conf > 1.0f)
                                conf = 1.0f;
                            gm.confidence = conf;
                            gm.lineNumber = 0;
                            if (!r.content.empty())
                                gm.line = r.content;
                            fr.matches.push_back(std::move(gm));
                            fr.matchCount = 1;
                            fr.wasSemanticSearch = true;
                            response.results.push_back(std::move(fr));
                            response.semanticMatches += 1;
                            response.totalMatches += 0; // semantic items are not line hits
                            taken++;
                            if (taken >= req.semanticLimit)
                                break;
                        }
                    }
                }
            } catch (...) {
            }
        }

        return Result<GrepResponse>(std::move(response));
    }

private:
    AppContext ctx_;
};

// Optional separate factory for direct construction (callers may wire this in a factory unit).
std::shared_ptr<IGrepService> makeGrepService(const AppContext& ctx) {
    return std::make_shared<GrepServiceImpl>(ctx);
}

} // namespace yams::app::services
