#include <yams/app/services/grep_mode_tls.h>
#include <yams/app/services/services.hpp>
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

// Simple glob matcher supporting '*' and '?'
static bool wildcardMatch(const std::string& text, const std::string& pattern) {
    // DP approach
    const size_t n = text.size();
    const size_t m = pattern.size();
    std::vector<std::vector<bool>> dp(n + 1, std::vector<bool>(m + 1, false));
    dp[0][0] = true;
    for (size_t j = 1; j <= m; ++j) {
        if (pattern[j - 1] == '*')
            dp[0][j] = dp[0][j - 1];
    }
    for (size_t i = 1; i <= n; ++i) {
        for (size_t j = 1; j <= m; ++j) {
            if (pattern[j - 1] == '*') {
                dp[i][j] = dp[i][j - 1] || dp[i - 1][j];
            } else if (pattern[j - 1] == '?' || pattern[j - 1] == text[i - 1]) {
                dp[i][j] = dp[i - 1][j - 1];
            }
        }
    }
    return dp[n][m];
}

static bool hasWildcard(const std::string& s) {
    return s.find('*') != std::string::npos || s.find('?') != std::string::npos;
}

static std::vector<std::string> splitToLines(const std::string& content) {
    std::vector<std::string> lines;
    std::istringstream iss(content);
    std::string line;
    while (std::getline(iss, line)) {
#ifdef _WIN32
        if (!line.empty() && (line.back() == '\r'))
            line.pop_back();
#else
        if (!line.empty() && (line.back() == '\r'))
            line.pop_back();
#endif
        lines.push_back(std::move(line));
    }
    return lines;
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
            if (wildcardMatch(filePath, f))
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

        auto docsRes = ctx_.metadataRepo->findDocumentsByPath("%");
        if (!docsRes) {
            return Error{ErrorCode::InternalError,
                         "Failed to enumerate documents: " + docsRes.error().message};
        }

        GrepResponse response;
        response.totalMatches = 0;
        response.regexMatches = 0;
        response.semanticMatches = 0;

        const auto& docs = docsRes.value();
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
                if (const char* m = std::getenv("YAMS_GREP_MODE")) {
                    std::string v(m);
                    std::transform(v.begin(), v.end(), v.begin(), ::tolower);
                    if (v == "hot_only" || v == "hot")
                        mode = Mode::HotOnly;
                    else if (v == "cold_only" || v == "cold")
                        mode = Mode::ColdOnly;
                }
                break;
            }
        }

        // Dynamic, bounded parallelism
        size_t hw = std::max<size_t>(1, std::thread::hardware_concurrency());
        size_t rec = hw > 1 ? (hw - 1) : 1;
#if !defined(_WIN32)
        double loads[3] = {0, 0, 0};
        if (getloadavg(loads, 3) == 3) {
            double load1 = loads[0];
            double capacity = std::max(0.0, static_cast<double>(hw) - load1);
            rec = static_cast<size_t>(std::clamp(capacity, 1.0, static_cast<double>(hw)));
        }
#endif
        // Be more aggressive: ensure at least half of cores in use
        rec = std::max(rec, hw / 2);
        if (const char* env = std::getenv("YAMS_GREP_CONCURRENCY"); env && *env) {
            try {
                auto v = static_cast<size_t>(std::stoul(env));
                if (v > 0)
                    rec = v;
            } catch (...) {
            }
        }
        size_t workers = std::clamp<size_t>(rec, 1, hw);
        workers = std::min(workers, docs.size() > 0 ? docs.size() : size_t{1});
        if (const char* gcap = std::getenv("YAMS_DAEMON_WORKERS_MAX"); gcap && *gcap) {
            try {
                auto cap = static_cast<size_t>(std::stoul(gcap));
                if (cap > 0)
                    workers = std::min(workers, cap);
            } catch (...) {
            }
        }

        std::atomic<size_t> next{0};
        std::mutex outMutex;
        std::vector<GrepFileResult> outResults;
        outResults.reserve(docs.size());
        std::vector<std::string> filesWith;
        std::vector<std::string> filesWithout;
        std::atomic<size_t> totalMatches{0};
        std::atomic<size_t> regexMatches{0};

        auto worker = [&]() {
            while (true) {
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
                            if (wildcardMatch(doc.filePath, pattern)) {
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
                            for (std::streamsize i = 0; i < n; ++i)
                                overflow(static_cast<unsigned char>(s[i]));
                            return n;
                        }
                    };
                    LineScanBuf sb(onLine);
                    std::ostream os(&sb);
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
                auto vecMgr = std::make_shared<yams::vector::VectorIndexManager>();
                yams::search::SearchEngineBuilder builder;
                builder.withVectorIndex(vecMgr).withMetadataRepo(ctx_.metadataRepo);
                auto opts = yams::search::SearchEngineBuilder::BuildOptions::makeDefault();
                opts.hybrid.final_top_k = static_cast<size_t>(std::max(1, req.semanticLimit)) * 3;
                auto engRes = builder.buildEmbedded(opts);
                if (engRes) {
                    auto eng = engRes.value();
                    auto hres = eng->search(req.pattern, opts.hybrid.final_top_k);
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
                                        if (wildcardMatch(path, p)) {
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
                            double conf = r.hybrid_score > 0 ? r.hybrid_score : r.vector_score;
                            if (conf < 0.0)
                                conf = 0.0;
                            if (conf > 1.0)
                                conf = 1.0;
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
