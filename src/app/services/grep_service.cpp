#include <yams/app/services/services.hpp>
#include <yams/metadata/metadata_repository.h>
#include <yams/search/search_engine_builder.h>
#include <yams/vector/vector_index_manager.h>

#include <algorithm>
#include <cctype>
#include <filesystem>
#include <future>
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
        for (const auto& doc : docs) {
            // Apply path filters (if provided)
            if (!pathFilterMatch(doc.filePath, req.paths)) {
                continue;
            }

            // Apply include patterns filter
            if (!req.includePatterns.empty()) {
                bool matches = false;
                for (const auto& pattern : req.includePatterns) {
                    if (hasWildcard(pattern)) {
                        if (wildcardMatch(doc.filePath, pattern)) {
                            matches = true;
                            break;
                        }
                    } else {
                        // Simple substring match for non-wildcard patterns
                        if (doc.filePath.find(pattern) != std::string::npos) {
                            matches = true;
                            break;
                        }
                    }
                }
                if (!matches) {
                    continue;
                }
            }

            // Apply tag filtering
            if (!metadataHasTags(ctx_.metadataRepo.get(), doc.id, req.tags, req.matchAllTags)) {
                continue;
            }

            // Retrieve content
            std::ostringstream oss;
            auto rs = ctx_.store->retrieveStream(doc.sha256Hash, oss, nullptr);
            if (!rs) {
                // Skip unreadable file
                continue;
            }
            const std::string content = oss.str();
            auto lines = splitToLines(content);

            GrepFileResult fileResult;
            fileResult.file = doc.filePath;
            fileResult.matchCount = 0;

            // Scan lines
            for (size_t i = 0; i < lines.size(); ++i) {
                const std::string& line = lines[i];
                bool matched = std::regex_search(line, re);
                if (req.invert)
                    matched = !matched;

                if (!matched)
                    continue;

                fileResult.matchCount++;
                response.totalMatches++;
                response.regexMatches++;

                if (!req.count) {
                    GrepMatch gm;
                    gm.matchType = "regex"; // Set match type for all regex matches
                    gm.confidence = 1.0;    // Regex matches have full confidence
                    if (req.lineNumbers)
                        gm.lineNumber = i + 1;
                    gm.line = line;
                    // Best-effort column positions for first match (only if not inverted)
                    if (!req.invert) {
                        std::smatch sm;
                        if (std::regex_search(line, sm, re)) {
                            gm.columnStart = static_cast<size_t>(sm.position()) + 1; // 1-based
                            gm.columnEnd = gm.columnStart + static_cast<size_t>(sm.length());
                        }
                    }

                    // Context: before
                    if (beforeContext > 0) {
                        size_t start =
                            (i > static_cast<size_t>(beforeContext)) ? (i - beforeContext) : 0;
                        for (size_t j = start; j < i; ++j) {
                            gm.before.push_back(lines[j]);
                        }
                    }
                    // Context: after
                    if (afterContext > 0) {
                        size_t end =
                            std::min(lines.size(), i + 1 + static_cast<size_t>(afterContext));
                        for (size_t j = i + 1; j < end; ++j) {
                            gm.after.push_back(lines[j]);
                        }
                    }

                    fileResult.matches.push_back(std::move(gm));
                }

                if (req.maxCount > 0 && static_cast<int>(fileResult.matchCount) >= req.maxCount) {
                    break;
                }
            }

            if (fileResult.matchCount > 0) {
                response.results.push_back(std::move(fileResult));
                response.filesWith.push_back(doc.filePath);
            } else {
                response.filesWithout.push_back(doc.filePath);
            }
        }

        // Perform semantic search unless disabled or incompatible output modes
        if (!req.regexOnly && !req.count && !req.filesWithMatches && !req.filesWithoutMatch &&
            !req.pathsOnly && req.semanticLimit > 0) {
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