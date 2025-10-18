#include <yams/app/services/retrieval_service.h>

#include <spdlog/spdlog.h>
#include <algorithm>
#include <filesystem>
#include <future>
#include <optional>
#include <sstream>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <yams/app/services/services.hpp>
#include <yams/app/services/session_service.hpp>
#include <yams/daemon/client/global_io_context.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/metadata_repository.h>

namespace yams::app::services {

static yams::daemon::ClientConfig makeClientConfig(const RetrievalOptions& opts) {
    yams::daemon::ClientConfig cfg;
    if (opts.socketPath && !opts.socketPath->empty()) {
        cfg.socketPath = *opts.socketPath;
    }
    if (opts.explicitDataDir && !opts.explicitDataDir->empty()) {
        cfg.dataDir = *opts.explicitDataDir;
    }
    cfg.enableChunkedResponses = opts.enableStreaming;
    cfg.progressiveOutput = opts.progressiveOutput;
    cfg.singleUseConnections = opts.singleUseConnections;
    cfg.maxChunkSize = opts.maxChunkSize;
    cfg.headerTimeout = std::chrono::milliseconds(opts.headerTimeoutMs);
    cfg.bodyTimeout = std::chrono::milliseconds(opts.bodyTimeoutMs);
    cfg.requestTimeout = std::chrono::milliseconds(opts.requestTimeoutMs);
    cfg.acceptCompressed = opts.acceptCompressed;
    return cfg;
}

Result<yams::daemon::GetResponse> RetrievalService::get(const GetOptions& req_opts,
                                                        const RetrievalOptions& opts) const {
    yams::daemon::GetRequest req;
    req.hash = req_opts.hash;
    req.acceptCompressed = req_opts.acceptCompressed || opts.acceptCompressed;
    req.name = req_opts.name;
    req.byName = req_opts.byName;
    req.fileType = req_opts.fileType;
    req.mimeType = req_opts.mimeType;
    req.extension = req_opts.extension;
    req.binaryOnly = req_opts.binaryOnly;
    req.textOnly = req_opts.textOnly;
    req.createdAfter = req_opts.createdAfter;
    req.createdBefore = req_opts.createdBefore;
    req.modifiedAfter = req_opts.modifiedAfter;
    req.modifiedBefore = req_opts.modifiedBefore;
    req.indexedAfter = req_opts.indexedAfter;
    req.indexedBefore = req_opts.indexedBefore;
    req.latest = req_opts.latest;
    req.oldest = req_opts.oldest;
    req.outputPath = req_opts.outputPath;
    req.metadataOnly = req_opts.metadataOnly;
    req.maxBytes = req_opts.maxBytes;
    req.chunkSize = req_opts.chunkSize;
    req.raw = req_opts.raw;
    req.extract = req_opts.extract;
    req.showGraph = req_opts.showGraph;
    req.graphDepth = req_opts.graphDepth;
    req.verbose = req_opts.verbose;

    yams::daemon::DaemonClient client(makeClientConfig(opts));
    std::promise<Result<yams::daemon::GetResponse>> p;
    auto f = p.get_future();
    boost::asio::co_spawn(
        yams::daemon::GlobalIOContext::global_executor(),
        [&client, req, p = std::move(p)]() mutable -> boost::asio::awaitable<void> {
            auto r = co_await client.get(req);
            p.set_value(std::move(r));
            co_return;
        },
        boost::asio::detached);

    try {
        if (f.wait_for(std::chrono::milliseconds(opts.requestTimeoutMs)) ==
            std::future_status::ready) {
            return f.get();
        }
    } catch (const std::exception& e) {
        return Error{ErrorCode::InternalError,
                     std::string("get failed with exception: ") + e.what()};
    }

    return Error{ErrorCode::Timeout, "get timed out"};
}

Result<yams::daemon::GrepResponse> RetrievalService::grep(const GrepOptions& req_opts,
                                                          const RetrievalOptions& opts) const {
    // TODO: Future enhancement - use path-tree metadata internally to find related/duplicate files
    // and enrich results with "Related files" section based on:
    // - Same parent directory (siblings)
    // - Similar centroid embeddings (similar code paths)
    // - Directory hierarchy relationships

    // Normal daemon grep request
    yams::daemon::GrepRequest req;
    req.pattern = req_opts.pattern;
    req.paths = req_opts.paths;
    req.caseInsensitive = req_opts.caseInsensitive;
    req.invertMatch = req_opts.invertMatch;
    req.contextLines = req_opts.contextLines;
    req.maxMatches = req_opts.maxMatches;
    req.includePatterns = req_opts.includePatterns;
    req.recursive = req_opts.recursive;
    req.wholeWord = req_opts.wholeWord;
    req.showLineNumbers = req_opts.showLineNumbers;
    req.showFilename = req_opts.showFilename;
    req.noFilename = req_opts.noFilename;
    req.countOnly = req_opts.countOnly;
    req.filesOnly = req_opts.filesOnly;
    req.filesWithoutMatch = req_opts.filesWithoutMatch;
    req.pathsOnly = req_opts.pathsOnly;
    req.literalText = req_opts.literalText;
    req.regexOnly = req_opts.regexOnly;
    req.semanticLimit = req_opts.semanticLimit;
    req.filterTags = req_opts.filterTags;
    req.matchAllTags = req_opts.matchAllTags;
    req.colorMode = req_opts.colorMode;
    req.beforeContext = req_opts.beforeContext;
    req.afterContext = req_opts.afterContext;
    req.showDiff = req_opts.showDiff;

    yams::daemon::DaemonClient client(makeClientConfig(opts));
    std::promise<Result<yams::daemon::GrepResponse>> p;
    auto future = p.get_future();
    boost::asio::co_spawn(
        yams::daemon::GlobalIOContext::global_executor(),
        [&client, req, p = std::move(p)]() mutable -> boost::asio::awaitable<void> {
            auto r = co_await client.streamingGrep(req);
            p.set_value(std::move(r));
            co_return;
        },
        boost::asio::detached);

    try {
        if (future.wait_for(std::chrono::milliseconds(opts.requestTimeoutMs)) ==
            std::future_status::ready) {
            return future.get();
        }
    } catch (const std::exception& e) {
        return Error{ErrorCode::InternalError,
                     std::string("grep failed with exception: ") + e.what()};
    }

    return Error{ErrorCode::Timeout, "grep timed out"};
}

Result<yams::daemon::GrepResponse>
RetrievalService::grep(const GrepOptions& req_opts, const RetrievalOptions& opts,
                       const std::optional<PathTreeOptions>& /*pathTree*/
) const {
    // PBI-058: PathTreeOptions overload currently delegates to standard grep.
    // Future: implement path-tree-aware behavior when enabled.
    return grep(req_opts, opts);
}

Result<yams::daemon::ListResponse> RetrievalService::list(const ListOptions& req_opts,
                                                          const RetrievalOptions& opts) const {
    yams::daemon::ListRequest req;
    req.limit = req_opts.limit;
    req.tags = req_opts.tags;
    req.format = req_opts.format;
    req.sortBy = req_opts.sortBy;
    req.fileType = req_opts.fileType;
    req.mimeType = req_opts.mimeType;
    req.extensions = req_opts.extensions;
    req.createdAfter = req_opts.createdAfter;
    req.createdBefore = req_opts.createdBefore;
    req.modifiedAfter = req_opts.modifiedAfter;
    req.modifiedBefore = req_opts.modifiedBefore;
    req.indexedAfter = req_opts.indexedAfter;
    req.indexedBefore = req_opts.indexedBefore;
    req.sinceTime = req_opts.sinceTime;
    req.changeWindow = req_opts.changeWindow;
    req.filterTags = req_opts.filterTags;
    req.namePattern = req_opts.namePattern;
    req.offset = req_opts.offset;
    req.recentCount = req_opts.recentCount;
    req.snippetLength = req_opts.snippetLength;
    req.recent = req_opts.recent;
    req.reverse = req_opts.reverse;
    req.verbose = req_opts.verbose;
    req.showSnippets = req_opts.showSnippets;
    req.showMetadata = req_opts.showMetadata;
    req.showTags = req_opts.showTags;
    req.groupBySession = req_opts.groupBySession;
    req.noSnippets = req_opts.noSnippets;
    req.pathsOnly = req_opts.pathsOnly;
    req.binaryOnly = req_opts.binaryOnly;
    req.textOnly = req_opts.textOnly;
    req.showChanges = req_opts.showChanges;
    req.showDiffTags = req_opts.showDiffTags;
    req.showDeleted = req_opts.showDeleted;
    req.matchAllTags = req_opts.matchAllTags;

    yams::daemon::DaemonClient client(makeClientConfig(opts));
    std::promise<Result<yams::daemon::ListResponse>> p2;
    auto f2 = p2.get_future();
    boost::asio::co_spawn(
        yams::daemon::GlobalIOContext::global_executor(),
        [&client, req, p = std::move(p2)]() mutable -> boost::asio::awaitable<void> {
            auto r = co_await client.streamingList(req);
            p.set_value(std::move(r));
            co_return;
        },
        boost::asio::detached);
    if (f2.wait_for(std::chrono::milliseconds(opts.requestTimeoutMs)) == std::future_status::ready)
        return f2.get();
    return Error{ErrorCode::Timeout, "list timed out"};
}

Result<void> RetrievalService::getToStdout(const GetInitOptions& req_opts,
                                           const RetrievalOptions& opts) const {
    yams::daemon::GetInitRequest req;
    req.hash = req_opts.hash;
    req.name = req_opts.name;
    req.byName = req_opts.byName;
    req.metadataOnly = req_opts.metadataOnly;
    req.maxBytes = req_opts.maxBytes;
    req.chunkSize = req_opts.chunkSize;

    yams::daemon::DaemonClient client(makeClientConfig(opts));
    std::promise<Result<void>> p2;
    auto f2 = p2.get_future();
    boost::asio::co_spawn(
        yams::daemon::GlobalIOContext::global_executor(),
        [&client, req, p = std::move(p2)]() mutable -> boost::asio::awaitable<void> {
            auto r = co_await client.getToStdout(req);
            p.set_value(std::move(r));
            co_return;
        },
        boost::asio::detached);
    if (f2.wait_for(std::chrono::milliseconds(opts.requestTimeoutMs)) == std::future_status::ready)
        return f2.get();
    return Error{ErrorCode::Timeout, "get timed out"};
}

Result<void> RetrievalService::getToFile(const GetInitOptions& req_opts,
                                         const std::filesystem::path& outputPath,
                                         const RetrievalOptions& opts) const {
    yams::daemon::GetInitRequest req;
    req.hash = req_opts.hash;
    req.name = req_opts.name;
    req.byName = req_opts.byName;
    req.metadataOnly = req_opts.metadataOnly;
    req.maxBytes = req_opts.maxBytes;
    req.chunkSize = req_opts.chunkSize;

    yams::daemon::DaemonClient client(makeClientConfig(opts));
    std::promise<Result<void>> p2;
    auto f2 = p2.get_future();
    boost::asio::co_spawn(
        yams::daemon::GlobalIOContext::global_executor(),
        [&client, req, outputPath, p = std::move(p2)]() mutable -> boost::asio::awaitable<void> {
            auto r = co_await client.getToFile(req, outputPath);
            p.set_value(std::move(r));
            co_return;
        },
        boost::asio::detached);
    if (f2.wait_for(std::chrono::milliseconds(opts.requestTimeoutMs)) == std::future_status::ready)
        return f2.get();
    return Error{ErrorCode::Timeout, "get timed out"};
}

Result<RetrievalService::ChunkedGetResult>
RetrievalService::getChunkedBuffer(const GetInitOptions& req_opts, std::size_t capBytes,
                                   const RetrievalOptions& opts) const {
    yams::daemon::GetInitRequest req;
    req.hash = req_opts.hash;
    req.name = req_opts.name;
    req.byName = req_opts.byName;
    req.metadataOnly = req_opts.metadataOnly;
    req.maxBytes = req_opts.maxBytes;
    req.chunkSize = req_opts.chunkSize;

    // Prefer native chunked protocol; fallback to unary Get on error/timeout
    {
        yams::daemon::DaemonClient client(makeClientConfig(opts));
        std::promise<Result<ChunkedGetResult>> p2;
        auto f2 = p2.get_future();
        boost::asio::co_spawn(
            yams::daemon::GlobalIOContext::global_executor(),
            [&client, req, capBytes, p = std::move(p2)]() mutable -> boost::asio::awaitable<void> {
                auto init = co_await client.getInit(req);
                if (!init) {
                    p.set_value(init.error());
                    co_return;
                }
                const auto& ir = init.value();
                std::string buffer;
                buffer.reserve(
                    static_cast<std::size_t>(std::min<std::uint64_t>(capBytes, ir.totalSize)));
                std::uint64_t offset = 0;
                const std::uint32_t step = ir.chunkSize > 0 ? ir.chunkSize : 64 * 1024;
                while (offset < ir.totalSize && buffer.size() < capBytes) {
                    yams::daemon::GetChunkRequest c{};
                    c.transferId = ir.transferId;
                    c.offset = offset;
                    c.length = static_cast<std::uint32_t>(
                        std::min<std::uint64_t>(step, ir.totalSize - offset));
                    auto cRes = co_await client.getChunk(c);
                    if (!cRes) {
                        yams::daemon::GetEndRequest e{};
                        e.transferId = ir.transferId;
                        (void)co_await client.getEnd(e);
                        p.set_value(cRes.error());
                        co_return;
                    }
                    const auto& chunk = cRes.value();
                    const auto wrote = chunk.data.size();
                    if (wrote > 0) {
                        auto remain = capBytes - buffer.size();
                        buffer.append(chunk.data.data(), std::min<std::size_t>(remain, wrote));
                    }
                    offset += static_cast<uint64_t>(wrote);
                    if (chunk.bytesRemaining == 0)
                        break;
                }
                yams::daemon::GetEndRequest end{};
                end.transferId = ir.transferId;
                (void)co_await client.getEnd(end);
                ChunkedGetResult out{ir, std::move(buffer)};
                p.set_value(Result<ChunkedGetResult>(std::move(out)));
                co_return;
            },
            boost::asio::detached);
        if (f2.wait_for(std::chrono::milliseconds(opts.requestTimeoutMs)) ==
            std::future_status::ready) {
            auto r = f2.get();
            if (r)
                return r;
            // If streaming is explicitly enabled, don't fallback; propagate error.
            if (opts.enableStreaming) {
                return r.error();
            }
            // Otherwise, fall through to unary fallback only for common transport limits
            auto ec = r.error().code;
            if (ec != ErrorCode::NotImplemented && ec != ErrorCode::Timeout &&
                ec != ErrorCode::NetworkError) {
                return r.error();
            }
        }
    }
    // Fallback to unary Get
    GetOptions greq_opts;
    greq_opts.hash = req.hash;
    greq_opts.name = req.name;
    greq_opts.byName = !req.name.empty();
    greq_opts.metadataOnly = false;
    auto r = get(greq_opts, opts);
    if (!r)
        return r.error();
    const auto& gr = r.value();
    if (!gr.hasContent)
        return Error{ErrorCode::NotFound, "content not available"};
    std::string content = gr.content;
    if (content.size() > capBytes)
        content.resize(capBytes);
    yams::daemon::GetInitResponse fake{};
    fake.transferId = 0;
    fake.totalSize = static_cast<uint64_t>(gr.size);
    fake.chunkSize = 64 * 1024;
    return ChunkedGetResult{fake, std::move(content)};
}

Result<std::string> RetrievalService::resolveHashPrefix(const std::string& hashPrefix,
                                                        bool preferOldest, bool preferLatest,
                                                        const RetrievalOptions& opts,
                                                        std::size_t limit) const {
    if (hashPrefix.size() < 6 || hashPrefix.size() > 64) {
        return Error{ErrorCode::InvalidArgument,
                     "Hash prefix must be between 6 and 64 hexadecimal characters."};
    }

    yams::daemon::SearchRequest sreq;
    sreq.query = hashPrefix;
    sreq.hashQuery = hashPrefix;
    sreq.searchType = "hash";
    sreq.limit = static_cast<uint32_t>(std::max<std::size_t>(limit, 1));
    sreq.showHash = true;
    sreq.pathsOnly = false;

    yams::daemon::DaemonClient client(makeClientConfig(opts));
    std::promise<Result<yams::daemon::SearchResponse>> promise;
    auto future = promise.get_future();
    boost::asio::co_spawn(
        yams::daemon::GlobalIOContext::global_executor(),
        [&client, sreq, p = std::move(promise)]() mutable -> boost::asio::awaitable<void> {
            auto r = co_await client.streamingSearch(sreq);
            p.set_value(std::move(r));
            co_return;
        },
        boost::asio::detached);

    if (future.wait_for(std::chrono::milliseconds(opts.requestTimeoutMs)) !=
        std::future_status::ready) {
        return Error{ErrorCode::Timeout, "search timed out"};
    }

    auto searchResult = future.get();
    if (!searchResult)
        return searchResult.error();

    const auto& resp = searchResult.value();
    if (resp.results.empty()) {
        return Error{ErrorCode::NotFound, "No documents match the provided hash prefix."};
    }

    struct Candidate {
        const yams::daemon::SearchResult* result;
        std::string hash;
        std::string path;
        int64_t indexed = 0;
    };

    std::vector<Candidate> matches;
    matches.reserve(resp.results.size());

    for (const auto& item : resp.results) {
        auto hashIt = item.metadata.find("hash");
        if (hashIt == item.metadata.end())
            continue;

        std::string hash = hashIt->second;
        std::string loweredHash = hash;
        std::transform(loweredHash.begin(), loweredHash.end(), loweredHash.begin(),
                       [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        std::string loweredPrefix = hashPrefix;
        std::transform(loweredPrefix.begin(), loweredPrefix.end(), loweredPrefix.begin(),
                       [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        if (loweredHash.compare(0, loweredPrefix.size(), loweredPrefix) != 0)
            continue;

        int64_t indexed = 0;
        if (auto idxIt = item.metadata.find("indexed"); idxIt != item.metadata.end()) {
            try {
                indexed = std::stoll(idxIt->second);
            } catch (...) {
                indexed = 0;
            }
        }

        matches.push_back(Candidate{&item, std::move(hash), item.path, indexed});
    }

    if (matches.empty()) {
        return Error{ErrorCode::NotFound, "No documents match the provided hash prefix."};
    }

    auto chooseByIndexed = [&](bool oldest) -> const Candidate& {
        const Candidate* best = &matches.front();
        for (const auto& entry : matches) {
            if (oldest) {
                if (entry.indexed < best->indexed)
                    best = &entry;
            } else {
                if (entry.indexed > best->indexed)
                    best = &entry;
            }
        }
        return *best;
    };

    const Candidate* chosen = nullptr;
    if (preferLatest && preferOldest) {
        // If both flags set, favor oldest first to match legacy behavior
        chosen = &chooseByIndexed(true);
    } else if (preferLatest) {
        chosen = &chooseByIndexed(false);
    } else if (preferOldest) {
        chosen = &chooseByIndexed(true);
    } else if (matches.size() == 1) {
        chosen = &matches.front();
    }

    if (!chosen) {
        std::ostringstream oss;
        oss << "Hash prefix '" << hashPrefix << "' is ambiguous (" << matches.size()
            << " matches). Provide a longer prefix or use --latest/--oldest."
            << " Matches include:";
        const std::size_t maxExamples = 5;
        for (std::size_t i = 0; i < std::min(matches.size(), maxExamples); ++i) {
            const auto& entry = matches[i];
            oss << "\n  " << entry.hash.substr(0, 12) << "...  "
                << (entry.path.empty() ? "(unknown path)" : entry.path);
        }
        if (matches.size() > maxExamples) {
            oss << "\n  ...";
        }
        return Error{ErrorCode::InvalidOperation, oss.str()};
    }

    return chosen->hash;
}

} // namespace yams::app::services

namespace yams::app::services {

Result<yams::daemon::GetResponse> RetrievalService::getByNameSmart(
    const std::string& name, bool oldest, bool includeContent, bool useSession,
    const std::string& sessionName, const RetrievalOptions& opts,
    std::function<Result<std::string>(const std::string&)> resolver) const {
    if (name.empty())
        return Error{ErrorCode::InvalidArgument, "empty name"};

    // 1) Try resolver (e.g., DocumentService::resolveNameToHash)
    if (resolver) {
        auto rh = resolver(name);
        if (rh) {
            GetOptions greq_opts;
            greq_opts.hash = rh.value();
            greq_opts.metadataOnly = !includeContent;
            return get(greq_opts, opts);
        }
    }

    // Helper: choose candidate by newest/oldest when multiple
    auto pick_by_time = [&](const std::vector<yams::daemon::ListEntry>& items)
        -> std::optional<yams::daemon::ListEntry> {
        if (items.empty())
            return std::nullopt;
        const yams::daemon::ListEntry* chosen = nullptr;
        for (const auto& it : items) {
            if (!chosen) {
                chosen = &it;
            } else if (oldest) {
                if (it.indexed < chosen->indexed)
                    chosen = &it;
            } else {
                if (it.indexed > chosen->indexed)
                    chosen = &it;
            }
        }
        return chosen ? std::optional<yams::daemon::ListEntry>(*chosen) : std::nullopt;
    };

    // Helper: attempt a List with a SQL LIKE pattern and return best candidate
    auto try_list_pattern = [&](const std::string& pat,
                                uint32_t limit) -> std::optional<yams::daemon::ListEntry> {
        ListOptions lreq_opts;
        lreq_opts.namePattern = pat;
        lreq_opts.limit = limit;
        lreq_opts.pathsOnly = false;
        auto lr = list(lreq_opts, opts);
        if (!lr || lr.value().items.empty())
            return std::nullopt;
        return pick_by_time(lr.value().items);
    };

    // 2) Path-like input: prefer exact path, then suffix subpath match
    bool looksPathLike =
        (name.find('/') != std::string::npos) || (name.find('\\') != std::string::npos);
    if (looksPathLike) {
        // Exact path
        if (auto exact = try_list_pattern(name, 1)) {
            GetOptions greq_opts;
            greq_opts.hash = exact->hash;
            greq_opts.metadataOnly = !includeContent;
            return get(greq_opts, opts);
        }
    }

    // 3) Try session-aware list by (base)name equality
    if (useSession) {
        try {
            auto sess = makeSessionService(nullptr);
            auto pats = sess->activeIncludePatterns(sessionName.empty()
                                                        ? std::optional<std::string>{}
                                                        : std::optional<std::string>{sessionName});
            for (const auto& pat : pats) {
                ListOptions lreq_opts;
                lreq_opts.namePattern = pat;
                lreq_opts.limit = 128;
                lreq_opts.pathsOnly = false;
                auto lres = list(lreq_opts, opts);
                if (lres) {
                    const auto& items = lres.value().items;
                    // Prefer exact basename match first within the session
                    std::vector<yams::daemon::ListEntry> eq;
                    eq.reserve(items.size());
                    for (const auto& item : items) {
                        if (item.name == name && !item.hash.empty())
                            eq.push_back(item);
                    }
                    if (!eq.empty()) {
                        auto chosen = pick_by_time(eq);
                        if (chosen) {
                            GetOptions greq_opts;
                            greq_opts.hash = chosen->hash;
                            greq_opts.metadataOnly = !includeContent;
                            return get(greq_opts, opts);
                        }
                    }

                    // If no exact match, allow session-scoped suffix matching on paths
                    if (looksPathLike) {
                        std::vector<yams::daemon::ListEntry> suffix;
                        for (const auto& item : items) {
                            if (!item.path.empty() && (item.path.size() >= name.size()) &&
                                item.path.rfind(name) == item.path.size() - name.size()) {
                                suffix.push_back(item);
                            }
                        }
                        if (!suffix.empty()) {
                            auto chosen = pick_by_time(suffix);
                            if (chosen) {
                                GetOptions greq_opts;
                                greq_opts.hash = chosen->hash;
                                greq_opts.metadataOnly = !includeContent;
                                return get(greq_opts, opts);
                            }
                        }
                    }

                    // Otherwise continue to general fallback below
                }
            }
        } catch (...) {
            // ignore session errors and continue
        }
    }

    // 4) Base-name list fallback (portable and fast): "% /name", then stem, then contains
    {
        std::optional<yams::daemon::ListEntry> cand;
        if (!cand)
            cand = try_list_pattern(std::string("%/") + name, 32);
        if (!cand) {
            std::string stem = name;
            try {
                stem = std::filesystem::path(name).stem().string();
            } catch (...) {
            }
            cand = try_list_pattern(std::string("%/") + stem + "%", 64);
        }
        if (!cand)
            cand = try_list_pattern(std::string("%") + name + "%", 64);
        if (cand) {
            GetOptions greq_opts;
            greq_opts.hash = cand->hash;
            greq_opts.metadataOnly = !includeContent;
            return get(greq_opts, opts);
        }
    }

    // 5) Hybrid search fallback for best match
    // PBI-040, task 040-1: Fast-path check if FTS5 is ready before attempting search
    if (!isFTS5Ready(opts)) {
        return Error{ErrorCode::NotFound,
                     "document not found (search index updating, try again in a few seconds)"};
    }

    try {
        yams::daemon::DaemonClient client(makeClientConfig(opts));
        yams::daemon::SearchRequest sreq;
        sreq.query = name;
        sreq.fuzzy = true;
        sreq.similarity = 0.7;
        sreq.searchType = "hybrid";
        sreq.limit = 1;
        sreq.pathsOnly = false;
        std::promise<Result<yams::daemon::SearchResponse>> p;
        auto f = p.get_future();
        boost::asio::co_spawn(
            yams::daemon::GlobalIOContext::global_executor(),
            [&client, sreq, pr = std::move(p)]() mutable -> boost::asio::awaitable<void> {
                auto r = co_await client.streamingSearch(sreq);
                pr.set_value(std::move(r));
                co_return;
            },
            boost::asio::detached);
        if (f.wait_for(std::chrono::milliseconds(opts.requestTimeoutMs)) !=
            std::future_status::ready) {
            return Error{ErrorCode::Timeout, "search timed out"};
        }
        auto sres = f.get();
        if (sres && !sres.value().results.empty()) {
            const auto& best = sres.value().results.front();
            std::string hash;
            auto it = best.metadata.find("hash");
            if (it != best.metadata.end())
                hash = it->second;
            GetOptions greq_opts;
            if (!hash.empty()) {
                greq_opts.hash = hash;
            } else {
                // Prefer filename-based name resolution for portability over absolute path
                try {
                    std::filesystem::path bp(best.path);
                    greq_opts.name = bp.filename().string();
                } catch (...) {
                    greq_opts.name = best.path; // fallback to provided path
                }
                greq_opts.byName = true;
            }
            greq_opts.metadataOnly = !includeContent;
            return get(greq_opts, opts);
        }
    } catch (...) {
    }

    return Error{ErrorCode::NotFound, "document not found by name"};
}

// PBI-040, task 040-1: Check if FTS5 index is ready for queries
bool RetrievalService::isFTS5Ready(const RetrievalOptions& opts) const {
    try {
        yams::daemon::DaemonClient client(makeClientConfig(opts));
        std::promise<Result<yams::daemon::StatusResponse>> p;
        auto f = p.get_future();
        boost::asio::co_spawn(
            yams::daemon::GlobalIOContext::global_executor(),
            [&client, pr = std::move(p)]() mutable -> boost::asio::awaitable<void> {
                auto r = co_await client.status();
                pr.set_value(std::move(r));
                co_return;
            },
            boost::asio::detached);

        // Fast timeout for readiness check (500ms)
        if (f.wait_for(std::chrono::milliseconds(500)) != std::future_status::ready) {
            return false; // Daemon not responsive -> not ready
        }

        auto status = f.get();
        if (!status) {
            return false; // Status query failed -> not ready
        }

        // Consider FTS5 ready if queue depth is low (< 100 tasks)
        // Threshold balances responsiveness vs. false negatives
        constexpr uint32_t kReadyThreshold = 100;
        return status.value().postIngestQueueDepth < kReadyThreshold;

    } catch (...) {
        return false; // Any error -> assume not ready
    }
}

} // namespace yams::app::services