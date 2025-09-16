#include <yams/app/services/retrieval_service.h>

#include <future>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/system_executor.hpp>
#include <yams/app/services/services.hpp>
#include <yams/app/services/session_service.hpp>

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
    return cfg;
}

Result<yams::daemon::GetResponse> RetrievalService::get(const yams::daemon::GetRequest& req,
                                                        const RetrievalOptions& opts) const {
    yams::daemon::DaemonClient client(makeClientConfig(opts));
    std::promise<Result<yams::daemon::GetResponse>> p2;
    auto f2 = p2.get_future();
    boost::asio::co_spawn(
        boost::asio::system_executor{},
        [&client, req, p = std::move(p2)]() mutable -> boost::asio::awaitable<void> {
            auto r = co_await client.get(req);
            p.set_value(std::move(r));
            co_return;
        },
        boost::asio::detached);
    if (f2.wait_for(std::chrono::milliseconds(opts.requestTimeoutMs)) == std::future_status::ready)
        return f2.get();
    return Error{ErrorCode::Timeout, "get timed out"};
}

Result<yams::daemon::GrepResponse> RetrievalService::grep(const yams::daemon::GrepRequest& req,
                                                          const RetrievalOptions& opts) const {
    yams::daemon::DaemonClient client(makeClientConfig(opts));
    std::promise<Result<yams::daemon::GrepResponse>> p2;
    auto f2 = p2.get_future();
    boost::asio::co_spawn(
        boost::asio::system_executor{},
        [&client, req, p = std::move(p2)]() mutable -> boost::asio::awaitable<void> {
            auto r = co_await client.streamingGrep(req);
            p.set_value(std::move(r));
            co_return;
        },
        boost::asio::detached);
    if (f2.wait_for(std::chrono::milliseconds(opts.requestTimeoutMs)) == std::future_status::ready)
        return f2.get();
    return Error{ErrorCode::Timeout, "grep timed out"};
}

Result<yams::daemon::ListResponse> RetrievalService::list(const yams::daemon::ListRequest& req,
                                                          const RetrievalOptions& opts) const {
    yams::daemon::DaemonClient client(makeClientConfig(opts));
    std::promise<Result<yams::daemon::ListResponse>> p2;
    auto f2 = p2.get_future();
    boost::asio::co_spawn(
        boost::asio::system_executor{},
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

Result<void> RetrievalService::getToStdout(const yams::daemon::GetInitRequest& req,
                                           const RetrievalOptions& opts) const {
    yams::daemon::DaemonClient client(makeClientConfig(opts));
    std::promise<Result<void>> p2;
    auto f2 = p2.get_future();
    boost::asio::co_spawn(
        boost::asio::system_executor{},
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

Result<void> RetrievalService::getToFile(const yams::daemon::GetInitRequest& req,
                                         const std::filesystem::path& outputPath,
                                         const RetrievalOptions& opts) const {
    yams::daemon::DaemonClient client(makeClientConfig(opts));
    std::promise<Result<void>> p2;
    auto f2 = p2.get_future();
    boost::asio::co_spawn(
        boost::asio::system_executor{},
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
RetrievalService::getChunkedBuffer(const yams::daemon::GetInitRequest& req, std::size_t capBytes,
                                   const RetrievalOptions& opts) const {
    // Prefer native chunked protocol; fallback to unary Get on error/timeout
    {
        yams::daemon::DaemonClient client(makeClientConfig(opts));
        std::promise<Result<ChunkedGetResult>> p2;
        auto f2 = p2.get_future();
        boost::asio::co_spawn(
            boost::asio::system_executor{},
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
    yams::daemon::GetRequest greq;
    greq.hash = req.hash;
    greq.name = req.name;
    greq.byName = !req.name.empty();
    greq.metadataOnly = false;
    auto r = get(greq, opts);
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
            yams::daemon::GetRequest greq;
            greq.hash = rh.value();
            greq.metadataOnly = !includeContent;
            return get(greq, opts);
        }
    }

    // 2) Try session-aware list by name
    if (useSession) {
        try {
            auto sess = makeSessionService(nullptr);
            auto pats = sess->activeIncludePatterns(sessionName.empty()
                                                        ? std::optional<std::string>{}
                                                        : std::optional<std::string>{sessionName});
            for (const auto& pat : pats) {
                yams::daemon::ListRequest lreq;
                lreq.namePattern = pat;
                lreq.limit = 200;
                lreq.pathsOnly = false;
                auto lres = list(lreq, opts);
                if (lres) {
                    const auto& items = lres.value().items;
                    const auto* chosen = static_cast<const yams::daemon::ListEntry*>(nullptr);
                    for (const auto& item : items) {
                        if (item.name == name && !item.hash.empty()) {
                            if (!chosen) {
                                chosen = &item;
                            } else {
                                if (oldest) {
                                    if (item.indexed < chosen->indexed)
                                        chosen = &item;
                                } else {
                                    if (item.indexed > chosen->indexed)
                                        chosen = &item;
                                }
                            }
                        }
                    }
                    if (chosen) {
                        yams::daemon::GetRequest greq;
                        greq.hash = chosen->hash;
                        greq.metadataOnly = !includeContent;
                        return get(greq, opts);
                    }
                }
            }
        } catch (...) {
            // ignore session errors and continue
        }
    }

    // 3) Hybrid search fallback for best match
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
            boost::asio::system_executor{},
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
            yams::daemon::GetRequest greq;
            if (!hash.empty()) {
                greq.hash = hash;
            } else {
                // Prefer filename-based name resolution for portability over absolute path
                try {
                    std::filesystem::path bp(best.path);
                    greq.name = bp.filename().string();
                } catch (...) {
                    greq.name = best.path; // fallback to provided path
                }
                greq.byName = true;
            }
            greq.metadataOnly = !includeContent;
            return get(greq, opts);
        }
    } catch (...) {
    }

    return Error{ErrorCode::NotFound, "document not found by name"};
}

} // namespace yams::app::services
