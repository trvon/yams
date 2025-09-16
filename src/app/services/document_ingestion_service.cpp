#include <yams/app/services/document_ingestion_service.h>

#include <algorithm>
#include <chrono>
#include <future>
#include <thread>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/system_executor.hpp>

namespace yams::app::services {

std::string DocumentIngestionService::normalizePath(const std::string& inPath) {
    if (inPath.empty() || inPath == "-")
        return inPath;
    std::error_code ec;
    auto abs = std::filesystem::absolute(inPath, ec);
    auto canon = std::filesystem::weakly_canonical(abs, ec);
    return (canon.empty() ? abs : canon).string();
}

Result<yams::daemon::AddDocumentResponse>
DocumentIngestionService::addViaDaemon(const AddOptions& opts) const {
    using namespace std::chrono_literals;

    yams::daemon::AddDocumentRequest dreq;
    dreq.path = normalizePath(opts.path);
    dreq.content = opts.content;
    dreq.name = opts.name;
    dreq.mimeType = opts.mimeType;
    dreq.disableAutoMime = opts.disableAutoMime;
    dreq.noEmbeddings = opts.noEmbeddings;
    dreq.collection = opts.collection;
    dreq.snapshotId = opts.snapshotId;
    dreq.snapshotLabel = opts.snapshotLabel;
    dreq.recursive = opts.recursive;
    dreq.includePatterns = opts.includePatterns;
    dreq.excludePatterns = opts.excludePatterns;
    dreq.tags = opts.tags;
    for (const auto& [k, v] : opts.metadata) {
        dreq.metadata[k] = v;
    }

    // Retriable daemon call with backoff
    std::string lastError;
    for (int attempt = 0; attempt <= std::max(0, opts.retries); ++attempt) {
        try {
            yams::daemon::ClientConfig cfg;
            if (opts.socketPath && !opts.socketPath->empty()) {
                cfg.socketPath = *opts.socketPath;
            }
            if (opts.explicitDataDir && !opts.explicitDataDir->empty()) {
                cfg.dataDir = *opts.explicitDataDir;
            }
            cfg.enableChunkedResponses = false;
            cfg.singleUseConnections = false; // use pooled, multiplexed connection
            cfg.requestTimeout = std::chrono::milliseconds(std::max(1, opts.timeoutMs));
            yams::daemon::DaemonClient client(cfg);

            std::promise<Result<yams::daemon::AddDocumentResponse>> p2;
            auto f2 = p2.get_future();
            boost::asio::co_spawn(
                boost::asio::system_executor{},
                [&client, dreq, pr = std::move(p2)]() mutable -> boost::asio::awaitable<void> {
                    auto r = co_await client.streamingAddDocument(dreq);
                    pr.set_value(std::move(r));
                    co_return;
                },
                boost::asio::detached);

            Result<yams::daemon::AddDocumentResponse> res =
                Error{ErrorCode::Timeout, "AddDocument timed out"};
            if (f2.wait_for(std::chrono::milliseconds(std::max(1, opts.timeoutMs))) ==
                std::future_status::ready) {
                res = f2.get();
            }
            if (res)
                return res;
            lastError = res.error().message;
        } catch (const std::exception& e) {
            lastError = e.what();
        } catch (...) {
            lastError = "unknown error";
        }

        if (attempt < opts.retries) {
            int delay = opts.backoffMs * (1 << attempt);
            if (delay < 0)
                delay = opts.backoffMs; // overflow guard
            std::this_thread::sleep_for(std::chrono::milliseconds(delay));
        }
    }
    return Error{ErrorCode::NetworkError,
                 lastError.empty() ? std::string("daemon add failed") : lastError};
}

} // namespace yams::app::services
