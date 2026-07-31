#include <yams/app/services/document_ingestion_service.h>

#include <spdlog/spdlog.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <mutex>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <yams/core/assert.hpp>
#include <yams/crypto/hasher.h>
#include <yams/daemon/client/await_result_sync.h>
#include <yams/daemon/client/global_io_context.h>
#include <yams/daemon/components/TuneAdvisor.h>

namespace yams::app::services {

namespace {

std::size_t resolveBatchConcurrency(std::size_t batchSize, int maxConcurrent) {
    if (batchSize == 0) {
        return 1;
    }

    if (maxConcurrent > 0) {
        return std::min<std::size_t>(static_cast<std::size_t>(maxConcurrent), batchSize);
    }

    if (!yams::daemon::TuneAdvisor::enableParallelIngest()) {
        return 1;
    }

    std::size_t cap = yams::daemon::TuneAdvisor::maxIngestWorkers();
    if (cap == 0) {
        cap = yams::daemon::TuneAdvisor::recommendedThreads();
    }

    const std::size_t storageCap = yams::daemon::TuneAdvisor::storagePoolSize();
    if (storageCap > 0) {
        cap = std::min(cap, storageCap);
    }
    if (cap == 0) {
        cap = 1;
    }

    const auto result = std::min(batchSize, cap);
    YAMS_POSTCONDITION(result >= 1, "batch concurrency must be at least 1");
    return result;
}

} // namespace

DocumentIngestionService::DocumentIngestionService(
    std::shared_ptr<yams::daemon::DaemonClient> client)
    : client_(std::move(client)) {}

static yams::daemon::ClientConfig
makeIngestionClientConfig(const std::optional<std::filesystem::path>& socketPath,
                          const std::optional<std::filesystem::path>& explicitDataDir,
                          int timeoutMs) {
    yams::daemon::ClientConfig cfg;
    if (socketPath && !socketPath->empty())
        cfg.socketPath = *socketPath;
    if (explicitDataDir && !explicitDataDir->empty())
        cfg.dataDir = *explicitDataDir;
    cfg.enableChunkedResponses = false;
    cfg.singleUseConnections = false;
    cfg.requestTimeout = std::chrono::milliseconds(std::max(1, timeoutMs));
    return cfg;
}

static bool isSameIngestionClientConfig(const yams::daemon::ClientConfig& lhs,
                                        const yams::daemon::ClientConfig& rhs) {
    return lhs.socketPath == rhs.socketPath && lhs.dataDir == rhs.dataDir &&
           lhs.requestTimeout == rhs.requestTimeout &&
           lhs.enableChunkedResponses == rhs.enableChunkedResponses &&
           lhs.singleUseConnections == rhs.singleUseConnections;
}

std::shared_ptr<yams::daemon::DaemonClient>
DocumentIngestionService::getOrCreateClient(const AddOptions& opts) const {
    const auto cfg =
        makeIngestionClientConfig(opts.socketPath, opts.explicitDataDir, opts.timeoutMs);
    std::lock_guard<std::mutex> lock{clientMutex_};
    if (client_ && !cachedClientConfig_) {
        // Caller provided a preconfigured client; preserve it unless we have an explicit
        // comparable config snapshot to replace against.
        return client_;
    }
    if (client_ && cachedClientConfig_ && isSameIngestionClientConfig(*cachedClientConfig_, cfg)) {
        return client_;
    }
    client_ = std::make_shared<yams::daemon::DaemonClient>(cfg);
    cachedClientConfig_ = cfg;
    return client_;
}

std::shared_ptr<yams::daemon::DaemonClient>
DocumentIngestionService::getOrCreateClient(const DeleteOptions& opts) const {
    const auto cfg =
        makeIngestionClientConfig(opts.socketPath, opts.explicitDataDir, opts.timeoutMs);
    std::lock_guard<std::mutex> lock{clientMutex_};
    if (client_ && !cachedClientConfig_) {
        return client_;
    }
    if (client_ && cachedClientConfig_ && isSameIngestionClientConfig(*cachedClientConfig_, cfg)) {
        return client_;
    }
    client_ = std::make_shared<yams::daemon::DaemonClient>(cfg);
    cachedClientConfig_ = cfg;
    return client_;
}

std::shared_ptr<yams::daemon::DaemonClient>
DocumentIngestionService::getOrCreateClient(const UpdateOptions& opts) const {
    const auto cfg =
        makeIngestionClientConfig(opts.socketPath, opts.explicitDataDir, opts.timeoutMs);
    std::lock_guard<std::mutex> lock{clientMutex_};
    if (client_ && !cachedClientConfig_) {
        return client_;
    }
    if (client_ && cachedClientConfig_ && isSameIngestionClientConfig(*cachedClientConfig_, cfg)) {
        return client_;
    }
    client_ = std::make_shared<yams::daemon::DaemonClient>(cfg);
    cachedClientConfig_ = cfg;
    return client_;
}

std::string DocumentIngestionService::normalizePath(const std::string& inPath) {
    if (inPath.empty() || inPath == "-")
        return inPath;
    std::error_code ec;
    auto abs = std::filesystem::absolute(inPath, ec);
    auto canon = std::filesystem::weakly_canonical(abs, ec);
    return (canon.empty() ? abs : canon).string();
}

yams::daemon::AddDocumentRequest DocumentIngestionService::buildRequest(const AddOptions& opts) {
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
    dreq.sessionId = opts.sessionId;
    dreq.recursive = opts.recursive;
    dreq.includePatterns = opts.includePatterns;
    dreq.excludePatterns = opts.excludePatterns;
    dreq.noGitignore = opts.noGitignore;
    dreq.tags = opts.tags;
    dreq.waitForProcessing = opts.waitForProcessing;
    dreq.waitTimeoutSeconds = opts.waitTimeoutSeconds;
    for (const auto& [k, v] : opts.metadata) {
        dreq.metadata[k] = v;
    }
    return dreq;
}

namespace {

boost::asio::awaitable<Result<yams::daemon::AddDocumentResponse>>
addViaDaemonWithClient(std::shared_ptr<yams::daemon::DaemonClient> client, AddOptions opts) {
    if (opts.path.empty() && opts.content.empty()) {
        co_return Error{ErrorCode::InvalidArgument,
                        "Provide a file path or inline content for add operation"};
    }
    if (!opts.path.empty() && !opts.content.empty()) {
        co_return Error{ErrorCode::InvalidArgument,
                        "Cannot specify both file path and inline content simultaneously"};
    }
    if (!opts.content.empty() && opts.name.empty()) {
        co_return Error{ErrorCode::InvalidArgument,
                        "Inline content requires a non-empty document name"};
    }

    auto dreq = DocumentIngestionService::buildRequest(opts);
    // Retriable daemon call with backoff
    std::string lastError;
    auto exec = co_await boost::asio::this_coro::executor;
    for (int attempt = 0; attempt <= std::max(0, opts.retries); ++attempt) {
        try {
            auto attemptStart = std::chrono::steady_clock::now();
            spdlog::debug("[addViaDaemonAsync] attempt={} path={} timeoutMs={}", attempt, opts.path,
                          opts.timeoutMs);

            auto res = co_await client->streamingAddDocument(dreq);

            auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                               std::chrono::steady_clock::now() - attemptStart)
                               .count();
            spdlog::debug("[addViaDaemonAsync] got result in {}ms", elapsed);

            if (res) {
                // Optional strong verification for single-file adds
                if (opts.verify) {
                    try {
                        std::error_code fec;
                        bool isFile = !dreq.path.empty() && !opts.recursive &&
                                      std::filesystem::is_regular_file(dreq.path, fec) && !fec;
                        if (isFile) {
                            auto hasher = yams::crypto::createSHA256Hasher();
                            std::string expected = hasher->hashFile(dreq.path);
                            const auto& ar = res.value();
                            auto fsize = std::filesystem::file_size(dreq.path);
                            bool ok = true;
                            std::string why;
                            if (!expected.empty() && !ar.hash.empty() && expected != ar.hash) {
                                ok = false;
                                why = "hash mismatch";
                            } else if (ar.size != 0 && ar.size != fsize) {
                                ok = false;
                                why = "size mismatch";
                            }
                            if (!ok) {
                                co_return Error{ErrorCode::InvalidData,
                                                std::string("verification failed: ") + why};
                            }
                        }
                    } catch (const std::exception& ex) {
                        co_return Error{ErrorCode::Unknown,
                                        std::string("verification error: ") + ex.what()};
                    } catch (...) {
                        co_return Error{ErrorCode::Unknown, "verification error: unknown"};
                    }
                }
                co_return res;
            }
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
            boost::asio::steady_timer timer{exec};
            timer.expires_after(std::chrono::milliseconds(delay));
            co_await timer.async_wait(boost::asio::use_awaitable);
        }
    }
    co_return Error{ErrorCode::NetworkError,
                    lastError.empty() ? std::string("daemon add failed") : lastError};
}

} // namespace

boost::asio::awaitable<Result<yams::daemon::AddDocumentResponse>>
DocumentIngestionService::addViaDaemonAsync(const AddOptions& opts) const {
    return addViaDaemonWithClient(getOrCreateClient(opts), opts);
}

Result<yams::daemon::AddDocumentResponse>
DocumentIngestionService::addViaDaemon(const AddOptions& opts) const {
    // Wait with timeout: sum of per-attempt timeouts + backoff
    const int retries = std::max(0, opts.retries);
    const auto totalMs = static_cast<std::int64_t>(retries + 1) * std::max(1, opts.timeoutMs) +
                         static_cast<std::int64_t>(retries) * std::max(0, opts.backoffMs) * 4;
    auto client = getOrCreateClient(opts);
    return yams::daemon::detail::awaitResultSync<yams::daemon::AddDocumentResponse>(
        yams::daemon::GlobalIOContext::global_executor(),
        [client = std::move(client), opts]() mutable {
            return addViaDaemonWithClient(std::move(client), std::move(opts));
        },
        std::chrono::milliseconds(std::max<std::int64_t>(1, totalMs)), "add");
}

boost::asio::awaitable<BatchAddResult>
DocumentIngestionService::addBatchAsync(const std::vector<AddOptions>& batch,
                                        int maxConcurrent) const {
    BatchAddResult out;
    out.results.resize(batch.size(), Error{ErrorCode::Unknown, "not started"});

    if (batch.empty())
        co_return out;

    const std::size_t concurrency = resolveBatchConcurrency(batch.size(), maxConcurrent);
    auto exec = co_await boost::asio::this_coro::executor;

    // Sliding-window worker pool: each worker pulls the next pending index as
    // soon as its previous request completes, keeping `concurrency` requests
    // in flight at all times. (The previous wave-barrier + 5 ms polling join
    // quantized every wave to the poll interval and stalled on stragglers.)
    auto nextIndex = std::make_shared<std::atomic<std::size_t>>(0);
    auto worker = [this, &batch, &out, nextIndex]() -> boost::asio::awaitable<void> {
        while (true) {
            const std::size_t index = nextIndex->fetch_add(1, std::memory_order_relaxed);
            if (index >= batch.size()) {
                break;
            }
            try {
                out.results[index] = co_await addViaDaemonAsync(batch[index]);
            } catch (const std::exception& e) {
                out.results[index] =
                    Error{ErrorCode::InternalError,
                          std::string("batch add failed with exception: ") + e.what()};
            } catch (...) {
                out.results[index] =
                    Error{ErrorCode::InternalError, "batch add failed with unknown exception"};
            }
        }
        co_return;
    };

    co_await boost::asio::async_initiate<decltype(boost::asio::use_awaitable), void()>(
        [exec, concurrency, worker](auto handler) {
            using HandlerT = std::decay_t<decltype(handler)>;
            auto handlerPtr = std::make_shared<HandlerT>(std::move(handler));
            auto remaining = std::make_shared<std::atomic<std::size_t>>(concurrency);
            for (std::size_t w = 0; w < concurrency; ++w) {
                boost::asio::co_spawn(exec, worker, [handlerPtr, remaining](std::exception_ptr) {
                    if (remaining->fetch_sub(1, std::memory_order_acq_rel) == 1) {
                        (*handlerPtr)();
                    }
                });
            }
        },
        boost::asio::use_awaitable);

    for (const auto& r : out.results) {
        if (r)
            ++out.succeeded;
        else
            ++out.failed;
    }
    YAMS_POSTCONDITION(out.succeeded + out.failed == batch.size(),
                       "batch accounting must match input size");
    co_return out;
}

int DocumentIngestionService::testing_resolveBatchConcurrency(std::size_t batchSize,
                                                              int maxConcurrent) {
    return static_cast<int>(resolveBatchConcurrency(batchSize, maxConcurrent));
}

Result<yams::daemon::DeleteResponse>
DocumentIngestionService::deleteDocument(const DeleteOptions& opts) const {
    yams::daemon::DeleteRequest dreq;
    dreq.names = opts.names;
    dreq.dryRun = opts.dryRun;
    dreq.force = true; // service layer assumes caller confirmed
    dreq.sessionId = opts.sessionId;
    // Set hash from first entry if only hashes provided
    if (!opts.hashes.empty() && opts.names.empty()) {
        dreq.hash = opts.hashes.front();
    }

    auto client = getOrCreateClient(opts);
    return yams::daemon::detail::awaitResultSync<yams::daemon::DeleteResponse>(
        yams::daemon::GlobalIOContext::global_executor(),
        [client = std::move(client), dreq = std::move(dreq)]() { return client->remove(dreq); },
        std::chrono::milliseconds(std::max(1, opts.timeoutMs)), "delete");
}

Result<yams::daemon::UpdateDocumentResponse>
DocumentIngestionService::updateDocument(const UpdateOptions& opts) const {
    yams::daemon::UpdateDocumentRequest dreq;
    dreq.hash = opts.hash;
    dreq.name = opts.name;
    dreq.addTags = opts.addTags;
    dreq.removeTags = opts.removeTags;
    dreq.metadata = opts.setMetadata;

    auto client = getOrCreateClient(opts);
    return yams::daemon::detail::awaitResultSync<yams::daemon::UpdateDocumentResponse>(
        yams::daemon::GlobalIOContext::global_executor(),
        [client = std::move(client), dreq = std::move(dreq)]() {
            return client->updateDocument(dreq);
        },
        std::chrono::milliseconds(std::max(1, opts.timeoutMs)), "update");
}

} // namespace yams::app::services
