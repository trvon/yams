#include <yams/app/services/document_ingestion_service.h>

#include <spdlog/spdlog.h>
#include <algorithm>
#include <chrono>
#include <filesystem>
#include <future>
#include <thread>
#include <condition_variable>
#include <mutex>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <yams/crypto/hasher.h>
#include <yams/daemon/client/global_io_context.h>

namespace yams::app::services {

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

std::shared_ptr<yams::daemon::DaemonClient>
DocumentIngestionService::getOrCreateClient(const AddOptions& opts) const {
    if (client_)
        return client_;
    return std::make_shared<yams::daemon::DaemonClient>(
        makeIngestionClientConfig(opts.socketPath, opts.explicitDataDir, opts.timeoutMs));
}

std::shared_ptr<yams::daemon::DaemonClient>
DocumentIngestionService::getOrCreateClient(const DeleteOptions& opts) const {
    if (client_)
        return client_;
    return std::make_shared<yams::daemon::DaemonClient>(
        makeIngestionClientConfig(opts.socketPath, opts.explicitDataDir, opts.timeoutMs));
}

std::shared_ptr<yams::daemon::DaemonClient>
DocumentIngestionService::getOrCreateClient(const UpdateOptions& opts) const {
    if (client_)
        return client_;
    return std::make_shared<yams::daemon::DaemonClient>(
        makeIngestionClientConfig(opts.socketPath, opts.explicitDataDir, opts.timeoutMs));
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

boost::asio::awaitable<Result<yams::daemon::AddDocumentResponse>>
DocumentIngestionService::addViaDaemonAsync(const AddOptions& opts) const {
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

    auto dreq = buildRequest(opts);
    auto client = getOrCreateClient(opts);

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

Result<yams::daemon::AddDocumentResponse>
DocumentIngestionService::addViaDaemon(const AddOptions& opts) const {
    // Sync wrapper: spawn the async coroutine and block on the result
    std::promise<Result<yams::daemon::AddDocumentResponse>> p;
    auto f = p.get_future();

    boost::asio::co_spawn(
        yams::daemon::GlobalIOContext::global_executor(),
        [this, &opts, p = std::move(p)]() mutable -> boost::asio::awaitable<void> {
            auto r = co_await addViaDaemonAsync(opts);
            p.set_value(std::move(r));
            co_return;
        },
        boost::asio::detached);

    // Wait with timeout: sum of per-attempt timeouts + backoff
    int totalMs = (opts.retries + 1) * opts.timeoutMs + opts.retries * opts.backoffMs * 4;
    if (f.wait_for(std::chrono::milliseconds(std::max(1, totalMs))) == std::future_status::ready) {
        return f.get();
    }
    return Error{ErrorCode::Timeout, "AddDocument timed out"};
}

BatchAddResult DocumentIngestionService::addBatch(const std::vector<AddOptions>& batch,
                                                  int maxConcurrent) const {
    BatchAddResult out;
    out.results.resize(batch.size(), Error{ErrorCode::Unknown, "not started"});

    if (batch.empty())
        return out;

    // Use a mutex + counter as a simple semaphore for concurrency control
    std::mutex mu;
    std::condition_variable cv;
    int inFlight = 0;
    std::atomic<size_t> completed{0};

    // Spawn all coroutines; each one acquires a semaphore slot
    for (size_t i = 0; i < batch.size(); ++i) {
        // Wait for a slot (blocking the calling thread is fine — this is the CLI sync path)
        {
            std::unique_lock lk(mu);
            cv.wait(lk, [&] { return inFlight < maxConcurrent; });
            ++inFlight;
        }

        boost::asio::co_spawn(
            yams::daemon::GlobalIOContext::global_executor(),
            [this, &batch, &out, &mu, &cv, &inFlight, &completed,
             i]() mutable -> boost::asio::awaitable<void> {
                auto r = co_await addViaDaemonAsync(batch[i]);
                {
                    std::lock_guard lk(mu);
                    out.results[i] = std::move(r);
                    --inFlight;
                }
                completed.fetch_add(1, std::memory_order_release);
                cv.notify_one();
                co_return;
            },
            boost::asio::detached);
    }

    // Wait for all to complete
    {
        std::unique_lock lk(mu);
        cv.wait(lk, [&] { return completed.load(std::memory_order_acquire) == batch.size(); });
    }

    for (const auto& r : out.results) {
        if (r)
            ++out.succeeded;
        else
            ++out.failed;
    }
    return out;
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
    std::promise<Result<yams::daemon::DeleteResponse>> p;
    std::promise<void> done;
    auto f = p.get_future();
    auto done_f = done.get_future();
    boost::asio::co_spawn(
        yams::daemon::GlobalIOContext::global_executor(),
        [client, dreq, p = std::move(p),
         d = std::move(done)]() mutable -> boost::asio::awaitable<void> {
            auto r = co_await client->remove(dreq);
            // remove() returns SuccessResponse; wrap into DeleteResponse
            yams::daemon::DeleteResponse dr;
            if (r) {
                dr.successCount = 1;
            }
            p.set_value(Result<yams::daemon::DeleteResponse>(std::move(dr)));
            d.set_value();
            co_return;
        },
        boost::asio::detached);

    try {
        if (f.wait_for(std::chrono::milliseconds(opts.timeoutMs)) == std::future_status::ready) {
            auto result = f.get();
            done_f.wait();
            return result;
        }
    } catch (const std::exception& e) {
        done_f.wait();
        return Error{ErrorCode::InternalError,
                     std::string("delete failed with exception: ") + e.what()};
    }
    done_f.wait();
    return Error{ErrorCode::Timeout, "delete timed out"};
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
    std::promise<Result<yams::daemon::UpdateDocumentResponse>> p;
    std::promise<void> done;
    auto f = p.get_future();
    auto done_f = done.get_future();
    boost::asio::co_spawn(
        yams::daemon::GlobalIOContext::global_executor(),
        [client, dreq, p = std::move(p),
         d = std::move(done)]() mutable -> boost::asio::awaitable<void> {
            auto r = co_await client->updateDocument(dreq);
            p.set_value(std::move(r));
            d.set_value();
            co_return;
        },
        boost::asio::detached);

    try {
        if (f.wait_for(std::chrono::milliseconds(opts.timeoutMs)) == std::future_status::ready) {
            auto result = f.get();
            done_f.wait();
            return result;
        }
    } catch (const std::exception& e) {
        done_f.wait();
        return Error{ErrorCode::InternalError,
                     std::string("update failed with exception: ") + e.what()};
    }
    done_f.wait();
    return Error{ErrorCode::Timeout, "update timed out"};
}

} // namespace yams::app::services
