#include <yams/app/services/document_ingestion_service.h>

#include <spdlog/spdlog.h>
#include <algorithm>
#include <chrono>
#include <filesystem>
#include <future>
#include <thread>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
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

Result<yams::daemon::AddDocumentResponse>
DocumentIngestionService::addViaDaemon(const AddOptions& opts) const {
    using namespace std::chrono_literals;

    if (opts.path.empty() && opts.content.empty()) {
        return Error{ErrorCode::InvalidArgument,
                     "Provide a file path or inline content for add operation"};
    }
    if (!opts.path.empty() && !opts.content.empty()) {
        return Error{ErrorCode::InvalidArgument,
                     "Cannot specify both file path and inline content simultaneously"};
    }
    if (!opts.content.empty()) {
        if (opts.name.empty()) {
            return Error{ErrorCode::InvalidArgument,
                         "Inline content requires a non-empty document name"};
        }
    }

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

    // Retriable daemon call with backoff
    std::string lastError;
    for (int attempt = 0; attempt <= std::max(0, opts.retries); ++attempt) {
        try {
            auto attemptStart = std::chrono::steady_clock::now();
            spdlog::debug("[addViaDaemon] attempt={} path={} timeoutMs={}", attempt, opts.path,
                          opts.timeoutMs);

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
            auto client = std::make_shared<yams::daemon::DaemonClient>(cfg);

            std::promise<Result<yams::daemon::AddDocumentResponse>> p2;
            auto f2 = p2.get_future();
            spdlog::debug("[addViaDaemon] spawning coroutine");
            boost::asio::co_spawn(
                yams::daemon::GlobalIOContext::global_executor(),
                [client, dreq, p2 = std::move(p2)]() mutable -> boost::asio::awaitable<void> {
                    spdlog::debug("[addViaDaemon] coroutine started");
                    auto r = co_await client->streamingAddDocument(dreq);
                    spdlog::debug("[addViaDaemon] coroutine got response");
                    p2.set_value(std::move(r));
                    co_return;
                },
                boost::asio::detached);

            spdlog::debug("[addViaDaemon] waiting on future for {}ms", opts.timeoutMs);
            Result<yams::daemon::AddDocumentResponse> res =
                Error{ErrorCode::Timeout, "AddDocument timed out"};
            if (f2.wait_for(std::chrono::milliseconds(std::max(1, opts.timeoutMs))) ==
                std::future_status::ready) {
                res = f2.get();
                auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                                   std::chrono::steady_clock::now() - attemptStart)
                                   .count();
                spdlog::debug("[addViaDaemon] got result in {}ms", elapsed);
            } else {
                auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                                   std::chrono::steady_clock::now() - attemptStart)
                                   .count();
                spdlog::warn("[addViaDaemon] TIMEOUT after {}ms (expected {}ms)", elapsed,
                             opts.timeoutMs);
            }
            if (res) {
                // Optional strong verification for single-file adds
                if (opts.verify) {
                    try {
                        // Verify only for explicit single-file path (non-recursive)
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
                                return Error{ErrorCode::InvalidData,
                                             std::string("verification failed: ") + why};
                            }
                        }
                    } catch (const std::exception& ex) {
                        return Error{ErrorCode::Unknown,
                                     std::string("verification error: ") + ex.what()};
                    } catch (...) {
                        return Error{ErrorCode::Unknown, "verification error: unknown"};
                    }
                }
                return res;
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
            std::this_thread::sleep_for(std::chrono::milliseconds(delay));
        }
    }
    return Error{ErrorCode::NetworkError,
                 lastError.empty() ? std::string("daemon add failed") : lastError};
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
