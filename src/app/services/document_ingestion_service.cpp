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

} // namespace yams::app::services
