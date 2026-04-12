#include <yams/daemon/client/in_process_transport.h>

#include <memory>
#include <optional>
#include <utility>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <yams/daemon/components/RequestDispatcher.h>
#include <yams/daemon/ipc/request_handler.h>
#include <yams/daemon/ipc/streaming_processor.h>

namespace yams::daemon {
namespace {

class DispatcherAdapter final : public RequestProcessor {
public:
    explicit DispatcherAdapter(RequestDispatcher* dispatcher) : dispatcher_(dispatcher) {}

    boost::asio::awaitable<Response> process(const Request& request) override {
        co_return co_await dispatcher_->dispatch(request);
    }

    boost::asio::awaitable<std::optional<Response>>
    process_streaming(const Request& request) override {
        co_return co_await dispatcher_->dispatch(request);
    }

    bool supports_streaming(const Request& request) const override {
        if (std::holds_alternative<SearchRequest>(request) ||
            std::holds_alternative<ListRequest>(request) ||
            std::holds_alternative<GrepRequest>(request) ||
            std::holds_alternative<AddDocumentRequest>(request) ||
            std::holds_alternative<BatchEmbeddingRequest>(request) ||
            std::holds_alternative<EmbedDocumentsRequest>(request) ||
            std::holds_alternative<RepairRequest>(request) ||
            std::holds_alternative<GenerateEmbeddingRequest>(request) ||
            std::holds_alternative<LoadModelRequest>(request)) {
            return true;
        }
        if (std::holds_alternative<GetInitRequest>(request) ||
            std::holds_alternative<GetChunkRequest>(request) ||
            std::holds_alternative<GetEndRequest>(request)) {
            return false;
        }
        return false;
    }

private:
    RequestDispatcher* dispatcher_;
};

Response make_stream_header(const Request& request) {
    if (std::holds_alternative<SearchRequest>(request)) {
        SearchResponse r;
        r.totalCount = 0;
        r.elapsed = std::chrono::milliseconds(0);
        return r;
    }
    if (std::holds_alternative<ListRequest>(request)) {
        ListResponse r;
        r.totalCount = 0;
        return r;
    }
    if (std::holds_alternative<GrepRequest>(request)) {
        GrepResponse r;
        r.totalMatches = 0;
        r.filesSearched = 0;
        return r;
    }
    return SuccessResponse{"Streaming response"};
}

bool is_control_request(const Request& request) {
    return std::holds_alternative<ShutdownRequest>(request) ||
           std::holds_alternative<PingRequest>(request) ||
           std::holds_alternative<StatusRequest>(request) ||
           std::holds_alternative<GetStatsRequest>(request) ||
           std::holds_alternative<PrepareSessionRequest>(request);
}

} // namespace

boost::asio::awaitable<Result<Response>> InProcessTransport::send_request(Request request) {
    Request ownedReq = std::move(request);

    if (!host_) {
        co_return Error{ErrorCode::NotInitialized, "Embedded service host unavailable"};
    }

    auto* dispatcher = host_->getDispatcher();
    if (!dispatcher) {
        co_return Error{ErrorCode::NotInitialized, "Embedded request dispatcher unavailable"};
    }

    auto callerExec = co_await boost::asio::this_coro::executor;
    auto result = std::make_shared<Result<Response>>();
    auto timer = std::make_shared<boost::asio::steady_timer>(
        callerExec, boost::asio::steady_timer::time_point::max());

    boost::asio::co_spawn(
        host_->getExecutor(),
        [dispatcher, request = std::move(ownedReq), result, timer,
         callerExec]() mutable -> boost::asio::awaitable<void> {
            try {
                auto response = co_await dispatcher->dispatch(request);
                *result = std::move(response);
            } catch (const std::exception& e) {
                *result = Error{ErrorCode::InternalError, e.what()};
            } catch (...) {
                *result = Error{ErrorCode::InternalError, "In-process request dispatch failed"};
            }
            boost::asio::post(callerExec, [timer] { timer->cancel(); });
            co_return;
        },
        boost::asio::detached);

    boost::system::error_code ec;
    co_await timer->async_wait(boost::asio::redirect_error(boost::asio::use_awaitable, ec));

    co_return std::move(*result);
}

boost::asio::awaitable<Result<void>> InProcessTransport::send_request_streaming(
    Request request, const HeaderCallback& onHeader, const ChunkCallback& onChunk,
    const ErrorCallback& onError, const CompleteCallback& onComplete) {
    if (!host_) {
        co_return Error{ErrorCode::NotInitialized, "Embedded service host unavailable"};
    }

    auto* dispatcher = host_->getDispatcher();
    if (!dispatcher) {
        co_return Error{ErrorCode::NotInitialized, "Embedded request dispatcher unavailable"};
    }

    auto callerExec = co_await boost::asio::this_coro::executor;
    auto result = std::make_shared<Result<void>>();
    auto timer = std::make_shared<boost::asio::steady_timer>(
        callerExec, boost::asio::steady_timer::time_point::max());

    boost::asio::co_spawn(
        host_->getExecutor(),
        [dispatcher, request = std::move(request), onHeader, onChunk, onError, onComplete, result,
         timer, callerExec]() mutable -> boost::asio::awaitable<void> {
            try {
                auto adapter = std::make_shared<DispatcherAdapter>(dispatcher);
                RequestHandler::Config cfg;
                auto processor = std::make_shared<StreamingRequestProcessor>(adapter, cfg);

                auto immediate = co_await processor->process_streaming(request);
                if (immediate.has_value()) {
                    if (onHeader) {
                        onHeader(immediate.value());
                    }
                    if (onChunk && !is_control_request(request)) {
                        (void)onChunk(immediate.value(), true);
                    }
                    if (onComplete) {
                        onComplete();
                    }
                    *result = Result<void>();
                    boost::asio::post(callerExec, [timer] { timer->cancel(); });
                    co_return;
                }

                if (onHeader) {
                    onHeader(make_stream_header(request));
                }

                bool done = false;
                while (!done) {
                    auto chunk = co_await processor->next_chunk();
                    if (onChunk) {
                        bool keepGoing = onChunk(chunk.data, chunk.is_last_chunk);
                        if (!keepGoing) {
                            done = true;
                        }
                    }
                    if (chunk.is_last_chunk) {
                        done = true;
                    }
                }

                if (onComplete) {
                    onComplete();
                }
                *result = Result<void>();
            } catch (const std::exception& e) {
                Error err{ErrorCode::InternalError, e.what()};
                if (onError) {
                    onError(err);
                }
                *result = err;
            } catch (...) {
                Error err{ErrorCode::InternalError, "In-process streaming dispatch failed"};
                if (onError) {
                    onError(err);
                }
                *result = err;
            }
            boost::asio::post(callerExec, [timer] { timer->cancel(); });
            co_return;
        },
        boost::asio::detached);

    boost::system::error_code ec;
    co_await timer->async_wait(boost::asio::redirect_error(boost::asio::use_awaitable, ec));

    if (!*result) {
        co_return result->error();
    }
    co_return Result<void>();
}

} // namespace yams::daemon
