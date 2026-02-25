#include <yams/daemon/client/in_process_transport.h>

#include <chrono>
#include <future>
#include <memory>
#include <optional>
#include <utility>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
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

boost::asio::awaitable<Result<Response>> InProcessTransport::send_request(const Request& req) {
    Request copy = req;
    co_return co_await send_request(std::move(copy));
}

boost::asio::awaitable<Result<Response>> InProcessTransport::send_request(Request&& req) {
    if (!host_) {
        co_return Error{ErrorCode::NotInitialized, "Embedded service host unavailable"};
    }

    auto* dispatcher = host_->getDispatcher();
    if (!dispatcher) {
        co_return Error{ErrorCode::NotInitialized, "Embedded request dispatcher unavailable"};
    }

    auto promise = std::make_shared<std::promise<Result<Response>>>();
    auto future = promise->get_future();

    boost::asio::co_spawn(
        host_->getExecutor(),
        [dispatcher, request = std::move(req), promise]() mutable -> boost::asio::awaitable<void> {
            try {
                auto response = co_await dispatcher->dispatch(request);
                promise->set_value(std::move(response));
            } catch (const std::exception& e) {
                promise->set_value(Error{ErrorCode::InternalError, e.what()});
            } catch (...) {
                promise->set_value(
                    Error{ErrorCode::InternalError, "In-process request dispatch failed"});
            }
            co_return;
        },
        boost::asio::detached);

    using namespace std::chrono_literals;
    boost::asio::steady_timer timer(co_await boost::asio::this_coro::executor);
    while (future.wait_for(0ms) != std::future_status::ready) {
        timer.expires_after(5ms);
        co_await timer.async_wait(boost::asio::use_awaitable);
    }

    co_return future.get();
}

boost::asio::awaitable<Result<void>>
InProcessTransport::send_request_streaming(const Request& req, HeaderCallback onHeader,
                                           ChunkCallback onChunk, ErrorCallback onError,
                                           CompleteCallback onComplete) {
    if (!host_) {
        co_return Error{ErrorCode::NotInitialized, "Embedded service host unavailable"};
    }

    auto* dispatcher = host_->getDispatcher();
    if (!dispatcher) {
        co_return Error{ErrorCode::NotInitialized, "Embedded request dispatcher unavailable"};
    }

    auto promise = std::make_shared<std::promise<Result<void>>>();
    auto future = promise->get_future();

    Request request = req;
    boost::asio::co_spawn(
        host_->getExecutor(),
        [dispatcher, request = std::move(request), onHeader = std::move(onHeader),
         onChunk = std::move(onChunk), onError = std::move(onError),
         onComplete = std::move(onComplete), promise]() mutable -> boost::asio::awaitable<void> {
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
                    promise->set_value(Result<void>());
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
                promise->set_value(Result<void>());
            } catch (const std::exception& e) {
                Error err{ErrorCode::InternalError, e.what()};
                if (onError) {
                    onError(err);
                }
                promise->set_value(err);
            } catch (...) {
                Error err{ErrorCode::InternalError, "In-process streaming dispatch failed"};
                if (onError) {
                    onError(err);
                }
                promise->set_value(err);
            }
            co_return;
        },
        boost::asio::detached);

    using namespace std::chrono_literals;
    boost::asio::steady_timer timer(co_await boost::asio::this_coro::executor);
    while (future.wait_for(0ms) != std::future_status::ready) {
        timer.expires_after(5ms);
        co_await timer.async_wait(boost::asio::use_awaitable);
    }

    auto result = future.get();
    if (!result) {
        co_return result.error();
    }

    co_return Result<void>();
}

} // namespace yams::daemon
