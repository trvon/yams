#include <yams/daemon/components/RequestDispatcher.h>
#include <yams/daemon/ipc/connection_fsm.h>
#include <yams/daemon/ipc/fsm_helpers.h>
#include <yams/daemon/ipc/fsm_metrics_registry.h>
#include <yams/daemon/ipc/latency_registry.h>
#include <yams/daemon/ipc/mux_metrics_registry.h>
#include <yams/daemon/ipc/proto_serializer.h>
#include <yams/daemon/ipc/request_context_registry.h>
#include <yams/daemon/ipc/request_handler.h>
#include <yams/daemon/ipc/stream_metrics_registry.h>
#include <yams/daemon/ipc/streaming_processor.h>
// Server tuning knobs
#include <yams/daemon/components/TuneAdvisor.h>

#include <spdlog/spdlog.h>
#include <array>
#include <chrono>
#include <span>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/experimental/awaitable_operators.hpp>
#include <boost/asio/local/stream_protocol.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/write.hpp>
#include <yams/compat/thread_stop_compat.h>
#if defined(TRACY_ENABLE)
#include <tracy/Tracy.hpp>
#endif

namespace yams::daemon {

namespace {
// Adapter class to convert RequestDispatcher to RequestProcessor interface
// Must be defined outside of constructor to avoid lifetime issues
class DispatcherAdapter : public RequestProcessor {
public:
    explicit DispatcherAdapter(RequestDispatcher* d) : dispatcher_(d) {}

    boost::asio::awaitable<Response> process(const Request& request) override {
        co_return co_await dispatcher_->dispatch(request);
    }

    boost::asio::awaitable<std::optional<Response>>
    process_streaming(const Request& request) override {
        // Return the full response - StreamingRequestProcessor will handle chunking
        co_return co_await dispatcher_->dispatch(request);
    }

    bool supports_streaming(const Request& request) const override {
        // Only enable streaming for request types that benefit from progressive output.
        // Explicitly disable streaming for chunked transfer control messages and simple unary ops.
        if (std::holds_alternative<SearchRequest>(request) ||
            std::holds_alternative<ListRequest>(request) ||
            std::holds_alternative<GrepRequest>(request) ||
            std::holds_alternative<AddDocumentRequest>(request) ||
            std::holds_alternative<BatchEmbeddingRequest>(request) ||
            std::holds_alternative<EmbedDocumentsRequest>(request) ||
            std::holds_alternative<GenerateEmbeddingRequest>(request) ||
            std::holds_alternative<LoadModelRequest>(request)) {
            return true;
        }
        // Never stream chunked get control messages
        if (std::holds_alternative<GetInitRequest>(request) ||
            std::holds_alternative<GetChunkRequest>(request) ||
            std::holds_alternative<GetEndRequest>(request)) {
            return false;
        }
        return false;
    }

    boost::asio::awaitable<ResponseChunk> next_chunk() override {
        // Not used when wrapped by StreamingRequestProcessor
        co_return ResponseChunk{};
    }

private:
    RequestDispatcher* dispatcher_;
};
} // anonymous namespace

// ============================================================================
// RequestHandler Implementation
// ============================================================================

RequestHandler::RequestHandler(std::shared_ptr<RequestProcessor> processor, Config config)
    : processor_(std::move(processor)), config_(std::move(config)) {}

RequestHandler::RequestHandler(RequestDispatcher* dispatcher, Config config)
    : dispatcher_(dispatcher), config_(std::move(config)) {
    // Centralized server knobs via TuneAdvisor
    config_.max_inflight_per_connection = TuneAdvisor::serverMaxInflightPerConn();
    config_.per_request_queue_cap = TuneAdvisor::serverQueueFramesCap();
    config_.total_queued_bytes_cap = TuneAdvisor::serverQueueBytesCap();
    config_.writer_budget_bytes_per_turn = TuneAdvisor::serverWriterBudgetBytesPerTurn();
    // Create a processor adapter that wraps the dispatcher
    if (dispatcher_) {
        auto adapter = std::make_shared<DispatcherAdapter>(dispatcher_);

        // Always wrap with streaming support
        spdlog::debug("RequestHandler: Wrapping processor with StreamingRequestProcessor");
        processor_ = std::make_shared<StreamingRequestProcessor>(adapter, config_);
    }
}

RequestHandler::~RequestHandler() {}

boost::asio::awaitable<std::vector<uint8_t>>
RequestHandler::handle_request(const std::vector<uint8_t>& request_data,
                               yams::compat::stop_token token) {
    (void)token; // unused
    using boost::asio::use_awaitable;
    try {
        // Parse the message from raw data
        auto message_result = ProtoSerializer::decode_payload(request_data);
        if (!message_result) {
            // Create error response
            ErrorResponse error;
            error.code = ErrorCode::InvalidArgument;
            error.message = "Failed to deserialize request: " + message_result.error().message;
            Response response(error);
            Message errorMsg;
            errorMsg.payload = response;
            auto encoded = ProtoSerializer::encode_payload(errorMsg);
            if (!encoded) {
                co_return std::vector<uint8_t>{}; // Return empty on encoding failure
            }
            co_return encoded.value();
        }

        const Message& message = message_result.value();

        // Extract the request from the message payload
        if (!std::holds_alternative<Request>(message.payload)) {
            ErrorResponse error;
            error.code = ErrorCode::InvalidArgument;
            error.message = "Message payload is not a Request";
            Response response(error);
            Message errorMsg;
            errorMsg.payload = response;
            auto encoded = ProtoSerializer::encode_payload(errorMsg);
            if (!encoded) {
                co_return std::vector<uint8_t>{};
            }
            co_return encoded.value();
        }

        const Request& request = std::get<Request>(message.payload);

        // Process the request
        Response response = co_await process_request(request);

        // Create response message
        Message responseMsg;
        responseMsg.requestId = message.requestId;
        responseMsg.payload = response;

        // Serialize and return the response
        auto encoded = ProtoSerializer::encode_payload(responseMsg);
        if (!encoded) {
            // Fallback error response
            ErrorResponse error;
            error.code = ErrorCode::InternalError;
            error.message = "Failed to encode response";
            Message errorMsg;
            errorMsg.requestId = message.requestId;
            errorMsg.payload = Response(error);
            auto errorEncoded = ProtoSerializer::encode_payload(errorMsg);
            co_return errorEncoded ? errorEncoded.value() : std::vector<uint8_t>{};
        }
        co_return encoded.value();

    } catch (const std::exception& e) {
        ErrorResponse error;
        error.code = ErrorCode::InternalError;
        error.message = std::string("Exception during request processing: ") + e.what();
        Message errorMsg;
        errorMsg.payload = Response(error);
        auto encoded = ProtoSerializer::encode_payload(errorMsg);
        co_return encoded ? encoded.value() : std::vector<uint8_t>{};
    }
}

// Note: Only the compat::stop_token variant is provided. On platforms where
// std::stop_token exists, yams::compat::stop_token aliases it, avoiding the
// need for an overload that would collide at the type level.

boost::asio::awaitable<void>
RequestHandler::handle_connection(boost::asio::local::stream_protocol::socket socket,
                                  yams::compat::stop_token token) {
    using boost::asio::use_awaitable;
    try {
#if defined(TRACY_ENABLE)
        // Treat each connection as a fiber for better cross-await stack attribution
        YAMS_FIBER_ENTER("ipc_conn");
        struct FiberGuard {
            ~FiberGuard() { YAMS_FIBER_LEAVE(); }
        } _fg;
#endif
        spdlog::debug("RequestHandler::handle_connection coroutine started");
        spdlog::debug("New connection established");
        // Wrap socket in shared_ptr so per-request coroutines can safely reference it
        using socket_t = boost::asio::local::stream_protocol::socket;
        auto sock = std::make_shared<socket_t>(std::move(socket));
        // Initialize per-connection FSM (adapter kept internal to source file)
        ConnectionFsm fsm;
        fsm.enable_metrics(true);
        fsm.on_connect(sock->native_handle());
        // Prepare write serialization for multiplexing
        if (config_.enable_multiplexing) {
            // Serialize all writes through a per-connection strand (sole writer guard)
            write_strand_exec_.emplace(boost::asio::make_strand(sock->get_executor()));
            inflight_.store(0, std::memory_order_relaxed);
        } else {
            write_strand_exec_.reset();
            inflight_.store(0, std::memory_order_relaxed);
        }
        // Note: downstream clients may close early (e.g., pager exits). Treat write-side resets
        // as non-fatal for the daemon; logs will be at debug level when detected.

        // Native boost::asio sockets don't have set_timeout, timeouts are handled per-operation
        // Use protocol maximum message size for inbound frame reader
        FrameReader reader(MAX_MESSAGE_SIZE);
        bool should_exit = false;

        uint64_t fsm_guard_fail_count = 0;
        // Track consecutive idle read timeouts to prevent leaking idle connections forever
        std::uint32_t consecutive_idle_timeouts = 0;
        constexpr std::uint32_t kMaxIdleTimeouts = 120; // ~4 minutes if read_timeout ≈ 2s
        while (!token.stop_requested() && sock->is_open()) {
            // Pause reads when backpressured to avoid amplifying write pressure
            if (fsm.backpressured()) {
                using namespace boost::asio;
                steady_timer bp_timer(co_await this_coro::executor);
                bp_timer.expires_after(
                    std::chrono::milliseconds(TuneAdvisor::backpressureReadPauseMs()));
                co_await bp_timer.async_wait(use_awaitable);
                continue;
            }
            // Read as much as is available (up to 4096), do not block for exact size
            std::array<uint8_t, 4096> buf{};
            spdlog::debug("About to co_await socket.async_read");
            // FSM guard: ensure reads are allowed before attempting to read from the socket
            bool can_read = true;
            std::string guard_err;
            try {
                fsm_helpers::require_can_read(fsm, "handle_connection:before_async_read");
            } catch (const std::exception& ex) {
                can_read = false;
                guard_err = ex.what();
            }
            if (!can_read) {
                // FSM not ready to read (e.g., mid-response). If FSM is already in Error/Closed
                // state, close immediately to allow recovery instead of spinning.
                const auto st = fsm.state();
                if (st == ConnectionFsm::State::Error || st == ConnectionFsm::State::Closed) {
                    spdlog::warn("Closing connection immediately due to FSM={} (context={}): {}",
                                 ConnectionFsm::to_string(st), "before_async_read", guard_err);
                    boost::system::error_code ignore_ec;
                    sock->close(ignore_ec);
                    break;
                }

                // Otherwise, progressively back off. Warn sparsely with state included.
                fsm_guard_fail_count++;
                if (fsm_guard_fail_count % 2000 == 1) {
                    spdlog::warn(
                        "FSM not ready for read — backing off (occurrences={} state={}): {}",
                        fsm_guard_fail_count, ConnectionFsm::to_string(st), guard_err);
                } else {
                    spdlog::debug("FSM not ready for read (state={}): {}",
                                  ConnectionFsm::to_string(st), guard_err);
                }

                using namespace boost::asio;
                steady_timer wait(co_await this_coro::executor);
                uint32_t base = TuneAdvisor::backpressureReadPauseMs();
                uint32_t mult = (fsm_guard_fail_count > 8000)   ? 16
                                : (fsm_guard_fail_count > 4000) ? 8
                                : (fsm_guard_fail_count > 2000) ? 4
                                : (fsm_guard_fail_count > 500)  ? 2
                                                                : 1;
                uint32_t delay_ms = std::min<uint32_t>(base * mult, 1500);
                wait.expires_after(std::chrono::milliseconds(delay_ms));
                co_await wait.async_wait(use_awaitable);

                // Close sooner under prolonged non-Error unreadable states to avoid resource burn.
                if (fsm_guard_fail_count >= 10000 || st == ConnectionFsm::State::Closing) {
                    spdlog::warn("Closing connection after prolonged unreadable FSM state "
                                 "(count={}, state={})",
                                 fsm_guard_fail_count, ConnectionFsm::to_string(st));
                    boost::system::error_code ignore_ec;
                    sock->close(ignore_ec);
                    break;
                }
                continue;
            }

            using namespace boost::asio::experimental::awaitable_operators;
            // Inactivity-based timeout for reads: reset timer every awaited read
            boost::asio::steady_timer timer(co_await boost::asio::this_coro::executor);
            timer.expires_after(config_.read_timeout);

            // Race the read against the timer
            auto read_or_timeout =
                co_await (boost::asio::async_read(*sock, boost::asio::buffer(buf),
                                                  boost::asio::transfer_at_least(1),
                                                  boost::asio::use_awaitable) ||
                          timer.async_wait(boost::asio::use_awaitable));

            size_t bytes_read = 0;
            if (read_or_timeout.index() == 1) {
                // Idle persistent connection; keep connection open and continue.
                // IMPORTANT: Do not signal FSM on idle timeouts. The FSM's on_timeout()
                // is reserved for operation deadlines (header/payload/write). Calling it
                // while in Connected (idle) can spuriously transition to Error.
                spdlog::debug(
                    "Read timeout (persistent) on socket {} after {} ms — keeping connection open",
                    sock->native_handle(),
                    std::chrono::duration_cast<std::chrono::milliseconds>(config_.read_timeout)
                        .count());
                // Bound idle lifetime to avoid FD/backlog exhaustion if clients vanish silently
                if (++consecutive_idle_timeouts >= kMaxIdleTimeouts) {
                    spdlog::info(
                        "Closing idle connection after {} consecutive read timeouts (fd={})",
                        consecutive_idle_timeouts, sock->native_handle());
                    boost::system::error_code ignore_ec;
                    sock->close(ignore_ec);
                    break;
                }
                continue;
            } else {
                // Header/payload bytes became available
                bytes_read = std::get<0>(read_or_timeout);
                spdlog::debug("socket.async_read returned {} bytes", bytes_read);
                consecutive_idle_timeouts = 0; // traffic observed, reset idle counter
                if (bytes_read == 0) {
                    spdlog::debug("Closing connection: peer sent EOF");
                    fsm.on_close_request();
                    break;
                }
            }

            if (bytes_read == 0) {
                // Defensive EOF handling
                spdlog::debug("Closing connection: peer sent EOF (0 bytes)");
                fsm.on_close_request();
                break;
            }

            // Drive FSM readable event only if the FSM is still alive
            if (fsm.alive()) {
                fsm.on_readable(static_cast<size_t>(bytes_read));
            } else {
                spdlog::debug("Closing connection: FSM not alive during read");
                boost::system::error_code ignore_ec;
                sock->close(ignore_ec);
                break;
            }

            // Feed data to frame reader
            spdlog::debug("Feeding {} bytes to frame reader", bytes_read);
            auto feed_result = reader.feed(buf.data(), bytes_read);
            stats_.bytes_received += feed_result.consumed;
            spdlog::debug("Frame reader consumed {} bytes, status={}", feed_result.consumed,
                          static_cast<int>(feed_result.status));

            if (feed_result.status == FrameReader::FrameStatus::InvalidFrame) {
                spdlog::error("Invalid frame received");
                // Fatal framing error: close to avoid poisoning persistent streams
                (void)co_await send_error(*sock, ErrorCode::InvalidArgument,
                                          "Invalid frame format");
                boost::system::error_code ignore_ec;
                sock->close(ignore_ec);
                break;
            }

            if (feed_result.status == FrameReader::FrameStatus::FrameTooLarge) {
                spdlog::error("Frame too large");
                (void)co_await send_error(*sock, ErrorCode::InvalidArgument,
                                          "Frame exceeds maximum size");
                boost::system::error_code ignore_ec;
                sock->close(ignore_ec);
                break;
            }

            // Process complete frames
            while (reader.has_frame()) {
                auto frame_result = reader.get_frame();
                if (!frame_result) {
                    spdlog::error("Closing connection: failed to get frame: {}",
                                  frame_result.error().message);
                    (void)co_await send_error(*sock, ErrorCode::SerializationError,
                                              "Failed to get frame");
                    boost::system::error_code ignore_ec;
                    sock->close(ignore_ec);
                    should_exit = true;
                    break;
                }

                // Parse the frame
                auto message_result = framer_.parse_frame(frame_result.value());
                if (!message_result) {
                    spdlog::error("Closing connection: failed to parse frame: {}",
                                  message_result.error().message);
                    // Map parse/serialization errors to ErrorResponse at server boundary.
                    // Try to recover request_id from header for correlation; if unavailable, use 0.
                    uint64_t correlated_id = 0;
                    if (frame_result &&
                        frame_result.value().size() >= sizeof(MessageFramer::FrameHeader)) {
                        MessageFramer::FrameHeader hdr{};
                        std::memcpy(&hdr, frame_result.value().data(), sizeof(hdr));
                        hdr.from_network();
                        // Only accept valid header for correlation
                        if (hdr.is_valid()) {
                            // request_id lives in payload; we cannot extract it without a
                            // successful decode. Keep 0 here but still send a shaped ErrorResponse
                            // so clients don't hang.
                            correlated_id = 0;
                        }
                    }
                    (void)co_await send_error(*sock, message_result.error().code,
                                              message_result.error().message, correlated_id);
                    boost::system::error_code ignore_ec;
                    sock->close(ignore_ec);
                    should_exit = true;
                    break;
                }

                // Extract request from message
                const auto& message = message_result.value();
                auto* request_ptr = std::get_if<Request>(&message.payload);
                if (!request_ptr) {
                    spdlog::error("Closing connection: received non-request message");
                    // Correlate error with the observed message requestId and close
                    (void)co_await send_error(*sock, ErrorCode::InvalidArgument,
                                              "Expected request message", message.requestId);
                    boost::system::error_code ignore_ec;
                    sock->close(ignore_ec);
                    should_exit = true;
                    break;
                }

                // Inform FSM that a valid request header/body has been parsed
                if (fsm.alive()) {
                    ConnectionFsm::FrameInfo finfo{};
                    finfo.payload_size = 0; // unknown at this level
                    fsm.on_header_parsed(finfo);
                    fsm.on_body_parsed();
                }

                // Handle the request with correlation id
                // Route through streaming-aware path so FSM transitions are captured
                spdlog::debug("RequestHandler::handle_connection: Routing request_id={} with "
                              "expectsStreamingResponse={}",
                              message.requestId, message.expectsStreamingResponse);
                if (config_.enable_multiplexing) {
                    auto cur = inflight_.load(std::memory_order_relaxed);
                    if (cur >= config_.max_inflight_per_connection) {
                        (void)co_await send_error(*sock, ErrorCode::ResourceExhausted,
                                                  "Too many in-flight requests", message.requestId);
                    } else {
                        inflight_.fetch_add(1, std::memory_order_relaxed);
                        // Create per-request context
                        {
                            std::lock_guard<std::mutex> lk(ctx_mtx_);
                            auto ctx = std::make_shared<RequestContext>();
                            ctx->start = std::chrono::steady_clock::now();
                            contexts_[message.requestId] = ctx;
                            RequestContextRegistry::instance().register_context(message.requestId,
                                                                                ctx);
                        }
                        MuxMetricsRegistry::instance().incrementActiveHandlers(1);
                        boost::asio::co_spawn(
                            sock->get_executor(),
                            [this, sock, req = *request_ptr, req_id = message.requestId,
                             expects = message.expectsStreamingResponse]()
                                -> boost::asio::awaitable<void> {
                                auto r = co_await handle_streaming_request(*sock, req, req_id,
                                                                           nullptr, expects);
                                if (!r) {
                                    spdlog::debug("Multiplexed request failed (requestId={}): {}",
                                                  req_id, r.error().message);
                                }
                                inflight_.fetch_sub(1, std::memory_order_relaxed);
                                MuxMetricsRegistry::instance().incrementActiveHandlers(-1);
                                // Mark context completed and erase
                                {
                                    std::lock_guard<std::mutex> lk(ctx_mtx_);
                                    auto it = contexts_.find(req_id);
                                    if (it != contexts_.end()) {
                                        it->second->completed.store(true,
                                                                    std::memory_order_relaxed);
                                        contexts_.erase(it);
                                    }
                                }
                                RequestContextRegistry::instance().deregister_context(req_id);
                                co_return;
                            },
                            boost::asio::detached);
                        if (fsm.alive()) {
                            fsm.on_response_complete(false);
                        }
                    }
                } else {
                    auto handle_result =
                        co_await handle_streaming_request(*sock, *request_ptr, message.requestId,
                                                          &fsm, message.expectsStreamingResponse);
                    if (!handle_result) {
                        // Downgrade common client-initiated close errors to debug to avoid noisy
                        // logs
                        const auto& msg = handle_result.error().message;
                        if (msg.find("Connection reset by peer") != std::string::npos ||
                            msg.find("Broken pipe") != std::string::npos ||
                            msg.find("EPIPE") != std::string::npos ||
                            msg.find("ECONNRESET") != std::string::npos) {
                            spdlog::debug("Request handling ended by client: {}", msg);
                        } else {
                            spdlog::error("Request handling failed: {}", msg);
                        }
                        should_exit = true;
                        break;
                    }
                }

                // Defensive: ensure FSM is ready to read the next request on persistent
                // connections. If the processor didn't drive on_response_complete() for any
                // reason, normalize state back to Connected.
                if (!config_.close_after_response) {
                    auto s = fsm.state();
                    if (s != ConnectionFsm::State::Connected &&
                        s != ConnectionFsm::State::ReadingHeader) {
                        spdlog::debug(
                            "Post-response FSM state was {} — forcing Connected for persistence",
                            ConnectionFsm::to_string(s));
                        if (fsm.alive()) {
                            fsm.on_response_complete(false);
                        }
                    }
                }
            }
            if (should_exit)
                break;
        }

        // Close out FSM lifecycle
        if (fsm.alive()) {
            fsm.on_close_request();
        }
        spdlog::debug("Connection handler exiting normally");
    } catch (const std::exception& e) {
        spdlog::error("RequestHandler::handle_connection unhandled exception: {}", e.what());
    } catch (...) {
        spdlog::error("RequestHandler::handle_connection unhandled unknown exception");
    }
}

boost::asio::awaitable<Result<void>>
RequestHandler::handle_request(boost::asio::local::stream_protocol::socket& socket,
                               const Request& request, uint64_t request_id) {
    // Always use streaming mode
    co_return co_await handle_streaming_request(socket, request, request_id);
}

boost::asio::awaitable<Result<Message>>
RequestHandler::read_message(boost::asio::local::stream_protocol::socket& socket,
                             FrameReader& reader) {
    using boost::asio::use_awaitable;
    // Read until we have a complete frame
    size_t read_loops = 0;
    while (!reader.has_frame()) {
        std::vector<uint8_t> buffer(4096);
        boost::system::error_code ec;

        size_t bytes_read = co_await boost::asio::async_read(
            socket, boost::asio::buffer(buffer, buffer.size()), boost::asio::transfer_at_least(1),
            boost::asio::redirect_error(boost::asio::use_awaitable, ec));

        if (ec) {
            if (ec == boost::asio::error::eof) {
                co_return Error{ErrorCode::NetworkError, "Connection closed"};
            }
            co_return Error{ErrorCode::NetworkError, ec.message()};
        }

        buffer.resize(bytes_read);
        if (buffer.empty()) {
            co_return Error{ErrorCode::NetworkError, "Connection closed"};
        }

        auto feed_result = reader.feed(buffer.data(), buffer.size());
        stats_.bytes_received += feed_result.consumed;
        FsmMetricsRegistry::instance().addBytesReceived(bytes_read);

        if (feed_result.status == FrameReader::FrameStatus::InvalidFrame) {
            co_return Error{ErrorCode::InvalidArgument, "Invalid frame"};
        }

        if (feed_result.status == FrameReader::FrameStatus::FrameTooLarge) {
            co_return Error{ErrorCode::InvalidArgument, "Frame too large"};
        }

        // Cooperative yield to allow other connections (e.g., status probes) to progress
        if (++read_loops % 8 == 0) {
            co_await boost::asio::post(boost::asio::use_awaitable);
        }
    }

    auto frame_result = reader.get_frame();
    if (!frame_result) {
        co_return frame_result.error();
    }

    auto parsed = framer_.parse_frame(frame_result.value());
    if (parsed) {
        FsmMetricsRegistry::instance().incrementHeaderReads(1);
    }
    co_return parsed;
}

boost::asio::awaitable<Result<void>>
RequestHandler::write_message(boost::asio::local::stream_protocol::socket& socket,
                              const Message& message) {
    using boost::asio::use_awaitable;

    // Check if socket is still open before attempting to write
    if (!socket.is_open()) {
        spdlog::debug("write_message: socket is not open, aborting write");
        co_return Error{ErrorCode::NetworkError, "Socket is closed"};
    }
    // Optional per-request streaming trace (env: YAMS_STREAM_TRACE)
    {
        static int trace = []() {
            if (const char* s = std::getenv("YAMS_STREAM_TRACE")) {
                std::string t(s);
                for (auto& c : t)
                    c = static_cast<char>(std::tolower(c));
                return (t == "1" || t == "true" || t == "on") ? 1 : 0;
            }
            return 0;
        }();
        if (trace) {
            const int mt =
                std::holds_alternative<Request>(message.payload)
                    ? static_cast<int>(getMessageType(std::get<Request>(message.payload)))
                    : static_cast<int>(getMessageType(std::get<Response>(message.payload)));
            spdlog::debug("stream-trace: write_message req_id={} type={} (pre-frame)",
                          message.requestId, mt);
        }
    }
    if (config_.enable_multiplexing) {
        // Frame entire message and enqueue for fair writer; treat as last
        auto framed = framer_.frame_message(message);
        if (!framed)
            co_return framed.error();
        if (write_strand_exec_)
            co_await boost::asio::dispatch(*write_strand_exec_, use_awaitable);
        auto enq = co_await enqueue_frame(message.requestId, std::move(framed.value()), true);
        if (!enq)
            co_return enq.error();
        if (!writer_running_) {
            writer_running_ = true;
            co_await writer_drain(socket);
        }
        co_return Result<void>();
    } else {
        // Frame once
        auto frame_result = framer_.frame_message(message);
        if (!frame_result) {
            co_return frame_result.error();
        }
        auto& frame = frame_result.value();

        // Check frame size doesn't exceed reasonable limits
        constexpr size_t MAX_FRAME_SIZE = 100 * 1024 * 1024; // 100MB
        if (frame.size() > MAX_FRAME_SIZE) {
            spdlog::error("write_message: frame size {} exceeds maximum {}", frame.size(),
                          MAX_FRAME_SIZE);
            co_return Error{ErrorCode::InvalidArgument, "Response too large to send"};
        }

        stats_.bytes_sent += frame.size();

        const auto msgType =
            std::holds_alternative<Request>(message.payload)
                ? static_cast<int>(getMessageType(std::get<Request>(message.payload)))
                : static_cast<int>(getMessageType(std::get<Response>(message.payload)));
        spdlog::debug(
            "write_message: framed message type={} request_id={} frame_size={} header_size={}",
            msgType, message.requestId, frame.size(), sizeof(MessageFramer::FrameHeader));

        // Always send the frame header immediately to unblock clients waiting for it
        if (frame.size() < sizeof(MessageFramer::FrameHeader)) {
            co_return Error{ErrorCode::SerializationError, "Framed message smaller than header"};
        }

        constexpr std::size_t headerSize = sizeof(MessageFramer::FrameHeader);

        // 1) Send header first
        {
            std::span<const uint8_t> header{frame.data(), headerSize};
            spdlog::debug("write_message: writing header {} bytes (request_id={})", header.size(),
                          message.requestId);
            boost::system::error_code ec;
            co_await boost::asio::async_write(
                socket, boost::asio::buffer(header),
                boost::asio::redirect_error(boost::asio::use_awaitable, ec));
            if (ec) {
                const auto& msg = ec.message();
                if (msg.find("Connection reset by peer") != std::string::npos ||
                    msg.find("Broken pipe") != std::string::npos ||
                    msg.find("EPIPE") != std::string::npos ||
                    msg.find("ECONNRESET") != std::string::npos) {
                    spdlog::debug("Client closed during header write: {}", msg);
                }
                co_return Error{ErrorCode::NetworkError, msg};
            }
            // FSM metrics: count payload writes and bytes sent (header)
            FsmMetricsRegistry::instance().incrementPayloadWrites(1);
            FsmMetricsRegistry::instance().addBytesSent(header.size());
            spdlog::debug("write_message: header write complete (request_id={})",
                          message.requestId);
        }

        // 2) Stream the payload in chunks; this allows the client to start reading immediately
        const std::size_t kChunk = std::max<std::size_t>(4096, config_.chunk_size);
        std::size_t offset = headerSize;
        std::size_t payload_written = 0;
        while (offset < frame.size()) {
            std::size_t to_write = std::min<std::size_t>(kChunk, frame.size() - offset);
            std::span<const uint8_t> chunk{frame.data() + offset, to_write};
            spdlog::debug("write_message: writing payload chunk {} bytes (request_id={})", to_write,
                          message.requestId);
            boost::system::error_code ec;
            co_await boost::asio::async_write(
                socket, boost::asio::buffer(chunk),
                boost::asio::redirect_error(boost::asio::use_awaitable, ec));
            if (ec) {
                const auto& msg = ec.message();
                if (msg.find("Connection reset by peer") != std::string::npos ||
                    msg.find("Broken pipe") != std::string::npos ||
                    msg.find("EPIPE") != std::string::npos ||
                    msg.find("ECONNRESET") != std::string::npos) {
                    spdlog::debug("Client closed during payload write: {}", msg);
                }
                co_return Error{ErrorCode::NetworkError, msg};
            }
            payload_written += to_write;
            FsmMetricsRegistry::instance().incrementPayloadWrites(1);
            FsmMetricsRegistry::instance().addBytesSent(to_write);
            if (to_write == 0) {
                using namespace std::chrono_literals;
                std::this_thread::sleep_for(1ms);
            } else if (config_.chunk_flush_delay_ms > 0) {
                using namespace std::chrono_literals;
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(config_.chunk_flush_delay_ms));
            }

            offset += to_write;
        }

        spdlog::debug("write_message: payload write complete ({} bytes) request_id={}",
                      payload_written, message.requestId);

        co_return Result<void>();
    }
}

boost::asio::awaitable<Response> RequestHandler::process_request(const Request& request) {
    if (processor_) {
        co_return co_await processor_->process(request);
    }

    co_return ErrorResponse{ErrorCode::NotImplemented, "Request processor not set"};
}

boost::asio::awaitable<std::optional<Response>>
RequestHandler::process_streaming_request(const Request& request) {
    if (!processor_) {
        co_return ErrorResponse{ErrorCode::NotImplemented, "Request processor not set"};
    }

    // Check if we should use streaming for this request: initialize via processor
    if (can_stream_request(request)) {
        // Initialize streaming; processor may return std::nullopt to indicate chunked mode
        co_return co_await processor_->process_streaming(request);
    }

    // Fall back to regular processing (unary)
    if (processor_) {
        auto r = co_await processor_->process(request);
        co_return std::optional<Response>{std::move(r)};
    }
    co_return std::optional<Response>{
        ErrorResponse{ErrorCode::NotImplemented, "Request processor not set"}};
}

bool RequestHandler::can_stream_request(const Request& request) const {
    if (!processor_) {
        return false;
    }

    // Check if streaming is enabled in config
    if (!config_.enable_streaming) {
        return false;
    }

    // Check if the processor supports streaming for this request type
    return processor_->supports_streaming(request);
}

bool RequestHandler::should_stream_request(const Request& request) const {
    // If force streaming is enabled, always stream
    if (config_.force_streaming) {
        return true;
    }

    // If streaming is disabled, never stream
    if (!config_.enable_streaming) {
        return false;
    }

    // Auto-detect if the request type benefits from streaming
    if (config_.auto_detect_streaming) {
        // These request types typically benefit from streaming
        if (std::holds_alternative<SearchRequest>(request) ||
            std::holds_alternative<ListRequest>(request) ||
            std::holds_alternative<GrepRequest>(request) ||
            // Auto-stream embeddings so clients without explicit streaming still get progress
            std::holds_alternative<BatchEmbeddingRequest>(request) ||
            std::holds_alternative<EmbedDocumentsRequest>(request)) {
            return true;
        }
    }

    return false;
}

boost::asio::awaitable<std::vector<Response>>
RequestHandler::collect_limited_chunks(const Request& request, size_t max_chunks) {
    (void)request;
    std::vector<Response> chunks;
    if (!processor_) {
        co_return chunks;
    }

    // Collect chunks until we reach the limit or get the last chunk
    size_t chunk_count = 0;
    bool last_chunk_received = false;

    while (chunk_count < max_chunks && !last_chunk_received) {
        auto chunk_result = co_await processor_->next_chunk();
        chunks.push_back(chunk_result.data);
        last_chunk_received = chunk_result.is_last_chunk;
        chunk_count++;

        if (last_chunk_received) {
            break;
        }
    }

    co_return chunks;
}

boost::asio::awaitable<Result<void>>
RequestHandler::handle_streaming_request(boost::asio::local::stream_protocol::socket& socket,
                                         const Request& request, uint64_t request_id,
                                         ConnectionFsm* fsm, bool client_expects_streaming) {
    using boost::asio::use_awaitable;
    auto start_time = std::chrono::steady_clock::now();

    try {
        // Check socket is open before processing
        if (!socket.is_open()) {
            spdlog::debug("handle_streaming_request: socket closed before processing request_id={}",
                          request_id);
            co_return Error{ErrorCode::NetworkError, "Socket closed"};
        }
        // Use the configured processor (server may decorate with streaming support)
        std::shared_ptr<RequestProcessor> proc = processor_;
        spdlog::debug("handle_streaming_request: processor type={} for request_id={}",
                      proc ? typeid(RequestProcessor).name() : "null", request_id);

        // Process the request. Respect the client's streaming hint strictly:
        // - If the client does NOT expect streaming, always take the unary path so a single
        //   complete frame is returned (prevents hanging unary clients on header-only frames).
        // - Certain control requests should always be unary regardless of client hint
        //   (Status, Ping, Shutdown, GetStats) to guarantee immediate completion.
        // - If the client expects streaming and the request can benefit from it, prefer streaming
        //   unless readiness forces unary.
        spdlog::debug("handle_streaming_request: preparing processing path (request_id={}, "
                      "client_expects_streaming={}, request_type={})",
                      request_id, client_expects_streaming, request.index());
        std::optional<Response> response_opt;

        // Preflight readiness gate: if the client expects streaming but core services are not
        // ready, prefer a lightweight streaming stub to avoid invoking heavy services during
        // startup; if stubbing is disabled, fall back to unary.
        bool force_unary = false;
        bool prefer_stub_stream = false;
        if (client_expects_streaming) {
            try {
                StatusRequest s;
                s.detailed = true;
                auto sr = co_await proc->process(Request{s});
                if (auto* st = std::get_if<StatusResponse>(&sr)) {
                    auto it_cs = st->readinessStates.find("content_store");
                    auto it_db = st->readinessStates.find("metadata_repo");
                    bool csReady = (it_cs != st->readinessStates.end() && it_cs->second);
                    bool dbReady = (it_db != st->readinessStates.end() && it_db->second);
                    // If core services are not ready, choose stub streaming when allowed
                    if (!csReady || !dbReady) {
                        prefer_stub_stream = config_.stream_stub_on_init;
                        force_unary = !prefer_stub_stream;
                    }
                    // If the search engine is not ready, also prefer stub streaming
                    auto it_se = st->readinessStates.find("search_engine");
                    bool seReady = (it_se != st->readinessStates.end() && it_se->second);
                    if (!seReady) {
                        prefer_stub_stream = config_.stream_stub_on_init;
                    }
                }
            } catch (...) {
                // On any error, do not stream to avoid half-open streams
                prefer_stub_stream = config_.stream_stub_on_init;
                force_unary = !prefer_stub_stream;
            }
        }

        // Always-unary requests (control/health) regardless of client hint
        const bool always_unary = std::holds_alternative<ShutdownRequest>(request) ||
                                  std::holds_alternative<PingRequest>(request) ||
                                  std::holds_alternative<StatusRequest>(request) ||
                                  std::holds_alternative<GetStatsRequest>(request) ||
                                  std::holds_alternative<PrepareSessionRequest>(request);

        if (!client_expects_streaming || force_unary || always_unary) {
            spdlog::debug("handle_streaming_request: non-streaming path selected (request_id={} "
                          "expects_streaming={} force_unary={} always_unary={})",
                          request_id, client_expects_streaming, force_unary, always_unary);
            response_opt = co_await proc->process(request);
        } else {
            // Default behavior: attempt streaming (processor may return std::nullopt to indicate
            // chunked mode) so header is emitted early and progress can be delivered.
            spdlog::debug("handle_streaming_request: calling process_streaming for request_id={}, "
                          "client_expects_streaming={}, request_type={}",
                          request_id, client_expects_streaming, request.index());
            response_opt = co_await proc->process_streaming(request);
        }

        spdlog::debug(
            "handle_streaming_request: process_streaming returned has_value={} for request_id={}",
            response_opt.has_value(), request_id);
        if (!response_opt.has_value()) {
            // No response means we should use the streaming interface
            // Force streaming for stream-capable requests to guarantee header-first behavior
            bool can_stream = can_stream_request(request);
            spdlog::debug(
                "handle_streaming_request: response_opt is empty, can_stream={} (request_id={})",
                can_stream, request_id);
            if (proc && can_stream) {
                spdlog::debug("handle_streaming_request: entering streaming mode (request_id={}, "
                              "client_expects={})",
                              request_id, client_expects_streaming);
                // Inform FSM we are transitioning to write header for streaming
                if (fsm) {
                    ConnectionFsm::FrameInfo finfo{};
                    finfo.payload_size = 0; // header-only
                    fsm->on_header_parsed(finfo);
                }
                // Validate FSM can transition to writing before streaming
                if (fsm) {
                    try {
                        fsm_helpers::require_can_write(*fsm,
                                                       "handle_streaming_request:enter_streaming");
                    } catch (const std::exception& ex) {
                        spdlog::error("FSM write guard failed before streaming (request_id={}): {}",
                                      request_id, ex.what());
                        co_return Error{ErrorCode::InternalError,
                                        std::string{"Invalid state for streaming: "} + ex.what()};
                    }
                }
                // If we prefer a stub stream (early startup), avoid invoking the processor and
                // emit a header + empty final chunk for streaming-capable types.
                Result<void> stream_result;
                if (prefer_stub_stream && (std::holds_alternative<SearchRequest>(request) ||
                                           std::holds_alternative<ListRequest>(request) ||
                                           std::holds_alternative<GrepRequest>(request))) {
                    stream_result = co_await stream_chunks_stub(socket, request, request_id, fsm);
                } else {
                    stream_result = co_await stream_chunks(socket, request, request_id, proc, fsm);
                }

                auto duration = std::chrono::steady_clock::now() - start_time;
                stats_.requests_processed++;
                stats_.total_processing_time += duration;

                if (duration < stats_.min_latency) {
                    stats_.min_latency = duration;
                }
                if (duration > stats_.max_latency) {
                    stats_.max_latency = duration;
                }

                co_return stream_result;
            }

            // If we got here, streaming failed but we reported no error
            auto duration = std::chrono::steady_clock::now() - start_time;
            stats_.requests_processed++;
            stats_.total_processing_time += duration;

            if (duration < stats_.min_latency) {
                stats_.min_latency = duration;
            }
            if (duration > stats_.max_latency) {
                stats_.max_latency = duration;
            }

            co_return Result<void>();
        }

        // We have a complete response. Honor the client's streaming expectation:
        // - If the client does NOT expect streaming or this is a control/health request,
        //   emit a single non-chunked frame (classic unary semantics).
        // - Otherwise, write header + immediate final chunk (one-shot streaming) so clients
        //   consuming streaming APIs can start reading as soon as the header is available.
        auto& response = response_opt.value();
        const bool is_control = std::holds_alternative<ShutdownRequest>(request) ||
                                std::holds_alternative<PingRequest>(request) ||
                                std::holds_alternative<StatusRequest>(request) ||
                                std::holds_alternative<GetStatsRequest>(request) ||
                                std::holds_alternative<PrepareSessionRequest>(request);
        if (!client_expects_streaming || is_control) {
            spdlog::debug("handle_streaming_request: writing classic unary (request_id={} type={} "
                          "expects_streaming={} is_control={})",
                          request_id, static_cast<int>(getMessageType(response)),
                          client_expects_streaming, is_control);
            // Frame a complete non-chunked message and send with write_message helper
            Message response_msg;
            response_msg.version = PROTOCOL_VERSION;
            response_msg.requestId = request_id;
            response_msg.timestamp = std::chrono::steady_clock::now();
            response_msg.payload = response;
            if (fsm) {
                try {
                    fsm_helpers::require_can_write(*fsm,
                                                   "handle_streaming_request:write_complete_unary");
                } catch (const std::exception& ex) {
                    co_return Error{ErrorCode::InternalError,
                                    std::string{"Invalid state before complete unary write: "} +
                                        ex.what()};
                }
            }
            auto write_result = co_await write_message(socket, response_msg);
            if (!write_result) {
                co_return write_result.error();
            }
        } else {
            spdlog::debug(
                "handle_streaming_request: one-shot streaming response type={} (request_id={})",
                static_cast<int>(getMessageType(response)), request_id);
            // Write header frame (no payload)
            if (fsm) {
                try {
                    fsm_helpers::require_can_write(*fsm, "handle_streaming_request:write_header");
                } catch (const std::exception& ex) {
                    spdlog::error("FSM write guard failed before header write (request_id={}): {}",
                                  request_id, ex.what());
                    co_return Error{ErrorCode::InternalError,
                                    std::string{"Invalid state before header write: "} + ex.what()};
                }
            }
            {
                int mt = static_cast<int>(getMessageType(response));
                spdlog::debug("handle_streaming_request: writing header for request_id={} type={}",
                              request_id, mt);
            }
            auto hdr_res = co_await write_header(socket, response, request_id, true, fsm);
            if (!hdr_res) {
                spdlog::debug("handle_streaming_request: write_header failed (request_id={}): {}",
                              request_id, hdr_res.error().message);
                co_return hdr_res.error();
            }
            if (fsm)
                fsm->on_stream_next(false);

            // Write final chunk with the same response payload
            auto chk_res = co_await write_chunk(socket, response, request_id, true, true, fsm);
            if (!chk_res) {
                spdlog::debug("handle_streaming_request: write_chunk failed (request_id={}): {}",
                              request_id, chk_res.error().message);
                co_return chk_res.error();
            }
        }

        // Signal FSM that response has completed
        if (fsm) {
            fsm->on_response_complete(config_.close_after_response);
            spdlog::debug(
                "one-shot streaming complete: close_after_response={} fsm_state={} request_id={}",
                config_.close_after_response, ConnectionFsm::to_string(fsm->state()), request_id);
        }
        if (config_.close_after_response) {
            // Optional graceful half-close: shutdown send side then briefly drain peer
            if (config_.graceful_half_close) {
                boost::system::error_code ig;
                socket.shutdown(boost::asio::socket_base::shutdown_send, ig);
                using namespace boost::asio::experimental::awaitable_operators;
                boost::asio::steady_timer timer(co_await boost::asio::this_coro::executor);
                timer.expires_after(config_.graceful_drain_timeout);
                std::array<uint8_t, 256> tmp{};
                co_await (boost::asio::async_read(socket, boost::asio::buffer(tmp),
                                                  boost::asio::transfer_at_least(1),
                                                  boost::asio::use_awaitable) ||
                          timer.async_wait(boost::asio::use_awaitable));
                // Close regardless after drain/timeout
                socket.close();
                spdlog::debug("graceful half-close complete (request_id={} fd={})", request_id,
                              socket.native_handle());
            } else {
                spdlog::debug("closing socket after response (request_id={} fd={})", request_id,
                              socket.native_handle());
                socket.close();
            }
        } else {
            spdlog::debug("keeping socket open after response (request_id={} fd={})", request_id,
                          socket.native_handle());
        }

        auto duration = std::chrono::steady_clock::now() - start_time;
        stats_.requests_processed++;
        stats_.total_processing_time += duration;

        if (duration < stats_.min_latency) {
            stats_.min_latency = duration;
        }
        if (duration > stats_.max_latency) {
            stats_.max_latency = duration;
        }

        co_return Result<void>();
    } catch (const std::exception& e) {
        stats_.requests_failed++;
        spdlog::error("Exception handling streaming request: {}", e.what());
        co_return Error{ErrorCode::InternalError, e.what()};
    }
}

boost::asio::awaitable<Result<void>>
RequestHandler::stream_chunks_stub(boost::asio::local::stream_protocol::socket& socket,
                                   const Request& request, uint64_t request_id,
                                   ConnectionFsm* fsm) {
    try {
        // Build a header-only response matching request type
        Response headerResponse;
        if (std::holds_alternative<SearchRequest>(request)) {
            SearchResponse r;
            r.totalCount = 0;
            r.elapsed = std::chrono::milliseconds(0);
            headerResponse = r;
        } else if (std::holds_alternative<ListRequest>(request)) {
            ListResponse r;
            r.totalCount = 0;
            headerResponse = r;
        } else if (std::holds_alternative<GrepRequest>(request)) {
            GrepResponse r;
            r.totalMatches = 0;
            r.filesSearched = 0;
            headerResponse = r;
        } else {
            headerResponse = SuccessResponse{"Streaming response"};
        }

        // Guard and write header
        if (fsm) {
            try {
                fsm_helpers::require_can_write(*fsm, "stream_chunks_stub:write_header");
            } catch (const std::exception& ex) {
                spdlog::error("FSM write guard failed before stub header (request_id={}): {}",
                              request_id, ex.what());
                co_return Error{ErrorCode::InternalError,
                                std::string{"Invalid state before stub header: "} + ex.what()};
            }
        }
        auto header_result = co_await write_header(socket, headerResponse, request_id, true, fsm);
        if (!header_result)
            co_return header_result.error();

        if (fsm)
            fsm->on_stream_next(false);

        // Emit a single final empty chunk
        Response finalResponse = headerResponse; // empty payloads
        auto chunk_result =
            co_await write_chunk(socket, finalResponse, request_id, true, true, fsm);
        if (!chunk_result)
            co_return chunk_result.error();

        if (fsm)
            fsm->on_response_complete(config_.close_after_response);
        co_return Result<void>();
    } catch (const std::exception& e) {
        co_return Error{ErrorCode::InternalError,
                        std::string{"Stub streaming failed: "} + e.what()};
    }
}

boost::asio::awaitable<Result<void>>
RequestHandler::write_header_frame(boost::asio::local::stream_protocol::socket& socket,
                                   const Message& message, bool flush, ConnectionFsm* fsm) {
    using boost::asio::use_awaitable;
    (void)flush;
    // Send first streaming frame as HEADER_ONLY to signal start of chunked transfer
    auto frame_result = framer_.frame_message_header(message /*meta only*/);
    if (!frame_result) {
        co_return frame_result.error();
    }

    auto& frame = frame_result.value();
    stats_.bytes_sent += frame.size();
    // Best-effort debug: inspect header flags
    if (frame.size() >= sizeof(MessageFramer::FrameHeader)) {
        auto info = framer_.get_frame_info(frame);
        if (info) {
            const auto& v = info.value();
            spdlog::debug("write_header_frame: header_only={} chunked={} last={} size={}B",
                          v.is_header_only, v.is_chunked, v.is_last_chunk,
                          static_cast<uint32_t>(frame.size()));
            if (v.is_header_only && v.payload_size != 0) {
                spdlog::warn(
                    "write_header_frame: header-only frame advertises payload_size={} — expected 0;"
                    " forcing zero-length header-only per protocol invariant",
                    v.payload_size);
            }
        }
    }

    // Backpressure accounting and write the header frame with timeout
    if (fsm)
        fsm->on_write_queued(frame.size());
    using namespace boost::asio::experimental::awaitable_operators;
    boost::asio::steady_timer timer(co_await boost::asio::this_coro::executor);
    timer.expires_after(config_.write_timeout);
    auto write_or_timeout = co_await (
        boost::asio::async_write(socket, boost::asio::buffer(frame), boost::asio::use_awaitable) ||
        timer.async_wait(boost::asio::use_awaitable));

    if (write_or_timeout.index() == 1) {
        const std::string msg = "Write timeout (header)";
        spdlog::debug("{}", msg);
        co_return Error{ErrorCode::Timeout, msg};
    }
    boost::system::error_code ec;
    if (ec) {
        const auto& msg = ec.message();
        if (msg.find("Connection reset by peer") != std::string::npos ||
            msg.find("Broken pipe") != std::string::npos ||
            msg.find("EPIPE") != std::string::npos || msg.find("ECONNRESET") != std::string::npos) {
            spdlog::debug("Client closed during header frame write: {}", msg);
        }
        co_return Error{ErrorCode::NetworkError, msg};
    }

    if (fsm)
        fsm->on_write_flushed(frame.size());
    co_return Result<void>();
}

boost::asio::awaitable<Result<void>>
RequestHandler::write_chunk_frame(boost::asio::local::stream_protocol::socket& socket,
                                  const Message& message, bool last_chunk, bool flush,
                                  ConnectionFsm* fsm) {
    using boost::asio::use_awaitable;
    (void)flush;
    // Frame chunk
    auto frame_result = framer_.frame_message_chunk(message, last_chunk);
    if (!frame_result) {
        co_return frame_result.error();
    }

    auto& frame = frame_result.value();
    stats_.bytes_sent += frame.size();

    // Extract header info for logging and to split header/payload writes
    const std::size_t headerSize = sizeof(MessageFramer::FrameHeader);
    MessageFramer::FrameHeader hdr{};
    if (frame.size() >= headerSize) {
        std::memcpy(&hdr, frame.data(), headerSize);
        hdr.from_network();
        spdlog::debug(
            "write_chunk_frame: header_only={} chunked={} last={} payload_size={}B frame={}B",
            hdr.is_header_only(), hdr.is_chunked(), hdr.is_last_chunk(), hdr.payload_size,
            static_cast<uint32_t>(frame.size()));
    }

    // 1) Send header first (unblocks client waiting on chunk header) with timeout
    {
        std::span<const uint8_t> header{frame.data(), headerSize};
        using namespace boost::asio::experimental::awaitable_operators;
        boost::asio::steady_timer timer(co_await boost::asio::this_coro::executor);
        timer.expires_after(config_.write_timeout);
        if (fsm)
            fsm->on_write_queued(header.size());
        auto write_or_timeout =
            co_await (boost::asio::async_write(socket, boost::asio::buffer(header),
                                               boost::asio::use_awaitable) ||
                      timer.async_wait(boost::asio::use_awaitable));
        if (write_or_timeout.index() == 1) {
            const std::string msg = "Write timeout (chunk header)";
            spdlog::debug("{}", msg);
            co_return Error{ErrorCode::Timeout, msg};
        }
        boost::system::error_code ec;
        if (ec) {
            const auto& msg = ec.message();
            if (msg.find("Connection reset by peer") != std::string::npos ||
                msg.find("Broken pipe") != std::string::npos ||
                msg.find("EPIPE") != std::string::npos ||
                msg.find("ECONNRESET") != std::string::npos) {
                spdlog::debug("Client closed during chunk header write: {}", msg);
            }
            co_return Error{ErrorCode::NetworkError, msg};
        }
        if (fsm)
            fsm->on_write_flushed(header.size());
    }

    // 2) Stream payload in chunks
    const std::size_t kWriteChunk = std::max<std::size_t>(4096, config_.chunk_size);
    std::size_t offset = headerSize;
    while (offset < frame.size()) {
        std::size_t to_write = std::min<std::size_t>(kWriteChunk, frame.size() - offset);
        std::span<const uint8_t> chunk{frame.data() + offset, to_write};

        using namespace boost::asio::experimental::awaitable_operators;
        boost::asio::steady_timer timer(co_await boost::asio::this_coro::executor);
        timer.expires_after(config_.write_timeout);
        if (fsm)
            fsm->on_write_queued(to_write);
        auto write_or_timeout =
            co_await (boost::asio::async_write(socket, boost::asio::buffer(chunk),
                                               boost::asio::use_awaitable) ||
                      timer.async_wait(boost::asio::use_awaitable));
        if (write_or_timeout.index() == 1) {
            const std::string msg = "Write timeout (chunk payload)";
            spdlog::debug("{}", msg);
            co_return Error{ErrorCode::Timeout, msg};
        }
        if (fsm)
            fsm->on_write_flushed(to_write);
        boost::system::error_code ec;
        if (ec) {
            const auto& msg = ec.message();
            if (msg.find("Connection reset by peer") != std::string::npos ||
                msg.find("Broken pipe") != std::string::npos ||
                msg.find("EPIPE") != std::string::npos ||
                msg.find("ECONNRESET") != std::string::npos) {
                spdlog::debug("Client closed during chunk payload write: {}", msg);
            }
            co_return Error{ErrorCode::NetworkError, msg};
        }

        offset += to_write;
        if (to_write == 0) {
            using namespace std::chrono_literals;
            std::this_thread::sleep_for(1ms);
        } else if (config_.chunk_flush_delay_ms > 0) {
            using namespace std::chrono_literals;
            std::this_thread::sleep_for(std::chrono::milliseconds(config_.chunk_flush_delay_ms));
        }
    }

    co_return Result<void>();
}

boost::asio::awaitable<Result<void>>
RequestHandler::write_header(boost::asio::local::stream_protocol::socket& socket, Response response,
                             uint64_t request_id, bool flush, ConnectionFsm* fsm) {
    using boost::asio::use_awaitable;
    // Create message envelope for response header
    Message response_msg;
    response_msg.version = PROTOCOL_VERSION;
    response_msg.requestId = request_id;
    response_msg.timestamp = std::chrono::steady_clock::now();
    response_msg.payload = std::move(response);
    // Debug header write (shows message type and flush status)
    spdlog::debug("stream: write_header req_id={} type={} flush={}", request_id,
                  static_cast<int>(getMessageType(response)), flush);
    if (config_.enable_multiplexing) {
        // Frame and enqueue for fair writer
        auto framed = framer_.frame_message_header(response_msg);
        if (!framed)
            co_return framed.error();
        if (write_strand_exec_)
            co_await boost::asio::dispatch(*write_strand_exec_, use_awaitable);
        auto enq = co_await enqueue_frame(request_id, std::move(framed.value()), false, fsm);
        if (!enq) {
            if (enq.error().code == ErrorCode::RateLimited ||
                enq.error().code == ErrorCode::ResourceExhausted) {
                co_return co_await write_error_immediate(socket, request_id, enq.error().code,
                                                         enq.error().message, fsm);
            }
            co_return enq.error();
        }
        if (!writer_running_) {
            writer_running_ = true;
            co_await writer_drain(socket, fsm);
        }
        co_return Result<void>{};
    } else {
        if (write_strand_exec_)
            co_await boost::asio::dispatch(*write_strand_exec_, use_awaitable);
        co_return co_await write_header_frame(socket, response_msg, flush, fsm);
    }
}

boost::asio::awaitable<Result<void>>
RequestHandler::write_chunk(boost::asio::local::stream_protocol::socket& socket, Response response,
                            uint64_t request_id, bool last_chunk, bool flush, ConnectionFsm* fsm) {
    using boost::asio::use_awaitable;
    // Create message envelope for response chunk
    Message response_msg;
    response_msg.version = PROTOCOL_VERSION;
    response_msg.requestId = request_id;
    response_msg.timestamp = std::chrono::steady_clock::now();
    response_msg.payload = std::move(response);
    if (config_.enable_multiplexing) {
        auto framed = framer_.frame_message_chunk(response_msg, last_chunk);
        if (!framed)
            co_return framed.error();
        if (write_strand_exec_)
            co_await boost::asio::dispatch(*write_strand_exec_, use_awaitable);
        auto enq = co_await enqueue_frame(request_id, std::move(framed.value()), last_chunk, fsm);
        if (!enq) {
            if (enq.error().code == ErrorCode::RateLimited ||
                enq.error().code == ErrorCode::ResourceExhausted) {
                co_return co_await write_error_immediate(socket, request_id, enq.error().code,
                                                         enq.error().message, fsm);
            }
            co_return enq.error();
        }
        if (!writer_running_) {
            writer_running_ = true;
            co_await writer_drain(socket, fsm);
        }
        co_return Result<void>{};
    } else {
        if (write_strand_exec_)
            co_await boost::asio::dispatch(*write_strand_exec_, use_awaitable);
        co_return co_await write_chunk_frame(socket, response_msg, last_chunk, flush, fsm);
    }
}

boost::asio::awaitable<Result<void>> RequestHandler::enqueue_frame(uint64_t request_id,
                                                                   std::vector<uint8_t> frame,
                                                                   bool last, ConnectionFsm* fsm) {
    // Enforce caps with typed errors
    auto& q = rr_queues_[request_id];
    if (q.size() >= config_.per_request_queue_cap) {
        co_return Error{ErrorCode::RateLimited, "per-request queue full"};
    }
    if (total_queued_bytes_ + frame.size() > config_.total_queued_bytes_cap) {
        co_return Error{ErrorCode::ResourceExhausted, "connection queue bytes cap"};
    }
    bool was_empty = q.empty();
    auto sz = frame.size();
    if (fsm)
        fsm->on_write_queued(sz);
    q.push_back(FrameItem{.data = std::move(frame), .last = last});
    total_queued_bytes_ += sz;
    MuxMetricsRegistry::instance().addQueuedBytes(static_cast<int64_t>(sz));
    if (was_empty) {
        rr_active_.push_back(request_id);
    }
    // Update per-request context counters
    {
        std::lock_guard<std::mutex> lk(ctx_mtx_);
        auto it = contexts_.find(request_id);
        if (it != contexts_.end()) {
            it->second->frames_enqueued.fetch_add(1, std::memory_order_relaxed);
            it->second->bytes_enqueued.fetch_add(sz, std::memory_order_relaxed);
        }
    }
    co_return Result<void>{};
}

// Immediate, queue-bypass error write used when enqueueing is denied due to caps.
boost::asio::awaitable<Result<void>>
RequestHandler::write_error_immediate(boost::asio::local::stream_protocol::socket& socket,
                                      uint64_t request_id, ErrorCode code,
                                      const std::string& message, ConnectionFsm* fsm) {
    using boost::asio::use_awaitable;
    // Honor write strand if present
    if (write_strand_exec_) {
        co_await boost::asio::dispatch(*write_strand_exec_, use_awaitable);
    }
    // FSM guard
    if (fsm) {
        try {
            fsm_helpers::require_can_write(*fsm, "write_error_immediate");
        } catch (const std::exception& ex) {
            co_return Error{ErrorCode::InternalError,
                            std::string{"FSM not writeable for immediate error: "} + ex.what()};
        }
    }
    ErrorResponse err;
    err.code = code;
    err.message = message;
    Message m;
    m.version = PROTOCOL_VERSION;
    m.requestId = request_id;
    m.timestamp = std::chrono::steady_clock::now();
    m.payload = Response(err);
    auto framed = framer_.frame_message(m);
    if (!framed) {
        co_return framed.error();
    }
    boost::system::error_code ec;
    co_await boost::asio::async_write(socket, boost::asio::buffer(framed.value()),
                                      boost::asio::redirect_error(use_awaitable, ec));
    if (ec) {
        co_return Error{ErrorCode::NetworkError, ec.message()};
    }
    co_return Result<void>{};
}

boost::asio::awaitable<void>
RequestHandler::writer_drain(boost::asio::local::stream_protocol::socket& socket,
                             ConnectionFsm* fsm) {
    using boost::asio::use_awaitable;
#if defined(TRACY_ENABLE)
    ZoneScopedN("RequestHandler::writer_drain");
#endif
    // Must be called on write strand
    while (!rr_active_.empty()) {
#if defined(TRACY_ENABLE)
        ZoneScopedN("writer_drain:turn");
#endif
        auto rid = rr_active_.front();
        rr_active_.pop_front();
        auto it = rr_queues_.find(rid);
        if (it == rr_queues_.end() || it->second.empty()) {
            continue;
        }
        // Drain up to budget bytes for this request turn
        // Dynamic budget: derive base from global mux metrics if set; otherwise config
        size_t base_budget = config_.writer_budget_bytes_per_turn;
        {
            auto snap = MuxMetricsRegistry::instance().snapshot();
            if (snap.writerBudgetBytes > 0)
                base_budget = static_cast<size_t>(snap.writerBudgetBytes);
        }
        size_t budget = base_budget;
        const size_t active = rr_active_.size();
        const size_t queued_bytes = total_queued_bytes_;
        const size_t queued_cap = config_.total_queued_bytes_cap;
        // Adaptive scaling via TuneAdvisor thresholds
        if (active <= TuneAdvisor::writerActiveLow1Threshold() && queued_bytes < (queued_cap / 8)) {
            budget = static_cast<size_t>(static_cast<double>(budget) *
                                         TuneAdvisor::writerScaleActiveLow1Mul());
        } else if (active <= TuneAdvisor::writerActiveLow2Threshold() &&
                   queued_bytes < (queued_cap / 4)) {
            budget = static_cast<size_t>(static_cast<double>(budget) *
                                         TuneAdvisor::writerScaleActiveLow2Mul());
        }
        if (active > TuneAdvisor::writerActiveHigh1Threshold())
            budget = static_cast<size_t>(static_cast<double>(budget) *
                                         TuneAdvisor::writerScaleActiveHigh1Mul());
        if (active > TuneAdvisor::writerActiveHigh2Threshold())
            budget = static_cast<size_t>(static_cast<double>(budget) *
                                         TuneAdvisor::writerScaleActiveHigh2Mul());
        const double halfFrac = TuneAdvisor::writerQueuedHalfThresholdFraction();
        const double threeQFrac = TuneAdvisor::writerQueuedThreeQuarterThresholdFraction();
        if (queued_bytes > static_cast<size_t>(queued_cap * threeQFrac))
            budget = static_cast<size_t>(static_cast<double>(budget) *
                                         TuneAdvisor::writerScaleQueuedThreeQuarterMul());
        else if (queued_bytes > static_cast<size_t>(queued_cap * halfFrac))
            budget = static_cast<size_t>(static_cast<double>(budget) *
                                         TuneAdvisor::writerScaleQueuedHalfMul());
        // Cap budget to a safe maximum (centralized via TuneAdvisor)
        size_t max_budget = TuneAdvisor::serverWriterBudgetMaxBytesPerTurn();
        if (budget > max_budget)
            budget = max_budget;
        // Reflect dynamic budget in mux metrics for observability
        MuxMetricsRegistry::instance().setWriterBudget(budget);
        while (budget > 0 && it != rr_queues_.end() && !it->second.empty()) {
            FrameItem item = std::move(it->second.front());
            it->second.pop_front();
            total_queued_bytes_ -= item.data.size();
            MuxMetricsRegistry::instance().addQueuedBytes(-static_cast<int64_t>(item.data.size()));
            // Adjust budget; allow single oversized frame to pass
            if (item.data.size() >= budget)
                budget = 0;
            else
                budget -= item.data.size();
            // Write the frame
            boost::system::error_code ec;
            co_await boost::asio::async_write(socket, boost::asio::buffer(item.data),
                                              boost::asio::redirect_error(use_awaitable, ec));
            if (fsm)
                fsm->on_write_flushed(item.data.size());
            if (ec) {
                spdlog::debug("writer_drain: write error: {}", ec.message());
                rr_active_.clear();
                break;
            }
            if (item.last && it->second.empty()) {
                rr_queues_.erase(it);
                break;
            }
        }
        // Re-enqueue if still has frames
        it = rr_queues_.find(rid);
        if (it != rr_queues_.end() && !it->second.empty()) {
            rr_active_.push_back(rid);
        }
    }
    writer_running_ = false;
    co_return;
}

boost::asio::awaitable<Result<void>>
RequestHandler::stream_chunks(boost::asio::local::stream_protocol::socket& socket,
                              const Request& request, uint64_t request_id,
                              std::shared_ptr<RequestProcessor> processor, ConnectionFsm* fsm) {
    using boost::asio::use_awaitable;
    // First, send a header-only response to tell client to expect chunks
    // Create an empty response or use the first chunk as header
    Response headerResponse;

    // For search requests, create an empty search response header
    if (std::holds_alternative<SearchRequest>(request)) {
        SearchResponse searchHeader;
        searchHeader.totalCount = 0; // Will be updated in chunks
        searchHeader.elapsed = std::chrono::milliseconds(0);
        headerResponse = searchHeader;
    }
    // For list requests, create an empty list response header
    else if (std::holds_alternative<ListRequest>(request)) {
        ListResponse listHeader;
        listHeader.totalCount = 0; // Will be updated in chunks
        headerResponse = listHeader;
    }
    // For grep requests, create an empty grep response header
    else if (std::holds_alternative<GrepRequest>(request)) {
        GrepResponse grepHeader;
        grepHeader.totalMatches = 0; // Will be updated in chunks
        grepHeader.filesSearched = 0;
        headerResponse = grepHeader;
    }
    // Default is a success response
    else {
        headerResponse = SuccessResponse{"Streaming response"};
    }

    // FSM guard before header write
    if (fsm) {
        try {
            fsm_helpers::require_can_write(*fsm, "stream_chunks:write_header");
        } catch (const std::exception& ex) {
            spdlog::error("FSM write guard failed before header (request_id={}): {}", request_id,
                          ex.what());
            co_return Error{ErrorCode::InternalError,
                            std::string{"Invalid state before streaming header: "} + ex.what()};
        }
    }
    // Send header frame
    auto header_result = co_await write_header(socket, headerResponse, request_id, true, fsm);
    if (!header_result) {
        co_return header_result.error();
    }
    spdlog::debug("stream_chunks: sent header-only frame (request_id={})", request_id);
    if (fsm) {
        // Move FSM from WritingHeader -> StreamingChunks
        fsm->on_stream_next(false);
    }

    // Stream chunks until we get the last one
    bool last_chunk_received = false;
    size_t chunk_count = 0;
    bool ttfb_recorded = false;
    auto header_time = std::chrono::steady_clock::now();
    StreamMetricsRegistry::instance().incStreams(1);

    while (!last_chunk_received) {
        spdlog::debug("stream_chunks: preparing chunk #{} (request_id={})", chunk_count + 1,
                      request_id);
        {
            std::lock_guard<std::mutex> lk(ctx_mtx_);
            auto it = contexts_.find(request_id);
            if (it != contexts_.end() && it->second->canceled.load(std::memory_order_relaxed)) {
                co_return Error{ErrorCode::OperationCancelled, "Request canceled"};
            }
        }
        auto chunk_result = co_await processor->next_chunk();
        last_chunk_received = chunk_result.is_last_chunk;
        // Belt-and-suspenders: if the payload is an ErrorResponse, force this to be the last
        try {
            if (std::holds_alternative<ErrorResponse>(chunk_result.data)) {
                last_chunk_received = true;
            }
        } catch (...) {
        }

        // Inspect chunk payload for common types to aid troubleshooting
        size_t item_count = 0;
        int msg_type = static_cast<int>(getMessageType(chunk_result.data));
        try {
            std::visit(
                [&](auto const& resp) {
                    using T = std::decay_t<decltype(resp)>;
                    if constexpr (std::is_same_v<T, SearchResponse>) {
                        item_count = resp.results.size();
                    } else if constexpr (std::is_same_v<T, ListResponse>) {
                        item_count = resp.items.size();
                    } else if constexpr (std::is_same_v<T, GrepResponse>) {
                        item_count = resp.matches.size();
                    }
                },
                chunk_result.data);
        } catch (const std::exception&) {
            // Defensive: treat unknown/valueless variants as keepalives
            item_count = 0;
        }
        // Streaming metrics: batches, keepalives, TTFB
        StreamMetricsRegistry::instance().incBatches(1);
        if (item_count == 0 && !last_chunk_received) {
            StreamMetricsRegistry::instance().incKeepalive(1);
        }
        if (!ttfb_recorded && item_count > 0) {
            auto now = std::chrono::steady_clock::now();
            auto ms =
                std::chrono::duration_cast<std::chrono::milliseconds>(now - header_time).count();
            if (ms >= 0)
                StreamMetricsRegistry::instance().addTtfb(static_cast<uint64_t>(ms));
            ttfb_recorded = true;
        }
        spdlog::debug("stream_chunks: chunk #{} type={} items={} last={} (request_id={})",
                      chunk_count + 1, msg_type, item_count, last_chunk_received, request_id);

        // FSM guard before chunk write
        if (fsm) {
            try {
                fsm_helpers::require_can_write(*fsm, "stream_chunks:write_chunk");
            } catch (const std::exception& ex) {
                spdlog::error("FSM write guard failed before chunk (request_id={}): {}", request_id,
                              ex.what());
                co_return Error{ErrorCode::InternalError,
                                std::string{"Invalid state before streaming chunk: "} + ex.what()};
            }
        }
        // Send chunk
        auto write_result = co_await write_chunk(socket, chunk_result.data, request_id,
                                                 last_chunk_received, true, fsm);
        if (!write_result) {
            co_return write_result.error();
        }
        if (fsm) {
            // Notify FSM; continue streaming or complete response
            if (last_chunk_received) {
                fsm->on_response_complete(config_.close_after_response);
                spdlog::info("streaming complete: close_after_response={} fsm_state={} chunks={} "
                             "request_id={}",
                             config_.close_after_response, ConnectionFsm::to_string(fsm->state()),
                             chunk_count + 1, request_id);
            } else {
                fsm->on_stream_next(false);
            }
        }

        chunk_count++;
    }

    // Note: Do not send any extra frames after the last chunk. An unsolicited
    // header-only frame here can remain unread by clients (who stop reading
    // immediately after completing a stream) and will poison the next request's
    // response boundary on persistent connections, resulting in header timeouts.

    spdlog::debug("Sent {} chunks for streaming response (request_id={})", chunk_count, request_id);
    if (config_.close_after_response) {
        if (config_.graceful_half_close) {
            boost::system::error_code ig;
            socket.shutdown(boost::asio::socket_base::shutdown_send, ig);
            using namespace boost::asio::experimental::awaitable_operators;
            boost::asio::steady_timer timer(co_await boost::asio::this_coro::executor);
            timer.expires_after(config_.graceful_drain_timeout);
            std::array<uint8_t, 256> tmp{};
            co_await (boost::asio::async_read(socket, boost::asio::buffer(tmp),
                                              boost::asio::transfer_at_least(1),
                                              boost::asio::use_awaitable) ||
                      timer.async_wait(boost::asio::use_awaitable));
            socket.close();
            spdlog::debug("graceful half-close complete (streaming) (request_id={} fd={})",
                          request_id, socket.native_handle());
        } else {
            spdlog::debug("closing socket after streaming response (request_id={} fd={})",
                          request_id, socket.native_handle());
            socket.close();
        }
        if (fsm) {
            fsm->on_close_request();
        }
    } else {
        spdlog::debug("keeping socket open after streaming response (request_id={} fd={})",
                      request_id, socket.native_handle());
    }
    co_return Result<void>();
}

boost::asio::awaitable<Result<void>>
RequestHandler::send_error(boost::asio::local::stream_protocol::socket& socket, ErrorCode code,
                           const std::string& message, uint64_t request_id) {
    using boost::asio::use_awaitable;
    ErrorResponse error{code, message};

    // Create message envelope
    Message error_msg;
    error_msg.version = PROTOCOL_VERSION;
    error_msg.requestId = request_id;
    error_msg.timestamp = std::chrono::steady_clock::now();
    error_msg.payload = Response{error};

    co_return co_await write_message(socket, error_msg);
}

// ============================================================================
// RequestRouter Implementation
// ============================================================================

boost::asio::awaitable<Response> RequestRouter::process(const Request& request) {
    auto type_hash = get_request_type_hash(request);

    auto it = handlers_.find(type_hash);
    if (it == handlers_.end()) {
        co_return ErrorResponse{ErrorCode::NotImplemented,
                                "No handler registered for request type"};
    }

    co_return co_await it->second(request);
}

size_t RequestRouter::get_request_type_hash(const Request& request) const {
    return std::visit([](const auto& req) { return typeid(req).hash_code(); }, request);
}

// ============================================================================
// MiddlewarePipeline Implementation
// ============================================================================

boost::asio::awaitable<Response> MiddlewarePipeline::process(const Request& request) {
    if (middleware_.empty()) {
        if (final_handler_) {
            co_return co_await final_handler_(request);
        }
        co_return ErrorResponse{ErrorCode::NotImplemented, "No handler configured"};
    }

    // Build the middleware chain
    std::function<boost::asio::awaitable<Response>(const Request&)> chain = final_handler_;

    for (auto it = middleware_.rbegin(); it != middleware_.rend(); ++it) {
        auto middleware = *it;
        auto next = chain;
        chain = [middleware, next](const Request& req) -> boost::asio::awaitable<Response> {
            co_return co_await middleware->process(req, next);
        };
    }

    co_return co_await chain(request);
}

// ============================================================================
// LoggingMiddleware Implementation
// ============================================================================

boost::asio::awaitable<Response> LoggingMiddleware::process(const Request& request, Next next) {
    auto start_time = std::chrono::steady_clock::now();

    // Log request
    spdlog::debug("Processing request type: {}",
                  std::visit([](const auto& req) { return typeid(req).name(); }, request));

    // Call next handler
    Response response = co_await next(request);

    // Log response
    auto duration = std::chrono::steady_clock::now() - start_time;
    auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();

    bool is_error = std::holds_alternative<ErrorResponse>(response);
    if (is_error) {
        auto& error = std::get<ErrorResponse>(response);
        spdlog::warn("Request failed: {} ({}ms)", error.message, duration_ms);
    } else {
        spdlog::debug("Request completed successfully ({}ms)", duration_ms);
    }

    co_return response;
}

// ============================================================================
// RateLimitMiddleware Implementation
// ============================================================================

RateLimitMiddleware::RateLimitMiddleware(Config config)
    : config_(config), tokens_(config.burst_size), last_refill_(std::chrono::steady_clock::now()) {}

boost::asio::awaitable<Response> RateLimitMiddleware::process(const Request& request, Next next) {
    // Refill tokens based on time elapsed
    {
        std::lock_guard lock(refill_mutex_);
        auto now = std::chrono::steady_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - last_refill_).count();

        if (elapsed > 0) {
            size_t new_tokens = elapsed * config_.requests_per_second;
            size_t current = tokens_.load();
            size_t max_tokens = config_.burst_size;

            if (current + new_tokens > max_tokens) {
                tokens_ = max_tokens;
            } else {
                tokens_ += new_tokens;
            }

            last_refill_ = now;
        }
    }

    // Try to consume a token
    size_t current = tokens_.load();
    while (current > 0) {
        if (tokens_.compare_exchange_weak(current, current - 1)) {
            // Token acquired, process request
            co_return co_await next(request);
        }
        // current was updated by compare_exchange, retry
    }

    // No tokens available
    co_return ErrorResponse{ErrorCode::RateLimited, "Rate limit exceeded"};
}

// ============================================================================
// AuthMiddleware Implementation
// ============================================================================

boost::asio::awaitable<Response> AuthMiddleware::process(const Request& request, Next next) {
    // Extract auth token from request metadata (placeholder)
    std::string token = ""; // TODO: Extract from request

    bool is_valid = co_await validator_(token);

    if (!is_valid) {
        co_return ErrorResponse{ErrorCode::Unauthorized, "Authentication failed"};
    }

    co_return co_await next(request);
}

} // namespace yams::daemon
