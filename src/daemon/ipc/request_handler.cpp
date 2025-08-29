#include <yams/daemon/ipc/connection_fsm.h>
#include <yams/daemon/ipc/message_serializer.h>
#include <yams/daemon/ipc/request_handler.h>

#include <spdlog/spdlog.h>
#include <array>
#include <chrono>

namespace yams::daemon {

// ============================================================================
// RequestHandler Implementation
// ============================================================================

RequestHandler::RequestHandler(std::shared_ptr<RequestProcessor> processor, Config config)
    : processor_(std::move(processor)), config_(std::move(config)) {}

RequestHandler::~RequestHandler() = default;

Task<void> RequestHandler::handle_connection(AsyncSocket<AsyncIOContext> socket,
                                             std::stop_token token) {
    try {
        spdlog::debug("RequestHandler::handle_connection coroutine started");
        spdlog::debug("New connection established");
        // Initialize per-connection FSM (adapter kept internal to source file)
        ConnectionFsm fsm;
        fsm.enable_metrics(true);
        fsm.on_connect(socket.get_fd());
        // Note: downstream clients may close early (e.g., pager exits). Treat write-side resets
        // as non-fatal for the daemon; logs will be at debug level when detected.

        // Set timeouts if configured
        if (config_.read_timeout.count() > 0) {
            socket.set_timeout(config_.read_timeout);
        }
        // Use protocol maximum message size for inbound frame reader
        FrameReader reader(MAX_MESSAGE_SIZE);
        bool should_exit = false;

        while (!token.stop_requested() && socket.is_valid()) {
            // Read as much as is available (up to 4096), do not block for exact size
            std::array<uint8_t, 4096> buf{};
            spdlog::debug("About to co_await socket.async_read");
            auto read_bytes = co_await socket.async_read(buf.data(), buf.size());
            spdlog::debug("socket.async_read returned with result={}", read_bytes.has_value());
            if (!read_bytes) {
                if (read_bytes.error().code == ErrorCode::NetworkError) {
                    spdlog::debug("Connection closed or network error from peer");
                } else {
                    spdlog::error("Read error: {}", read_bytes.error().message);
                }
                break;
            }

            if (read_bytes.value() == 0) {
                // Transient zero-byte read can occur with non-blocking IO; peek for real EOF
                std::array<uint8_t, 1> peek{};
                auto try_peek = co_await socket.async_read(peek.data(), peek.size());
                if (!try_peek) {
                    spdlog::debug("Zero-byte read followed by error: {}", try_peek.error().message);
                    break;
                }
                if (try_peek.value() == 0) {
                    spdlog::debug("Connection closed (EOF confirmed)");
                    break;
                }
                // We read 1 byte; feed it first, then continue with main buffer below
                spdlog::debug("Recovered from transient zero-byte read; feeding 1-byte peek");
                auto feed_result = reader.feed(peek.data(), try_peek.value());
                stats_.bytes_received += feed_result.consumed;
                if (feed_result.status == FrameReader::FrameStatus::InvalidFrame) {
                    spdlog::error("Invalid frame received (after peek)");
                    co_await send_error(socket, ErrorCode::InvalidArgument, "Invalid frame format");
                    break;
                }
                if (feed_result.status == FrameReader::FrameStatus::FrameTooLarge) {
                    spdlog::error("Frame too large (after peek)");
                    co_await send_error(socket, ErrorCode::InvalidArgument,
                                        "Frame exceeds maximum size");
                    break;
                }
                // After handling peeked byte(s), continue loop to read more normally
                continue;
            }

            // Drive FSM readable event
            fsm.on_readable(static_cast<size_t>(read_bytes.value()));

            // Feed data to frame reader
            spdlog::debug("Feeding {} bytes to frame reader", read_bytes.value());
            auto feed_result = reader.feed(buf.data(), read_bytes.value());
            stats_.bytes_received += feed_result.consumed;
            spdlog::debug("Frame reader consumed {} bytes, status={}", feed_result.consumed,
                          static_cast<int>(feed_result.status));

            if (feed_result.status == FrameReader::FrameStatus::InvalidFrame) {
                spdlog::error("Invalid frame received");
                co_await send_error(socket, ErrorCode::InvalidArgument, "Invalid frame format");
                break;
            }

            if (feed_result.status == FrameReader::FrameStatus::FrameTooLarge) {
                spdlog::error("Frame too large");
                co_await send_error(socket, ErrorCode::InvalidArgument,
                                    "Frame exceeds maximum size");
                break;
            }

            // Process complete frames
            while (reader.has_frame()) {
                auto frame_result = reader.get_frame();
                if (!frame_result) {
                    spdlog::error("Failed to get frame: {}", frame_result.error().message);
                    continue;
                }

                // Parse the frame
                auto message_result = framer_.parse_frame(frame_result.value());
                if (!message_result) {
                    spdlog::error("Failed to parse frame: {}", message_result.error().message);
                    co_await send_error(socket, ErrorCode::SerializationError,
                                        "Failed to parse message");
                    continue;
                }

                // Inform FSM that a request header/body has been parsed
                ConnectionFsm::FrameInfo finfo{};
                finfo.payload_size = 0; // unknown at this level
                fsm.on_header_parsed(finfo);
                fsm.on_body_parsed();

                // Extract request from message
                const auto& message = message_result.value();
                auto* request_ptr = std::get_if<Request>(&message.payload);
                if (!request_ptr) {
                    spdlog::error("Received non-request message");
                    co_await send_error(socket, ErrorCode::InvalidArgument,
                                        "Expected request message");
                    continue;
                }

                // Handle the request with correlation id
                // Route through streaming-aware path so FSM transitions are captured
                auto handle_result = co_await handle_streaming_request(socket, *request_ptr,
                                                                       message.requestId, &fsm);
                if (!handle_result) {
                    // Downgrade common client-initiated close errors to debug to avoid noisy logs
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
            if (should_exit)
                break;
        }

        // Close out FSM lifecycle
        fsm.on_close_request();
        spdlog::debug("Connection handler exiting normally");
    } catch (const std::exception& e) {
        spdlog::error("RequestHandler::handle_connection unhandled exception: {}", e.what());
    } catch (...) {
        spdlog::error("RequestHandler::handle_connection unhandled unknown exception");
    }
}

Task<Result<void>> RequestHandler::handle_request(AsyncSocket<AsyncIOContext>& socket,
                                                  const Request& request, uint64_t request_id) {
    // Use streaming mode if enabled
    if (config_.enable_streaming) {
        co_return co_await handle_streaming_request(socket, request, request_id);
    }

    auto start_time = std::chrono::steady_clock::now();

    try {
        // Process the request
        Response response = co_await process_request(request);

        // Trace response type before framing and writing
        spdlog::debug("handle_request: got response type {} for request_id={}",
                      static_cast<int>(getMessageType(response)), request_id);

        // Create message envelope for response (preserve requestId for correlation)
        Message response_msg;
        response_msg.version = PROTOCOL_VERSION;
        response_msg.requestId = request_id;
        response_msg.timestamp = std::chrono::steady_clock::now();
        response_msg.payload = response;

        // Send response
        spdlog::debug("handle_request: writing response message for request_id={}", request_id);
        auto write_result = co_await write_message(socket, response_msg);
        if (!write_result) {
            spdlog::debug("handle_request: write_message failed for request_id={} msg={}",
                          request_id, write_result.error().message);
        } else {
            spdlog::debug("handle_request: write_message succeeded for request_id={}", request_id);
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

        if (config_.close_after_response) {
            socket.close();
        }
        co_return write_result;

    } catch (const std::exception& e) {
        stats_.requests_failed++;
        spdlog::error("Exception handling request: {}", e.what());
        co_return Error{ErrorCode::InternalError, e.what()};
    }
}

Task<Result<Message>> RequestHandler::read_message(AsyncSocket<AsyncIOContext>& socket,
                                                   FrameReader& reader) {
    // Read until we have a complete frame
    while (!reader.has_frame()) {
        auto read_result = co_await socket.async_read_exact(4096);

        if (!read_result) {
            co_return read_result.error();
        }

        auto& data = read_result.value();
        if (data.empty()) {
            co_return Error{ErrorCode::NetworkError, "Connection closed"};
        }

        auto feed_result = reader.feed(data.data(), data.size());
        stats_.bytes_received += feed_result.consumed;

        if (feed_result.status == FrameReader::FrameStatus::InvalidFrame) {
            co_return Error{ErrorCode::InvalidArgument, "Invalid frame"};
        }

        if (feed_result.status == FrameReader::FrameStatus::FrameTooLarge) {
            co_return Error{ErrorCode::InvalidArgument, "Frame too large"};
        }
    }

    auto frame_result = reader.get_frame();
    if (!frame_result) {
        co_return frame_result.error();
    }

    co_return framer_.parse_frame(frame_result.value());
}

Task<Result<void>> RequestHandler::write_message(AsyncSocket<AsyncIOContext>& socket,
                                                 const Message& message) {
    // Frame once
    auto frame_result = framer_.frame_message(message);
    if (!frame_result) {
        co_return frame_result.error();
    }
    auto& frame = frame_result.value();
    stats_.bytes_sent += frame.size();

    const auto msgType =
        std::holds_alternative<Request>(message.payload)
            ? static_cast<int>(getMessageType(std::get<Request>(message.payload)))
            : static_cast<int>(getMessageType(std::get<Response>(message.payload)));
    spdlog::debug(
        "write_message: framed message type={} request_id={} frame_size={} header_size={}", msgType,
        message.requestId, frame.size(), sizeof(MessageFramer::FrameHeader));

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
        auto r = co_await socket.async_write_all(header);
        if (!r) {
            const auto& msg = r.error().message;
            if (msg.find("Connection reset by peer") != std::string::npos ||
                msg.find("Broken pipe") != std::string::npos ||
                msg.find("EPIPE") != std::string::npos ||
                msg.find("ECONNRESET") != std::string::npos) {
                spdlog::debug("Client closed during header write: {}", msg);
            }
            co_return r.error();
        }
        spdlog::debug("write_message: header write complete (request_id={})", message.requestId);
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
        auto r = co_await socket.async_write_all(chunk);
        if (!r) {
            const auto& msg = r.error().message;
            if (msg.find("Connection reset by peer") != std::string::npos ||
                msg.find("Broken pipe") != std::string::npos ||
                msg.find("EPIPE") != std::string::npos ||
                msg.find("ECONNRESET") != std::string::npos) {
                spdlog::debug("Client closed during payload write: {}", msg);
            }
            co_return r.error();
        }
        payload_written += to_write;
        if (to_write == 0) {
            using namespace std::chrono_literals;
            std::this_thread::sleep_for(1ms);
        } else if (config_.chunk_flush_delay_ms > 0) {
            using namespace std::chrono_literals;
            std::this_thread::sleep_for(std::chrono::milliseconds(config_.chunk_flush_delay_ms));
        }

        offset += to_write;
    }

    spdlog::debug("write_message: payload write complete ({} bytes) request_id={}", payload_written,
                  message.requestId);

    co_return Result<void>();
}

Task<Response> RequestHandler::process_request(const Request& request) {
    if (processor_) {
        co_return co_await processor_->process(request);
    }

    co_return ErrorResponse{ErrorCode::NotImplemented, "Request processor not set"};
}

Task<std::optional<Response>> RequestHandler::process_streaming_request(const Request& request) {
    if (!processor_) {
        co_return ErrorResponse{ErrorCode::NotImplemented, "Request processor not set"};
    }

    // Check if we should use streaming for this request: initialize via processor
    if (can_stream_request(request)) {
        // Initialize streaming; processor may return std::nullopt to indicate chunked mode
        co_return co_await processor_->process_streaming(request);
    }

    // Fall back to regular processing
    co_return co_await processor_->process_streaming(request);
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
            std::holds_alternative<GrepRequest>(request)) {
            return true;
        }
    }

    return false;
}

Task<std::vector<Response>> RequestHandler::collect_limited_chunks(const Request& request,
                                                                   size_t max_chunks) {
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

Task<Result<void>> RequestHandler::handle_streaming_request(AsyncSocket<AsyncIOContext>& socket,
                                                            const Request& request,
                                                            uint64_t request_id,
                                                            ConnectionFsm* fsm) {
    auto start_time = std::chrono::steady_clock::now();

    try {
        // Use the configured processor (server may decorate with streaming support)
        std::shared_ptr<RequestProcessor> proc = processor_;

        // Process the request with streaming support (may return no value to indicate chunking)
        auto response_opt = co_await proc->process_streaming(request);

        if (!response_opt.has_value()) {
            // No response means we should use the streaming interface
            if (proc && can_stream_request(request)) {
                spdlog::debug("handle_streaming_request: entering streaming mode (request_id={})",
                              request_id);
                // Inform FSM we are transitioning to write header for streaming
                if (fsm) {
                    ConnectionFsm::FrameInfo finfo{};
                    finfo.payload_size = 0; // header-only
                    fsm->on_header_parsed(finfo);
                }
                auto stream_result = co_await stream_chunks(socket, request, request_id, proc, fsm);

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

        // We have a complete response, write header first
        auto& response = response_opt.value();

        // Create message envelope for response (preserve requestId for correlation)
        Message response_msg;
        response_msg.version = PROTOCOL_VERSION;
        response_msg.requestId = request_id;
        response_msg.timestamp = std::chrono::steady_clock::now();
        response_msg.payload = response;

        // Send a single non-chunked frame for complete responses
        spdlog::debug(
            "handle_streaming_request: writing complete response (non-chunked) for request_id={}",
            request_id);
        auto write_result = co_await write_message(socket, response_msg);
        if (!write_result) {
            spdlog::debug("handle_streaming_request: write_message failed for request_id={} msg={}",
                          request_id, write_result.error().message);
            co_return write_result.error();
        }

        if (config_.close_after_response) {
            socket.close();
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

Task<Result<void>> RequestHandler::write_header_frame(AsyncSocket<AsyncIOContext>& socket,
                                                      const Message& message, bool flush) {
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
        }
    }

    // Write the header frame
    auto r = co_await socket.async_write_all(frame);
    if (!r) {
        const auto& msg = r.error().message;
        if (msg.find("Connection reset by peer") != std::string::npos ||
            msg.find("Broken pipe") != std::string::npos ||
            msg.find("EPIPE") != std::string::npos || msg.find("ECONNRESET") != std::string::npos) {
            spdlog::debug("Client closed during header frame write: {}", msg);
        }
        co_return r.error();
    }

    co_return Result<void>();
}

Task<Result<void>> RequestHandler::write_chunk_frame(AsyncSocket<AsyncIOContext>& socket,
                                                     const Message& message, bool last_chunk,
                                                     bool flush) {
    (void)flush;
    // Frame chunk
    auto frame_result = framer_.frame_message_chunk(message, last_chunk);
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
            spdlog::debug("write_chunk_frame: header_only={} chunked={} last={} size={}B",
                          v.is_header_only, v.is_chunked, v.is_last_chunk,
                          static_cast<uint32_t>(frame.size()));
        }
    }

    // Stream the chunk in smaller pieces
    const std::size_t kWriteChunk = std::max<std::size_t>(4096, config_.chunk_size);
    std::size_t offset = 0;

    while (offset < frame.size()) {
        std::size_t to_write = std::min<std::size_t>(kWriteChunk, frame.size() - offset);
        std::span<const uint8_t> chunk{frame.data() + offset, to_write};

        auto r = co_await socket.async_write_all(chunk);
        if (!r) {
            const auto& msg = r.error().message;
            if (msg.find("Connection reset by peer") != std::string::npos ||
                msg.find("Broken pipe") != std::string::npos ||
                msg.find("EPIPE") != std::string::npos ||
                msg.find("ECONNRESET") != std::string::npos) {
                spdlog::debug("Client closed during chunk write: {}", msg);
            }
            co_return r.error();
        }

        offset += to_write;

        // Small pause to prevent CPU spinning on zero-byte writes
        if (to_write == 0) {
            using namespace std::chrono_literals;
            std::this_thread::sleep_for(1ms);
        }
    }

    co_return Result<void>();
}

Task<Result<void>> RequestHandler::write_header(AsyncSocket<AsyncIOContext>& socket,
                                                const Response& response, uint64_t request_id,
                                                bool flush) {
    // Create message envelope for response header
    Message response_msg;
    response_msg.version = PROTOCOL_VERSION;
    response_msg.requestId = request_id;
    response_msg.timestamp = std::chrono::steady_clock::now();
    response_msg.payload = response;

    return write_header_frame(socket, response_msg, flush);
}

Task<Result<void>> RequestHandler::write_chunk(AsyncSocket<AsyncIOContext>& socket,
                                               const Response& response, uint64_t request_id,
                                               bool last_chunk, bool flush) {
    // Create message envelope for response chunk
    Message response_msg;
    response_msg.version = PROTOCOL_VERSION;
    response_msg.requestId = request_id;
    response_msg.timestamp = std::chrono::steady_clock::now();
    response_msg.payload = response;

    return write_chunk_frame(socket, response_msg, last_chunk, flush);
}

Task<Result<void>> RequestHandler::stream_chunks(AsyncSocket<AsyncIOContext>& socket,
                                                 const Request& request, uint64_t request_id,
                                                 std::shared_ptr<RequestProcessor> processor,
                                                 ConnectionFsm* fsm) {
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

    // Send header frame
    auto header_result = co_await write_header(socket, headerResponse, request_id, true);
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

    while (!last_chunk_received) {
        auto chunk_result = co_await processor->next_chunk();
        last_chunk_received = chunk_result.is_last_chunk;

        // Send chunk
        auto write_result =
            co_await write_chunk(socket, chunk_result.data, request_id, last_chunk_received, true);
        if (!write_result) {
            co_return write_result.error();
        }
        if (fsm) {
            // Notify FSM; if this was the last chunk, transition towards Closing
            fsm->on_stream_next(last_chunk_received);
        }

        chunk_count++;
    }

    spdlog::debug("Sent {} chunks for streaming response (request_id={})", chunk_count, request_id);
    if (config_.close_after_response) {
        socket.close();
    }
    if (fsm) {
        fsm->on_close_request();
    }
    co_return Result<void>();
}

Task<Result<void>> RequestHandler::send_error(AsyncSocket<AsyncIOContext>& socket, ErrorCode code,
                                              const std::string& message) {
    ErrorResponse error{code, message};

    // Create message envelope
    Message error_msg;
    error_msg.version = PROTOCOL_VERSION;
    error_msg.requestId = 0;
    error_msg.timestamp = std::chrono::steady_clock::now();
    error_msg.payload = Response{error};

    co_return co_await write_message(socket, error_msg);
}

// ============================================================================
// RequestRouter Implementation
// ============================================================================

Task<Response> RequestRouter::process(const Request& request) {
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

Task<Response> MiddlewarePipeline::process(const Request& request) {
    if (middleware_.empty()) {
        if (final_handler_) {
            co_return co_await final_handler_(request);
        }
        co_return ErrorResponse{ErrorCode::NotImplemented, "No handler configured"};
    }

    // Build the middleware chain
    std::function<Task<Response>(const Request&)> chain = final_handler_;

    for (auto it = middleware_.rbegin(); it != middleware_.rend(); ++it) {
        auto middleware = *it;
        auto next = chain;
        chain = [middleware, next](const Request& req) -> Task<Response> {
            co_return co_await middleware->process(req, next);
        };
    }

    co_return co_await chain(request);
}

// ============================================================================
// LoggingMiddleware Implementation
// ============================================================================

Task<Response> LoggingMiddleware::process(const Request& request, Next next) {
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

Task<Response> RateLimitMiddleware::process(const Request& request, Next next) {
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

Task<Response> AuthMiddleware::process(const Request& request, Next next) {
    // Extract auth token from request metadata (placeholder)
    std::string token = ""; // TODO: Extract from request

    bool is_valid = co_await validator_(token);

    if (!is_valid) {
        co_return ErrorResponse{ErrorCode::Unauthorized, "Authentication failed"};
    }

    co_return co_await next(request);
}

} // namespace yams::daemon
