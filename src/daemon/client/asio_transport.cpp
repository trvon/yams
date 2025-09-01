#include <yams/daemon/client/asio_transport.h>
#include <yams/daemon/client/global_io_context.h>

#include <span>


#include <spdlog/spdlog.h>
#include <cstring>
#include <thread>
#include <vector>
#include <atomic>
#include <optional>
#include <chrono>
#include <mutex>
#include <memory>

#ifndef _WIN32
#include <fcntl.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>
#endif

namespace yams::daemon {





AsioTransportAdapter::AsioTransportAdapter(const Options& opts) : opts_(opts) {}

Task<Result<Response>> AsioTransportAdapter::send_request(const Request& req) {
    fsm_.on_connect(-1); // Placeholder fd

#ifndef _WIN32
    auto& ioctx = yams::daemon::GlobalIOContext::instance().get_io_context();
    // Connect with per-request timeout using AsyncIOContext
    auto fdres = co_await ioctx.async_connect_unix(opts_.socketPath, opts_.requestTimeout);
    if (!fdres) {
        fsm_.on_error(static_cast<int>(fdres.error().code));
        co_return fdres.error();
    }
    int fd = fdres.value();
    fsm_.on_connect(fd);
    DefaultAsyncSocket sock(fd, ioctx);

    Message msg;
    msg.version = PROTOCOL_VERSION;
    msg.requestId =
        static_cast<uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count());
    msg.timestamp = std::chrono::steady_clock::now();
    msg.payload = req;
    msg.clientVersion = "yams-client-0.3.4";

    MessageFramer framer;
    auto framed = framer.frame_message(msg);
    if (!framed) {
        fsm_.on_error(static_cast<int>(ErrorCode::InvalidData));
        co_return framed.error();
    }

    fsm_.on_writable(0); // Transition to writing
    auto wres =
        co_await sock.async_write_all(std::span<const uint8_t>(framed.value().data(), framed.value().size()));
    if (!wres) {
        fsm_.on_error(static_cast<int>(wres.error().code));
        co_return wres.error();
    }

    fsm_.on_readable(0); // Transition to reading
    std::vector<uint8_t> headerBuf(sizeof(MessageFramer::FrameHeader));
    auto r1 = co_await sock.async_read_exact(headerBuf.size());
    if (!r1) {
        fsm_.on_error(static_cast<int>(r1.error().code));
        co_return r1.error();
    }

    MessageFramer::FrameHeader netHeader;
    std::memcpy(&netHeader, r1.value().data(), sizeof(MessageFramer::FrameHeader));
    MessageFramer::FrameHeader header = netHeader;
    header.from_network();
    {
        ConnectionFsm::FrameInfo info{0u, header.flags, header.payload_size};
        fsm_.on_header_parsed(info);
    }

    if (!header.is_valid()) {
        fsm_.on_error(static_cast<int>(ErrorCode::InvalidData));
        co_return Error{ErrorCode::InvalidData, "Invalid frame header"};
    }

    auto r2 = co_await sock.async_read_exact(header.payload_size);
    if (!r2) {
        fsm_.on_error(static_cast<int>(r2.error().code));
        co_return r2.error();
    }
    fsm_.on_body_parsed();

    std::vector<uint8_t> complete;
    complete.reserve(sizeof(MessageFramer::FrameHeader) + header.payload_size);
    complete.insert(complete.end(), r1.value().begin(), r1.value().end());
    complete.insert(complete.end(), r2.value().begin(), r2.value().end());

    auto parsed = framer.parse_frame(complete);

    if (!parsed) {
        fsm_.on_error(static_cast<int>(parsed.error().code));
        co_return parsed.error();
    }
    if (!std::holds_alternative<Response>(parsed.value().payload)) {
        fsm_.on_error(static_cast<int>(ErrorCode::InvalidData));
        co_return Error{ErrorCode::InvalidData, "Expected response frame"};
    }
    auto out = std::get<Response>(parsed.value().payload);
    fsm_.on_response_complete(true);
    co_return out;
#else
    fsm_.on_error(ErrorCode::InternalError);
    co_return Error{ErrorCode::InternalError, "Asio transport not supported on Windows"};
#endif
}

Task<Result<void>> AsioTransportAdapter::send_request_streaming(
    const Request& req, AsioTransportAdapter::HeaderCallback onHeader,
    AsioTransportAdapter::ChunkCallback onChunk, AsioTransportAdapter::ErrorCallback onError,
    AsioTransportAdapter::CompleteCallback onComplete) {
    fsm_.on_connect(-1); // Placeholder fd
#ifndef _WIN32
    auto& ioctx = yams::daemon::GlobalIOContext::instance().get_io_context();
    // Connect with per-request timeout using AsyncIOContext
    auto fdres = co_await ioctx.async_connect_unix(opts_.socketPath, opts_.requestTimeout);
    if (!fdres) {
        fsm_.on_error(static_cast<int>(fdres.error().code));
        co_return fdres.error();
    }
    int fd = fdres.value();
    fsm_.on_connect(fd);
    DefaultAsyncSocket sock(fd, ioctx);

    Message msg;
    msg.version = PROTOCOL_VERSION;
    msg.requestId =
        static_cast<uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count());
    msg.timestamp = std::chrono::steady_clock::now();
    msg.payload = req;
    msg.clientVersion = "yams-client-0.3.4";

    MessageFramer framer;
    auto framed = framer.frame_message(msg);
    if (!framed) {
        fsm_.on_error(static_cast<int>(ErrorCode::InvalidData));
        co_return framed.error();
    }

    fsm_.on_writable(0);
    auto wres =
        co_await sock.async_write_all(std::span<const uint8_t>(framed.value().data(), framed.value().size()));
    if (!wres) {
        fsm_.on_error(static_cast<int>(wres.error().code));
        co_return wres.error();
    }

    fsm_.on_readable(0);
    std::vector<uint8_t> headerBuf(sizeof(MessageFramer::FrameHeader));
    auto r1 = co_await sock.async_read_exact(headerBuf.size());
    if (!r1) {
        fsm_.on_error(static_cast<int>(r1.error().code));
        co_return r1.error();
    }

    MessageFramer::FrameHeader header;
    std::memcpy(&header, r1.value().data(), sizeof(MessageFramer::FrameHeader));
    header.from_network();
    {
        ConnectionFsm::FrameInfo info{0u, header.flags, header.payload_size};
        fsm_.on_header_parsed(info);
    }
    if (!header.is_valid()) {
        fsm_.on_error(static_cast<int>(ErrorCode::InvalidData));
        co_return Error{ErrorCode::InvalidData, "Invalid frame header"};
    }

    Response headerResponse;
    bool haveParsedHeaderPayload = false;
    if (!header.is_header_only()) {
        auto r2 = co_await sock.async_read_exact(header.payload_size);
        if (!r2) {
            fsm_.on_error(static_cast<int>(r2.error().code));
            co_return r2.error();
        }
        fsm_.on_body_parsed();
        std::vector<uint8_t> complete;
        complete.reserve(sizeof(MessageFramer::FrameHeader) + header.payload_size);
        complete.insert(complete.end(), r1.value().begin(), r1.value().end());
        complete.insert(complete.end(), r2.value().begin(), r2.value().end());
        auto parsed = framer.parse_frame(complete);
        if (!parsed) {
            fsm_.on_error(static_cast<int>(parsed.error().code));
            co_return parsed.error();
        }
        if (!std::holds_alternative<Response>(parsed.value().payload)) {
            fsm_.on_error(static_cast<int>(ErrorCode::InvalidData));
            co_return Error{ErrorCode::InvalidData, "Expected response"};
        }
        headerResponse = std::get<Response>(parsed.value().payload);
        haveParsedHeaderPayload = true;
        if (auto* er = std::get_if<ErrorResponse>(&headerResponse)) {
            onError(Error{er->code, er->message});
            fsm_.on_error(static_cast<int>(er->code));
            co_return Error{er->code, er->message};
        }
    } else {
        // Synthesize minimal header based on request type
        if (std::holds_alternative<SearchRequest>(req)) {
            headerResponse = SearchResponse{};
        } else if (std::holds_alternative<ListRequest>(req)) {
            headerResponse = ListResponse{};
        } else if (std::holds_alternative<GrepRequest>(req)) {
            headerResponse = GrepResponse{};
        } else if (std::holds_alternative<AddDocumentRequest>(req)) {
            headerResponse = SuccessResponse{"Streaming"};
        } else {
            headerResponse = SuccessResponse{"Streaming"};
        }
    }

    onHeader(headerResponse);

    if (!header.is_chunked() || header.is_last_chunk()) {
        if (haveParsedHeaderPayload) {
            onChunk(headerResponse, true);
        }
        onComplete();
        fsm_.on_response_complete(true);
        co_return Result<void>();
    }

    fsm_.on_stream_next(false);
    bool last = false;
    while (!last) {
        std::vector<uint8_t> chunkHeaderBuf(sizeof(MessageFramer::FrameHeader));
        auto ch = co_await sock.async_read_exact(chunkHeaderBuf.size());
        if (!ch) {
            onError(ch.error());
            fsm_.on_error(static_cast<int>(ch.error().code));
            co_return ch.error();
        }
        MessageFramer::FrameHeader chdr;
        std::memcpy(&chdr, ch.value().data(), sizeof(MessageFramer::FrameHeader));
        chdr.from_network();
        {
            ConnectionFsm::FrameInfo info{0u, chdr.flags, chdr.payload_size};
            fsm_.on_header_parsed(info);
        }
        if (!chdr.is_valid() || !chdr.is_chunked()) {
            Error e{ErrorCode::InvalidData, "Invalid chunk header"};
            onError(e);
            fsm_.on_error(static_cast<int>(e.code));
            co_return e;
        }
        last = chdr.is_last_chunk();
        auto cpl = co_await sock.async_read_exact(chdr.payload_size);
        if (!cpl) {
            onError(cpl.error());
            fsm_.on_error(static_cast<int>(cpl.error().code));
            co_return cpl.error();
        }
        fsm_.on_body_parsed();
        std::vector<uint8_t> complete;
        complete.reserve(sizeof(MessageFramer::FrameHeader) + chdr.payload_size);
        complete.insert(complete.end(), ch.value().begin(), ch.value().end());
        complete.insert(complete.end(), cpl.value().begin(), cpl.value().end());
        auto parsed = framer.parse_frame(complete);
        if (!parsed) {
            onError(parsed.error());
            fsm_.on_error(static_cast<int>(parsed.error().code));
            co_return parsed.error();
        }
        if (!std::holds_alternative<Response>(parsed.value().payload)) {
            Error e{ErrorCode::InvalidData, "Expected response in chunk"};
            onError(e);
            fsm_.on_error(static_cast<int>(e.code));
            co_return e;
        }
        auto resp = std::get<Response>(parsed.value().payload);
        bool cont = onChunk(resp, last);
        if (!cont)
            break;
        fsm_.on_stream_next(last);
    }

    onComplete();
    fsm_.on_response_complete(true);
    co_return Result<void>();
#else
    fsm_.on_error(ErrorCode::InternalError);
    co_return Error{ErrorCode::InternalError, "Asio transport not supported on Windows"};
#endif
}

} // namespace yams::daemon