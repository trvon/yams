#pragma once

#include <functional>

#include <boost/asio/awaitable.hpp>

#include <yams/core/types.h>
#include <yams/daemon/ipc/ipc_protocol.h>

namespace yams::daemon {

class IClientTransport {
public:
    using HeaderCallback = std::function<void(const Response&)>;
    using ChunkCallback = std::function<bool(const Response&, bool)>;
    using ErrorCallback = std::function<void(const Error&)>;
    using CompleteCallback = std::function<void()>;

    virtual ~IClientTransport() = default;

    virtual boost::asio::awaitable<Result<Response>> send_request(const Request& req) = 0;
    virtual boost::asio::awaitable<Result<Response>> send_request(Request&& req) = 0;

    virtual boost::asio::awaitable<Result<void>>
    send_request_streaming(const Request& req, HeaderCallback onHeader, ChunkCallback onChunk,
                           ErrorCallback onError, CompleteCallback onComplete) = 0;
};

} // namespace yams::daemon
