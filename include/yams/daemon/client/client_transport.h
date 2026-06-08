#pragma once

#include <functional>

namespace boost {
namespace asio {
class any_io_executor;
template <typename T, typename Executor> class awaitable;
} // namespace asio
} // namespace boost

#if __has_include(<yams/core/types.h>)
#include <yams/core/types.h>
#elif __has_include("yams/core/types.h")
#include "yams/core/types.h"
#else
#include "../../core/types.h"
#endif
#if __has_include(<yams/daemon/ipc/ipc_protocol.h>)
#include <yams/daemon/ipc/ipc_protocol.h>
#elif __has_include("yams/daemon/ipc/ipc_protocol.h")
#include "yams/daemon/ipc/ipc_protocol.h"
#else
#include "../ipc/ipc_protocol.h"
#endif

namespace yams::daemon {

class IClientTransport {
public:
    using HeaderCallback = std::function<void(const Response&)>;
    using ChunkCallback = std::function<bool(const Response&, bool)>;
    using ErrorCallback = std::function<void(const Error&)>;
    using CompleteCallback = std::function<void()>;

    virtual ~IClientTransport() = default;

    virtual boost::asio::awaitable<Result<Response>, boost::asio::any_io_executor>
    send_request(Request req) = 0;

    virtual boost::asio::awaitable<Result<void>, boost::asio::any_io_executor>
    send_request_streaming(Request req, const HeaderCallback& onHeader,
                           const ChunkCallback& onChunk, const ErrorCallback& onError,
                           const CompleteCallback& onComplete) = 0;
};

} // namespace yams::daemon
