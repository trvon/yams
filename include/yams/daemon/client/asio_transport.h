#pragma once

#include <filesystem>
#include <functional>
#include <memory>
#include <optional>
#include <yams/core/types.h>
#include <yams/daemon/ipc/async_socket.h>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/daemon/ipc/message_framing.h>

namespace yams::daemon {

// A minimal transport that uses the existing AsyncIOContext/AsyncSocket to
// send framed requests and receive framed responses (unary and streaming).
class AsioTransportAdapter {
public:
    struct Options {
        std::filesystem::path socketPath;
        std::chrono::milliseconds headerTimeout{30000};
        std::chrono::milliseconds bodyTimeout{60000};
        std::chrono::milliseconds requestTimeout{5000};
    };

    static Result<Response> send_request(const Request& req, const Options& opts);

    using HeaderCallback = std::function<void(const Response&)>;
    using ChunkCallback = std::function<bool(const Response&, bool)>;
    using ErrorCallback = std::function<void(const Error&)>;
    using CompleteCallback = std::function<void()>;

    static Result<void> send_request_streaming(const Request& req, const Options& opts,
                                               HeaderCallback onHeader, ChunkCallback onChunk,
                                               ErrorCallback onError, CompleteCallback onComplete);
};

} // namespace yams::daemon
