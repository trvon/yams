#pragma once

#include <memory>

#if __has_include(<yams/daemon/client/client_transport.h>)
#include <yams/daemon/client/client_transport.h>
#elif __has_include("yams/daemon/client/client_transport.h")
#include "yams/daemon/client/client_transport.h"
#else
#include "client_transport.h"
#endif
#if __has_include(<yams/daemon/embedded_service_host.h>)
#include <yams/daemon/embedded_service_host.h>
#elif __has_include("yams/daemon/embedded_service_host.h")
#include "yams/daemon/embedded_service_host.h"
#else
#include "../embedded_service_host.h"
#endif

namespace yams::daemon {

class InProcessTransport final : public IClientTransport {
public:
    explicit InProcessTransport(std::shared_ptr<EmbeddedServiceHost> host)
        : host_(std::move(host)) {}

    boost::asio::awaitable<Result<Response>, boost::asio::any_io_executor>
    send_request(Request request) override;
    boost::asio::awaitable<Result<void>, boost::asio::any_io_executor>
    send_request_streaming(Request request, const HeaderCallback& onHeader,
                           const ChunkCallback& onChunk, const ErrorCallback& onError,
                           const CompleteCallback& onComplete) override;

private:
    std::shared_ptr<EmbeddedServiceHost> host_;
};

} // namespace yams::daemon
