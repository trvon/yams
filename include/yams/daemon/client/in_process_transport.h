#pragma once

#include <memory>

#include <yams/daemon/client/client_transport.h>
#include <yams/daemon/embedded_service_host.h>

namespace yams::daemon {

class InProcessTransport final : public IClientTransport {
public:
    explicit InProcessTransport(std::shared_ptr<EmbeddedServiceHost> host)
        : host_(std::move(host)) {}

    boost::asio::awaitable<Result<Response>> send_request(const Request& req) override;
    boost::asio::awaitable<Result<Response>> send_request(Request&& req) override;
    boost::asio::awaitable<Result<void>>
    send_request_streaming(const Request& req, HeaderCallback onHeader, ChunkCallback onChunk,
                           ErrorCallback onError, CompleteCallback onComplete) override;

private:
    std::shared_ptr<EmbeddedServiceHost> host_;
};

} // namespace yams::daemon
