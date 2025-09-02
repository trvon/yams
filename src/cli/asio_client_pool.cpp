#include <yams/cli/asio_client_pool.hpp>

#include <coroutine>
#include <spdlog/spdlog.h>
#include <yams/core/task.h>
#include <yams/daemon/client/asio_transport.h>
#include <yams/daemon/client/daemon_client.h>

namespace yams::cli {

using yams::daemon::Message;
using yams::daemon::MessageFramer;
using yams::daemon::PingRequest;
using yams::daemon::PongResponse;
using yams::daemon::Request;
using yams::daemon::Response;
using yams::daemon::StatusRequest;
using yams::daemon::StatusResponse;

yams::Task<yams::Result<yams::daemon::Response>>
AsioClientPool::async_call_variant(const yams::daemon::Request& req) {
    using namespace yams::daemon;
    auto sock =
        cfg_.socketPath.empty() ? yams::daemon::DaemonClient::resolveSocketPath() : cfg_.socketPath;
    if (sock.empty()) {
        co_return Error{ErrorCode::InvalidState, "Could not resolve daemon socket path"};
    }
    AsioTransportAdapter::Options opts{};
    opts.socketPath = sock;
    opts.headerTimeout = cfg_.header_timeout;
    opts.bodyTimeout = cfg_.body_timeout;
    opts.requestTimeout = cfg_.request_timeout;
    AsioTransportAdapter transport(opts);
    auto r = co_await transport.send_request(req);
    co_return r;
}

yams::Task<yams::Result<void>> AsioClientPool::async_ping() {
    using namespace yams::daemon;
    Request req = PingRequest{};
    auto sock =
        cfg_.socketPath.empty() ? yams::daemon::DaemonClient::resolveSocketPath() : cfg_.socketPath;
    if (sock.empty()) {
        co_return Error{ErrorCode::InvalidState, "Could not resolve daemon socket path"};
    }
    AsioTransportAdapter::Options opts{};
    opts.socketPath = sock;
    opts.headerTimeout = cfg_.header_timeout;
    opts.bodyTimeout = cfg_.body_timeout;
    opts.requestTimeout = cfg_.request_timeout;
    AsioTransportAdapter transport(opts);
    auto r = co_await transport.send_request(req);
    if (!r) {
        co_return r.error();
    }
    auto& resp = r.value();
    if (std::holds_alternative<PongResponse>(resp)) {
        co_return Result<void>();
    }
    if (auto* er = std::get_if<ErrorResponse>(&resp)) {
        co_return Error{er->code, er->message};
    }
    co_return Error{ErrorCode::InvalidData, "Unexpected response variant for Ping"};
}

yams::Task<yams::Result<StatusResponse>> AsioClientPool::async_status() {
    using namespace yams::daemon;
    Request req = StatusRequest{};
    auto sock =
        cfg_.socketPath.empty() ? yams::daemon::DaemonClient::resolveSocketPath() : cfg_.socketPath;
    if (sock.empty()) {
        co_return Error{ErrorCode::InvalidState, "Could not resolve daemon socket path"};
    }
    AsioTransportAdapter::Options opts{};
    opts.socketPath = sock;
    opts.headerTimeout = cfg_.header_timeout;
    opts.bodyTimeout = cfg_.body_timeout;
    opts.requestTimeout = cfg_.request_timeout;
    AsioTransportAdapter transport(opts);
    auto r = co_await transport.send_request(req);
    if (!r) {
        co_return r.error();
    }
    auto& resp = r.value();
    if (auto* ok = std::get_if<StatusResponse>(&resp)) {
        co_return *ok;
    }
    if (auto* er = std::get_if<ErrorResponse>(&resp)) {
        co_return Error{er->code, er->message};
    }
    co_return Error{ErrorCode::InvalidData, "Unexpected response variant for Status"};
}



} // namespace yams::cli
