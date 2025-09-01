#pragma once

#include <filesystem>
#include <mutex>
#include <optional>
#include <memory>
#include <chrono>
#include <variant>
#include <span>

#include <yams/core/types.h>
#include <yams/daemon/ipc/response_of.hpp>
#include <yams/daemon/ipc/message_framing.h>

// Forward-declare Task to avoid requiring coroutine support in all includers
namespace yams { template <typename T> class Task; }

// Forward declarations for sync helpers are present in impl; no heavy async includes here

namespace yams::cli {

// Minimal Boost.Asio-based client pool focused on Ping/Status for first wiring.
class AsioClientPool {
public:
    struct Config {
        std::size_t min_clients = 1;
        std::size_t max_clients = 8;
        std::chrono::milliseconds request_timeout{5000};
        std::chrono::milliseconds header_timeout{30000};
        std::chrono::milliseconds body_timeout{60000};
    std::filesystem::path socketPath; // Optional override; empty = resolve automatically
        bool verbose = false;
    };

    AsioClientPool() = default;

    explicit AsioClientPool(Config cfg) : cfg_(std::move(cfg)) {}

    // Simple one-shot ping using a pooled connection
    yams::Result<void> ping();

    // Simple status call
    yams::Result<yams::daemon::StatusResponse> status();

    // Generic unary call helper mapping Request variant to typed Response
    template <typename TReq, typename TResp = yams::daemon::ResponseOfT<TReq>>
    yams::Result<TResp> call(const TReq& req) {
    yams::daemon::Request vreq = req; // construct variant explicitly
    auto res = roundtrip(vreq);
        if (!res)
            return res.error();
        auto& msg = res.value();
        if (auto* resp = std::get_if<yams::daemon::Response>(&msg.payload)) {
            if (auto* ok = std::get_if<TResp>(resp))
                return *ok;
            if (auto* er = std::get_if<yams::daemon::ErrorResponse>(resp))
                return Error{er->code, er->message};
            return Error{ErrorCode::InvalidData, "Unexpected response variant"};
        }
        return Error{ErrorCode::InvalidData, "Unexpected message kind"};
    }

    // ===== Async (coroutine) API =====
    // Non-blocking ping
    yams::Task<yams::Result<void>> async_ping();

    // Non-blocking status
    yams::Task<yams::Result<yams::daemon::StatusResponse>> async_status();

    // Non-blocking generic unary call (variant-based). Callers can downcast to specific type.
    yams::Task<yams::Result<yams::daemon::Response>> async_call_variant(
        const yams::daemon::Request& req);

private:
    // Send a framed request and receive a full framed response
    yams::Result<yams::daemon::Message> roundtrip(const yams::daemon::Request& req);

private:
    Config cfg_{};
};

} // namespace yams::cli
