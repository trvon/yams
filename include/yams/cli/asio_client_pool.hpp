#pragma once

#include <chrono>
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <variant>

#include <yams/core/types.h>
#include <yams/daemon/ipc/message_framing.h>
#include <yams/daemon/ipc/response_of.hpp>

// Forward-declare Task to avoid requiring coroutine support in all includers
namespace yams {
template <typename T> class Task;
}

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

    // ===== Async (coroutine) API =====
    // Non-blocking ping
    yams::Task<yams::Result<void>> async_ping();

    // Non-blocking status
    yams::Task<yams::Result<yams::daemon::StatusResponse>> async_status();

    // Non-blocking generic unary call (variant-based). Callers can downcast to specific type.
    yams::Task<yams::Result<yams::daemon::Response>>
    async_call_variant(const yams::daemon::Request& req);

    // Generic async unary call helper mapping Request variant to typed Response
    template <typename TReq, typename TResp = yams::daemon::ResponseOfT<TReq>>
    yams::Task<yams::Result<TResp>> async_call(const TReq& req) {
        yams::daemon::Request vreq = req; // construct variant explicitly
        auto res = co_await async_call_variant(vreq);
        if (!res)
            co_return res.error();
        auto& resp = res.value();
        if (auto* ok = std::get_if<TResp>(&resp))
            co_return *ok;
        if (auto* er = std::get_if<yams::daemon::ErrorResponse>(&resp))
            co_return Error{er->code, er->message};
        co_return Error{ErrorCode::InvalidData, "Unexpected response variant"};
    }

private:
    Config cfg_{};
};

} // namespace yams::cli
