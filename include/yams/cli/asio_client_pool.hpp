#pragma once

#include <filesystem>
#include <mutex>
#include <optional>
#include <boost/asio/connect.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/local/stream_protocol.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/write.hpp>

#include <yams/core/types.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/daemon/ipc/message_framing.h>

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
        bool verbose = false;
    };

    AsioClientPool() : AsioClientPool(Config{}) {}

    explicit AsioClientPool(Config cfg)
        : cfg_(cfg), io_(std::make_shared<boost::asio::io_context>()) {}

    // Simple one-shot ping using a pooled connection
    yams::Result<void> ping();

    // Simple status call
    yams::Result<yams::daemon::StatusResponse> status();

    // Generic unary call helper mapping Request variant to typed Response
    template <typename TReq, typename TResp = yams::daemon::ResponseOfT<TReq>>
    yams::Result<TResp> call(const TReq& req) {
        auto conn = acquire();
        auto res = roundtrip(req, conn);
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

private:
    enum class State {
        Disconnected,
        Connected,
        WritingHeader,
        WritingChunks,
        ReadingHeader,
        ReadingBody,
        StreamingChunks,
        Closed
    };

    struct Connection {
        explicit Connection(std::shared_ptr<boost::asio::io_context> io)
            : strand(boost::asio::make_strand(*io)), socket(strand) {}

        boost::asio::strand<boost::asio::io_context::executor_type> strand;
        boost::asio::local::stream_protocol::socket socket;
        bool connected = false;
        State state = State::Disconnected;

        yams::Result<void> connect(const std::filesystem::path& socketPath);
        void close();
        bool is_open() const { return socket.is_open(); }
        bool can_write() const { return connected && (state == State::Connected); }
        bool can_read_header() const {
            return connected && (state == State::WritingHeader || state == State::Connected);
        }
    };

    // Acquire a connection (very lightweight initial impl: single shared connection guarded by
    // mutex)
    std::shared_ptr<Connection> acquire();

    // Send a framed request and receive a full framed response
    yams::Result<yams::daemon::Message> roundtrip(const yams::daemon::Request& req,
                                                  std::shared_ptr<Connection> conn);

private:
    Config cfg_{};
    std::shared_ptr<boost::asio::io_context> io_;
    std::mutex mtx_;
    std::weak_ptr<Connection> weakConn_;
};

} // namespace yams::cli
