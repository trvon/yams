#pragma once

#include <atomic>
#include <filesystem>
#include <memory>
#include <thread>
#include <yams/core/types.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/ipc/async_socket.h>
#include <yams/daemon/ipc/request_handler.h>

namespace yams::daemon {

class RequestDispatcher;

// Simple UNIX domain socket acceptor that feeds connections into RequestHandler
class UnixIpcServer {
public:
    struct Config {
        std::filesystem::path socketPath;
        int backlog{128};
        // Socket file permissions (umask will be applied on process); default 0660
        unsigned int permission_octal{0660};
        // Request handler config
        RequestHandler::Config handlerCfg{};
    };

    UnixIpcServer(const Config& cfg, RequestDispatcher* dispatcher, StateComponent* state);
    ~UnixIpcServer();

    Result<void> start();
    Result<void> stop();

    bool is_running() const { return running_.load(); }
    std::size_t active_connections() const { return active_.load(); }

private:
    struct DispatcherProcessor : public RequestProcessor {
        explicit DispatcherProcessor(RequestDispatcher* d) : dispatcher(d) {}
        Task<Response> process(const Request& request) override;
        RequestDispatcher* dispatcher{nullptr};
    };

    // Internal helpers
    Result<void> bind_and_listen();
    void accept_loop(std::stop_token token);
    void handle_client(int client_fd, std::stop_token token);

    Config cfg_{};
    RequestDispatcher* dispatcher_{nullptr};
    StateComponent* state_{nullptr};

    int listen_fd_{-1};
    std::atomic<bool> running_{false};
    std::jthread accept_thread_{};

    // Shared IO context for all connections
    std::unique_ptr<AsyncIOContext> io_{};
    std::jthread io_thread_{};

    std::atomic<std::size_t> active_{0};
};

} // namespace yams::daemon
