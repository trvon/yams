#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <thread>

#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <yams/app/services/document_ingestion_service.h>
#include <yams/app/services/retrieval_service.h>
#include <yams/daemon/client/client_transport.h>

namespace {

using namespace std::chrono_literals;

class DelayedTransport final : public yams::daemon::IClientTransport {
public:
    explicit DelayedTransport(std::chrono::milliseconds delay)
        : delay_(delay), completedFuture_(completed_.get_future().share()) {}

    boost::asio::awaitable<yams::Result<yams::daemon::Response>, boost::asio::any_io_executor>
    send_request(yams::daemon::Request request) override {
        auto executor = co_await boost::asio::this_coro::executor;
        boost::asio::steady_timer timer(executor, delay_);
        co_await timer.async_wait(boost::asio::use_awaitable);
        complete();

        if (std::holds_alternative<yams::daemon::DeleteRequest>(request)) {
            co_return yams::daemon::DeleteResponse{};
        }
        co_return yams::daemon::StatusResponse{};
    }

    boost::asio::awaitable<yams::Result<void>, boost::asio::any_io_executor>
    send_request_streaming(yams::daemon::Request, const HeaderCallback&, const ChunkCallback&,
                           const ErrorCallback&, const CompleteCallback& onComplete) override {
        auto executor = co_await boost::asio::this_coro::executor;
        boost::asio::steady_timer timer(executor, delay_);
        co_await timer.async_wait(boost::asio::use_awaitable);
        if (onComplete) {
            onComplete();
        }
        complete();
        co_return yams::Result<void>{};
    }

    std::shared_future<void> completedFuture() const { return completedFuture_; }

private:
    void complete() {
        bool expected = false;
        if (completedSet_.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
            completed_.set_value();
        }
    }

    std::chrono::milliseconds delay_;
    std::promise<void> completed_;
    std::shared_future<void> completedFuture_;
    std::atomic<bool> completedSet_{false};
};

struct IoRunner {
    IoRunner() : work_(boost::asio::make_work_guard(io_)), thread_([this] { io_.run(); }) {}

    ~IoRunner() {
        work_.reset();
        io_.stop();
    }

    boost::asio::io_context io_;
    boost::asio::executor_work_guard<boost::asio::io_context::executor_type> work_;
    std::jthread thread_;
};

} // namespace

TEST_CASE("RetrievalService returns at its request deadline while owned work drains",
          "[app][service][deadline]") {
    IoRunner runner;
    auto transport = std::make_shared<DelayedTransport>(250ms);

    yams::app::services::RetrievalOptions options;
    options.executor = runner.io_.get_executor();
    options.transport = transport;
    options.transportMode = yams::daemon::ClientTransportMode::InProcess;
    options.autoStart = false;
    options.requestTimeoutMs = 20;

    yams::app::services::RetrievalService service;
    const auto started = std::chrono::steady_clock::now();
    const auto result = service.status(options);
    const auto elapsed = std::chrono::steady_clock::now() - started;

    REQUIRE_FALSE(result);
    CHECK(result.error().code == yams::ErrorCode::Timeout);
    CHECK(elapsed < 150ms);
    REQUIRE(transport->completedFuture().wait_for(1s) == std::future_status::ready);
}

TEST_CASE("DocumentIngestionService delete returns at its request deadline",
          "[app][service][deadline]") {
    IoRunner runner;
    auto transport = std::make_shared<DelayedTransport>(250ms);

    yams::daemon::ClientConfig config;
    config.executor = runner.io_.get_executor();
    config.transport = transport;
    config.transportMode = yams::daemon::ClientTransportMode::InProcess;
    config.autoStart = false;
    auto client = std::make_shared<yams::daemon::DaemonClient>(config);

    yams::app::services::DeleteOptions options;
    options.hashes = {"deadline-test"};
    options.timeoutMs = 20;

    yams::app::services::DocumentIngestionService service(std::move(client));
    const auto started = std::chrono::steady_clock::now();
    const auto result = service.deleteDocument(options);
    const auto elapsed = std::chrono::steady_clock::now() - started;

    REQUIRE_FALSE(result);
    CHECK(result.error().code == yams::ErrorCode::Timeout);
    CHECK(elapsed < 150ms);
    REQUIRE(transport->completedFuture().wait_for(1s) == std::future_status::ready);
}
