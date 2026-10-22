// SPDX-License-Identifier: GPL-3.0-or-later

// pi-lens-ignore: fatal error
#include <catch2/catch_test_macros.hpp>

#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/ipc/message_framing.h>
#include <yams/daemon/ipc/request_context_registry.h>
#include <yams/daemon/ipc/request_handler.h>

#include <boost/asio.hpp>

#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <stdexcept>
#include <thread>
#include <vector>

using namespace yams::daemon;

namespace {

class CountingProcessor final : public RequestProcessor {
public:
    boost::asio::awaitable<Response> process(const Request&) override {
        unary_calls_++;
        StatusResponse s{};
        s.running = true;
        s.ready = false;
        s.version = "test";
        s.overallStatus = "initializing";
        co_return Response{std::in_place_type<StatusResponse>, s};
    }

    boost::asio::awaitable<std::optional<Response>> process_streaming(const Request&) override {
        streaming_calls_++;
        co_return std::optional<Response>{std::nullopt};
    }

    bool supports_streaming(const Request&) const override { return true; }

    int unary_calls() const { return unary_calls_.load(); }
    int streaming_calls() const { return streaming_calls_.load(); }

private:
    std::atomic<int> unary_calls_{0};
    std::atomic<int> streaming_calls_{0};
};

class ListOneShotProcessor final : public RequestProcessor {
public:
    boost::asio::awaitable<Response> process(const Request&) override { co_return makeResponse(); }

    boost::asio::awaitable<std::optional<Response>> process_streaming(const Request&) override {
        co_return makeResponse();
    }

    bool supports_streaming(const Request&) const override { return true; }

private:
    static Response makeResponse() {
        ListResponse response;
        response.totalCount = 2;
        response.items = {
            ListEntry{.hash = "hash-a", .path = "/docs/a.txt", .fileName = "a.txt"},
            ListEntry{.hash = "hash-b", .path = "/docs/b.txt", .fileName = "b.txt"},
        };
        return Response{std::in_place_type<ListResponse>, std::move(response)};
    }
};

class SearchOneShotProcessor final : public RequestProcessor {
public:
    boost::asio::awaitable<Response> process(const Request&) override { co_return makeResponse(); }

    boost::asio::awaitable<std::optional<Response>> process_streaming(const Request&) override {
        co_return makeResponse();
    }

    bool supports_streaming(const Request&) const override { return true; }

private:
    static Response makeResponse() {
        SearchResponse response;
        response.totalCount = 2;
        response.results = {
            SearchResult{.id = "hash-a", .path = "/docs/a.txt", .score = 1.0},
            SearchResult{.id = "hash-b", .path = "/docs/b.txt", .score = 0.5},
        };
        return Response{std::in_place_type<SearchResponse>, std::move(response)};
    }
};

class EntrySignal {
public:
    void notify() {
        std::lock_guard<std::mutex> lock(mutex_);
        ++count_;
        cv_.notify_all();
    }

    bool waitFor(std::size_t count, std::chrono::milliseconds timeout) {
        std::unique_lock<std::mutex> lock(mutex_);
        return cv_.wait_for(lock, timeout, [&] { return count_ >= count; });
    }

private:
    std::mutex mutex_;
    std::condition_variable cv_;
    std::size_t count_{0};
};

class AsyncGate {
public:
    boost::asio::awaitable<void> wait() {
        auto executor = co_await boost::asio::this_coro::executor;
        auto timer = std::make_shared<boost::asio::steady_timer>(executor);
        timer->expires_at(boost::asio::steady_timer::time_point::max());
        {
            std::lock_guard<std::mutex> lock(mutex_);
            ++entered_;
            cv_.notify_all();
            if (released_) {
                co_return;
            }
            waiters_.push_back(timer);
        }

        // This gate is used only by the harness's single io_context runner. A posted release
        // cannot cancel this timer before async_wait is initiated on that same executor.
        boost::system::error_code error;
        co_await timer->async_wait(boost::asio::redirect_error(boost::asio::use_awaitable, error));
    }

    bool waitForEntrants(std::size_t count, std::chrono::milliseconds timeout) {
        std::unique_lock<std::mutex> lock(mutex_);
        return cv_.wait_for(lock, timeout, [&] { return entered_ >= count; });
    }

    void release() {
        std::vector<std::shared_ptr<boost::asio::steady_timer>> waiters;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            released_ = true;
            waiters.swap(waiters_);
        }
        for (const auto& timer : waiters) {
            boost::asio::post(timer->get_executor(), [timer] { (void)timer->cancel(); });
        }
    }

private:
    std::mutex mutex_;
    std::condition_variable cv_;
    bool released_{false};
    std::size_t entered_{0};
    std::vector<std::shared_ptr<boost::asio::steady_timer>> waiters_;
};

class RequestLifecycleSignal {
public:
    void onJob(bool started) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (started) {
            ++started_;
        } else {
            ++finished_;
        }
        cv_.notify_all();
    }

    bool waitForStarted(std::size_t count, std::chrono::milliseconds timeout) {
        std::unique_lock<std::mutex> lock(mutex_);
        return cv_.wait_for(lock, timeout, [&] { return started_ >= count; });
    }

    bool waitForFinished(std::size_t count, std::chrono::milliseconds timeout) {
        std::unique_lock<std::mutex> lock(mutex_);
        return cv_.wait_for(lock, timeout, [&] { return finished_ >= count; });
    }

private:
    std::mutex mutex_;
    std::condition_variable cv_;
    std::size_t started_{0};
    std::size_t finished_{0};
};

class BlockingActivityProcessor final : public RequestProcessor {
public:
    boost::asio::awaitable<Response> process(const Request& request) override {
        if (std::holds_alternative<StatusRequest>(request)) {
            co_await healthGate_.wait();
            StatusResponse response{};
            response.running = true;
            response.ready = true;
            response.version = "test";
            response.overallStatus = "ready";
            co_return Response{std::in_place_type<StatusResponse>, std::move(response)};
        }

        unaryEntered_.fetch_add(1, std::memory_order_release);
        co_await queryGate_.wait();
        co_return makeSearchResponse();
    }

    boost::asio::awaitable<std::optional<Response>>
    process_streaming(const Request& request) override {
        if (std::holds_alternative<SearchRequest>(request)) {
            streamingEntered_.fetch_add(1, std::memory_order_release);
            co_await queryGate_.wait();
            co_return makeSearchResponse();
        }
        co_return co_await RequestProcessor::process_streaming(request);
    }

    bool supports_streaming(const Request& request) const override {
        return std::holds_alternative<SearchRequest>(request);
    }

    [[nodiscard]] int unaryEntered() const { return unaryEntered_.load(std::memory_order_acquire); }
    [[nodiscard]] int streamingEntered() const {
        return streamingEntered_.load(std::memory_order_acquire);
    }
    AsyncGate& queryGate() { return queryGate_; }
    AsyncGate& healthGate() { return healthGate_; }
    void releaseAll() {
        healthGate_.release();
        queryGate_.release();
    }

private:
    static Response makeSearchResponse() {
        SearchResponse response;
        response.totalCount = 0;
        return Response{std::in_place_type<SearchResponse>, std::move(response)};
    }

    AsyncGate queryGate_;
    AsyncGate healthGate_;
    std::atomic<int> unaryEntered_{0};
    std::atomic<int> streamingEntered_{0};
};

class CancelableSearchProcessor final : public RequestProcessor {
public:
    boost::asio::awaitable<Response> process(const Request& request) override {
        const auto* search = std::get_if<SearchRequest>(&request);
        cancellationSignalPresent_.store(search && search->cancellationSignal,
                                         std::memory_order_release);
        cancellationSignal_ = search ? search->cancellationSignal : nullptr;
        co_await gate_.wait();
        if (cancellationSignal_ && cancellationSignal_->load(std::memory_order_acquire)) {
            cancellationObserved_.notify();
        }
        SearchResponse response;
        co_return Response{std::in_place_type<SearchResponse>, std::move(response)};
    }

    bool supports_streaming(const Request&) const override { return false; }
    AsyncGate& gate() { return gate_; }
    bool waitForCancellation(std::chrono::milliseconds timeout) {
        return cancellationObserved_.waitFor(1, timeout);
    }
    [[nodiscard]] bool cancellationSignalPresent() const {
        return cancellationSignalPresent_.load(std::memory_order_acquire);
    }
    void releaseAll() { gate_.release(); }

private:
    AsyncGate gate_;
    EntrySignal cancellationObserved_;
    std::shared_ptr<const std::atomic<bool>> cancellationSignal_;
    std::atomic<bool> cancellationSignalPresent_{false};
};

class ThrowingProcessor final : public RequestProcessor {
public:
    boost::asio::awaitable<Response> process(const Request&) override {
        entered_.notify();
        throw std::runtime_error("processor failure");
        co_return SuccessResponse{"unreachable"};
    }

    bool waitForEntry(std::chrono::milliseconds timeout) { return entered_.waitFor(1, timeout); }

private:
    EntrySignal entered_;
};

class RequestHandlerHarness {
public:
    RequestHandlerHarness(std::shared_ptr<RequestProcessor> processor,
                          RequestHandler::Config config, std::function<void()> releaseBlocked)
        : client_(io_), server_(io_), work_(boost::asio::make_work_guard(io_)),
          releaseBlocked_(std::move(releaseBlocked)) {
        boost::asio::local::connect_pair(client_, server_);
        handler_ = std::make_shared<RequestHandler>(std::move(processor), std::move(config));
        auto serverSocket =
            std::make_shared<boost::asio::local::stream_protocol::socket>(std::move(server_));
        auto finished = std::make_shared<std::promise<void>>();
        finished_ = finished->get_future();
        boost::asio::co_spawn(
            io_,
            [handler = handler_, serverSocket, token = stopSource_.get_token(),
             finished]() -> boost::asio::awaitable<void> {
                co_await handler->handle_connection(serverSocket, token, 1);
                finished->set_value();
            },
            boost::asio::detached);
        ioThread_ = std::thread([this] { io_.run(); });
    }

    ~RequestHandlerHarness() { (void)shutdownAndJoin(std::chrono::milliseconds(250)); }

    bool send(uint64_t requestId, Request request, bool expectsStreaming = false) {
        Message message;
        message.version = 1;
        message.requestId = requestId;
        message.expectsStreamingResponse = expectsStreaming;
        message.payload = std::move(request);
        std::vector<uint8_t> frame;
        if (!framer_.frame_message_into(message, frame)) {
            return false;
        }
        boost::system::error_code error;
        boost::asio::write(client_, boost::asio::buffer(frame), error);
        return !error;
    }

    void closeClient() {
        boost::system::error_code ignored;
        client_.close(ignored);
    }

    bool waitForHandlerExit(std::chrono::milliseconds timeout) {
        return finished_.wait_for(timeout) == std::future_status::ready;
    }

    bool shutdownAndJoin(std::chrono::milliseconds timeout) {
        if (joined_) {
            return handlerExited_;
        }
        stopSource_.request_stop();
        closeClient();
        if (releaseBlocked_) {
            releaseBlocked_();
        }
        handlerExited_ = waitForHandlerExit(timeout);
        work_.reset();
        if (!handlerExited_) {
            io_.stop();
        }
        if (ioThread_.joinable()) {
            ioThread_.join();
        }
        joined_ = true;
        return handlerExited_;
    }

private:
    boost::asio::io_context io_;
    boost::asio::local::stream_protocol::socket client_;
    boost::asio::local::stream_protocol::socket server_;
    boost::asio::executor_work_guard<boost::asio::io_context::executor_type> work_;
    std::shared_ptr<RequestHandler> handler_;
    yams::compat::stop_source stopSource_;
    MessageFramer framer_{1024 * 1024};
    std::future<void> finished_;
    std::thread ioThread_;
    std::function<void()> releaseBlocked_;
    bool joined_{false};
    bool handlerExited_{false};
};

} // namespace

TEST_CASE("RequestHandler: StatusRequest forces unary even when client expects streaming",
          "[daemon][ipc][status][unary]") {
#ifdef _WIN32
    SKIP("Unix domain socket tests skipped on Windows");
#endif

    boost::asio::io_context io;

    boost::asio::local::stream_protocol::socket client_sock(io);
    boost::asio::local::stream_protocol::socket server_sock(io);
    boost::asio::local::connect_pair(client_sock, server_sock);

    RequestHandler::Config cfg;
    cfg.enable_multiplexing = false;
    cfg.enable_streaming = true;

    auto processor = std::make_shared<CountingProcessor>();
    auto handler = std::make_shared<RequestHandler>(processor, cfg);

    // Run the handler on the server socket.
    yams::compat::stop_source stop_source;
    auto server_sock_ptr =
        std::make_shared<boost::asio::local::stream_protocol::socket>(std::move(server_sock));
    boost::asio::co_spawn(
        io,
        [handler, server_sock_ptr,
         token = stop_source.get_token()]() -> boost::asio::awaitable<void> {
            co_await handler->handle_connection(server_sock_ptr, token, 1);
            co_return;
        },
        boost::asio::detached);

    // Drive the async server while the client uses sync I/O.
    auto work = boost::asio::make_work_guard(io);
    std::thread io_thread([&io]() { io.run(); });

    // Send StatusRequest that asks for streaming.
    Message msg;
    msg.version = 1;
    msg.requestId = 7;
    msg.expectsStreamingResponse = true;
    StatusRequest sreq;
    sreq.detailed = true;
    msg.payload = Request{std::in_place_type<StatusRequest>, sreq};

    MessageFramer framer(1024 * 1024);
    std::vector<uint8_t> frame;
    auto fr = framer.frame_message_into(msg, frame);
    REQUIRE(fr);
    boost::asio::write(client_sock, boost::asio::buffer(frame));

    // Read one response frame and verify it's not chunked.
    std::array<uint8_t, MessageFramer::HEADER_SIZE> hdr_buf{};
    boost::asio::read(client_sock, boost::asio::buffer(hdr_buf));
    auto hdr_res = framer.parse_header(std::span<const uint8_t>(hdr_buf.data(), hdr_buf.size()));
    REQUIRE(hdr_res);
    const auto hdr = hdr_res.value();
    REQUIRE(hdr.is_chunked() == false);

    std::vector<uint8_t> payload(hdr.payload_size);
    boost::asio::read(client_sock, boost::asio::buffer(payload));

    // Confirm handler used the unary processor path.
    REQUIRE(processor->unary_calls() == 1);
    REQUIRE(processor->streaming_calls() == 0);

    // Stop the connection loop.
    stop_source.request_stop();
    {
        boost::system::error_code ec;
        client_sock.close(ec);
    }
    work.reset();
    io_thread.join();
}

TEST_CASE("RequestHandler: one-shot list header does not repeat final entries",
          "[daemon][ipc][list][streaming][regression]") {
#ifdef _WIN32
    SKIP("Unix domain socket tests skipped on Windows");
#endif

    boost::asio::io_context io;
    boost::asio::local::stream_protocol::socket clientSock(io);
    boost::asio::local::stream_protocol::socket serverSock(io);
    boost::asio::local::connect_pair(clientSock, serverSock);

    RequestHandler::Config config;
    config.enable_multiplexing = false;
    config.enable_streaming = true;

    auto handler =
        std::make_shared<RequestHandler>(std::make_shared<ListOneShotProcessor>(), config);
    yams::compat::stop_source stopSource;
    auto serverSockPtr =
        std::make_shared<boost::asio::local::stream_protocol::socket>(std::move(serverSock));
    boost::asio::co_spawn(
        io,
        [handler, serverSockPtr, token = stopSource.get_token()]() -> boost::asio::awaitable<void> {
            co_await handler->handle_connection(serverSockPtr, token, 1);
            co_return;
        },
        boost::asio::detached);

    auto work = boost::asio::make_work_guard(io);
    std::thread ioThread([&io]() { io.run(); });

    Message request;
    request.version = 1;
    request.requestId = 8;
    request.expectsStreamingResponse = true;
    request.payload = Request{std::in_place_type<ListRequest>, ListRequest{}};

    MessageFramer framer(1024 * 1024);
    std::vector<uint8_t> frame;
    REQUIRE(framer.frame_message_into(request, frame));
    boost::asio::write(clientSock, boost::asio::buffer(frame));

    auto readResponse = [&]() {
        std::array<uint8_t, MessageFramer::HEADER_SIZE> headerBytes{};
        boost::asio::read(clientSock, boost::asio::buffer(headerBytes));
        auto parsedHeader =
            framer.parse_header(std::span<const uint8_t>(headerBytes.data(), headerBytes.size()));
        REQUIRE(parsedHeader);
        std::vector<uint8_t> framed(headerBytes.begin(), headerBytes.end());
        const auto payloadOffset = framed.size();
        framed.resize(payloadOffset + parsedHeader.value().payload_size);
        boost::asio::read(clientSock, boost::asio::buffer(framed.data() + payloadOffset,
                                                          parsedHeader.value().payload_size));
        auto parsed = framer.parse_frame(framed);
        REQUIRE(parsed);
        return std::pair{parsedHeader.value(), std::move(parsed.value())};
    };

    auto [headerFrame, headerMessage] = readResponse();
    REQUIRE(headerFrame.is_header_only());
    const auto* headerResponse =
        std::get_if<ListResponse>(&std::get<Response>(headerMessage.payload));
    REQUIRE(headerResponse != nullptr);
    CHECK(headerResponse->items.empty());
    CHECK(headerResponse->totalCount == 2);

    auto [finalFrame, finalMessage] = readResponse();
    REQUIRE(finalFrame.is_last_chunk());
    const auto* finalResponse =
        std::get_if<ListResponse>(&std::get<Response>(finalMessage.payload));
    REQUIRE(finalResponse != nullptr);
    REQUIRE(finalResponse->items.size() == 2);
    CHECK(finalResponse->items[0].hash == "hash-a");
    CHECK(finalResponse->items[1].hash == "hash-b");

    stopSource.request_stop();
    {
        boost::system::error_code error;
        clientSock.close(error);
    }
    work.reset();
    ioThread.join();
}

TEST_CASE("RequestHandler: multiplexed request activity tracks executing non-health work",
          "[daemon][ipc][activity][multiplex]") {
#ifdef _WIN32
    SKIP("Unix domain socket tests skipped on Windows");
#endif

    StateComponent state;
    state.stats.activeConnections.store(1, std::memory_order_relaxed);
    RequestLifecycleSignal lifecycle;

    RequestHandler::Config config;
    config.enable_multiplexing = true;
    config.enable_streaming = true;
    config.health_check_counter = &state.stats.healthCheckConnections;
    config.active_non_health_request_counter = &state.stats.activeNonHealthRequests;
    config.worker_job_signal = [&](bool started) { lifecycle.onJob(started); };

    auto processor = std::make_shared<BlockingActivityProcessor>();
    RequestHandlerHarness harness(processor, config, [processor] { processor->releaseAll(); });

    REQUIRE(harness.send(
        101, Request{std::in_place_type<SearchRequest>, SearchRequest{.query = "unary"}}, false));
    REQUIRE(harness.send(
        102, Request{std::in_place_type<SearchRequest>, SearchRequest{.query = "streaming"}},
        true));
    REQUIRE(processor->queryGate().waitForEntrants(2, std::chrono::seconds(2)));
    REQUIRE(lifecycle.waitForStarted(2, std::chrono::seconds(2)));
    CHECK(processor->unaryEntered() == 1);
    CHECK(processor->streamingEntered() == 1);
    CHECK(state.stats.activeNonHealthRequests.load(std::memory_order_relaxed) == 2);
    CHECK(state.stats.activeConnections.load(std::memory_order_relaxed) == 1);

    REQUIRE(harness.send(103, Request{std::in_place_type<StatusRequest>, StatusRequest{}}, true));
    REQUIRE(processor->healthGate().waitForEntrants(1, std::chrono::seconds(2)));
    REQUIRE(lifecycle.waitForStarted(3, std::chrono::seconds(2)));
    CHECK(state.stats.healthCheckConnections.load(std::memory_order_relaxed) == 1);
    CHECK(state.stats.activeNonHealthRequests.load(std::memory_order_relaxed) == 2);

    processor->healthGate().release();
    REQUIRE(lifecycle.waitForFinished(1, std::chrono::seconds(2)));
    CHECK(state.stats.healthCheckConnections.load(std::memory_order_relaxed) == 0);
    CHECK(state.stats.activeNonHealthRequests.load(std::memory_order_relaxed) == 2);

    processor->queryGate().release();
    REQUIRE(lifecycle.waitForFinished(3, std::chrono::seconds(2)));
    CHECK(state.stats.activeNonHealthRequests.load(std::memory_order_relaxed) == 0);
    CHECK(state.stats.activeConnections.load(std::memory_order_relaxed) == 1);
    CHECK(harness.shutdownAndJoin(std::chrono::seconds(2)));
}

TEST_CASE("RequestHandler: unopened socket skips processing and preserves activity counters",
          "[daemon][ipc][activity][early-error]") {
#ifdef _WIN32
    SKIP("Unix domain socket tests skipped on Windows");
#endif

    StateComponent state;
    state.stats.healthCheckConnections.store(13, std::memory_order_relaxed);
    state.stats.activeNonHealthRequests.store(17, std::memory_order_relaxed);
    std::atomic<int> workerSignals{0};
    auto processor = std::make_shared<CountingProcessor>();
    RequestHandler::Config config;
    config.health_check_counter = &state.stats.healthCheckConnections;
    config.active_non_health_request_counter = &state.stats.activeNonHealthRequests;
    config.worker_job_signal = [&](bool) { workerSignals.fetch_add(1); };
    auto handler = std::make_shared<RequestHandler>(processor, config);
    const std::array<Request, 2> requests{
        Request{std::in_place_type<SearchRequest>, SearchRequest{.query = "closed"}},
        Request{std::in_place_type<StatusRequest>, StatusRequest{}}};

    // Handler, requests and borrowed counter/signal storage outlive each io_context, including
    // failed REQUIRE cleanup. No thread, private flag or processor policy emulates the error.
    for (const auto& request : requests) {
        boost::asio::io_context io;
        boost::asio::local::stream_protocol::socket unopened(io);
        REQUIRE_FALSE(unopened.is_open());
        auto result = boost::asio::co_spawn(io, handler->handle_request(unopened, request, 1),
                                            boost::asio::use_future);
        io.run_for(std::chrono::milliseconds(250));
        REQUIRE(result.wait_for(std::chrono::milliseconds(0)) == std::future_status::ready);
        const auto outcome = result.get();
        REQUIRE_FALSE(outcome.has_value());
        CHECK(outcome.error().code == yams::ErrorCode::NetworkError);
        CHECK(outcome.error().message == "Socket closed");
        CHECK(workerSignals.load() == 0);
        CHECK(processor->unary_calls() == 0);
        CHECK(processor->streaming_calls() == 0);
        CHECK(state.stats.healthCheckConnections.load(std::memory_order_relaxed) == 13);
        CHECK(state.stats.activeNonHealthRequests.load(std::memory_order_relaxed) == 17);
    }
}

TEST_CASE("RequestHandler: processor exception releases non-health request activity",
          "[daemon][ipc][activity][exception]") {
#ifdef _WIN32
    SKIP("Unix domain socket tests skipped on Windows");
#endif

    StateComponent state;
    state.stats.activeConnections.store(1, std::memory_order_relaxed);
    RequestLifecycleSignal lifecycle;
    RequestHandler::Config config;
    config.enable_multiplexing = true;
    config.enable_streaming = false;
    config.active_non_health_request_counter = &state.stats.activeNonHealthRequests;
    config.worker_job_signal = [&](bool started) { lifecycle.onJob(started); };

    auto processor = std::make_shared<ThrowingProcessor>();
    RequestHandlerHarness harness(processor, config, [] {});
    REQUIRE(harness.send(
        201, Request{std::in_place_type<SearchRequest>, SearchRequest{.query = "throw"}}));
    REQUIRE(processor->waitForEntry(std::chrono::seconds(2)));
    REQUIRE(lifecycle.waitForStarted(1, std::chrono::seconds(2)));
    REQUIRE(lifecycle.waitForFinished(1, std::chrono::seconds(2)));
    CHECK(state.stats.activeNonHealthRequests.load(std::memory_order_relaxed) == 0);
    CHECK(state.stats.activeConnections.load(std::memory_order_relaxed) == 1);
    CHECK(harness.shutdownAndJoin(std::chrono::seconds(2)));
}

TEST_CASE("RequestHandler: explicit cancellation releases non-health request activity",
          "[daemon][ipc][activity][cancel]") {
#ifdef _WIN32
    SKIP("Unix domain socket tests skipped on Windows");
#endif

    constexpr uint64_t kRequestId = 301;
    StateComponent state;
    state.stats.activeConnections.store(1, std::memory_order_relaxed);
    RequestLifecycleSignal lifecycle;
    RequestHandler::Config config;
    config.enable_multiplexing = true;
    config.enable_streaming = false;
    config.active_non_health_request_counter = &state.stats.activeNonHealthRequests;
    config.worker_job_signal = [&](bool started) { lifecycle.onJob(started); };

    auto processor = std::make_shared<CancelableSearchProcessor>();
    RequestHandlerHarness harness(processor, config, [processor] { processor->releaseAll(); });
    REQUIRE(harness.send(
        kRequestId, Request{std::in_place_type<SearchRequest>, SearchRequest{.query = "cancel"}}));
    REQUIRE(processor->gate().waitForEntrants(1, std::chrono::seconds(2)));
    REQUIRE(lifecycle.waitForStarted(1, std::chrono::seconds(2)));
    REQUIRE(processor->cancellationSignalPresent());
    CHECK(state.stats.activeNonHealthRequests.load(std::memory_order_relaxed) == 1);
    REQUIRE(RequestContextRegistry::instance().cancel(kRequestId));

    processor->gate().release();
    REQUIRE(processor->waitForCancellation(std::chrono::seconds(2)));
    REQUIRE(lifecycle.waitForFinished(1, std::chrono::seconds(2)));
    CHECK(state.stats.activeNonHealthRequests.load(std::memory_order_relaxed) == 0);
    CHECK(state.stats.activeConnections.load(std::memory_order_relaxed) == 1);
    CHECK(harness.shutdownAndJoin(std::chrono::seconds(2)));
}

TEST_CASE("RequestHandler: disconnect releases non-health request activity",
          "[daemon][ipc][activity][disconnect]") {
#ifdef _WIN32
    SKIP("Unix domain socket tests skipped on Windows");
#endif

    StateComponent state;
    state.stats.activeConnections.store(1, std::memory_order_relaxed);
    RequestLifecycleSignal lifecycle;
    RequestHandler::Config config;
    config.enable_multiplexing = true;
    config.enable_streaming = false;
    config.active_non_health_request_counter = &state.stats.activeNonHealthRequests;
    config.worker_job_signal = [&](bool started) { lifecycle.onJob(started); };

    auto processor = std::make_shared<CancelableSearchProcessor>();
    RequestHandlerHarness harness(processor, config, [processor] { processor->releaseAll(); });
    REQUIRE(harness.send(
        401, Request{std::in_place_type<SearchRequest>, SearchRequest{.query = "disconnect"}}));
    REQUIRE(processor->gate().waitForEntrants(1, std::chrono::seconds(2)));
    REQUIRE(lifecycle.waitForStarted(1, std::chrono::seconds(2)));
    CHECK(state.stats.activeNonHealthRequests.load(std::memory_order_relaxed) == 1);

    harness.closeClient();
    REQUIRE(harness.waitForHandlerExit(std::chrono::seconds(2)));
    processor->gate().release();
    REQUIRE(processor->waitForCancellation(std::chrono::seconds(2)));
    REQUIRE(lifecycle.waitForFinished(1, std::chrono::seconds(2)));
    CHECK(state.stats.activeNonHealthRequests.load(std::memory_order_relaxed) == 0);
    CHECK(harness.shutdownAndJoin(std::chrono::seconds(2)));
}

TEST_CASE("RequestHandler: one-shot search header does not repeat final results",
          "[daemon][ipc][search][streaming][regression]") {
#ifdef _WIN32
    SKIP("Unix domain socket tests skipped on Windows");
#endif

    boost::asio::io_context io;
    boost::asio::local::stream_protocol::socket clientSock(io);
    boost::asio::local::stream_protocol::socket serverSock(io);
    boost::asio::local::connect_pair(clientSock, serverSock);

    RequestHandler::Config config;
    config.enable_multiplexing = false;
    config.enable_streaming = true;

    auto handler =
        std::make_shared<RequestHandler>(std::make_shared<SearchOneShotProcessor>(), config);
    yams::compat::stop_source stopSource;
    auto serverSockPtr =
        std::make_shared<boost::asio::local::stream_protocol::socket>(std::move(serverSock));
    boost::asio::co_spawn(
        io,
        [handler, serverSockPtr, token = stopSource.get_token()]() -> boost::asio::awaitable<void> {
            co_await handler->handle_connection(serverSockPtr, token, 1);
            co_return;
        },
        boost::asio::detached);

    auto work = boost::asio::make_work_guard(io);
    std::thread ioThread([&io]() { io.run(); });

    Message request;
    request.version = 1;
    request.requestId = 9;
    request.expectsStreamingResponse = true;
    request.payload = Request{std::in_place_type<SearchRequest>, SearchRequest{}};

    MessageFramer framer(1024 * 1024);
    std::vector<uint8_t> frame;
    REQUIRE(framer.frame_message_into(request, frame));
    boost::asio::write(clientSock, boost::asio::buffer(frame));

    auto readResponse = [&]() {
        std::array<uint8_t, MessageFramer::HEADER_SIZE> headerBytes{};
        boost::asio::read(clientSock, boost::asio::buffer(headerBytes));
        auto parsedHeader =
            framer.parse_header(std::span<const uint8_t>(headerBytes.data(), headerBytes.size()));
        REQUIRE(parsedHeader);
        std::vector<uint8_t> framed(headerBytes.begin(), headerBytes.end());
        const auto payloadOffset = framed.size();
        framed.resize(payloadOffset + parsedHeader.value().payload_size);
        boost::asio::read(clientSock, boost::asio::buffer(framed.data() + payloadOffset,
                                                          parsedHeader.value().payload_size));
        auto parsed = framer.parse_frame(framed);
        REQUIRE(parsed);
        return std::pair{parsedHeader.value(), std::move(parsed.value())};
    };

    auto [headerFrame, headerMessage] = readResponse();
    REQUIRE(headerFrame.is_header_only());
    const auto* headerResponse =
        std::get_if<SearchResponse>(&std::get<Response>(headerMessage.payload));
    REQUIRE(headerResponse != nullptr);
    CHECK(headerResponse->results.empty());
    CHECK(headerResponse->totalCount == 2);

    auto [finalFrame, finalMessage] = readResponse();
    REQUIRE(finalFrame.is_last_chunk());
    const auto* finalResponse =
        std::get_if<SearchResponse>(&std::get<Response>(finalMessage.payload));
    REQUIRE(finalResponse != nullptr);
    REQUIRE(finalResponse->results.size() == 2);
    CHECK(finalResponse->results[0].id == "hash-a");
    CHECK(finalResponse->results[1].id == "hash-b");

    stopSource.request_stop();
    {
        boost::system::error_code error;
        clientSock.close(error);
    }
    work.reset();
    ioThread.join();
}
