#include <catch2/catch_test_macros.hpp>

#include <yams/daemon/components/DaemonLifecycleFsm.h>
#include <yams/daemon/components/RequestDispatcher.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/ipc/message_framing.h>
#include <yams/daemon/ipc/request_context_registry.h>
#include <yams/daemon/ipc/request_handler.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/metadata_repository.h>

#include <boost/asio.hpp>

#include <array>
#include <chrono>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>

using namespace std::chrono_literals;
using namespace yams;
using namespace yams::daemon;

namespace {

std::shared_ptr<metadata::ConnectionPool> getRepairCancelTestPool() {
    static std::shared_ptr<metadata::ConnectionPool> pool = [] {
        metadata::ConnectionPoolConfig cfg{};
        cfg.enableWAL = false;
        cfg.enableForeignKeys = false;
        auto p = std::make_shared<metadata::ConnectionPool>(":memory:", cfg);
        (void)p->initialize();
        return p;
    }();
    return pool;
}

class BlockingRepairMetadataRepository final : public metadata::MetadataRepository {
public:
    BlockingRepairMetadataRepository() : metadata::MetadataRepository(*getRepairCancelTestPool()) {}

    Result<std::vector<metadata::DocumentInfo>>
    queryDocuments(const metadata::DocumentQueryOptions&) override {
        std::unique_lock<std::mutex> lk(mu_);
        entered_ = true;
        cv_.notify_all();
        cv_.wait(lk, [&] { return released_; });
        return std::vector<metadata::DocumentInfo>{};
    }

    bool waitUntilEntered(std::chrono::milliseconds timeout) {
        std::unique_lock<std::mutex> lk(mu_);
        return cv_.wait_for(lk, timeout, [&] { return entered_; });
    }

    void release() {
        std::lock_guard<std::mutex> lk(mu_);
        released_ = true;
        cv_.notify_all();
    }

private:
    std::mutex mu_;
    std::condition_variable cv_;
    bool entered_{false};
    bool released_{false};
};

template <typename Fn> class ScopeExit {
public:
    explicit ScopeExit(Fn fn) : fn_(std::move(fn)) {}
    ~ScopeExit() { fn_(); }

private:
    Fn fn_;
};

bool waitForCondition(std::chrono::milliseconds timeout, const std::function<bool()>& predicate) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) {
            return true;
        }
        std::this_thread::sleep_for(10ms);
    }
    return predicate();
}

std::optional<Message> readMessageWithTimeout(boost::asio::local::stream_protocol::socket& socket,
                                              MessageFramer& framer,
                                              std::chrono::milliseconds timeout) {
    auto waitForBytes = [&](std::size_t bytes) {
        auto deadline = std::chrono::steady_clock::now() + timeout;
        boost::system::error_code ec;
        while (std::chrono::steady_clock::now() < deadline) {
            if (socket.available(ec) >= bytes) {
                return !ec;
            }
            if (ec) {
                return false;
            }
            std::this_thread::sleep_for(5ms);
        }
        return false;
    };

    if (!waitForBytes(MessageFramer::HEADER_SIZE)) {
        return std::nullopt;
    }

    std::array<uint8_t, MessageFramer::HEADER_SIZE> headerBuf{};
    boost::asio::read(socket, boost::asio::buffer(headerBuf));

    auto headerResult =
        framer.parse_header(std::span<const uint8_t>(headerBuf.data(), headerBuf.size()));
    REQUIRE(headerResult);

    std::vector<uint8_t> frame(headerBuf.begin(), headerBuf.end());
    const auto payloadSize = headerResult.value().payload_size;
    if (payloadSize > 0) {
        if (!waitForBytes(payloadSize)) {
            return std::nullopt;
        }
        std::vector<uint8_t> payload(payloadSize);
        boost::asio::read(socket, boost::asio::buffer(payload));
        frame.insert(frame.end(), payload.begin(), payload.end());
    }

    auto messageResult = framer.parse_frame(frame);
    REQUIRE(messageResult);
    return messageResult.value();
}

} // namespace

TEST_CASE("RequestHandler: canceled RepairRequest streaming completes without deadlock",
          "[daemon][ipc][repair][cancel]") {
#ifdef _WIN32
    SKIP("Unix domain socket tests skipped on Windows");
#endif

    DaemonConfig config;
    StateComponent state;
    DaemonLifecycleFsm lifecycle;
    ServiceManager serviceManager(config, state, lifecycle);

    auto repo = std::make_shared<BlockingRepairMetadataRepository>();
    serviceManager.__test_setMetadataRepo(repo);
    serviceManager.startRepairService([] { return size_t{0}; });
    ScopeExit stopRepair([&] { serviceManager.stopRepairService(); });
    ScopeExit releaseRepo([&] { repo->release(); });

    RequestDispatcher dispatcher(nullptr, &serviceManager, &state);

    // io_context must outlive the RequestHandler: the handler's internal
    // write_strand_exec_ holds a strand bound to io's strand_executor_service,
    // and destroying io first would invalidate the strand before ~RequestHandler
    // releases it.
    boost::asio::io_context io;

    RequestHandler::Config handlerConfig;
    handlerConfig.enable_multiplexing = true;
    handlerConfig.enable_streaming = true;
    handlerConfig.stream_chunk_timeout = 25ms;
    auto handler = std::make_shared<RequestHandler>(&dispatcher, handlerConfig);

    boost::asio::local::stream_protocol::socket clientSock(io);
    boost::asio::local::stream_protocol::socket serverSock(io);
    boost::asio::local::connect_pair(clientSock, serverSock);

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
    ScopeExit stopIo([&] {
        stopSource.request_stop();
        boost::system::error_code ec;
        clientSock.close(ec);
        work.reset();
        if (ioThread.joinable()) {
            ioThread.join();
        }
    });

    constexpr uint64_t kRequestId = 77;
    RepairRequest repairReq;
    repairReq.repairDownloads = true;

    Message request;
    request.version = 1;
    request.requestId = kRequestId;
    request.expectsStreamingResponse = true;
    request.payload = Request{std::in_place_type<RepairRequest>, repairReq};

    MessageFramer framer(1024 * 1024);
    std::vector<uint8_t> frame;
    REQUIRE(framer.frame_message_into(request, frame));
    boost::asio::write(clientSock, boost::asio::buffer(frame));

    auto headerMessage = readMessageWithTimeout(clientSock, framer, 2s);
    REQUIRE(headerMessage.has_value());
    CHECK(headerMessage->requestId == kRequestId);

    REQUIRE(repo->waitUntilEntered(2s));
    REQUIRE(waitForCondition(
        2s, [&] { return RequestContextRegistry::instance().cancel(kRequestId); }));

    bool sawCancelled = false;
    for (int i = 0; i < 4 && !sawCancelled; ++i) {
        auto message = readMessageWithTimeout(clientSock, framer, 2s);
        REQUIRE(message.has_value());
        CHECK(message->requestId == kRequestId);

        auto* payload = std::get_if<Response>(&message->payload);
        REQUIRE(payload != nullptr);
        if (auto* error = std::get_if<ErrorResponse>(payload)) {
            CHECK(error->code == ErrorCode::OperationCancelled);
            CHECK(error->message == "Request canceled");
            sawCancelled = true;
        }
    }

    REQUIRE(sawCancelled);
}
