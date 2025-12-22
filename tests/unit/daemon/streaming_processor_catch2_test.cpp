// Catch2 migration of streaming_processor_test.cpp
// Migration: yams-3s4 (daemon unit tests)

#include <catch2/catch_test_macros.hpp>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_future.hpp>
#include <thread>

#include <yams/daemon/ipc/request_handler.h>
#include <yams/daemon/ipc/streaming_processor.h>

using namespace yams::daemon;

namespace {

class DummyProcessor : public RequestProcessor {
public:
    boost::asio::awaitable<Response> process(const Request& request) override {
        if (auto* be = std::get_if<BatchEmbeddingRequest>(&request)) {
            BatchEmbeddingResponse resp;
            resp.dimensions = 1;
            resp.modelUsed = be->modelName;
            resp.processingTimeMs = 0;
            resp.successCount = be->texts.size();
            resp.failureCount = 0;
            resp.embeddings.resize(be->texts.size());
            co_return Response{std::move(resp)};
        }
        if (auto* ed = std::get_if<EmbedDocumentsRequest>(&request)) {
            EmbedDocumentsResponse resp{};
            resp.requested = ed->documentHashes.size();
            resp.embedded = resp.requested;
            resp.skipped = 0;
            resp.failed = 0;
            co_return Response{std::move(resp)};
        }
        co_return Response{SuccessResponse{"ok"}};
    }
};

template <typename T> static T get_as(const Response& r) {
    if (auto* v = std::get_if<T>(&r))
        return *v;
    FAIL("Unexpected response variant");
    return T{};
}

} // namespace

TEST_CASE("StreamingProcessor batch embedding streams working and final",
          "[daemon][streaming][.windows_skip]") {
#ifdef _WIN32
    // Skip on Windows: ProtoSerializer round-trip in streaming processor
    // loses Request variant data due to cross-TU/ODR normalization issues
    SKIP("Streaming processor tests skipped on Windows (protobuf serialization issue)");
#endif
    boost::asio::io_context io;
    auto guard = boost::asio::make_work_guard(io);
    std::thread io_thread([&] { io.run(); });
    auto delegate = std::make_shared<DummyProcessor>();
    RequestHandler::Config cfg;
    cfg.worker_executor = io.get_executor();
    StreamingRequestProcessor sp(delegate, cfg);

    BatchEmbeddingRequest req{};
    req.modelName = "all-MiniLM-L6-v2";
    req.normalize = true;
    req.batchSize = 4;
    req.texts = {"a", "b", "c", "d", "e", "f", "g", "h", "i"};

    auto f1 =
        boost::asio::co_spawn(io, sp.process_streaming(Request{req}), boost::asio::use_future);
    auto opt = f1.get();
    REQUIRE_FALSE(opt.has_value());

    // First chunk: started keepalive or immediate working event
    auto f2 = boost::asio::co_spawn(io, sp.next_chunk(), boost::asio::use_future);
    auto ch1 = f2.get();
    (void)ch1;
    if (std::holds_alternative<EmbeddingEvent>(ch1.data)) {
        auto ev = get_as<EmbeddingEvent>(ch1.data);
        REQUIRE_FALSE(ev.modelName.empty());
    }

    // Drive a couple more chunks; ensure we ultimately get a final BatchEmbeddingResponse
    bool got_final = false;
    for (int i = 0; i < 20 && !got_final; ++i) {
        auto fut = boost::asio::co_spawn(io, sp.next_chunk(), boost::asio::use_future);
        auto ch = fut.get();
        if (std::holds_alternative<BatchEmbeddingResponse>(ch.data)) {
            auto resp = get_as<BatchEmbeddingResponse>(ch.data);
            REQUIRE(resp.successCount == req.texts.size());
            REQUIRE(ch.is_last_chunk);
            got_final = true;
        }
    }
    REQUIRE(got_final);

    guard.reset();
    io.stop();
    io_thread.join();
}

TEST_CASE("StreamingProcessor embed documents streams working and final",
          "[daemon][streaming][.windows_skip]") {
#ifdef _WIN32
    SKIP("Streaming processor tests skipped on Windows (protobuf serialization issue)");
#endif
    boost::asio::io_context io;
    auto guard = boost::asio::make_work_guard(io);
    std::thread io_thread([&] { io.run(); });
    auto delegate = std::make_shared<DummyProcessor>();
    RequestHandler::Config cfg;
    cfg.worker_executor = io.get_executor();
    StreamingRequestProcessor sp(delegate, cfg);

    EmbedDocumentsRequest req{};
    req.modelName = "all-MiniLM-L6-v2";
    req.normalize = true;
    req.batchSize = 3;
    req.documentHashes = {"h1", "h2", "h3", "h4", "h5", "h6", "h7"};

    auto f1 =
        boost::asio::co_spawn(io, sp.process_streaming(Request{req}), boost::asio::use_future);
    auto opt = f1.get();
    REQUIRE_FALSE(opt.has_value());

    // First chunk
    auto f2 = boost::asio::co_spawn(io, sp.next_chunk(), boost::asio::use_future);
    auto ch1 = f2.get();
    (void)ch1;
    if (std::holds_alternative<EmbeddingEvent>(ch1.data)) {
        auto ev = get_as<EmbeddingEvent>(ch1.data);
        REQUIRE_FALSE(ev.modelName.empty());
    }

    bool got_final = false;
    for (int i = 0; i < 20 && !got_final; ++i) {
        auto fut = boost::asio::co_spawn(io, sp.next_chunk(), boost::asio::use_future);
        auto ch = fut.get();
        if (std::holds_alternative<EmbedDocumentsResponse>(ch.data)) {
            auto resp = get_as<EmbedDocumentsResponse>(ch.data);
            REQUIRE(resp.requested == req.documentHashes.size());
            REQUIRE(ch.is_last_chunk);
            got_final = true;
        }
    }
    REQUIRE(got_final);

    guard.reset();
    io.stop();
    io_thread.join();
}
