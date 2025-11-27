// Copyright tests: internal
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_future.hpp>
#include <gtest/gtest.h>
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
    ADD_FAILURE() << "Unexpected response variant";
    return T{};
}

TEST(StreamingProcessorTest, BatchEmbeddingStreamsWorkingAndFinal) {
#ifdef _WIN32
    // Skip on Windows: ProtoSerializer round-trip in streaming processor
    // loses Request variant data due to cross-TU/ODR normalization issues
    GTEST_SKIP() << "Streaming processor tests skipped on Windows (protobuf serialization issue)";
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
    EXPECT_FALSE(opt.has_value());

    // First chunk: started keepalive or immediate working event
    auto f2 = boost::asio::co_spawn(io, sp.next_chunk(), boost::asio::use_future);
    auto ch1 = f2.get();
    (void)ch1; // allow any first chunk
    // If it's an EmbeddingEvent, accept started/working; otherwise it may be a SuccessResponse
    // keepalive
    if (std::holds_alternative<EmbeddingEvent>(ch1.data)) {
        auto ev = get_as<EmbeddingEvent>(ch1.data);
        EXPECT_FALSE(ev.modelName.empty());
    }

    // Drive a couple more chunks; ensure we ultimately get a final BatchEmbeddingResponse
    bool got_final = false;
    for (int i = 0; i < 20 && !got_final; ++i) {
        auto fut = boost::asio::co_spawn(io, sp.next_chunk(), boost::asio::use_future);
        auto ch = fut.get();
        if (std::holds_alternative<BatchEmbeddingResponse>(ch.data)) {
            auto resp = get_as<BatchEmbeddingResponse>(ch.data);
            EXPECT_EQ(resp.successCount, req.texts.size());
            EXPECT_TRUE(ch.is_last_chunk);
            got_final = true;
        }
    }
    EXPECT_TRUE(got_final);

    guard.reset();
    io.stop();
    io_thread.join();
}

TEST(StreamingProcessorTest, EmbedDocumentsStreamsWorkingAndFinal) {
#ifdef _WIN32
    // Skip on Windows: ProtoSerializer round-trip in streaming processor
    // loses Request variant data due to cross-TU/ODR normalization issues
    GTEST_SKIP() << "Streaming processor tests skipped on Windows (protobuf serialization issue)";
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
    EXPECT_FALSE(opt.has_value());

    // First chunk
    auto f2 = boost::asio::co_spawn(io, sp.next_chunk(), boost::asio::use_future);
    auto ch1 = f2.get();
    (void)ch1;
    if (std::holds_alternative<EmbeddingEvent>(ch1.data)) {
        auto ev = get_as<EmbeddingEvent>(ch1.data);
        EXPECT_FALSE(ev.modelName.empty());
    }

    bool got_final = false;
    for (int i = 0; i < 20 && !got_final; ++i) {
        auto fut = boost::asio::co_spawn(io, sp.next_chunk(), boost::asio::use_future);
        auto ch = fut.get();
        if (std::holds_alternative<EmbedDocumentsResponse>(ch.data)) {
            auto resp = get_as<EmbedDocumentsResponse>(ch.data);
            EXPECT_EQ(resp.requested, req.documentHashes.size());
            EXPECT_TRUE(ch.is_last_chunk);
            got_final = true;
        }
    }
    EXPECT_TRUE(got_final);

    guard.reset();
    io.stop();
    io_thread.join();
}

} // namespace
