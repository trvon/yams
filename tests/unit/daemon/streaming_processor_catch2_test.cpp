// Catch2 migration of streaming_processor_test.cpp
// Migration: yams-3s4 (daemon unit tests)

#include <catch2/catch_test_macros.hpp>

#include <map>
#include <string>
#include <utility>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_future.hpp>

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

class SearchProcessor : public RequestProcessor {
public:
    SearchResponse response;
    std::size_t calls{0};

    boost::asio::awaitable<Response> process(const Request&) override {
        ++calls;
        co_return Response{response};
    }
};

template <typename T> static T get_as(const Response& r) {
    if (auto* v = std::get_if<T>(&r))
        return *v;
    FAIL("Unexpected response variant");
    return T{};
}

} // namespace

TEST_CASE("StreamingProcessor batch embedding streams working and final", "[daemon][streaming]") {
#ifdef _WIN32
    // Skip on Windows: ProtoSerializer round-trip in streaming processor
    // loses Request variant data due to cross-TU/ODR normalization issues
    SKIP("Streaming processor tests skipped on Windows (protobuf serialization issue)");
#endif
    // Run io_context synchronously from main thread to avoid coroutine frame destruction races
    boost::asio::io_context io;
    auto delegate = std::make_shared<DummyProcessor>();
    RequestHandler::Config cfg;
    cfg.worker_executor = io.get_executor();
    auto sp = std::make_unique<StreamingRequestProcessor>(delegate, cfg);

    // Create request and verify variant is valid
    BatchEmbeddingRequest beReq{};
    beReq.modelName = "all-MiniLM-L6-v2";
    beReq.normalize = true;
    beReq.batchSize = 4;
    beReq.texts = {"a", "b", "c", "d", "e", "f", "g", "h", "i"};
    Request request{std::move(beReq)};
    INFO("Test: Request variant index=" << request.index() << " holds_BE="
                                        << std::holds_alternative<BatchEmbeddingRequest>(request));
    REQUIRE_FALSE(request.valueless_by_exception());
    REQUIRE(std::holds_alternative<BatchEmbeddingRequest>(request));
    const size_t expectedTextCount = 9;

    // Spawn coroutine and run io_context until it completes
    bool process_done = false;
    std::optional<Response> opt;
    boost::asio::co_spawn(io, sp->process_streaming(std::move(request)),
                          [&](std::exception_ptr, std::optional<Response> result) {
                              opt = std::move(result);
                              process_done = true;
                          });
    io.run();
    io.restart();
    REQUIRE(process_done);
    REQUIRE_FALSE(opt.has_value()); // streaming mode returns nullopt

    // First chunk: started keepalive or immediate working event
    RequestProcessor::ResponseChunk ch1;
    std::exception_ptr ex1;
    boost::asio::co_spawn(io, sp->next_chunk(),
                          [&](std::exception_ptr e, RequestProcessor::ResponseChunk result) {
                              ex1 = e;
                              if (!e)
                                  ch1 = std::move(result);
                          });
    io.run();
    io.restart();
    if (ex1)
        std::rethrow_exception(ex1);
    if (std::holds_alternative<EmbeddingEvent>(ch1.data)) {
        auto ev = get_as<EmbeddingEvent>(ch1.data);
        REQUIRE_FALSE(ev.modelName.empty());
    }

    // Drive a couple more chunks; ensure we ultimately get a final BatchEmbeddingResponse
    bool got_final = false;
    for (int i = 0; i < 20 && !got_final; ++i) {
        RequestProcessor::ResponseChunk ch;
        std::exception_ptr ex;
        boost::asio::co_spawn(io, sp->next_chunk(),
                              [&](std::exception_ptr e, RequestProcessor::ResponseChunk result) {
                                  ex = e;
                                  if (!e)
                                      ch = std::move(result);
                              });
        io.run();
        io.restart();
        if (ex)
            std::rethrow_exception(ex);
        if (std::holds_alternative<BatchEmbeddingResponse>(ch.data)) {
            auto resp = get_as<BatchEmbeddingResponse>(ch.data);
            REQUIRE(resp.successCount == expectedTextCount);
            REQUIRE(ch.is_last_chunk);
            got_final = true;
        }
    }
    REQUIRE(got_final);

    // Cleanup - all coroutine frames already destroyed since io.run() completed
    sp.reset();
}

TEST_CASE("StreamingProcessor preserves search metadata across response sizes",
          "[daemon][streaming][search_stats]") {
    boost::asio::io_context io;
    auto delegate = std::make_shared<SearchProcessor>();
    RequestHandler::Config cfg;
    cfg.worker_executor = io.get_executor();
    cfg.chunk_size = 512 * 1024;
    std::size_t resultCount = 12;
    std::size_t snippetSize = 8;
    bool expectStreaming = false;
    bool expectMultiplePages = false;

    SECTION("small response uses the one-shot path") {}
    SECTION("response larger than the default chunk retains metadata") {
        snippetSize = 64 * 1024;
        expectStreaming = true;
    }
    SECTION("metadata survives multiple result pages") {
        cfg.chunk_size = 512;
        expectStreaming = true;
        expectMultiplePages = true;
    }
    SECTION("oversized stats survive an empty result page") {
        resultCount = 0;
        expectStreaming = true;
        delegate->response.searchStats["large_debug_trace"] = std::string(600 * 1024, 'x');
    }

    auto& expected = delegate->response;
    expected.totalCount = resultCount + 7;
    expected.elapsed = std::chrono::milliseconds(123);
    expected.traceId = "trace-search-stats";
    expected.queryInfo = "hybrid search";
    expected.searchStats["topology_policy"] = "shadow";
    expected.searchStats["vector_exact_distance_evaluations"] = "137";
    for (std::size_t i = 0; i < resultCount; ++i) {
        SearchResult result{};
        result.id = "doc-" + std::to_string(i);
        result.path = "/corpus/" + result.id;
        result.score = 1.0 / (1.0 + i);
        result.snippet = std::string(snippetSize, 's');
        expected.results.push_back(std::move(result));
    }

    StreamingRequestProcessor processor(delegate, cfg);
    // Reuse the same processor to ensure metadata from the first request cannot leak.
    for (int attempt = 0; attempt < 2; ++attempt) {
        CAPTURE(attempt);
        if (attempt == 1) {
            expected.traceId = "trace-second-search";
            expected.queryInfo = "second query";
            expected.searchStats.clear();
            expected.searchStats["second_request"] = "only";
            if (resultCount == 0) {
                expected.searchStats["large_debug_trace"] = std::string(600 * 1024, 'y');
            }
        }
        Request request{SearchRequest{}};
        auto pending = boost::asio::co_spawn(io, processor.process_streaming(request),
                                             boost::asio::use_future);
        io.run();
        io.restart();
        auto immediate = pending.get();
        REQUIRE(immediate.has_value() == !expectStreaming);

        SearchResponse received;
        std::size_t dataPages = 0;
        if (immediate) {
            received = get_as<SearchResponse>(*immediate);
        } else {
            auto starter =
                boost::asio::co_spawn(io, processor.next_chunk(), boost::asio::use_future);
            io.run();
            io.restart();
            auto heartbeat = starter.get();
            REQUIRE_FALSE(heartbeat.is_last_chunk);
            REQUIRE(get_as<SearchResponse>(heartbeat.data).results.empty());

            bool finished = false;
            // Bounded by the item count, with one final page even for empty results.
            for (std::size_t page = 0; page <= resultCount && !finished; ++page) {
                auto next =
                    boost::asio::co_spawn(io, processor.next_chunk(), boost::asio::use_future);
                io.run();
                io.restart();
                auto chunk = next.get();
                auto response = get_as<SearchResponse>(chunk.data);
                REQUIRE(response.totalCount == expected.totalCount);
                REQUIRE(response.elapsed == expected.elapsed);
                if (page == 0) {
                    CHECK(response.traceId == expected.traceId);
                    CHECK(response.queryInfo == expected.queryInfo);
                    CHECK(response.searchStats == expected.searchStats);
                } else {
                    CHECK(response.traceId.empty());
                    CHECK(response.queryInfo.empty());
                    CHECK(response.searchStats.empty());
                }
                received.results.insert(received.results.end(), response.results.begin(),
                                        response.results.end());
                received.searchStats.insert(response.searchStats.begin(),
                                            response.searchStats.end());
                if (!response.traceId.empty())
                    received.traceId = response.traceId;
                if (!response.queryInfo.empty())
                    received.queryInfo = response.queryInfo;
                received.totalCount = response.totalCount;
                received.elapsed = response.elapsed;
                ++dataPages;
                finished = chunk.is_last_chunk;
            }
            REQUIRE(finished);
            if (expectMultiplePages)
                REQUIRE(dataPages > 1);
        }
        CHECK(received.searchStats == expected.searchStats);
        CHECK(received.queryInfo == expected.queryInfo);
        CHECK(received.traceId == expected.traceId);
        CHECK(received.totalCount == expected.totalCount);
        CHECK(received.elapsed == expected.elapsed);
        REQUIRE(received.results.size() == expected.results.size());
        for (std::size_t i = 0; i < resultCount; ++i) {
            CHECK(received.results[i].id == expected.results[i].id);
            CHECK(received.results[i].snippet == expected.results[i].snippet);
            CHECK(received.results[i].score == expected.results[i].score);
        }
        REQUIRE(delegate->calls == static_cast<std::size_t>(attempt + 1));
    }
}

TEST_CASE("StreamingProcessor embed documents streams working and final", "[daemon][streaming]") {
#ifdef _WIN32
    SKIP("Streaming processor tests skipped on Windows (protobuf serialization issue)");
#endif
    // Run io_context synchronously from main thread to avoid coroutine frame destruction races
    boost::asio::io_context io;
    auto delegate = std::make_shared<DummyProcessor>();
    RequestHandler::Config cfg;
    cfg.worker_executor = io.get_executor();
    auto sp = std::make_unique<StreamingRequestProcessor>(delegate, cfg);

    // Create request and verify variant is valid
    EmbedDocumentsRequest edReq{};
    edReq.modelName = "all-MiniLM-L6-v2";
    edReq.normalize = true;
    edReq.batchSize = 3;
    edReq.documentHashes = {"h1", "h2", "h3", "h4", "h5", "h6", "h7"};
    Request request{std::move(edReq)};
    INFO("Test: Request variant index=" << request.index() << " holds_ED="
                                        << std::holds_alternative<EmbedDocumentsRequest>(request));
    REQUIRE_FALSE(request.valueless_by_exception());
    REQUIRE(std::holds_alternative<EmbedDocumentsRequest>(request));
    const size_t expectedHashCount = 7;

    // Spawn coroutine and run io_context until it completes
    bool process_done = false;
    std::optional<Response> opt;
    boost::asio::co_spawn(io, sp->process_streaming(std::move(request)),
                          [&](std::exception_ptr, std::optional<Response> result) {
                              opt = std::move(result);
                              process_done = true;
                          });
    io.run();
    io.restart();
    REQUIRE(process_done);
    REQUIRE_FALSE(opt.has_value()); // streaming mode returns nullopt

    // First chunk
    RequestProcessor::ResponseChunk ch1;
    boost::asio::co_spawn(io, sp->next_chunk(),
                          [&](std::exception_ptr, RequestProcessor::ResponseChunk result) {
                              ch1 = std::move(result);
                          });
    io.run();
    io.restart();
    if (std::holds_alternative<EmbeddingEvent>(ch1.data)) {
        auto ev = get_as<EmbeddingEvent>(ch1.data);
        REQUIRE_FALSE(ev.modelName.empty());
    }

    bool got_final = false;
    for (int i = 0; i < 20 && !got_final; ++i) {
        RequestProcessor::ResponseChunk ch;
        boost::asio::co_spawn(io, sp->next_chunk(),
                              [&](std::exception_ptr, RequestProcessor::ResponseChunk result) {
                                  ch = std::move(result);
                              });
        io.run();
        io.restart();
        if (std::holds_alternative<EmbedDocumentsResponse>(ch.data)) {
            auto resp = get_as<EmbedDocumentsResponse>(ch.data);
            REQUIRE(resp.requested == expectedHashCount);
            REQUIRE(ch.is_last_chunk);
            got_final = true;
        }
    }
    REQUIRE(got_final);

    // Cleanup - all coroutine frames already destroyed since io.run() completed
    sp.reset();
}
