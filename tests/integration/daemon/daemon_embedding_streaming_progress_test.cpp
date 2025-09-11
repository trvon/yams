#include <filesystem>
#include <random>
#include <thread>
#include "test_async_helpers.h"
#include <gtest/gtest.h>
#include <yams/daemon/client/asio_transport.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/daemon.h>
#include <yams/daemon/ipc/ipc_protocol.h>

using namespace std::chrono_literals;
namespace fs = std::filesystem;

static std::string randid(size_t n = 8) {
    static const char* cs = "abcdefghijklmnopqrstuvwxyz0123456789";
    thread_local std::mt19937_64 rng{std::random_device{}()};
    std::uniform_int_distribution<size_t> dist(0, 35);
    std::string out;
    out.reserve(n);
    for (size_t i = 0; i < n; ++i)
        out.push_back(cs[dist(rng)]);
    return out;
}

TEST(DaemonEmbeddingStreamingIT, BatchEmbeddingEmitsProgressAndFinal) {
    if (std::getenv("GTEST_DISCOVERY_MODE"))
        GTEST_SKIP();

    auto id = randid();
    fs::path tmp = fs::temp_directory_path() / ("yams_embed_stream_" + id);
    fs::create_directories(tmp);
    fs::path sock = tmp / ("daemon_" + id + ".sock");
    fs::path pid = tmp / ("daemon_" + id + ".pid");
    fs::path log = tmp / ("daemon_" + id + ".log");
    fs::path data = tmp / "data";
    fs::create_directories(data);

    yams::daemon::DaemonConfig cfg;
    cfg.dataDir = data;
    cfg.socketPath = sock;
    cfg.pidFile = pid;
    cfg.logFile = log;
    cfg.enableModelProvider = true;          // mock provider is acceptable in tests
    cfg.modelPoolConfig.lazyLoading = false; // simulate keep_model_hot

    yams::daemon::YamsDaemon daemon(cfg);
    auto started = daemon.start();
    ASSERT_TRUE(started) << started.error().message;
    struct Stop {
        yams::daemon::YamsDaemon* d;
        ~Stop() {
            if (d)
                (void)d->stop();
        }
    } stop{&daemon};

    yams::daemon::ClientConfig ccfg;
    ccfg.socketPath = sock;
    ccfg.requestTimeout = 10s;
    yams::daemon::DaemonClient client(ccfg);

    // Wait briefly for readiness
    bool ready = false;
    for (int i = 0; i < 50; ++i) {
        auto s = yams::cli::run_sync(client.status(), 500ms);
        if (s && s->running) {
            ready = true;
            break;
        }
        std::this_thread::sleep_for(50ms);
    }
    ASSERT_TRUE(ready);

    yams::daemon::BatchEmbeddingRequest req{};
    req.modelName = "all-MiniLM-L6-v2";
    req.normalize = true;
    req.batchSize = 4;
    req.texts = {"a", "b", "c", "d", "e", "f", "g", "h", "i"};

    // Use the transport directly to assert progress events + final
    yams::daemon::AsioTransportAdapter::Options opts;
    opts.socketPath = sock;
    opts.headerTimeout = 5s;
    opts.bodyTimeout = 15s;
    opts.requestTimeout = 10s;
    yams::daemon::AsioTransportAdapter transport(opts);

    std::atomic<size_t> progress_events{0};
    std::optional<yams::daemon::BatchEmbeddingResponse> final;
    auto onHeader = [](const yams::daemon::Response&) {};
    auto onChunk = [&](const yams::daemon::Response& r, bool last) {
        if (auto* ev = std::get_if<yams::daemon::EmbeddingEvent>(&r)) {
            if (ev->processed > 0)
                progress_events.fetch_add(1, std::memory_order_relaxed);
            return true;
        }
        if (auto* fin = std::get_if<yams::daemon::BatchEmbeddingResponse>(&r)) {
            if (!last)
                return true; // ensure it's the last
            final = *fin;
            return true;
        }
        if (auto* err = std::get_if<yams::daemon::ErrorResponse>(&r)) {
            // Stop on error
            (void)last;
            return false;
        }
        return true;
    };
    std::optional<yams::daemon::Error> error;
    auto onError = [&](const yams::daemon::Error& e) { error = e; };
    auto onComplete = []() {};

    auto sres =
        yams::cli::run_sync(transport.send_request_streaming(yams::daemon::Request{req}, onHeader,
                                                             onChunk, onError, onComplete),
                            10s);
    ASSERT_TRUE(sres) << (error ? error->message : "streaming failed");
    ASSERT_TRUE(final.has_value());
    EXPECT_EQ(final->successCount, req.texts.size());
    EXPECT_GT(progress_events.load(), 0u);
}

TEST(DaemonEmbeddingStreamingIT, EmbedDocumentsEmitsProgressAndFinal) {
    if (std::getenv("GTEST_DISCOVERY_MODE"))
        GTEST_SKIP();

    auto id = randid();
    fs::path tmp = fs::temp_directory_path() / ("yams_embed_docs_stream_" + id);
    fs::create_directories(tmp);
    fs::path sock = tmp / ("daemon_" + id + ".sock");
    fs::path pid = tmp / ("daemon_" + id + ".pid");
    fs::path log = tmp / ("daemon_" + id + ".log");
    fs::path data = tmp / "data";
    fs::create_directories(data);

    yams::daemon::DaemonConfig cfg;
    cfg.dataDir = data;
    cfg.socketPath = sock;
    cfg.pidFile = pid;
    cfg.logFile = log;
    cfg.enableModelProvider = true;
    cfg.modelPoolConfig.lazyLoading = false;

    yams::daemon::YamsDaemon daemon(cfg);
    auto started = daemon.start();
    ASSERT_TRUE(started) << started.error().message;
    struct Stop {
        yams::daemon::YamsDaemon* d;
        ~Stop() {
            if (d)
                (void)d->stop();
        }
    } stop{&daemon};

    yams::daemon::ClientConfig ccfg;
    ccfg.socketPath = sock;
    ccfg.requestTimeout = 10s;
    yams::daemon::DaemonClient client(ccfg);

    bool ready = false;
    for (int i = 0; i < 50; ++i) {
        auto s = yams::cli::run_sync(client.status(), 500ms);
        if (s && s->running) {
            ready = true;
            break;
        }
        std::this_thread::sleep_for(50ms);
    }
    ASSERT_TRUE(ready);

    // Add some small documents
    std::vector<std::string> hashes;
    for (int i = 0; i < 5; ++i) {
        yams::daemon::AddDocumentRequest add{};
        add.name = "doc_" + std::to_string(i) + "_" + id + ".txt";
        add.mimeType = "text/plain";
        add.content = std::string("hello world ") + std::to_string(i);
        add.noEmbeddings = true; // add content first without embeddings
        auto ar = yams::cli::run_sync(client.streamingAddDocument(add), 10s);
        ASSERT_TRUE(ar) << ar.error().message;
        hashes.push_back(ar->hash);
    }

    // Now request embeddings for those documents via streaming
    yams::daemon::EmbedDocumentsRequest ed{};
    ed.modelName = "all-MiniLM-L6-v2";
    ed.normalize = true;
    ed.batchSize = 2;
    ed.skipExisting = false;
    ed.documentHashes = hashes;

    yams::daemon::AsioTransportAdapter::Options opts;
    opts.socketPath = sock;
    opts.headerTimeout = 5s;
    opts.bodyTimeout = 15s;
    opts.requestTimeout = 10s;
    yams::daemon::AsioTransportAdapter transport(opts);

    std::atomic<size_t> events{0};
    std::optional<yams::daemon::EmbedDocumentsResponse> fin;
    auto onHeader = [](const yams::daemon::Response&) {};
    auto onChunk = [&](const yams::daemon::Response& r, bool last) {
        if (auto* ev = std::get_if<yams::daemon::EmbeddingEvent>(&r)) {
            if (ev->processed > 0)
                events.fetch_add(1, std::memory_order_relaxed);
            return true;
        }
        if (auto* end = std::get_if<yams::daemon::EmbedDocumentsResponse>(&r)) {
            if (!last)
                return true;
            fin = *end;
            return true;
        }
        if (auto* er = std::get_if<yams::daemon::ErrorResponse>(&r)) {
            (void)last;
            return false;
        }
        return true;
    };
    std::optional<yams::daemon::Error> err;
    auto onError = [&](const yams::daemon::Error& e) { err = e; };
    auto onComplete = []() {};

    auto sres =
        yams::cli::run_sync(transport.send_request_streaming(yams::daemon::Request{ed}, onHeader,
                                                             onChunk, onError, onComplete),
                            15s);
    ASSERT_TRUE(sres) << (err ? err->message : "streaming failed");
    ASSERT_TRUE(fin.has_value());
    EXPECT_EQ(fin->requested, hashes.size());
    EXPECT_EQ(fin->embedded, hashes.size());
    EXPECT_GT(events.load(), 0u);
}
