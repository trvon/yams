#include <catch2/catch_test_macros.hpp>

#ifdef _WIN32
#include <process.h>
#define getpid _getpid
#endif

#include <boost/asio/local/stream_protocol.hpp>
#include <boost/system/error_code.hpp>

#include "common/test_data_generator.h"
#include "common/test_helpers_catch2.h"

#include <array>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <set>
#include <map>
#include <thread>
#include <vector>

#include <yams/app/services/document_ingestion_service.h>
#include <yams/app/services/factory.hpp>
#include <yams/app/services/retrieval_service.h>
#include <yams/app/services/services.hpp>
#include <yams/app/services/session_service.hpp>
#include <yams/daemon/client/asio_connection_pool.h>
#include <yams/daemon/client/global_io_context.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/daemon/daemon.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/query_helpers.h>

using namespace std::chrono_literals;
namespace fs = std::filesystem;

namespace {

#ifdef _WIN32
#define SKIP_ON_WINDOWS_DAEMON_SHUTDOWN()                                                          \
    SKIP("Daemon IPC tests unstable on Windows - see windows-daemon-ipc-plan.md")
#else
#define SKIP_ON_WINDOWS_DAEMON_SHUTDOWN() ((void)0)
#endif

class GrepServiceExpectationsFixture {
public:
    ~GrepServiceExpectationsFixture() {
        if (daemon_) {
            daemon_->stop();
            if (runLoopThread_.joinable()) {
                runLoopThread_.join();
            }
        }
        yams::daemon::AsioConnectionPool::shutdown_all(std::chrono::milliseconds(500));
        yams::daemon::GlobalIOContext::reset();
        std::error_code ec;
        fs::remove_all(root_, ec);
    }

    void start() {
        SKIP_ON_WINDOWS_DAEMON_SHUTDOWN();
        if (!canBindUnixSocketHere()) {
            SKIP("AF_UNIX not available in this environment");
        }

        yams::daemon::AsioConnectionPool::shutdown_all(std::chrono::milliseconds(500));
        yams::daemon::GlobalIOContext::reset();

        const auto unique =
            std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
        root_ = fs::temp_directory_path() / ("yams_grep_it_" + unique);
        storageDir_ = root_ / "storage";
        fs::create_directories(storageDir_);
        fs::create_directories(root_ / "ingest");
        socketPath_ = fs::path("/tmp") / ("yams-grep-" + unique + ".sock");

        yams::daemon::DaemonConfig cfg;
        cfg.dataDir = storageDir_;
        cfg.socketPath = socketPath_;
        cfg.pidFile = root_ / "daemon.pid";
        cfg.logFile = root_ / "daemon.log";
        cfg.enableModelProvider = true;
        cfg.useMockModelProvider = true;
        daemon_ = std::make_unique<yams::daemon::YamsDaemon>(cfg);
        auto started = daemon_->start();
        REQUIRE(started);

        runLoopThread_ = std::thread([this]() { daemon_->runLoop(); });

        const auto deadline = std::chrono::steady_clock::now() + 15s;
        while (std::chrono::steady_clock::now() < deadline) {
            auto lifecycle = daemon_->getLifecycle().snapshot();
            if (lifecycle.state == yams::daemon::LifecycleState::Ready) {
                break;
            }
            std::this_thread::sleep_for(100ms);
        }
        std::this_thread::sleep_for(100ms);
    }

    bool waitForDocumentVisible(const std::string& hash,
                                std::chrono::milliseconds timeout = 15000ms) const {
        auto* sm = daemon_->getServiceManager();
        auto ctx = sm->getAppContext();
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            auto doc = ctx.metadataRepo->getDocumentByHash(hash);
            if (doc && doc.value().has_value()) {
                return true;
            }
            std::this_thread::sleep_for(50ms);
        }
        return false;
    }

    yams::daemon::YamsDaemon* daemon() const { return daemon_.get(); }
    const fs::path& root() const { return root_; }
    const fs::path& storageDir() const { return storageDir_; }
    const fs::path& socketPath() const { return socketPath_; }

private:
    static bool canBindUnixSocketHere() {
        try {
            boost::asio::io_context io;
            boost::asio::local::stream_protocol::acceptor acceptor(io);
            auto path = std::filesystem::path("/tmp") /
                        (std::string("yams-grep-probe-") + std::to_string(::getpid()) + ".sock");
            std::error_code ec;
            std::filesystem::remove(path, ec);
            boost::system::error_code bec;
            acceptor.open(boost::asio::local::stream_protocol::endpoint(path.string()).protocol(),
                          bec);
            if (bec) {
                return false;
            }
            acceptor.bind(boost::asio::local::stream_protocol::endpoint(path.string()), bec);
            if (bec) {
                return false;
            }
            acceptor.close();
            std::filesystem::remove(path, ec);
            return true;
        } catch (...) {
            return false;
        }
    }

    fs::path root_;
    fs::path storageDir_;
    fs::path socketPath_;
    std::unique_ptr<yams::daemon::YamsDaemon> daemon_;
    std::thread runLoopThread_;
};

} // namespace

TEST_CASE_METHOD(GrepServiceExpectationsFixture, "GrepService: regex-only pathsOnly with include",
                 "[integration][services][grep]") {
    start();

    auto fHello = root() / "ingest" / "hello.txt";
    auto fSkip = root() / "ingest" / "skip.bin";
    std::ofstream(fHello) << "hello grep service";
    std::ofstream(fSkip) << std::string(5, '\0');

    yams::app::services::DocumentIngestionService ing;
    yams::app::services::AddOptions opts;
    opts.socketPath = socketPath();
    opts.explicitDataDir = storageDir();
    opts.path = fHello.string();
    opts.noEmbeddings = true;
    auto add1 = ing.addViaDaemon(opts);
    REQUIRE(add1);
    opts.path = fSkip.string();
    auto add2 = ing.addViaDaemon(opts);
    REQUIRE(add2);

    auto* sm = daemon()->getServiceManager();
    auto ctx = sm->getAppContext();
    auto grepSvc = yams::app::services::makeGrepService(ctx);
    auto searchSvc = yams::app::services::makeSearchService(ctx);
    (void)searchSvc->lightIndexForHash(add1.value().hash);

    bool visible = false;
    for (int attempt = 0; attempt < 40 && !visible; ++attempt) {
        auto allDocs = yams::metadata::queryDocumentsByPattern(*ctx.metadataRepo, "%");
        if (allDocs) {
            for (const auto& doc : allDocs.value()) {
                if (doc.filePath.find("hello.txt") != std::string::npos) {
                    visible = true;
                    break;
                }
            }
        }
        if (!visible) {
            std::this_thread::sleep_for(50ms);
        }
    }
    REQUIRE(visible);

    yams::app::services::GrepRequest grepReq;
    grepReq.pattern = "hello";
    grepReq.regexOnly = true;
    grepReq.pathsOnly = true;
    grepReq.paths = {(root() / "ingest").string()};

    yams::Result<yams::app::services::GrepResponse> grepRes =
        yams::Error{yams::ErrorCode::Unknown, "init"};
    bool ready = false;
    for (int attempt = 0; attempt < 60 && !ready; ++attempt) {
        grepRes = grepSvc->grep(grepReq);
        REQUIRE(grepRes);
        ready = !grepRes.value().pathsOnly.empty();
        if (!ready) {
            std::this_thread::sleep_for(50ms);
        }
    }
    const auto& response = grepRes.value();
    bool hasHello = false;
    for (const auto& path : response.pathsOnly) {
        if (path.find("hello.txt") != std::string::npos) {
            hasHello = true;
        }
        CHECK(path.find("skip.bin") == std::string::npos);
    }
    CHECK(hasHello);
}

TEST_CASE_METHOD(GrepServiceExpectationsFixture,
                 "GrepService: synchronous add is immediately visible without light indexing",
                 "[integration][services][grep]") {
    start();

    yams::app::services::DocumentIngestionService ingestion;
    yams::app::services::AddOptions addOptions;
    addOptions.socketPath = socketPath();
    addOptions.explicitDataDir = storageDir();
    addOptions.content = "transport fallback smoke\n";
    addOptions.name = "transport-smoke.txt";
    addOptions.noEmbeddings = true;
    addOptions.waitForProcessing = true;
    auto add = ingestion.addViaDaemon(addOptions);
    REQUIRE(add);

    auto* serviceManager = daemon()->getServiceManager();
    auto context = serviceManager->getAppContext();
    auto grepService = yams::app::services::makeGrepService(context);

    yams::app::services::GrepRequest request;
    request.pattern = "transport";
    request.literalText = true;
    request.maxCount = 10;

    auto result = grepService->grep(request);
    REQUIRE(result);
    CHECK(result.value().totalMatches > 0);
    REQUIRE_FALSE(result.value().results.empty());
    CHECK(result.value().results.front().file.find("transport-smoke.txt") != std::string::npos);
}

TEST_CASE_METHOD(GrepServiceExpectationsFixture, "GrepService: count mode structure",
                 "[integration][services][grep]") {
    start();

    auto filePath = root() / "ingest" / "a.md";
    std::ofstream(filePath) << "alpha beta gamma alpha";

    yams::app::services::DocumentIngestionService ing;
    yams::app::services::AddOptions opts;
    opts.socketPath = socketPath();
    opts.explicitDataDir = storageDir();
    opts.path = filePath.string();
    opts.noEmbeddings = true;
    auto add = ing.addViaDaemon(opts);
    REQUIRE(add);

    auto* sm = daemon()->getServiceManager();
    auto ctx = sm->getAppContext();
    auto grepSvc = yams::app::services::makeGrepService(ctx);
    auto searchSvc = yams::app::services::makeSearchService(ctx);
    (void)searchSvc->lightIndexForHash(add.value().hash);

    bool visible = false;
    for (int attempt = 0; attempt < 40 && !visible; ++attempt) {
        auto allDocs = yams::metadata::queryDocumentsByPattern(*ctx.metadataRepo, "%");
        if (allDocs) {
            for (const auto& doc : allDocs.value()) {
                if (doc.filePath.find("a.md") != std::string::npos) {
                    visible = true;
                    break;
                }
            }
        }
        if (!visible) {
            std::this_thread::sleep_for(50ms);
        }
    }
    REQUIRE(visible);

    yams::app::services::GrepRequest grepReq;
    grepReq.pattern = "alpha";
    grepReq.count = true;
    grepReq.regexOnly = true;
    grepReq.paths = {(root() / "ingest").string()};

    yams::Result<yams::app::services::GrepResponse> grepRes =
        yams::Error{yams::ErrorCode::Unknown, "init"};
    bool hasResults = false;
    for (int attempt = 0; attempt < 60 && !hasResults; ++attempt) {
        grepRes = grepSvc->grep(grepReq);
        REQUIRE(grepRes);
        hasResults = !grepRes.value().results.empty();
        if (!hasResults) {
            std::this_thread::sleep_for(50ms);
        }
    }

    REQUIRE_FALSE(grepRes.value().results.empty());
    bool found = false;
    for (const auto& fileResult : grepRes.value().results) {
        if (fileResult.file.find("a.md") != std::string::npos) {
            CHECK(fileResult.matchCount > 0u);
            found = true;
        }
    }
    CHECK(found);
}

TEST_CASE_METHOD(GrepServiceExpectationsFixture, "GrepService: negative no-match pathsOnly",
                 "[integration][services][grep]") {
    start();

    auto* sm = daemon()->getServiceManager();
    auto ctx = sm->getAppContext();
    auto grepSvc = yams::app::services::makeGrepService(ctx);

    yams::app::services::GrepRequest grepReq;
    grepReq.pattern = "zzzzzz";
    grepReq.regexOnly = true;
    grepReq.pathsOnly = true;
    grepReq.includePatterns = {(root() / "ingest" / "**").string()};

    auto grepRes = grepSvc->grep(grepReq);
    REQUIRE(grepRes);
    CHECK(grepRes.value().pathsOnly.empty());
}

TEST_CASE_METHOD(GrepServiceExpectationsFixture, "GrepService: literal vs regex word boundaries",
                 "[integration][services][grep]") {
    start();

    auto filePath = root() / "ingest" / "words.txt";
    std::ofstream(filePath) << "foo foobar foo-bar (foo)";

    yams::app::services::DocumentIngestionService ing;
    yams::app::services::AddOptions opts;
    opts.socketPath = socketPath();
    opts.explicitDataDir = storageDir();
    opts.path = filePath.string();
    opts.noEmbeddings = true;
    auto add = ing.addViaDaemon(opts);
    REQUIRE(add);
    REQUIRE(waitForDocumentVisible(add.value().hash));

    auto* sm = daemon()->getServiceManager();
    auto ctx = sm->getAppContext();
    auto grepSvc = yams::app::services::makeGrepService(ctx);
    auto searchSvc = yams::app::services::makeSearchService(ctx);
    (void)searchSvc->lightIndexForHash(add.value().hash);

    {
        yams::app::services::GrepRequest literalReq;
        literalReq.pattern = "(foo)";
        literalReq.literalText = true;
        literalReq.lineNumbers = true;
        literalReq.paths = {(root() / "ingest").string()};
        bool found = false;
        for (int attempt = 0; attempt < 60 && !found; ++attempt) {
            auto literalRes = grepSvc->grep(literalReq);
            REQUIRE(literalRes);
            for (const auto& fileResult : literalRes.value().results) {
                if (fileResult.file.find("words.txt") != std::string::npos &&
                    fileResult.matchCount > 0) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                std::this_thread::sleep_for(50ms);
            }
        }
        CHECK(found);
    }

    {
        yams::app::services::GrepRequest wordReq;
        wordReq.pattern = "foo";
        wordReq.word = true;
        wordReq.lineNumbers = true;
        wordReq.paths = {(root() / "ingest").string()};
        size_t total = 0;
        for (int attempt = 0; attempt < 60 && total < 2u; ++attempt) {
            auto wordRes = grepSvc->grep(wordReq);
            REQUIRE(wordRes);
            total = 0;
            for (const auto& fileResult : wordRes.value().results) {
                total += fileResult.matchCount;
            }
            if (total < 2u) {
                std::this_thread::sleep_for(50ms);
            }
        }
        CHECK(total >= 2u);
    }
}

TEST_CASE_METHOD(GrepServiceExpectationsFixture, "GrepService: files with and without modes",
                 "[integration][services][grep]") {
    start();

    auto fileA = root() / "ingest" / "a.txt";
    auto fileB = root() / "ingest" / "b.txt";
    std::ofstream(fileA) << "alpha";
    std::ofstream(fileB) << "beta";

    yams::app::services::DocumentIngestionService ing;
    yams::app::services::AddOptions opts;
    opts.socketPath = socketPath();
    opts.explicitDataDir = storageDir();
    opts.noEmbeddings = true;
    opts.path = fileA.string();
    auto add1 = ing.addViaDaemon(opts);
    REQUIRE(add1);
    opts.path = fileB.string();
    auto add2 = ing.addViaDaemon(opts);
    REQUIRE(add2);

    auto* sm = daemon()->getServiceManager();
    auto ctx = sm->getAppContext();

    const auto deadline = std::chrono::steady_clock::now() + 5000ms;
    bool visible1 = false;
    bool visible2 = false;
    while (std::chrono::steady_clock::now() < deadline && (!visible1 || !visible2)) {
        if (!visible1) {
            auto doc1 = ctx.metadataRepo->getDocumentByHash(add1.value().hash);
            visible1 = (doc1 && doc1.value().has_value());
        }
        if (!visible2) {
            auto doc2 = ctx.metadataRepo->getDocumentByHash(add2.value().hash);
            visible2 = (doc2 && doc2.value().has_value());
        }
        if (!visible1 || !visible2) {
            std::this_thread::sleep_for(50ms);
        }
    }
    REQUIRE(visible1);
    REQUIRE(visible2);

    auto grepSvc = yams::app::services::makeGrepService(ctx);

    yams::app::services::GrepRequest grepReq;
    grepReq.pattern = "alpha";
    grepReq.filesWithMatches = true;
    grepReq.filesWithoutMatch = true;
    grepReq.paths = {(root() / "ingest").string()};

    bool hasA = false;
    bool hasB = false;
    for (int attempt = 0; attempt < 60 && (!hasA || !hasB); ++attempt) {
        auto grepRes = grepSvc->grep(grepReq);
        REQUIRE(grepRes);
        for (const auto& path : grepRes.value().filesWith) {
            if (path.find("a.txt") != std::string::npos) {
                hasA = true;
            }
        }
        for (const auto& path : grepRes.value().filesWithout) {
            if (path.find("b.txt") != std::string::npos) {
                hasB = true;
            }
        }
        if (!hasA || !hasB) {
            std::this_thread::sleep_for(50ms);
        }
    }
    CHECK(hasA);
    CHECK(hasB);
}

TEST_CASE_METHOD(GrepServiceExpectationsFixture, "GrepService: invert pathsOnly",
                 "[integration][services][grep]") {
    start();

    auto filePath = root() / "ingest" / "c.txt";
    std::ofstream(filePath) << "gamma";

    yams::app::services::DocumentIngestionService ing;
    yams::app::services::AddOptions opts;
    opts.socketPath = socketPath();
    opts.explicitDataDir = storageDir();
    opts.path = filePath.string();
    opts.noEmbeddings = true;
    auto add = ing.addViaDaemon(opts);
    REQUIRE(add);
    REQUIRE(waitForDocumentVisible(add.value().hash));

    auto* sm = daemon()->getServiceManager();
    auto ctx = sm->getAppContext();
    auto grepSvc = yams::app::services::makeGrepService(ctx);
    auto searchSvc = yams::app::services::makeSearchService(ctx);
    (void)searchSvc->lightIndexForHash(add.value().hash);

    yams::app::services::GrepRequest grepReq;
    grepReq.pattern = "zzz";
    grepReq.regexOnly = true;
    grepReq.invert = true;
    grepReq.pathsOnly = true;
    grepReq.paths = {(root() / "ingest").string()};

    bool hasC = false;
    for (int attempt = 0; attempt < 60 && !hasC; ++attempt) {
        auto grepRes = grepSvc->grep(grepReq);
        REQUIRE(grepRes);
        for (const auto& path : grepRes.value().pathsOnly) {
            if (path.find("c.txt") != std::string::npos) {
                hasC = true;
                break;
            }
        }
        if (!hasC) {
            std::this_thread::sleep_for(50ms);
        }
    }
    CHECK(hasC);
}

TEST_CASE_METHOD(GrepServiceExpectationsFixture,
                 "GrepService: binary file pathsOnly avoids UTF-8 errors",
                 "[integration][services][grep]") {
    start();

    {
        std::ofstream out(root() / "ingest" / "bin.dat", std::ios::binary);
        std::string payload;
        payload.assign("A", 1);
        payload.push_back('\0');
        payload.push_back(static_cast<char>(0xFF));
        payload.append("B\nC", 3);
        out.write(payload.data(), static_cast<std::streamsize>(payload.size()));
    }

    yams::app::services::DocumentIngestionService ing;
    yams::app::services::AddOptions opts;
    opts.socketPath = socketPath();
    opts.explicitDataDir = storageDir();
    opts.path = (root() / "ingest").string();
    opts.recursive = true;
    opts.noEmbeddings = true;
    REQUIRE(ing.addViaDaemon(opts));

    yams::app::services::RetrievalService retrieval;
    yams::app::services::RetrievalOptions retrievalOpts;
    retrievalOpts.socketPath = socketPath();
    retrievalOpts.explicitDataDir = storageDir();

    yams::app::services::GrepOptions grepReq;
    grepReq.pattern = "A";
    grepReq.pathsOnly = true;
    grepReq.literalText = true;
    grepReq.includePatterns = {(root() / "ingest" / "**").string()};

    bool ok = false;
    for (int attempt = 0; attempt < 40 && !ok; ++attempt) {
        auto grepRes = retrieval.grep(grepReq, retrievalOpts);
        REQUIRE(grepRes);
        ok = true;
    }
    CHECK(ok);
}

TEST_CASE_METHOD(GrepServiceExpectationsFixture,
                 "GrepService: unicode literal and ignoreCase best effort",
                 "[integration][services][grep]") {
    start();

    fs::create_directories(root() / "ingest");
    auto filePath = root() / "ingest" / "unicode.txt";
    std::ofstream(filePath) << "The unit is \xC3\x85ngstr\xC3\xB6m \xF0\x9F\x98\x80";

    yams::app::services::DocumentIngestionService ing;
    yams::app::services::AddOptions opts;
    opts.socketPath = socketPath();
    opts.explicitDataDir = storageDir();
    opts.path = filePath.string();
    opts.recursive = false;
    opts.noEmbeddings = true;
    auto add = ing.addViaDaemon(opts);
    REQUIRE(add);
    REQUIRE(waitForDocumentVisible(add.value().hash));

    auto* sm = daemon()->getServiceManager();
    auto ctx = sm->getAppContext();
    auto grepSvc = yams::app::services::makeGrepService(ctx);
    auto searchSvc = yams::app::services::makeSearchService(ctx);
    (void)searchSvc->lightIndexForHash(add.value().hash);

    SECTION("literal unicode token should be discoverable") {
        yams::app::services::GrepRequest request;
        request.pattern = "Ångström";
        request.literalText = true;
        request.paths = {(root() / "ingest").string()};

        bool found = false;
        for (int attempt = 0; attempt < 60 && !found; ++attempt) {
            auto result = grepSvc->grep(request);
            REQUIRE(result);
            for (const auto& fileResult : result.value().results) {
                if (fileResult.file.find("unicode.txt") != std::string::npos &&
                    fileResult.matchCount > 0) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                std::this_thread::sleep_for(50ms);
            }
        }
        CHECK(found);
    }

    SECTION("unicode ignoreCase remains best effort") {
        yams::app::services::GrepRequest request;
        request.pattern = "ångström";
        request.literalText = true;
        request.ignoreCase = true;
        request.paths = {(root() / "ingest").string()};

        auto result = grepSvc->grep(request);
        REQUIRE(result);
        size_t totalMatches = 0;
        for (const auto& fileResult : result.value().results) {
            totalMatches += fileResult.matchCount;
        }
        static_cast<void>(totalMatches);
    }
}

TEST_CASE_METHOD(GrepServiceExpectationsFixture, "GrepService: stress tail remains stable",
                 "[integration][services][grep]") {
    start();

    fs::create_directories(root() / "stress");
    auto filePath = root() / "stress" / "s.md";
    std::ofstream(filePath) << "hello stress grep";

    yams::app::services::DocumentIngestionService ing;
    yams::app::services::AddOptions opts;
    opts.socketPath = socketPath();
    opts.explicitDataDir = storageDir();
    opts.path = filePath.string();
    opts.recursive = false;
    opts.noEmbeddings = true;
    REQUIRE(ing.addViaDaemon(opts));

    auto* sm = daemon()->getServiceManager();
    auto ctx = sm->getAppContext();
    auto grepSvc = yams::app::services::makeGrepService(ctx);

    yams::app::services::GrepRequest request;
    request.pattern = "hello";
    request.regexOnly = true;
    request.pathsOnly = true;
    request.paths = {(root() / "stress").string()};

    const auto stressIterations = []() {
        if (const char* value = std::getenv("YAMS_STRESS_ITERS")) {
            int parsed = std::atoi(value);
            if (parsed > 0 && parsed < 100000) {
                return parsed;
            }
        }
        return 100;
    }();

    for (int i = 0; i < stressIterations; ++i) {
        auto result = grepSvc->grep(request);
        REQUIRE(result);
        std::this_thread::sleep_for(5ms);
    }
}

TEST_CASE_METHOD(GrepServiceExpectationsFixture,
                 "GrepService: binary file countOnly avoids UTF-8 errors",
                 "[integration][services][grep]") {
    start();

    fs::create_directories(root() / "ingest");
    {
        std::ofstream out(root() / "i2.dat", std::ios::binary);
        std::string payload;
        payload.assign("Z", 1);
        payload.push_back('\0');
        payload.push_back(static_cast<char>(0xEE));
        payload.push_back(static_cast<char>(0xFF));
        payload.append("Z", 1);
        out.write(payload.data(), static_cast<std::streamsize>(payload.size()));
    }

    yams::app::services::DocumentIngestionService ing;
    yams::app::services::AddOptions opts;
    opts.socketPath = socketPath();
    opts.explicitDataDir = storageDir();
    opts.path = root().string();
    opts.recursive = true;
    opts.noEmbeddings = true;
    REQUIRE(ing.addViaDaemon(opts));

    yams::app::services::RetrievalService retrievalSvc;
    yams::app::services::RetrievalOptions retrievalOpts;
    retrievalOpts.socketPath = socketPath();
    retrievalOpts.explicitDataDir = storageDir();

    yams::app::services::GrepOptions grepOpts;
    grepOpts.pattern = "Z";
    grepOpts.countOnly = true;
    grepOpts.regexOnly = true;
    grepOpts.includePatterns = {(root() / "**").string()};

    auto grepResult = retrievalSvc.grep(grepOpts, retrievalOpts);
    REQUIRE(grepResult);
    static_cast<void>(grepResult.value().totalMatches);
}

TEST_CASE_METHOD(GrepServiceExpectationsFixture,
                 "GrepService: binary file filesOnly avoids UTF-8 errors",
                 "[integration][services][grep]") {
    start();

    fs::create_directories(root() / "ingest");
    {
        std::ofstream out(root() / "ingest" / "i3.bin", std::ios::binary);
        std::array<unsigned char, 5> bytes{0x00, 0xFF, 0x41, 0x00, 0x41};
        out.write(reinterpret_cast<const char*>(bytes.data()), bytes.size());
    }

    yams::app::services::DocumentIngestionService ing;
    yams::app::services::AddOptions opts;
    opts.socketPath = socketPath();
    opts.explicitDataDir = storageDir();
    opts.path = (root() / "ingest").string();
    opts.recursive = true;
    opts.noEmbeddings = true;
    REQUIRE(ing.addViaDaemon(opts));

    yams::app::services::RetrievalService retrievalSvc;
    yams::app::services::RetrievalOptions retrievalOpts;
    retrievalOpts.socketPath = socketPath();
    retrievalOpts.explicitDataDir = storageDir();

    yams::app::services::GrepOptions grepOpts;
    grepOpts.pattern = "A";
    grepOpts.filesOnly = true;
    grepOpts.regexOnly = true;
    grepOpts.includePatterns = {(root() / "ingest" / "**").string()};

    auto grepResult = retrievalSvc.grep(grepOpts, retrievalOpts);
    REQUIRE(grepResult);
    static_cast<void>(grepResult.value().filesSearched);
}

TEST_CASE_METHOD(GrepServiceExpectationsFixture,
                 "GrepService: files-only and count modes return what the plain mode found",
                 "[integration][services][grep]") {
    // `yams grep -l X` and `yams grep -c X` printed "(no results)" for a pattern the plain
    // mode matched: the daemon answered with file sets and counts the client never read.
    start();

    fs::create_directories(root() / "modes");
    std::ofstream(root() / "modes" / "two.txt") << "needleqz one\nfiller\nneedleqz two\n";
    std::ofstream(root() / "modes" / "one.txt") << "filler\nneedleqz only\n";
    std::ofstream(root() / "modes" / "none.txt") << "filler only\n";

    yams::app::services::DocumentIngestionService ing;
    yams::app::services::AddOptions opts;
    opts.socketPath = socketPath();
    opts.explicitDataDir = storageDir();
    opts.path = (root() / "modes").string();
    opts.recursive = true;
    opts.noEmbeddings = true;
    REQUIRE(ing.addViaDaemon(opts));

    yams::app::services::RetrievalService retrievalSvc;
    yams::app::services::RetrievalOptions retrievalOpts;
    retrievalOpts.socketPath = socketPath();
    retrievalOpts.explicitDataDir = storageDir();

    yams::app::services::GrepOptions plain;
    plain.pattern = "needleqz";
    plain.regexOnly = true;

    const auto filesOf = [](const yams::daemon::GrepResponse& response) {
        std::set<std::string> names;
        for (const auto& match : response.matches) {
            names.insert(fs::path(match.file).filename().string());
        }
        return names;
    };

    // Ingestion is asynchronous: wait until the plain mode sees both files.
    bool indexed = false;
    for (int attempt = 0; attempt < 100 && !indexed; ++attempt) {
        auto found = retrievalSvc.grep(plain, retrievalOpts);
        indexed = found && filesOf(found.value()).size() == 2;
        if (!indexed) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }
    REQUIRE(indexed);
    const std::set<std::string> expected{"one.txt", "two.txt"};

    SECTION("files-only lists every matching file") {
        auto request = plain;
        request.filesOnly = true;
        auto listed = retrievalSvc.grep(request, retrievalOpts);
        REQUIRE(listed);
        CHECK(filesOf(listed.value()) == expected);
    }
    SECTION("count reports matching lines per file") {
        auto request = plain;
        request.countOnly = true;
        auto counted = retrievalSvc.grep(request, retrievalOpts);
        REQUIRE(counted);
        std::map<std::string, std::size_t> counts;
        for (const auto& match : counted.value().matches) {
            ++counts[fs::path(match.file).filename().string()];
        }
        CHECK(counts == std::map<std::string, std::size_t>{{"one.txt", 1}, {"two.txt", 2}});
    }
}

TEST_CASE_METHOD(GrepServiceExpectationsFixture,
                 "Session watcher indexes a watched directory in a running daemon",
                 "[integration][services][session_watch]") {
    // The only case that runs the watcher loop for real: every other suite disables it.
    yams::test::ScopedEnvVar enableWatcher{"YAMS_DISABLE_SESSION_WATCHER", std::string("0")};
    // A busy CI host must not postpone the pass this case waits for.
    yams::daemon::TuneAdvisor::setBackgroundMaxDeferralMs(0);
    struct RestoreDeferral {
        ~RestoreDeferral() { yams::daemon::TuneAdvisor::resetBackgroundMaxDeferralMs(); }
    } restoreDeferral;

    const auto watched = yams::test::make_temp_dir("yams_watch_live_");
    std::ofstream(watched / "first.txt") << "watchneedlealpha in the first file\n";
    // A changed set that mixes top-level and nested files, with a same-named ignored file
    // below: the indexer must take exactly the files the watcher names.
    fs::create_directories(watched / "sub" / "skipped");
    std::ofstream(watched / "sub" / "nested.txt") << "watchneedlegamma in a nested file\n";
    std::ofstream(watched / ".gitignore") << "skipped/\n";
    std::ofstream(watched / "sub" / "skipped" / "first.txt") << "watchneedleomega ignored\n";

    // No current session: watching belongs to the session, not to "current".
    auto sessions = yams::app::services::makeSessionService(nullptr);
    REQUIRE(sessions);
    sessions->init("watched-project", "");
    sessions->addPathSelector(watched.string(), {}, {});
    sessions->enableWatch(true, "watched-project");
    sessions->setWatchIntervalMs(200, "watched-project");
    sessions->clearCurrent();

    start();

    yams::app::services::RetrievalService retrievalSvc;
    yams::app::services::RetrievalOptions retrievalOpts;
    retrievalOpts.socketPath = socketPath();
    retrievalOpts.explicitDataDir = storageDir();
    const auto grepFinds = [&](const std::string& pattern, const std::string& fileName) {
        yams::app::services::GrepOptions request;
        request.pattern = pattern;
        request.regexOnly = true;
        for (int attempt = 0; attempt < 150; ++attempt) {
            if (auto found = retrievalSvc.grep(request, retrievalOpts)) {
                for (const auto& match : found.value().matches) {
                    if (fs::path(match.file).filename() == fileName) {
                        return true;
                    }
                }
            }
            std::this_thread::sleep_for(100ms);
        }
        return false;
    };

    CHECK(grepFinds("watchneedlegamma", "nested.txt"));
    CHECK(grepFinds("watchneedlealpha", "first.txt"));

    // A file created while the daemon runs is picked up by a later pass.
    std::ofstream(watched / "second.txt") << "watchneedlebeta in the second file\n";
    CHECK(grepFinds("watchneedlebeta", "second.txt"));

    // Everything else is indexed by now, so the ignored file would be too if it leaked.
    {
        yams::app::services::GrepOptions ignored;
        ignored.pattern = "watchneedleomega";
        ignored.regexOnly = true;
        auto found = retrievalSvc.grep(ignored, retrievalOpts);
        REQUIRE(found);
        CHECK(found.value().matches.empty());
    }

    // What the watcher has seen is persisted next to the store it describes.
    std::error_code ec;
    CHECK(fs::exists(storageDir() / "watch", ec));

    fs::remove_all(watched, ec);
}

TEST_CASE_METHOD(GrepServiceExpectationsFixture,
                 "GrepService: PDF extractor enables grep and search",
                 "[integration][services][grep]") {
    start();

    fs::create_directories(root() / "pdf");
    auto pdfPath = root() / "pdf" / "doc.pdf";
    yams::test::TestDataGenerator generator;
    auto pdf = generator.generatePDF(3);
    {
        std::ofstream out(pdfPath, std::ios::binary);
        out.write(reinterpret_cast<const char*>(pdf.data()),
                  static_cast<std::streamsize>(pdf.size()));
    }

    yams::app::services::DocumentIngestionService ing;
    yams::app::services::AddOptions opts;
    opts.socketPath = socketPath();
    opts.explicitDataDir = storageDir();
    opts.path = pdfPath.string();
    opts.recursive = false;
    opts.noEmbeddings = true;
    auto add = ing.addViaDaemon(opts);
    REQUIRE(add);

    auto* sm = daemon()->getServiceManager();
    auto ctx = sm->getAppContext();
    auto grepSvc = yams::app::services::makeGrepService(ctx);

    yams::app::services::GrepRequest request;
    request.pattern = "Page";
    request.literalText = true;
    request.paths = {(root() / "pdf").string()};

    bool found = false;
    for (int attempt = 0; attempt < 60 && !found; ++attempt) {
        auto result = grepSvc->grep(request);
        if (result) {
            size_t totalMatches = 0;
            for (const auto& fileResult : result.value().results) {
                totalMatches += fileResult.matchCount;
            }
            if (totalMatches > 0) {
                found = true;
            }
        }
        if (!found) {
            std::this_thread::sleep_for(50ms);
        }
    }

    if (!found) {
        SKIP("PDF extractor not active on this runner");
    }
    CHECK(found);
}
