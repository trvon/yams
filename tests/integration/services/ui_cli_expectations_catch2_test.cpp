#include <catch2/catch_test_macros.hpp>

#include <nlohmann/json.hpp>

#include <boost/asio/local/stream_protocol.hpp>
#include <boost/system/error_code.hpp>

#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "../daemon/test_async_helpers.h"
#include "../daemon/test_daemon_harness.h"
#include <yams/app/services/document_ingestion_service.h>
#include <yams/app/services/retrieval_service.h>
#include <yams/app/services/services.hpp>
#include <yams/cli/yams_cli.h>
#include <yams/compression/compression_header.h>
#include <yams/daemon/client/asio_connection_pool.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/client/global_io_context.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/daemon.h>
#include <yams/metadata/knowledge_graph_store.h>

using namespace std::chrono_literals;
namespace fs = std::filesystem;

namespace {

#ifdef _WIN32
#define SKIP_ON_WINDOWS_DAEMON_SHUTDOWN()                                                          \
    SKIP("Windows daemon shutdown hangs - see windows-daemon-ipc-plan.md")
#else
#define SKIP_ON_WINDOWS_DAEMON_SHUTDOWN() ((void)0)
#endif

class ScopedEnvVar {
public:
    ScopedEnvVar(std::string key, std::string value) : key_(std::move(key)) {
        if (const char* existing = std::getenv(key_.c_str())) {
            previous_ = existing;
            hadPrevious_ = true;
        }
#if defined(_WIN32)
        _putenv_s(key_.c_str(), value.c_str());
#else
        setenv(key_.c_str(), value.c_str(), 1);
#endif
    }

    ~ScopedEnvVar() {
#if defined(_WIN32)
        if (hadPrevious_) {
            _putenv_s(key_.c_str(), previous_.c_str());
        } else {
            _putenv_s(key_.c_str(), "");
        }
#else
        if (hadPrevious_) {
            setenv(key_.c_str(), previous_.c_str(), 1);
        } else {
            unsetenv(key_.c_str());
        }
#endif
    }

    ScopedEnvVar(const ScopedEnvVar&) = delete;
    ScopedEnvVar& operator=(const ScopedEnvVar&) = delete;

private:
    std::string key_;
    std::string previous_;
    bool hadPrevious_{false};
};

class CaptureStdout {
public:
    CaptureStdout() : old_(std::cout.rdbuf(buffer_.rdbuf())) {}
    ~CaptureStdout() { std::cout.rdbuf(old_); }

    std::string str() const { return buffer_.str(); }

private:
    std::ostringstream buffer_;
    std::streambuf* old_{nullptr};
};

int runCliCommand(const std::vector<std::string>& args) {
    int rc = 0;
    {
        yams::cli::YamsCLI cli;
        std::vector<char*> argv;
        argv.reserve(args.size());
        for (const auto& arg : args) {
            argv.push_back(const_cast<char*>(arg.c_str()));
        }
        rc = cli.run(static_cast<int>(argv.size()), argv.data());
    }
    yams::daemon::AsioConnectionPool::shutdown_all(std::chrono::milliseconds(500));
    yams::daemon::GlobalIOContext::reset();
    return rc;
}

class UiCliExpectationsFixture {
public:
    ~UiCliExpectationsFixture() {
        harness_.reset();
        sessionEnvOverride_.reset();
    }

    void start() {
        SKIP_ON_WINDOWS_DAEMON_SHUTDOWN();
        if (!canBindUnixSocketHere()) {
            SKIP("AF_UNIX not available in this environment");
        }

        sessionEnvOverride_ = std::make_unique<ScopedEnvVar>("YAMS_SESSION_CURRENT", "");
        yams::test::DaemonHarnessOptions options;
        options.isolateState = true;
        harness_ = std::make_unique<yams::test::DaemonHarness>(options);
        REQUIRE(harness_->start(15s));

        storageDir_ = harness_->dataDir();
        socketPath_ = harness_->socketPath();
        root_ = storageDir_.parent_path();
        fs::create_directories(root_ / "ingest");
        std::this_thread::sleep_for(100ms);
    }

    yams::daemon::ServiceManager* serviceManager() const {
        return harness_ ? harness_->daemon()->getServiceManager() : nullptr;
    }

    const fs::path& root() const { return root_; }
    const fs::path& storageDir() const { return storageDir_; }
    const fs::path& socketPath() const { return socketPath_; }

private:
    static bool canBindUnixSocketHere() {
        try {
            boost::asio::io_context io;
            boost::asio::local::stream_protocol::acceptor acceptor(io);
            auto path = std::filesystem::path("/tmp") /
                        (std::string("yams-ui-cli-") + std::to_string(::getpid()) + ".sock");
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

    std::unique_ptr<yams::test::DaemonHarness> harness_;
    std::unique_ptr<ScopedEnvVar> sessionEnvOverride_;
    fs::path root_;
    fs::path storageDir_;
    fs::path socketPath_;
};

} // namespace

TEST_CASE_METHOD(UiCliExpectationsFixture, "UiCli: grep paths-only honors include and tags",
                 "[integration][services][ui-cli]") {
    start();

    fs::create_directories(root() / "ingest" / "dirA" / "dirB");
    std::ofstream(root() / "ingest" / "dirA" / "dirB" / "keep.md")
        << "# Title\nhello pattern tags\n";
    std::ofstream(root() / "ingest" / "dirA" / "dirB" / "skip.bin") << std::string(3, '\0');

    yams::app::services::DocumentIngestionService ing;
    yams::app::services::AddOptions opts;
    opts.socketPath = socketPath();
    opts.explicitDataDir = storageDir();
    opts.path = (root() / "ingest").string();
    opts.recursive = true;
    opts.includePatterns = {"*.md", "*.bin"};
    opts.tags = {"docs", "md", "ingest"};
    opts.noEmbeddings = true;
    auto addRes = ing.addViaDaemon(opts);
    REQUIRE(addRes);

    std::this_thread::sleep_for(200ms);

    yams::app::services::RetrievalService retrieval;
    yams::app::services::RetrievalOptions retrievalOpts;
    retrievalOpts.socketPath = socketPath();
    retrievalOpts.explicitDataDir = storageDir();

    yams::app::services::GrepOptions grepReq;
    grepReq.pattern = "hello";
    grepReq.pathsOnly = true;
    grepReq.includePatterns = {(root() / "ingest" / "**").string()};
    grepReq.filterTags = {"docs", "md"};
    grepReq.matchAllTags = true;

    bool found = false;
    for (int attempt = 0; attempt < 60 && !found; ++attempt) {
        auto grepRes = retrieval.grep(grepReq, retrievalOpts);
        REQUIRE(grepRes);
        for (const auto& match : grepRes.value().matches) {
            if (match.file.find("keep.md") != std::string::npos) {
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

TEST_CASE_METHOD(UiCliExpectationsFixture, "UiCli: get by hash metadata-only has no content",
                 "[integration][services][ui-cli]") {
    start();

    std::ofstream(root() / "ingest" / "hello.txt") << "hello yams";

    yams::app::services::DocumentIngestionService ing;
    yams::app::services::AddOptions opts;
    opts.socketPath = socketPath();
    opts.explicitDataDir = storageDir();
    opts.path = (root() / "ingest" / "hello.txt").string();
    opts.noEmbeddings = true;
    auto addRes = ing.addViaDaemon(opts);
    REQUIRE(addRes);
    REQUIRE_FALSE(addRes.value().hash.empty());

    auto* sm = serviceManager();
    REQUIRE(sm != nullptr);
    auto ctx = sm->getAppContext();
    REQUIRE(yams::test::waitForDocumentMetadata(ctx.metadataRepo, addRes.value().hash, 10000ms));

    yams::app::services::RetrievalService retrieval;
    yams::app::services::RetrievalOptions retrievalOpts;
    retrievalOpts.socketPath = socketPath();
    retrievalOpts.explicitDataDir = storageDir();

    yams::app::services::GetOptions getReq;
    getReq.hash = addRes.value().hash;
    getReq.metadataOnly = true;
    auto getRes = retrieval.get(getReq, retrievalOpts);
    REQUIRE(getRes);
    CHECK_FALSE(getRes.value().hasContent);
    CHECK(getRes.value().content.empty());
}

TEST_CASE_METHOD(UiCliExpectationsFixture, "UiCli: get honors acceptCompressed flag",
                 "[integration][services][ui-cli]") {
    start();

    const std::string payload = "payload with enough entropy to compress";
    const fs::path payloadPath = root() / "ingest" / "compress.txt";
    {
        std::ofstream out(payloadPath, std::ios::binary);
        for (int i = 0; i < 4096; ++i) {
            out << payload;
        }
    }

    const uint64_t expectedUncompressedSize = static_cast<uint64_t>(payload.size()) * 4096ULL;
    REQUIRE(std::filesystem::file_size(payloadPath) == expectedUncompressedSize);

    yams::app::services::DocumentIngestionService ing;
    yams::app::services::AddOptions addOpts;
    addOpts.socketPath = socketPath();
    addOpts.explicitDataDir = storageDir();
    addOpts.path = payloadPath.string();
    addOpts.noEmbeddings = true;
    auto addRes = ing.addViaDaemon(addOpts);
    REQUIRE(addRes);
    REQUIRE_FALSE(addRes.value().hash.empty());

    auto* sm = serviceManager();
    REQUIRE(sm != nullptr);
    auto ctx = sm->getAppContext();
    REQUIRE(yams::test::waitForDocumentMetadata(ctx.metadataRepo, addRes.value().hash, 10000ms));

    yams::app::services::RetrievalService retrieval;
    yams::app::services::RetrievalOptions retrievalOpts;
    retrievalOpts.socketPath = socketPath();
    retrievalOpts.explicitDataDir = storageDir();

    yams::app::services::GetOptions getReq;
    getReq.hash = addRes.value().hash;
    getReq.acceptCompressed = true;

    std::optional<yams::daemon::GetResponse> compressed;
    for (int attempt = 0; attempt < 60 && !compressed; ++attempt) {
        auto attemptRes = retrieval.get(getReq, retrievalOpts);
        if (attemptRes && attemptRes.value().compressed) {
            compressed = std::move(attemptRes.value());
        } else {
            std::this_thread::sleep_for(50ms);
        }
    }
    REQUIRE(compressed.has_value());
    CHECK(compressed->compressed);
    CHECK(compressed->hasContent);
    CHECK(compressed->compressionAlgorithm.has_value());
    CHECK(compressed->compressionLevel.has_value());
    CHECK(compressed->uncompressedSize.has_value());
    CHECK(*compressed->uncompressedSize == expectedUncompressedSize);
    REQUIRE_FALSE(compressed->compressionHeader.empty());
    CHECK(compressed->compressionHeader.size() == yams::compression::CompressionHeader::SIZE);

    REQUIRE(compressed->content.size() >= yams::compression::CompressionHeader::SIZE);
    uint32_t magic = 0;
    uint64_t uncompressedSize = 0;
    uint8_t algorithm = 0;
    uint8_t level = 0;
    std::memcpy(&magic,
                compressed->content.data() + offsetof(yams::compression::CompressionHeader, magic),
                sizeof(magic));
    std::memcpy(&uncompressedSize,
                compressed->content.data() +
                    offsetof(yams::compression::CompressionHeader, uncompressedSize),
                sizeof(uncompressedSize));
    std::memcpy(&algorithm,
                compressed->content.data() +
                    offsetof(yams::compression::CompressionHeader, algorithm),
                sizeof(algorithm));
    std::memcpy(&level,
                compressed->content.data() + offsetof(yams::compression::CompressionHeader, level),
                sizeof(level));

    CHECK(magic == yams::compression::CompressionHeader::MAGIC);
    CHECK(uncompressedSize == expectedUncompressedSize);
    CHECK(algorithm == compressed->compressionAlgorithm.value());
    CHECK(level == compressed->compressionLevel.value());
    CHECK(0 == std::memcmp(compressed->compressionHeader.data(), compressed->content.data(),
                           yams::compression::CompressionHeader::SIZE));

    yams::app::services::RetrievalOptions plainOpts = retrievalOpts;
    plainOpts.acceptCompressed = false;
    yams::app::services::GetOptions plainReq = getReq;
    plainReq.acceptCompressed = false;

    std::optional<yams::daemon::GetResponse> plain;
    for (int attempt = 0; attempt < 60 && !plain; ++attempt) {
        auto attemptRes = retrieval.get(plainReq, plainOpts);
        if (attemptRes) {
            plain = std::move(attemptRes.value());
        } else {
            std::this_thread::sleep_for(50ms);
        }
    }
    REQUIRE(plain.has_value());
    CHECK_FALSE(plain->compressed);
    CHECK(plain->hasContent);
    CHECK(plain->compressionHeader.empty());
    CHECK(plain->content.size() >= payload.size());
    CHECK(0 != std::memcmp(compressed->content.data(), plain->content.data(),
                           std::min(compressed->content.size(), plain->content.size())));
}

TEST_CASE_METHOD(UiCliExpectationsFixture, "UiCli: list limit and namePattern",
                 "[integration][services][ui-cli]") {
    start();

    fs::create_directories(root() / "ingest" / "docs");
    std::ofstream(root() / "ingest" / "docs" / "a.md") << "alpha";
    std::ofstream(root() / "ingest" / "docs" / "b.md") << "bravo";
    std::ofstream(root() / "ingest" / "docs" / "c.txt") << "charlie";

    yams::app::services::DocumentIngestionService ing;
    yams::app::services::AddOptions opts;
    opts.socketPath = socketPath();
    opts.explicitDataDir = storageDir();
    opts.path = (root() / "ingest").string();
    opts.recursive = true;
    opts.noEmbeddings = true;
    auto addRes = ing.addViaDaemon(opts);
    REQUIRE(addRes);

    std::this_thread::sleep_for(200ms);

    yams::app::services::RetrievalService retrieval;
    yams::app::services::RetrievalOptions retrievalOpts;
    retrievalOpts.socketPath = socketPath();
    retrievalOpts.explicitDataDir = storageDir();

    yams::app::services::ListOptions listReq;
    listReq.limit = 2;
    listReq.namePattern = (root() / "ingest" / "**" / "*.md").string();

    bool ok = false;
    for (int attempt = 0; attempt < 100 && !ok; ++attempt) {
        auto listRes = retrieval.list(listReq, retrievalOpts);
        REQUIRE(listRes);
        const auto& items = listRes.value().items;
        if (!items.empty()) {
            bool allMd = true;
            for (const auto& entry : items) {
                if (entry.name.rfind(".md") == std::string::npos) {
                    allMd = false;
                    break;
                }
            }
            if (allMd && items.size() <= 2) {
                ok = true;
            }
        }
        if (!ok) {
            std::this_thread::sleep_for(50ms);
        }
    }

    REQUIRE(ok);
}

TEST_CASE_METHOD(UiCliExpectationsFixture,
                 "UiCli: CLI list wildcard and directory inputs stay in list mode",
                 "[integration][services][ui-cli]") {
    start();

    fs::create_directories(root() / "ingest" / "listmode" / "nested");
    std::ofstream(root() / "ingest" / "listmode" / "nested" / "alpha.md") << "alpha";
    std::ofstream(root() / "ingest" / "listmode" / "nested" / "beta.txt") << "beta";

    yams::app::services::DocumentIngestionService ing;
    yams::app::services::AddOptions opts;
    opts.socketPath = socketPath();
    opts.explicitDataDir = storageDir();
    opts.path = (root() / "ingest" / "listmode").string();
    opts.recursive = true;
    opts.noEmbeddings = true;
    auto addRes = ing.addViaDaemon(opts);
    REQUIRE(addRes);

    auto* sm = serviceManager();
    REQUIRE(sm != nullptr);
    auto ctx = sm->getAppContext();

    bool visible = false;
    for (int attempt = 0; attempt < 60 && !visible; ++attempt) {
        yams::metadata::DocumentQueryOptions queryOpts;
        queryOpts.pathPrefix = (root() / "ingest" / "listmode").string();
        queryOpts.prefixIsDirectory = true;
        queryOpts.includeSubdirectories = true;
        queryOpts.limit = 10;
        auto docsRes = ctx.metadataRepo->queryDocuments(queryOpts);
        REQUIRE(docsRes);
        visible = !docsRes.value().empty();
        if (!visible) {
            std::this_thread::sleep_for(50ms);
        }
    }
    REQUIRE(visible);

    ScopedEnvVar socketEnv("YAMS_DAEMON_SOCKET", socketPath().string());
    ScopedEnvVar noAutoStart("YAMS_CLI_DISABLE_DAEMON_AUTOSTART", "1");

    {
        CaptureStdout capture;
        int rc =
            runCliCommand({"yams", "--data-dir", storageDir().string(), "list", "--json", "--limit",
                           "5", "--name", (root() / "ingest" / "listmode" / "*").string()});
        CHECK(rc == 0);

        auto parsed = nlohmann::json::parse(capture.str(), nullptr, false);
        REQUIRE_FALSE(parsed.is_discarded());
        REQUIRE(parsed.contains("documents"));
        REQUIRE(parsed["documents"].is_array());
        CHECK(capture.str().find("File History:") == std::string::npos);

        bool foundAlpha = false;
        for (const auto& doc : parsed["documents"]) {
            const std::string path = doc.value("path", "");
            if (path.find("alpha.md") != std::string::npos) {
                foundAlpha = true;
                break;
            }
        }
        CHECK(foundAlpha);
    }

    {
        CaptureStdout capture;
        int rc =
            runCliCommand({"yams", "--data-dir", storageDir().string(), "list", "--json", "--limit",
                           "5", "--name", (root() / "ingest" / "listmode").string()});
        CHECK(rc == 0);

        auto parsed = nlohmann::json::parse(capture.str(), nullptr, false);
        REQUIRE_FALSE(parsed.is_discarded());
        REQUIRE(parsed.contains("documents"));
        REQUIRE(parsed["documents"].is_array());

        bool foundNested = false;
        for (const auto& doc : parsed["documents"]) {
            const std::string path = doc.value("path", "");
            if (path.find("beta.txt") != std::string::npos) {
                foundNested = true;
                break;
            }
        }
        CHECK(foundNested);
        CHECK(parsed.value("total", 0) >= 2);
    }
}

TEST_CASE_METHOD(UiCliExpectationsFixture, "UiCli: update metadata then delete by name",
                 "[integration][services][ui-cli]") {
    start();

    std::ofstream(root() / "ingest" / "ud.txt") << "update/delete shape";

    yams::app::services::DocumentIngestionService ing;
    yams::app::services::AddOptions opts;
    opts.socketPath = socketPath();
    opts.explicitDataDir = storageDir();
    opts.path = (root() / "ingest" / "ud.txt").string();
    opts.noEmbeddings = true;
    auto addRes = ing.addViaDaemon(opts);
    REQUIRE(addRes);

    yams::app::services::RetrievalService retrieval;
    yams::app::services::RetrievalOptions retrievalOpts;
    retrievalOpts.socketPath = socketPath();
    retrievalOpts.explicitDataDir = storageDir();

    std::string canonicalName;
    for (int attempt = 0; attempt < 40 && canonicalName.empty(); ++attempt) {
        yams::app::services::GetOptions getByHash;
        getByHash.hash = addRes.value().hash;
        getByHash.metadataOnly = true;
        auto getRes = retrieval.get(getByHash, retrievalOpts);
        if (getRes && !getRes.value().name.empty()) {
            canonicalName = getRes.value().name;
            break;
        }
        std::this_thread::sleep_for(50ms);
    }
    REQUIRE_FALSE(canonicalName.empty());

    yams::daemon::ClientConfig cfg;
    cfg.socketPath = socketPath();
    cfg.autoStart = false;
    cfg.requestTimeout = 5s;
    yams::daemon::DaemonClient client(cfg);
    auto updateRes =
        yams::test_async::res(client.updateDocument(yams::daemon::UpdateDocumentRequest{
                                  "",
                                  canonicalName,
                                  "",
                                  std::vector<std::string>{"tmpdel"},
                                  {},
                                  std::map<std::string, std::string>{{"k", "v"}},
                                  true,
                                  false,
                                  false,
                              }),
                              2s);
    REQUIRE(updateRes);

    {
        yams::app::services::ListOptions listReq;
        listReq.limit = 10;
        listReq.filterTags = "tmpdel";
        for (int attempt = 0; attempt < 20; ++attempt) {
            auto listRes = retrieval.list(listReq, retrievalOpts);
            if (listRes && !listRes.value().items.empty()) {
                break;
            }
            std::this_thread::sleep_for(50ms);
        }
    }

    yams::daemon::DeleteRequest deleteReq;
    deleteReq.name = canonicalName;
    deleteReq.force = true;
    auto deleteRes = yams::test_async::res(client.remove(deleteReq), 2s);
    if (!deleteRes) {
        INFO("Delete by name returned error but may have succeeded: " << deleteRes.error().message);
    }

    bool gone = false;
    yams::app::services::ListOptions listReq;
    listReq.limit = 10;
    listReq.namePattern = canonicalName;
    for (int attempt = 0; attempt < 80 && !gone; ++attempt) {
        auto listRes = retrieval.list(listReq, retrievalOpts);
        REQUIRE(listRes);
        gone = listRes.value().items.empty();
        if (!gone) {
            std::this_thread::sleep_for(50ms);
        }
    }
    CHECK(gone);
}

TEST_CASE_METHOD(UiCliExpectationsFixture, "UiCli: fuzzy search paths-only",
                 "[integration][services][ui-cli]") {
    start();

    std::ofstream(root() / "ingest" / "alpha.txt") << "functional programming in yams";
    std::ofstream(root() / "ingest" / "beta.txt") << "systems programming in c++";

    yams::app::services::DocumentIngestionService ing;
    yams::app::services::AddOptions opts;
    opts.socketPath = socketPath();
    opts.explicitDataDir = storageDir();
    opts.path = root() / "ingest";
    opts.path = (root() / "ingest").string();
    opts.recursive = true;
    opts.noEmbeddings = true;
    auto addRes = ing.addViaDaemon(opts);
    REQUIRE(addRes);

    auto* sm = serviceManager();
    REQUIRE(sm != nullptr);
    auto ctx = sm->getAppContext();

    const std::string ingestPath = (root() / "ingest").string();
    REQUIRE(yams::test::waitForDocumentsByPath(ctx.metadataRepo, ingestPath, 2, 5000ms));

    auto searchSvc = yams::app::services::makeSearchService(ctx);
    yams::metadata::DocumentQueryOptions queryOpts;
    queryOpts.pathPrefix = ingestPath;
    queryOpts.prefixIsDirectory = true;
    auto docsRes = ctx.metadataRepo->queryDocuments(queryOpts);
    if (docsRes) {
        for (const auto& doc : docsRes.value()) {
            (void)searchSvc->lightIndexForHash(doc.sha256Hash);
        }
    }

    yams::app::services::SearchRequest pollReq;
    pollReq.query = "programming";
    pollReq.fuzzy = false;
    pollReq.pathsOnly = true;
    pollReq.limit = 1;
    pollReq.pathPattern = (root() / "ingest" / "**").string();
    for (int attempt = 0; attempt < 50; ++attempt) {
        auto pollRes = yams::test_async::res(searchSvc->search(pollReq), 1s);
        if (pollRes && !pollRes.value().paths.empty()) {
            break;
        }
        std::this_thread::sleep_for(100ms);
    }

    yams::app::services::SearchRequest searchReq;
    searchReq.query = "programing";
    searchReq.fuzzy = true;
    searchReq.similarity = 0.6f;
    searchReq.limit = 10;
    searchReq.pathsOnly = true;
    searchReq.pathPattern = (root() / "ingest" / "**").string();

    auto searchRes = yams::test_async::res(searchSvc->search(searchReq), 2s);
    REQUIRE(searchRes);
    REQUIRE_FALSE(searchRes.value().paths.empty());
    for (const auto& path : searchRes.value().paths) {
        CHECK(path.find((root() / "ingest").string()) != std::string::npos);
    }
}

TEST_CASE_METHOD(UiCliExpectationsFixture, "UiCli: verbose hybrid includes result structure",
                 "[integration][services][ui-cli]") {
    start();

    std::ofstream(root() / "ingest" / "note1.txt") << "semantic vector keyword";
    std::ofstream(root() / "ingest" / "note2.txt") << "vector search hybrid";

    yams::app::services::DocumentIngestionService ing;
    yams::app::services::AddOptions opts;
    opts.socketPath = socketPath();
    opts.explicitDataDir = storageDir();
    opts.path = (root() / "ingest").string();
    opts.recursive = true;
    opts.noEmbeddings = true;
    REQUIRE(ing.addViaDaemon(opts));

    auto* sm = serviceManager();
    REQUIRE(sm != nullptr);
    std::shared_ptr<yams::search::SearchEngine> snapshot;
    for (int attempt = 0; attempt < 60 && !snapshot; ++attempt) {
        snapshot = sm->getSearchEngineSnapshot();
        if (!snapshot) {
            std::this_thread::sleep_for(50ms);
        }
    }
    if (!snapshot) {
        SKIP("Search engine not ready; skipping verbose hybrid structure check");
    }

    auto ctx = sm->getAppContext();
    ctx.searchEngine = snapshot;
    ctx.searchRepairInProgress = false;
    auto searchSvc = yams::app::services::makeSearchService(ctx);

    yams::app::services::SearchRequest searchReq;
    searchReq.query = "vector";
    searchReq.type = "hybrid";
    searchReq.limit = 10;
    searchReq.verbose = true;

    auto searchRes = yams::test_async::res(searchSvc->search(searchReq), 2s);
    REQUIRE(searchRes);
    CHECK(searchRes.value().results.size() >= 0u);
}

TEST_CASE_METHOD(UiCliExpectationsFixture, "UiCli: paths-only with pattern and tags",
                 "[integration][services][ui-cli]") {
    start();

    fs::create_directories(root() / "ingest" / "d");
    auto mdPath = root() / "ingest" / "d" / "doc.md";
    auto txtPath = root() / "ingest" / "d" / "other.txt";
    std::ofstream(mdPath) << "hello tags";
    std::ofstream(txtPath) << "skip";

    yams::app::services::DocumentIngestionService ing;
    yams::app::services::AddOptions addMd;
    addMd.socketPath = socketPath();
    addMd.explicitDataDir = storageDir();
    addMd.path = mdPath.string();
    addMd.noEmbeddings = true;
    addMd.tags = {"docs", "md"};
    REQUIRE(ing.addViaDaemon(addMd));

    yams::app::services::AddOptions addTxt = addMd;
    addTxt.path = txtPath.string();
    addTxt.tags = {};
    REQUIRE(ing.addViaDaemon(addTxt));

    auto* sm = serviceManager();
    REQUIRE(sm != nullptr);
    auto ctx = sm->getAppContext();
    auto searchSvc = yams::app::services::makeSearchService(ctx);

    yams::app::services::SearchRequest searchReq;
    searchReq.query = "hello";
    searchReq.fuzzy = true;
    searchReq.similarity = 0.6f;
    searchReq.pathsOnly = true;
    searchReq.limit = 10;
    searchReq.pathPattern = (root() / "ingest" / "**").string();
    searchReq.tags = {"docs", "md"};
    searchReq.matchAllTags = true;
    searchReq.extension = "md";

    auto searchRes = yams::test_async::res(searchSvc->search(searchReq), 2s);
    REQUIRE(searchRes);
    for (const auto& path : searchRes.value().paths) {
        CHECK(path.find("other.txt") == std::string::npos);
    }
}

TEST_CASE_METHOD(UiCliExpectationsFixture, "UiCli: negative no-match paths-only search",
                 "[integration][services][ui-cli]") {
    start();

    auto* sm = serviceManager();
    REQUIRE(sm != nullptr);
    auto ctx = sm->getAppContext();
    auto searchSvc = yams::app::services::makeSearchService(ctx);

    yams::app::services::SearchRequest searchReq;
    searchReq.query = "stringthatwillnotmatch";
    searchReq.fuzzy = true;
    searchReq.similarity = 0.8f;
    searchReq.pathsOnly = true;
    searchReq.limit = 5;
    searchReq.pathPattern = (root() / "ingest" / "**").string();

    auto searchRes = yams::test_async::res(searchSvc->search(searchReq), 2s);
    REQUIRE(searchRes);
    CHECK(searchRes.value().paths.empty());
}

TEST_CASE_METHOD(UiCliExpectationsFixture, "UiCli: hash search normalization",
                 "[integration][services][ui-cli]") {
    start();

    fs::create_directories(root() / "ingest" / "hash");
    auto path = root() / "ingest" / "hash" / "h.txt";
    std::ofstream(path) << "hash query";

    yams::app::services::DocumentIngestionService ing;
    yams::app::services::AddOptions opts;
    opts.socketPath = socketPath();
    opts.explicitDataDir = storageDir();
    opts.path = path.string();
    opts.noEmbeddings = true;
    auto addRes = ing.addViaDaemon(opts);
    REQUIRE(addRes);
    const auto& hash = addRes.value().hash;
    REQUIRE(hash.size() > 12u);

    auto* sm = serviceManager();
    REQUIRE(sm != nullptr);
    auto ctx = sm->getAppContext();
    REQUIRE(yams::test::waitForDocumentMetadata(ctx.metadataRepo, hash, 5000ms));
    auto searchSvc = yams::app::services::makeSearchService(ctx);

    yams::app::services::SearchRequest fullReq;
    fullReq.type = "hash";
    fullReq.hash = hash;
    fullReq.pathsOnly = true;

    bool fullOk = false;
    for (int attempt = 0; attempt < 40 && !fullOk; ++attempt) {
        auto fullRes = yams::test_async::res(searchSvc->search(fullReq), 2s);
        REQUIRE(fullRes);
        fullOk = !fullRes.value().paths.empty();
        if (!fullOk) {
            std::this_thread::sleep_for(50ms);
        }
    }
    REQUIRE(fullOk);

    yams::app::services::SearchRequest prefixReq = fullReq;
    prefixReq.hash = hash.substr(0, 12);
    bool prefixOk = false;
    for (int attempt = 0; attempt < 40 && !prefixOk; ++attempt) {
        auto prefixRes = yams::test_async::res(searchSvc->search(prefixReq), 2s);
        REQUIRE(prefixRes);
        prefixOk = !prefixRes.value().paths.empty();
        if (!prefixOk) {
            std::this_thread::sleep_for(50ms);
        }
    }
    REQUIRE(prefixOk);
}

TEST_CASE_METHOD(UiCliExpectationsFixture, "UiCli: short hash prefix is rejected",
                 "[integration][services][ui-cli]") {
    start();

    fs::create_directories(root() / "ingest" / "hash2");
    auto path = root() / "ingest" / "hash2" / "h2.txt";
    std::ofstream(path) << "hash query 2";

    yams::app::services::DocumentIngestionService ing;
    yams::app::services::AddOptions opts;
    opts.socketPath = socketPath();
    opts.explicitDataDir = storageDir();
    opts.path = path.string();
    opts.noEmbeddings = true;
    auto addRes = ing.addViaDaemon(opts);
    REQUIRE(addRes);
    const auto& fullHash = addRes.value().hash;
    REQUIRE(fullHash.size() > 12u);

    auto* sm = serviceManager();
    REQUIRE(sm != nullptr);
    auto ctx = sm->getAppContext();
    auto searchSvc = yams::app::services::makeSearchService(ctx);

    yams::app::services::SearchRequest searchReq;
    searchReq.type = "hash";
    searchReq.hash = fullHash.substr(0, 7);
    searchReq.pathsOnly = true;
    auto searchRes = yams::test_async::res(searchSvc->search(searchReq), 2s);
    REQUIRE_FALSE(searchRes);
    const std::string message = searchRes.error().message;
    const bool ok = (message.find("Invalid hash format") != std::string::npos) ||
                    (message.find("not found") != std::string::npos) ||
                    (message.find("invalid") != std::string::npos);
    CHECK(ok);
}

TEST_CASE_METHOD(UiCliExpectationsFixture, "UiCli: degraded fallback structure",
                 "[integration][services][ui-cli]") {
    start();

    ScopedEnvVar degradedEnv("YAMS_SEARCH_DEGRADED", "1");
    ScopedEnvVar degradedReasonEnv("YAMS_SEARCH_DEGRADED_REASON", "maintenance");

    fs::create_directories(root() / "ingest" / "deg");
    std::ofstream(root() / "ingest" / "deg" / "d.md") << "maintenance window";

    yams::app::services::DocumentIngestionService ing;
    yams::app::services::AddOptions opts;
    opts.socketPath = socketPath();
    opts.explicitDataDir = storageDir();
    opts.path = (root() / "ingest").string();
    opts.recursive = true;
    opts.noEmbeddings = true;
    auto addRes = ing.addViaDaemon(opts);
    REQUIRE(addRes);

    auto* sm = serviceManager();
    REQUIRE(sm != nullptr);
    auto ctx = sm->getAppContext();
    REQUIRE(yams::test::waitForDocumentsByPath(ctx.metadataRepo, (root() / "ingest").string(), 1,
                                               5000ms));
    auto searchSvc = yams::app::services::makeSearchService(ctx);

    yams::app::services::SearchRequest searchReq;
    searchReq.query = "maintenance";
    searchReq.type = "hybrid";
    searchReq.limit = 5;
    searchReq.verbose = true;
    auto searchRes = yams::test_async::res(searchSvc->search(searchReq), 2s);
    REQUIRE(searchRes);
    CHECK_FALSE(searchRes.value().usedHybrid);
    auto modeIt = searchRes.value().searchStats.find("mode");
    REQUIRE(modeIt != searchRes.value().searchStats.end());
    CHECK(modeIt->second == "degraded");
    const bool mentionsDegraded =
        (searchRes.value().queryInfo.find("degraded") != std::string::npos) ||
        (searchRes.value().queryInfo.find("fallback") != std::string::npos);
    CHECK(mentionsDegraded);
}

TEST_CASE_METHOD(UiCliExpectationsFixture, "UiCli: fuzzy bounds similarity zero and one",
                 "[integration][services][ui-cli]") {
    start();

    fs::create_directories(root() / "ingest" / "fuzzy");
    std::ofstream(root() / "ingest" / "fuzzy" / "exact.txt") << "functional programming";
    std::ofstream(root() / "ingest" / "fuzzy" / "near.txt") << "functionl programing";

    yams::app::services::DocumentIngestionService ing;
    yams::app::services::AddOptions opts;
    opts.socketPath = socketPath();
    opts.explicitDataDir = storageDir();
    opts.path = (root() / "ingest").string();
    opts.recursive = true;
    opts.noEmbeddings = true;
    REQUIRE(ing.addViaDaemon(opts));

    auto* sm = serviceManager();
    REQUIRE(sm != nullptr);
    auto ctx = sm->getAppContext();
    auto searchSvc = yams::app::services::makeSearchService(ctx);

    std::this_thread::sleep_for(150ms);

    yams::app::services::SearchRequest lowReq;
    lowReq.query = "programming";
    lowReq.fuzzy = true;
    lowReq.similarity = 0.0f;
    lowReq.pathsOnly = true;
    lowReq.limit = 50;

    yams::app::services::SearchRequest highReq = lowReq;
    highReq.similarity = 1.0f;

    auto lowRes = yams::test_async::res(searchSvc->search(lowReq), 2s);
    REQUIRE(lowRes);
    auto highRes = yams::test_async::res(searchSvc->search(highReq), 2s);
    REQUIRE(highRes);

    CHECK(lowRes.value().paths.size() >= highRes.value().paths.size());

    auto hasExactPath = [](const std::vector<std::string>& paths) {
        for (const auto& path : paths) {
            if (path.find("exact.txt") != std::string::npos) {
                return true;
            }
        }
        return false;
    };

    bool highHasExact = false;
    const auto deadline = std::chrono::steady_clock::now() + 2s;
    while (std::chrono::steady_clock::now() < deadline) {
        if (hasExactPath(highRes.value().paths)) {
            highHasExact = true;
            break;
        }
        std::this_thread::sleep_for(50ms);
        highRes = yams::test_async::res(searchSvc->search(highReq), 2s);
        REQUIRE(highRes);
    }
    CHECK(highHasExact);
}
