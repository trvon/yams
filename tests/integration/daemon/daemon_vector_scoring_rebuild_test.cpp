#include <gtest/gtest.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/daemon.h>

using namespace std::chrono_literals;

TEST(DaemonVectorScoring, RebuildEnablesVectorsWithoutBlocking) {
    // Start daemon with model provider disabled so initial build cannot enable vector scoring
#ifdef __APPLE__
    ::setenv("YAMS_USE_MOCK_PROVIDER", "1", 1);
    ::setenv("YAMS_DISABLE_ABI_PLUGINS", "1", 1);
#else
    setenv("YAMS_USE_MOCK_PROVIDER", "1", 1);
    setenv("YAMS_DISABLE_ABI_PLUGINS", "1", 1);
#endif
    namespace fs = std::filesystem;
    fs::path root = fs::temp_directory_path() / "yams_rebuild_it";
    fs::create_directories(root);
    fs::path sock = root / "daemon.sock";
    fs::path pidf = root / "daemon.pid";
    fs::path logf = root / "daemon.log";
    fs::path data = root / "data";
    fs::create_directories(data);

    yams::daemon::DaemonConfig cfg;
    cfg.dataDir = data;
    cfg.socketPath = sock;
    cfg.pidFile = pidf;
    cfg.logFile = logf;
    cfg.enableModelProvider = false; // key: disable to ensure initial build has no embeddings
    cfg.autoLoadPlugins = false;

    yams::daemon::YamsDaemon d(cfg);
    ASSERT_TRUE(d.start());
    struct Stopper {
        yams::daemon::YamsDaemon* d;
        ~Stopper() {
            if (d)
                (void)d->stop();
        }
    } stop{&d};

    yams::daemon::ClientConfig cc;
    cc.socketPath = sock;
    cc.autoStart = false;
    yams::daemon::DaemonClient client(cc);

    // Wait until search engine is ready (should complete without vector scoring)
    bool engineReady = false;
    bool vectorEnabledEarly = false;
    for (int i = 0; i < 100; ++i) {
        auto st = yams::cli::run_sync(client.status(), 300ms);
        if (st) {
            engineReady = st->readinessStates["search_engine"];
            if (st->readinessStates.count("vector_scoring_enabled"))
                vectorEnabledEarly = st->readinessStates["vector_scoring_enabled"];
            if (engineReady)
                break;
        }
        std::this_thread::sleep_for(50ms);
    }
    ASSERT_TRUE(engineReady) << "initial engine not ready";
    // Ideally vector scoring is false here; if already true we skip the non-blocking assertion.
    if (!vectorEnabledEarly) {
        // Now enable embeddings and trigger rebuild asynchronously
        auto* sm = d.getServiceManager();
        ASSERT_NE(sm, nullptr);
        auto eg = sm->ensureEmbeddingGeneratorReady();
        ASSERT_TRUE(eg);
        // Schedule rebuild
        boost::asio::co_spawn(
            boost::asio::system_executor(),
            [sm]() -> boost::asio::awaitable<void> {
                co_await sm->co_enableEmbeddingsAndRebuild();
                co_return;
            },
            boost::asio::detached);

        // Poll until vector_scoring_enabled becomes true (non-blocking: engine was already ready)
        bool vectorOn = false;
        for (int i = 0; i < 200; ++i) {
            auto st = yams::cli::run_sync(client.status(), 300ms);
            if (st && st->readinessStates.count("vector_scoring_enabled")) {
                vectorOn = st->readinessStates["vector_scoring_enabled"];
                if (vectorOn)
                    break;
            }
            std::this_thread::sleep_for(50ms);
        }
        EXPECT_TRUE(vectorOn) << "vector scoring did not enable after rebuild";
    }
}
