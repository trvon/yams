#include <atomic>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <thread>
#include <vector>
#include <gtest/gtest.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/version.hpp>
#include <yams/cli/asio_client_pool.hpp>

namespace yams::daemon::test {

using namespace std::chrono_literals;
namespace fs = std::filesystem;

class IpcClientTest : public ::testing::Test {
protected:
    void SetUp() override {
        cleanupTestFiles();
        // Point client to a temp path; no daemon is started in these tests.
        clientConfig_.socketPath = fs::temp_directory_path() / "test_ipc_daemon.sock";
        clientConfig_.connectTimeout = 500ms;
        clientConfig_.requestTimeout = 1000ms;
        clientConfig_.autoStart = false; // never spawn external daemon during unit tests

    poolConfig_.socketPath = clientConfig_.socketPath; // ensure pool also targets nowhere
    }

    void TearDown() override { cleanupTestFiles(); }

    void cleanupTestFiles() {
        std::error_code ec;
        fs::remove(fs::temp_directory_path() / "test_ipc_daemon.sock", ec);
        fs::remove(fs::temp_directory_path() / "test_ipc_daemon.pid", ec);
        fs::remove(fs::temp_directory_path() / "test_ipc_daemon.log", ec);
        fs::remove_all(fs::temp_directory_path() / "yams_test_ipc_data", ec);
    }

    ClientConfig clientConfig_;
    yams::cli::AsioClientPool::Config poolConfig_;
};

// Basic connect should fail when no daemon is present
TEST_F(IpcClientTest, BasicConnectionFailsWhenNoDaemon) {
    DaemonClient client(clientConfig_);
    auto result = client.connect();
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, ErrorCode::NetworkError);
    EXPECT_FALSE(client.isConnected());
}

// Generic call() with PingRequest should fail gracefully without daemon
TEST_F(IpcClientTest, GenericCallPing) {
    yams::cli::AsioClientPool pool{poolConfig_};
    auto result = pool.ping();
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, ErrorCode::NetworkError);
}

// Generic call() with StatusRequest should fail gracefully without daemon
TEST_F(IpcClientTest, GenericCallStatus) {
    yams::cli::AsioClientPool pool{poolConfig_};
    StatusRequest req{true};
    auto result = pool.call<StatusRequest, StatusResponse>(req);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, ErrorCode::NetworkError);
}

// Generic call() with SearchRequest should fail gracefully without daemon
TEST_F(IpcClientTest, GenericCallSearch) {
    SearchRequest req{"test query", 10,    false, false, 0.8, {}, "keyword", false,
                      false,        false, false, false, 0,   0,  0,         ""};
    yams::cli::AsioClientPool pool{poolConfig_};
    auto result = pool.call<SearchRequest, SearchResponse>(req);
    EXPECT_FALSE(result);
    EXPECT_EQ(result.error().code, ErrorCode::NetworkError);
}

// Error mapping still yields NetworkError without a daemon
TEST_F(IpcClientTest, ErrorResponseMapping) {
    GetRequest req; // invalid by design
    req.hash = "";
    req.byName = false;
    yams::cli::AsioClientPool pool{poolConfig_};
    auto result = pool.call<GetRequest, ErrorResponse>(req);
    EXPECT_FALSE(result);
    EXPECT_EQ(result.error().code, ErrorCode::NetworkError);
}

// Concurrent calls should all fail when daemon is absent
TEST_F(IpcClientTest, ConcurrentGenericCalls) {
    const int numThreads = 5;
    const int callsPerThread = 10;

    std::atomic<int> successCount{0};
    std::atomic<int> failCount{0};

    std::vector<std::thread> threads;
    for (int t = 0; t < numThreads; ++t) {
        threads.emplace_back([this, &successCount, &failCount, callsPerThread]() {
            yams::cli::AsioClientPool pool{poolConfig_};
            for (int i = 0; i < callsPerThread; ++i) {
                if (i % 3 == 0) {
                    auto r = pool.ping();
                    if (!r)
                        failCount++;
                } else if (i % 3 == 1) {
                    StatusRequest req{false};
                    auto r = pool.call<StatusRequest, StatusResponse>(req);
                    if (!r)
                        failCount++;
                } else {
                    SearchRequest req{"test", 5,     false, false, 0.7, {}, "keyword", false,
                                      false,  false, false, false, 0,   0,  0,         ""};
                    auto r = pool.call<SearchRequest, SearchResponse>(req);
                    if (!r)
                        failCount++;
                }
            }
        });
    }

    for (auto& t : threads)
        t.join();

    EXPECT_EQ(successCount, 0);
    EXPECT_EQ(failCount, numThreads * callsPerThread);
}

// Env flags should not change the absence-of-daemon behavior
TEST_F(IpcClientTest, ProviderBackedGeneratorAvailable) {
    ::setenv("YAMS_USE_MOCK_PROVIDER", "1", 1);
    SearchRequest req{"provider", 5,     false, false, 0.7, {}, "keyword", false,
                      false,      false, false, false, 0,   0,  0,         ""};
    yams::cli::AsioClientPool pool{poolConfig_};
    auto result = pool.call<SearchRequest, SearchResponse>(req);
    EXPECT_FALSE(result);
    EXPECT_EQ(result.error().code, ErrorCode::NetworkError);

    ::unsetenv("YAMS_USE_MOCK_PROVIDER");
}

TEST_F(IpcClientTest, ProviderBackedInitializationStatus) {
    ::setenv("YAMS_USE_MOCK_PROVIDER", "1", 1);

    StatusRequest sreq{true};
    yams::cli::AsioClientPool pool{poolConfig_};
    auto sres = pool.call<StatusRequest, StatusResponse>(sreq);
    EXPECT_FALSE(sres);
    EXPECT_EQ(sres.error().code, ErrorCode::NetworkError);

    ::unsetenv("YAMS_USE_MOCK_PROVIDER");
}

TEST_F(IpcClientTest, ProviderDisabledFallbackToFuzzyOrFTS) {
    ::setenv("YAMS_DISABLE_ONNX", "1", 1);

    SearchRequest req{"fallback case", 5,     false, false, 0.7, {}, "keyword", false,
                      false,           false, false, false, 0,   0,  0,         ""};
    req.fuzzy = true;
    req.similarity = 0.7;

    yams::cli::AsioClientPool pool{poolConfig_};
    auto result = pool.call<SearchRequest, SearchResponse>(req);
    EXPECT_FALSE(result);
    EXPECT_EQ(result.error().code, ErrorCode::NetworkError);

    ::unsetenv("YAMS_DISABLE_ONNX");
}

// Circuit breaker should handle repeated failures gracefully
TEST_F(IpcClientTest, CircuitBreaker) {
    for (int i = 0; i < 10; ++i) {
    yams::cli::AsioClientPool pool{poolConfig_};
    auto result = pool.ping();
        EXPECT_FALSE(result);
    }
}

// Auto-reconnect is not possible without a daemon
TEST_F(IpcClientTest, AutoReconnect) {
    yams::cli::AsioClientPool pool{poolConfig_};
    auto pingResult = pool.ping();
    EXPECT_FALSE(pingResult);
}

// Short timeouts still yield NetworkError without a daemon
TEST_F(IpcClientTest, RequestTimeout) {
    ClientConfig shortTimeoutConfig = clientConfig_;
    shortTimeoutConfig.requestTimeout = std::chrono::milliseconds(1);

    DaemonClient client(shortTimeoutConfig);
    auto connectResult = client.connect();
    ASSERT_FALSE(connectResult);

    SearchRequest req{"complex query", 1000,  false, false, 0.7, {}, "keyword", false,
                      false,           false, false, false, 0,   0,  0,         ""};
    yams::cli::AsioClientPool pool{poolConfig_};
    auto result = pool.call<SearchRequest, SearchResponse>(req);
    EXPECT_FALSE(result);
    EXPECT_EQ(result.error().code, yams::ErrorCode::NetworkError);
}

// Model operations shouldn't succeed without a daemon
TEST_F(IpcClientTest, ModelOperations) {
    LoadModelRequest loadReq{"test-model"};
    yams::cli::AsioClientPool pool{poolConfig_};
    auto loadResult = pool.call<LoadModelRequest, SuccessResponse>(loadReq);
    EXPECT_FALSE(loadResult);

    ModelStatusRequest statusReq{"test-model"};
    auto statusResult = pool.call<ModelStatusRequest, ModelStatusResponse>(statusReq);
    EXPECT_FALSE(statusResult);
}

} // namespace yams::daemon::test