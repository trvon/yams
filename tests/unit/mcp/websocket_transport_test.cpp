#include <gtest/gtest.h>
#include <yams/mcp/mcp_server.h>
#include <nlohmann/json.hpp>
#include <thread>
#include <chrono>
#include <atomic>

using namespace yams::mcp;
using json = nlohmann::json;

class WebSocketTransportTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Default configuration for testing
        config.host = "localhost";
        config.port = 8080;
        config.path = "/mcp";
        config.useSSL = false;
        config.connectTimeout = std::chrono::milliseconds{1000};
        config.receiveTimeout = std::chrono::milliseconds{5000};
        config.maxMessageSize = 1024 * 1024;
        config.enablePingPong = true;
        config.pingInterval = std::chrono::seconds{30};
    }
    
    WebSocketTransport::Config config;
};

TEST_F(WebSocketTransportTest, Configuration) {
    WebSocketTransport transport(config);
    
    // Test that transport can be created with valid configuration
    EXPECT_FALSE(transport.isConnected());
    
    // Configuration should be preserved
    EXPECT_NO_THROW(transport.close());
}

TEST_F(WebSocketTransportTest, ConfigurationValidation) {
    // Test various configuration scenarios
    
    // Valid configuration
    WebSocketTransport::Config validConfig = config;
    EXPECT_NO_THROW(WebSocketTransport transport(validConfig));
    
    // Test with SSL enabled
    WebSocketTransport::Config sslConfig = config;
    sslConfig.useSSL = true;
    sslConfig.port = 443;
    EXPECT_NO_THROW(WebSocketTransport transport(sslConfig));
    
    // Test with custom timeouts
    WebSocketTransport::Config timeoutConfig = config;
    timeoutConfig.connectTimeout = std::chrono::milliseconds{10000};
    timeoutConfig.receiveTimeout = std::chrono::milliseconds{30000};
    EXPECT_NO_THROW(WebSocketTransport transport(timeoutConfig));
}

TEST_F(WebSocketTransportTest, ConnectionFailure) {
    // Test connection to non-existent server
    WebSocketTransport transport(config);
    
    // Should not be connected initially
    EXPECT_FALSE(transport.isConnected());
    
    // Connection should fail (no server running on localhost:8080)
    bool connected = transport.connect();
    EXPECT_FALSE(connected);
    EXPECT_FALSE(transport.isConnected());
}

TEST_F(WebSocketTransportTest, ConnectionTimeout) {
    // Test connection timeout with very short timeout
    WebSocketTransport::Config timeoutConfig = config;
    timeoutConfig.connectTimeout = std::chrono::milliseconds{100}; // Very short timeout
    
    WebSocketTransport transport(timeoutConfig);
    
    auto start = std::chrono::steady_clock::now();
    bool connected = transport.connect();
    auto end = std::chrono::steady_clock::now();
    
    EXPECT_FALSE(connected);
    
    // Should timeout within reasonable time (allowing some margin)
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    EXPECT_LT(duration.count(), 1000); // Should be much less than 1 second
}

TEST_F(WebSocketTransportTest, SendWithoutConnection) {
    WebSocketTransport transport(config);
    
    json testMessage = {
        {"jsonrpc", "2.0"},
        {"method", "test"},
        {"id", 1}
    };
    
    // Should throw exception when trying to send without connection
    EXPECT_THROW(transport.send(testMessage), std::runtime_error);
}

TEST_F(WebSocketTransportTest, ReceiveTimeout) {
    WebSocketTransport transport(config);
    
    // Receiving without connection should return empty JSON
    json result = transport.receive();
    EXPECT_TRUE(result.is_null() || result.empty());
}

TEST_F(WebSocketTransportTest, MultipleCloseOperations) {
    WebSocketTransport transport(config);
    
    // Multiple close operations should be safe
    transport.close();
    transport.close();
    transport.close();
    
    EXPECT_FALSE(transport.isConnected());
}

TEST_F(WebSocketTransportTest, ReconnectionScenario) {
    WebSocketTransport transport(config);
    
    // Initial state
    EXPECT_FALSE(transport.isConnected());
    
    // First connection attempt (will fail)
    bool connected1 = transport.connect();
    EXPECT_FALSE(connected1);
    
    // Reconnection attempt
    transport.reconnect();
    EXPECT_FALSE(transport.isConnected()); // Still no server
    
    // Multiple reconnection attempts should be safe
    transport.reconnect();
    transport.reconnect();
    
    EXPECT_FALSE(transport.isConnected());
}

TEST_F(WebSocketTransportTest, WaitForConnection) {
    WebSocketTransport transport(config);
    
    // Should return false quickly since no connection is possible
    auto start = std::chrono::steady_clock::now();
    bool connected = transport.waitForConnection(std::chrono::milliseconds{500});
    auto end = std::chrono::steady_clock::now();
    
    EXPECT_FALSE(connected);
    
    // Should have waited approximately the timeout duration
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    EXPECT_GE(duration.count(), 400); // Allow some margin
    EXPECT_LE(duration.count(), 600);
}

TEST_F(WebSocketTransportTest, SSLConfiguration) {
    WebSocketTransport::Config sslConfig = config;
    sslConfig.useSSL = true;
    sslConfig.port = 443;
    
    // Should be able to create SSL transport
    EXPECT_NO_THROW(WebSocketTransport transport(sslConfig));
    
    WebSocketTransport transport(sslConfig);
    EXPECT_FALSE(transport.isConnected());
    
    // Connection will fail (no server), but should not crash
    bool connected = transport.connect();
    EXPECT_FALSE(connected);
}

TEST_F(WebSocketTransportTest, ConfigurationCopyAndMove) {
    // Test that configuration is properly copied
    WebSocketTransport::Config config1 = config;
    config1.host = "example.com";
    config1.port = 9090;
    
    WebSocketTransport transport1(config1);
    WebSocketTransport transport2(config); // Original config
    
    // Both transports should be created successfully
    EXPECT_FALSE(transport1.isConnected());
    EXPECT_FALSE(transport2.isConnected());
}

TEST_F(WebSocketTransportTest, ThreadSafety) {
    WebSocketTransport transport(config);
    std::atomic<int> operations{0};
    std::atomic<bool> stopFlag{false};
    
    // Launch multiple threads that try to connect/disconnect
    std::vector<std::thread> threads;
    
    for (int i = 0; i < 3; ++i) {
        threads.emplace_back([&transport, &operations, &stopFlag]() {
            while (!stopFlag.load()) {
                try {
                    transport.connect();
                    operations++;
                    std::this_thread::sleep_for(std::chrono::milliseconds{10});
                    transport.close();
                    operations++;
                    std::this_thread::sleep_for(std::chrono::milliseconds{10});
                } catch (const std::exception&) {
                    // Expected - no server running
                    operations++;
                }
            }
        });
    }
    
    // Let threads run for a short time
    std::this_thread::sleep_for(std::chrono::milliseconds{200});
    stopFlag = true;
    
    // Wait for all threads to complete
    for (auto& thread : threads) {
        thread.join();
    }
    
    // Should have performed multiple operations without crashing
    EXPECT_GT(operations.load(), 0);
    EXPECT_FALSE(transport.isConnected());
}

// Note: Full integration tests with actual WebSocket server would require
// setting up a test WebSocket server, which is complex for unit tests.
// Integration tests should cover the complete WebSocket protocol interaction.