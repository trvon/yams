#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <yams/mcp/mcp_server.h>
#include <yams/api/content_store.h>
#include <yams/search/search_executor.h>
#include <memory>
#include <thread>
#include <chrono>

using namespace yams::mcp;
using namespace yams;
using json = nlohmann::json;

// Mock transport for testing
class MockTransport : public ITransport {
public:
    MOCK_METHOD(void, send, (const json& message), (override));
    MOCK_METHOD(json, receive, (), (override));
    MOCK_METHOD(bool, isConnected, (), (const, override));
    MOCK_METHOD(void, close, (), (override));
    
    // Helper to simulate receiving a message
    void simulateReceive(const json& message) {
        receivedMessages.push(message);
    }
    
    json getLastSentMessage() const {
        return lastSentMessage;
    }
    
private:
    std::queue<json> receivedMessages;
    json lastSentMessage;
};

// Mock content store for testing
class MockContentStore : public api::IContentStore {
public:
    MOCK_METHOD(api::StoreResult, store, (const std::filesystem::path& path, const api::ContentMetadata& metadata), (override));
    MOCK_METHOD(api::RetrieveResult, retrieve, (const std::string& hash, const std::filesystem::path& outputPath), (override));
    MOCK_METHOD(api::SearchResult, search, (const std::string& query, size_t limit), (override));
    MOCK_METHOD(api::StorageStats, getStats, (), (override));
    MOCK_METHOD(api::DeleteResult, deleteContent, (const std::string& hash), (override));
    MOCK_METHOD(api::Result<api::ContentInfo>, getContentInfo, (const std::string& hash), (override));
    MOCK_METHOD(api::Result<std::vector<api::ContentSummary>>, listContent, (size_t limit, size_t offset), (override));
    MOCK_METHOD(api::Result<void>, updateMetadata, (const std::string& hash, const api::ContentMetadata& metadata), (override));
    MOCK_METHOD(api::Result<bool>, exists, (const std::string& hash), (override));
};

// Mock search executor for testing
class MockSearchExecutor : public search::SearchExecutor {
public:
    MOCK_METHOD(search::Result<search::SearchResults>, search, (const search::SearchRequest& request), (override));
    MOCK_METHOD(search::Result<void>, indexDocument, (const search::Document& document), (override));
    MOCK_METHOD(search::Result<void>, removeDocument, (const std::string& documentId), (override));
    MOCK_METHOD(search::SearchStats, getStats, (), (const, override));
    MOCK_METHOD(search::Result<void>, optimizeIndex, (), (override));
};

class MCPServerTest : public ::testing::Test {
protected:
    void SetUp() override {
        mockTransport = std::make_unique<MockTransport>();
        mockTransportPtr = mockTransport.get();
        
        mockContentStore = std::make_shared<MockContentStore>();
        mockSearchExecutor = std::make_shared<MockSearchExecutor>();
        
        // Default expectations
        EXPECT_CALL(*mockTransportPtr, isConnected())
            .WillRepeatedly(::testing::Return(true));
            
        server = std::make_unique<MCPServer>(
            mockContentStore, 
            mockSearchExecutor, 
            std::move(mockTransport)
        );
    }
    
    void TearDown() override {
        if (server) {
            server->stop();
        }
    }
    
    std::unique_ptr<MCPServer> server;
    std::shared_ptr<MockContentStore> mockContentStore;
    std::shared_ptr<MockSearchExecutor> mockSearchExecutor;
    MockTransport* mockTransportPtr; // Raw pointer for expectations
    std::unique_ptr<MockTransport> mockTransport;
};

TEST_F(MCPServerTest, InitializeRequest) {
    json initRequest = {
        {"jsonrpc", "2.0"},
        {"id", 1},
        {"method", "initialize"},
        {"params", {
            {"clientInfo", {
                {"name", "test-client"},
                {"version", "1.0.0"}
            }}
        }}
    };
    
    json expectedResponse;
    
    // Set expectations
    EXPECT_CALL(*mockTransportPtr, receive())
        .WillOnce(::testing::Return(initRequest))
        .WillOnce(::testing::Return(json{})); // End loop
        
    EXPECT_CALL(*mockTransportPtr, send(::testing::_))
        .WillOnce(::testing::SaveArg<0>(&expectedResponse));
        
    EXPECT_CALL(*mockTransportPtr, isConnected())
        .WillOnce(::testing::Return(true))
        .WillOnce(::testing::Return(false)); // End loop
    
    // Run server briefly
    std::thread serverThread([this]() {
        server->start();
    });
    
    // Give server time to process
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    serverThread.join();
    
    // Verify response
    ASSERT_TRUE(expectedResponse.contains("jsonrpc"));
    ASSERT_TRUE(expectedResponse.contains("id"));
    ASSERT_TRUE(expectedResponse.contains("result"));
    EXPECT_EQ(expectedResponse["jsonrpc"], "2.0");
    EXPECT_EQ(expectedResponse["id"], 1);
    
    auto result = expectedResponse["result"];
    ASSERT_TRUE(result.contains("protocolVersion"));
    ASSERT_TRUE(result.contains("serverInfo"));
    ASSERT_TRUE(result.contains("capabilities"));
}

TEST_F(MCPServerTest, ListToolsRequest) {
    json toolsRequest = {
        {"jsonrpc", "2.0"},
        {"id", 2},
        {"method", "tools/list"}
    };
    
    json expectedResponse;
    
    EXPECT_CALL(*mockTransportPtr, receive())
        .WillOnce(::testing::Return(toolsRequest))
        .WillOnce(::testing::Return(json{}));
        
    EXPECT_CALL(*mockTransportPtr, send(::testing::_))
        .WillOnce(::testing::SaveArg<0>(&expectedResponse));
        
    EXPECT_CALL(*mockTransportPtr, isConnected())
        .WillOnce(::testing::Return(true))
        .WillOnce(::testing::Return(false));
    
    std::thread serverThread([this]() {
        server->start();
    });
    
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    serverThread.join();
    
    // Verify response contains tools
    ASSERT_TRUE(expectedResponse.contains("result"));
    auto result = expectedResponse["result"];
    ASSERT_TRUE(result.contains("tools"));
    
    auto tools = result["tools"];
    ASSERT_TRUE(tools.is_array());
    EXPECT_GT(tools.size(), 0);
    
    // Check for expected tools
    bool hasSearchTool = false;
    bool hasStoreTool = false;
    
    for (const auto& tool : tools) {
        ASSERT_TRUE(tool.contains("name"));
        auto name = tool["name"];
        
        if (name == "search_documents") {
            hasSearchTool = true;
            ASSERT_TRUE(tool.contains("description"));
            ASSERT_TRUE(tool.contains("inputSchema"));
        } else if (name == "store_document") {
            hasStoreTool = true;
            ASSERT_TRUE(tool.contains("description"));
            ASSERT_TRUE(tool.contains("inputSchema"));
        }
    }
    
    EXPECT_TRUE(hasSearchTool);
    EXPECT_TRUE(hasStoreTool);
}

TEST_F(MCPServerTest, SearchDocumentsTool) {
    json searchRequest = {
        {"jsonrpc", "2.0"},
        {"id", 3},
        {"method", "tools/call"},
        {"params", {
            {"name", "search_documents"},
            {"arguments", {
                {"query", "test search"},
                {"limit", 5}
            }}
        }}
    };
    
    // Mock search executor response
    search::SearchResults mockResults;
    mockResults.totalResults = 2;
    mockResults.results = search::ResultList(); // You'd populate this with actual results
    
    EXPECT_CALL(*mockSearchExecutor, search(::testing::_))
        .WillOnce(::testing::Return(search::Result<search::SearchResults>(mockResults)));
    
    json expectedResponse;
    
    EXPECT_CALL(*mockTransportPtr, receive())
        .WillOnce(::testing::Return(searchRequest))
        .WillOnce(::testing::Return(json{}));
        
    EXPECT_CALL(*mockTransportPtr, send(::testing::_))
        .WillOnce(::testing::SaveArg<0>(&expectedResponse));
        
    EXPECT_CALL(*mockTransportPtr, isConnected())
        .WillOnce(::testing::Return(true))
        .WillOnce(::testing::Return(false));
    
    std::thread serverThread([this]() {
        server->start();
    });
    
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    serverThread.join();
    
    // Verify search response
    ASSERT_TRUE(expectedResponse.contains("result"));
    auto result = expectedResponse["result"];
    ASSERT_TRUE(result.contains("total"));
    EXPECT_EQ(result["total"], 2);
}

TEST_F(MCPServerTest, InvalidJsonRpcRequest) {
    json invalidRequest = {
        {"method", "test"}  // Missing jsonrpc and id
    };
    
    json expectedResponse;
    
    EXPECT_CALL(*mockTransportPtr, receive())
        .WillOnce(::testing::Return(invalidRequest))
        .WillOnce(::testing::Return(json{}));
        
    EXPECT_CALL(*mockTransportPtr, send(::testing::_))
        .WillOnce(::testing::SaveArg<0>(&expectedResponse));
        
    EXPECT_CALL(*mockTransportPtr, isConnected())
        .WillOnce(::testing::Return(true))
        .WillOnce(::testing::Return(false));
    
    std::thread serverThread([this]() {
        server->start();
    });
    
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    serverThread.join();
    
    // Should return error response
    ASSERT_TRUE(expectedResponse.contains("jsonrpc"));
    ASSERT_TRUE(expectedResponse.contains("error"));
    EXPECT_EQ(expectedResponse["jsonrpc"], "2.0");
    
    auto error = expectedResponse["error"];
    EXPECT_EQ(error["code"], -32600);
}

TEST_F(MCPServerTest, UnknownMethod) {
    json unknownRequest = {
        {"jsonrpc", "2.0"},
        {"id", 4},
        {"method", "unknown/method"}
    };
    
    json expectedResponse;
    
    EXPECT_CALL(*mockTransportPtr, receive())
        .WillOnce(::testing::Return(unknownRequest))
        .WillOnce(::testing::Return(json{}));
        
    EXPECT_CALL(*mockTransportPtr, send(::testing::_))
        .WillOnce(::testing::SaveArg<0>(&expectedResponse));
        
    EXPECT_CALL(*mockTransportPtr, isConnected())
        .WillOnce(::testing::Return(true))
        .WillOnce(::testing::Return(false));
    
    std::thread serverThread([this]() {
        server->start();
    });
    
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    serverThread.join();
    
    // Should return method not found error
    ASSERT_TRUE(expectedResponse.contains("error"));
    auto error = expectedResponse["error"];
    EXPECT_EQ(error["code"], -32601);
    EXPECT_EQ(error["message"], "Method not found");
}

TEST_F(MCPServerTest, ServerLifecycle) {
    EXPECT_FALSE(server->isRunning());
    
    // Test that server can be started and stopped
    EXPECT_CALL(*mockTransportPtr, isConnected())
        .WillOnce(::testing::Return(false)); // Immediately exit
    
    std::thread serverThread([this]() {
        server->start();
    });
    
    // Brief wait for server to start
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    
    server->stop();
    serverThread.join();
    
    EXPECT_FALSE(server->isRunning());
}