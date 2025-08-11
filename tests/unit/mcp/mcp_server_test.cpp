#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <yams/mcp/mcp_server.h>
#include <yams/api/content_store.h>
#include <yams/search/search_executor.h>
#include <yams/metadata/metadata_repository.h>
#include <memory>
#include <thread>
#include <chrono>
#include <queue>

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
    MOCK_METHOD(Result<api::StoreResult>, store, (const std::filesystem::path& path, const api::ContentMetadata& metadata, api::ProgressCallback progress), (override));
    MOCK_METHOD(Result<api::RetrieveResult>, retrieve, (const std::string& hash, const std::filesystem::path& outputPath, api::ProgressCallback progress), (override));
    MOCK_METHOD(Result<api::StoreResult>, storeStream, (std::istream& stream, const api::ContentMetadata& metadata, api::ProgressCallback progress), (override));
    MOCK_METHOD(Result<api::RetrieveResult>, retrieveStream, (const std::string& hash, std::ostream& output, api::ProgressCallback progress), (override));
    MOCK_METHOD(Result<api::StoreResult>, storeBytes, (std::span<const std::byte> data, const api::ContentMetadata& metadata), (override));
    MOCK_METHOD(Result<std::vector<std::byte>>, retrieveBytes, (const std::string& hash), (override));
    MOCK_METHOD(Result<bool>, exists, (const std::string& hash), (const, override));
    MOCK_METHOD(Result<bool>, remove, (const std::string& hash), (override));
    MOCK_METHOD(Result<api::ContentMetadata>, getMetadata, (const std::string& hash), (const, override));
    MOCK_METHOD(Result<void>, updateMetadata, (const std::string& hash, const api::ContentMetadata& metadata), (override));
    MOCK_METHOD(std::vector<Result<api::StoreResult>>, storeBatch, (const std::vector<std::filesystem::path>& paths, const std::vector<api::ContentMetadata>& metadata), (override));
    MOCK_METHOD(std::vector<Result<bool>>, removeBatch, (const std::vector<std::string>& hashes), (override));
    MOCK_METHOD(api::ContentStoreStats, getStats, (), (const, override));
    MOCK_METHOD(api::HealthStatus, checkHealth, (), (const, override));
    MOCK_METHOD(Result<void>, verify, (api::ProgressCallback progress), (override));
    MOCK_METHOD(Result<void>, compact, (api::ProgressCallback progress), (override));
    MOCK_METHOD(Result<void>, garbageCollect, (api::ProgressCallback progress), (override));
};

// Mock search executor for testing
class MockSearchExecutor : public search::SearchExecutor {
public:
    MOCK_METHOD(Result<search::SearchResults>, search, (const search::SearchRequest& request), (override));
    MOCK_METHOD(Result<void>, indexDocument, (const search::Document& document), (override));
    MOCK_METHOD(Result<void>, removeDocument, (const std::string& documentId), (override));
    MOCK_METHOD(search::SearchStats, getStats, (), (const, override));
    MOCK_METHOD(Result<void>, optimizeIndex, (), (override));
};

// Mock metadata repository for testing
class MockMetadataRepository : public metadata::MetadataRepository {
public:
    MOCK_METHOD(Result<std::vector<metadata::DocumentInfo>>, findDocumentsByPath, (const std::string& pattern), (override));
    MOCK_METHOD(Result<std::unordered_map<std::string, metadata::MetadataValue>>, getAllMetadata, (int64_t documentId), (override));
    MOCK_METHOD(Result<std::optional<metadata::ContentInfo>>, getContent, (int64_t documentId), (override));
};

class MCPServerTest : public ::testing::Test {
protected:
    void SetUp() override {
        mockTransport = std::make_unique<MockTransport>();
        mockTransportPtr = mockTransport.get();
        
        mockContentStore = std::make_shared<MockContentStore>();
        mockSearchExecutor = std::make_shared<MockSearchExecutor>();
        mockMetadataRepo = std::make_shared<MockMetadataRepository>();
        
        // Default expectations
        EXPECT_CALL(*mockTransportPtr, isConnected())
            .WillRepeatedly(::testing::Return(true));
            
        server = std::make_unique<MCPServer>(
            mockContentStore, 
            mockSearchExecutor, 
            mockMetadataRepo,
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
    std::shared_ptr<MockMetadataRepository> mockMetadataRepo;
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

TEST_F(MCPServerTest, DeleteByNameTool) {
    json deleteRequest = {
        {"jsonrpc", "2.0"},
        {"id", 4},
        {"method", "tools/call"},
        {"params", {
            {"name", "delete_by_name"},
            {"arguments", {
                {"name", "test.txt"},
                {"dry_run", true}
            }}
        }}
    };
    
    // Mock metadata repository response for name resolution
    std::vector<metadata::DocumentInfo> mockDocs;
    metadata::DocumentInfo doc;
    doc.fileName = "test.txt";
    doc.sha256Hash = "abc123def456";
    mockDocs.push_back(doc);
    
    EXPECT_CALL(*mockMetadataRepo, findDocumentsByPath("%/test.txt"))
        .WillOnce(::testing::Return(Result<std::vector<metadata::DocumentInfo>>(mockDocs)));
    
    json expectedResponse;
    
    EXPECT_CALL(*mockTransportPtr, receive())
        .WillOnce(::testing::Return(deleteRequest))
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
    
    // Verify delete response
    ASSERT_TRUE(expectedResponse.contains("result"));
    auto result = expectedResponse["result"];
    ASSERT_TRUE(result.contains("dry_run"));
    EXPECT_EQ(result["dry_run"], true);
    ASSERT_TRUE(result.contains("deleted"));
    EXPECT_GT(result["deleted"].size(), 0);
}

TEST_F(MCPServerTest, GetByNameTool) {
    json getRequest = {
        {"jsonrpc", "2.0"},
        {"id", 5},
        {"method", "tools/call"},
        {"params", {
            {"name", "get_by_name"},
            {"arguments", {
                {"name", "document.txt"}
            }}
        }}
    };
    
    // Mock metadata repository and store responses
    std::vector<metadata::DocumentInfo> mockDocs;
    metadata::DocumentInfo doc;
    doc.fileName = "document.txt";
    doc.sha256Hash = "def789abc123";
    mockDocs.push_back(doc);
    
    EXPECT_CALL(*mockMetadataRepo, findDocumentsByPath("%/document.txt"))
        .WillOnce(::testing::Return(Result<std::vector<metadata::DocumentInfo>>(mockDocs)));
        
    EXPECT_CALL(*mockContentStore, exists(doc.sha256Hash))
        .WillOnce(::testing::Return(Result<bool>(true)));
        
    EXPECT_CALL(*mockContentStore, retrieveStream(doc.sha256Hash, ::testing::_, nullptr))
        .WillOnce(::testing::Return(Result<api::RetrieveResult>(api::RetrieveResult{true, 100})));
    
    json expectedResponse;
    
    EXPECT_CALL(*mockTransportPtr, receive())
        .WillOnce(::testing::Return(getRequest))
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
    
    // Verify get response
    ASSERT_TRUE(expectedResponse.contains("result"));
    auto result = expectedResponse["result"];
    ASSERT_TRUE(result.contains("name"));
    EXPECT_EQ(result["name"], "document.txt");
    ASSERT_TRUE(result.contains("hash"));
    EXPECT_EQ(result["hash"], "def789abc123");
}

TEST_F(MCPServerTest, CatDocumentTool) {
    json catRequest = {
        {"jsonrpc", "2.0"},
        {"id", 6},
        {"method", "tools/call"},
        {"params", {
            {"name", "cat_document"},
            {"arguments", {
                {"hash", "xyz987fedcba"}
            }}
        }}
    };
    
    // Mock content store responses
    EXPECT_CALL(*mockContentStore, exists("xyz987fedcba"))
        .WillOnce(::testing::Return(Result<bool>(true)));
        
    EXPECT_CALL(*mockContentStore, retrieveStream("xyz987fedcba", ::testing::_, nullptr))
        .WillOnce(::testing::Return(Result<api::RetrieveResult>(api::RetrieveResult{true, 50})));
    
    json expectedResponse;
    
    EXPECT_CALL(*mockTransportPtr, receive())
        .WillOnce(::testing::Return(catRequest))
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
    
    // Verify cat response
    ASSERT_TRUE(expectedResponse.contains("result"));
    auto result = expectedResponse["result"];
    ASSERT_TRUE(result.contains("hash"));
    EXPECT_EQ(result["hash"], "xyz987fedcba");
    ASSERT_TRUE(result.contains("content"));
}

TEST_F(MCPServerTest, ListDocumentsTool) {
    json listRequest = {
        {"jsonrpc", "2.0"},
        {"id", 7},
        {"method", "tools/call"},
        {"params", {
            {"name", "list_documents"},
            {"arguments", {
                {"limit", 10}
            }}
        }}
    };
    
    // Mock metadata repository response
    std::vector<metadata::DocumentInfo> mockDocs;
    metadata::DocumentInfo doc1, doc2;
    doc1.fileName = "file1.txt";
    doc1.sha256Hash = "hash1";
    doc1.fileSize = 100;
    doc2.fileName = "file2.txt"; 
    doc2.sha256Hash = "hash2";
    doc2.fileSize = 200;
    mockDocs.push_back(doc1);
    mockDocs.push_back(doc2);
    
    EXPECT_CALL(*mockMetadataRepo, findDocumentsByPath("%"))
        .WillOnce(::testing::Return(Result<std::vector<metadata::DocumentInfo>>(mockDocs)));
    
    json expectedResponse;
    
    EXPECT_CALL(*mockTransportPtr, receive())
        .WillOnce(::testing::Return(listRequest))
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
    
    // Verify list response
    ASSERT_TRUE(expectedResponse.contains("result"));
    auto result = expectedResponse["result"];
    ASSERT_TRUE(result.contains("documents"));
    ASSERT_TRUE(result.contains("count"));
    EXPECT_EQ(result["count"], 2);
    
    auto docs = result["documents"];
    EXPECT_EQ(docs.size(), 2);
    EXPECT_EQ(docs[0]["name"], "file1.txt");
    EXPECT_EQ(docs[1]["name"], "file2.txt");
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