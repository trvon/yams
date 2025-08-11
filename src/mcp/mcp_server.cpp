#include <yams/mcp/mcp_server.h>
#include <spdlog/spdlog.h>
#include <iostream>
#include <fstream>
#include <sstream>
#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl/stream.hpp>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <atomic>
#include <memory>

namespace beast = boost::beast;
namespace http = beast::http;
namespace websocket = beast::websocket;
namespace net = boost::asio;
namespace ssl = net::ssl;
using tcp = net::ip::tcp;

namespace yams::mcp {

// StdioTransport implementation
StdioTransport::StdioTransport() {
    // Ensure unbuffered I/O for stdio communication
    std::ios::sync_with_stdio(false);
    std::cin.tie(nullptr);
}

void StdioTransport::send(const json& message) {
    if (!closed_) {
        std::cout << message.dump() << std::endl;
        std::cout.flush();
    }
}

json StdioTransport::receive() {
    if (closed_) {
        return json{};
    }
    
    std::string line;
    if (std::getline(std::cin, line)) {
        try {
            return json::parse(line);
        } catch (const json::parse_error& e) {
            spdlog::error("Failed to parse JSON: {}", e.what());
            return json{};
        }
    }
    
    closed_ = true;
    return json{};
}

// WebSocketTransport implementation using Boost.Beast
class WebSocketTransport::Impl {
public:
    explicit Impl(const Config& config) 
        : config_(config), ioc_(), resolver_(ioc_), connected_(false) {
        // Initialize SSL context if needed
        if (config_.useSSL) {
            sslContext_ = std::make_unique<ssl::context>(ssl::context::tlsv12_client);
            sslContext_->set_default_verify_paths();
            sslContext_->set_verify_mode(ssl::verify_peer);
        }
    }
    
    ~Impl() {
        close();
    }
    
    bool connect() {
        std::lock_guard<std::mutex> lock(connectionMutex_);
        
        if (connected_) {
            return true;
        }
        
        try {
            spdlog::info("Connecting to WebSocket: {}://{}:{}{}", 
                        config_.useSSL ? "wss" : "ws", config_.host, config_.port, config_.path);
            
            // Resolve the host
            auto const results = resolver_.resolve(config_.host, std::to_string(config_.port));
            
            beast::error_code ec;
            
            if (config_.useSSL) {
                // SSL WebSocket connection
                sslStream_ = std::make_unique<websocket::stream<beast::ssl_stream<tcp::socket>>>(ioc_, *sslContext_);
                
                // Connect the underlying socket
                auto ep = net::connect(beast::get_lowest_layer(*sslStream_), results.begin(), results.end());
                
                // Set SNI hostname
                if (!SSL_set_tlsext_host_name(sslStream_->next_layer().native_handle(), config_.host.c_str())) {
                    throw std::runtime_error("Failed to set SNI hostname");
                }
                
                // Perform SSL handshake
                sslStream_->next_layer().handshake(ssl::stream_base::client, ec);
                if (ec) {
                    spdlog::error("SSL handshake error: {}", ec.message());
                    return false;
                }
                
                // Set WebSocket options
                sslStream_->set_option(websocket::stream_base::timeout::suggested(
                    beast::role_type::client));
                sslStream_->set_option(websocket::stream_base::decorator(
                    [](websocket::request_type& req) {
                        req.set(http::field::user_agent, "YAMS-MCP-Client/1.0");
                    }));
                
                // Perform WebSocket handshake
                std::string host_port = config_.host + ':' + std::to_string(config_.port);
                sslStream_->handshake(host_port, config_.path, ec);
                
            } else {
                // Plain WebSocket connection
                plainStream_ = std::make_unique<websocket::stream<tcp::socket>>(ioc_);
                
                // Connect the underlying socket
                auto ep = net::connect(plainStream_->next_layer(), results.begin(), results.end());
                
                // Set WebSocket options
                plainStream_->set_option(websocket::stream_base::timeout::suggested(
                    beast::role_type::client));
                plainStream_->set_option(websocket::stream_base::decorator(
                    [](websocket::request_type& req) {
                        req.set(http::field::user_agent, "YAMS-MCP-Client/1.0");
                    }));
                
                // Perform WebSocket handshake
                std::string host_port = config_.host + ':' + std::to_string(config_.port);
                plainStream_->handshake(host_port, config_.path, ec);
            }
            
            if (ec) {
                spdlog::error("WebSocket handshake error: {}", ec.message());
                return false;
            }
            
            connected_ = true;
            spdlog::info("WebSocket connected successfully");
            
            // Start the read thread
            readThread_ = std::thread([this] {
                this->readLoop();
            });
            
            return true;
            
        } catch (const std::exception& e) {
            spdlog::error("WebSocket connection exception: {}", e.what());
            connected_ = false;
            return false;
        }
    }
    
    void send(const json& message) {
        if (!connected_) {
            throw std::runtime_error("WebSocket not connected");
        }
        
        std::string messageStr = message.dump();
        beast::error_code ec;
        
        try {
            std::lock_guard<std::mutex> lock(connectionMutex_);
            
            if (config_.useSSL && sslStream_) {
                sslStream_->write(net::buffer(messageStr), ec);
            } else if (plainStream_) {
                plainStream_->write(net::buffer(messageStr), ec);
            } else {
                throw std::runtime_error("WebSocket stream not initialized");
            }
            
            if (ec) {
                spdlog::error("WebSocket send error: {}", ec.message());
                throw std::runtime_error("WebSocket send failed: " + ec.message());
            }
        } catch (const std::exception& e) {
            spdlog::error("WebSocket send exception: {}", e.what());
            throw;
        }
    }
    
    json receive() {
        std::unique_lock<std::mutex> lock(messageMutex_);
        
        // Wait for message with timeout
        bool hasMessage = messageCondition_.wait_for(lock, config_.receiveTimeout,
            [this] { return !messageQueue_.empty() || !connected_; });
        
        if (!hasMessage || messageQueue_.empty()) {
            if (!connected_) {
                spdlog::debug("WebSocket disconnected during receive");
                return json{};
            }
            spdlog::debug("WebSocket receive timeout");
            return json{};
        }
        
        json message = messageQueue_.front();
        messageQueue_.pop();
        return message;
    }
    
    bool isConnected() const {
        return connected_;
    }
    
    void close() {
        std::lock_guard<std::mutex> lock(connectionMutex_);
        
        if (!connected_) {
            return;
        }
        
        connected_ = false;
        
        try {
            beast::error_code ec;
            
            if (config_.useSSL && sslStream_) {
                sslStream_->close(websocket::close_code::normal, ec);
                if (ec) {
                    spdlog::warn("WebSocket SSL close error: {}", ec.message());
                }
            } else if (plainStream_) {
                plainStream_->close(websocket::close_code::normal, ec);
                if (ec) {
                    spdlog::warn("WebSocket close error: {}", ec.message());
                }
            }
            
            if (readThread_.joinable()) {
                readThread_.join();
            }
            
        } catch (const std::exception& e) {
            spdlog::error("WebSocket close exception: {}", e.what());
        }
        
        // Clear message queue
        {
            std::lock_guard<std::mutex> msgLock(messageMutex_);
            std::queue<json> empty;
            messageQueue_.swap(empty);
        }
        
        connectionCondition_.notify_all();
        messageCondition_.notify_all();
    }
    
private:
    void readLoop() {
        beast::flat_buffer buffer;
        
        while (connected_) {
            try {
                beast::error_code ec;
                
                if (config_.useSSL && sslStream_) {
                    sslStream_->read(buffer, ec);
                } else if (plainStream_) {
                    plainStream_->read(buffer, ec);
                } else {
                    break;
                }
                
                if (ec == websocket::error::closed) {
                    spdlog::debug("WebSocket connection closed by peer");
                    break;
                }
                
                if (ec) {
                    spdlog::error("WebSocket read error: {}", ec.message());
                    break;
                }
                
                // Convert buffer to string and parse JSON
                std::string message = beast::buffers_to_string(buffer.data());
                buffer.consume(buffer.size());
                
                if (!message.empty()) {
                    onMessage(message);
                }
                
            } catch (const std::exception& e) {
                spdlog::error("WebSocket read loop exception: {}", e.what());
                break;
            }
        }
        
        // Mark as disconnected when read loop exits
        connected_ = false;
        messageCondition_.notify_all();
    }
    
    void onMessage(const std::string& payload) {
        try {
            json message = json::parse(payload);
            
            std::lock_guard<std::mutex> lock(messageMutex_);
            messageQueue_.push(std::move(message));
            messageCondition_.notify_one();
            
        } catch (const json::parse_error& e) {
            spdlog::error("WebSocket message parse error: {}", e.what());
        }
    }
    
private:
    Config config_;
    net::io_context ioc_;
    tcp::resolver resolver_;
    std::unique_ptr<ssl::context> sslContext_;
    std::unique_ptr<websocket::stream<tcp::socket>> plainStream_;
    std::unique_ptr<websocket::stream<beast::ssl_stream<tcp::socket>>> sslStream_;
    
    std::atomic<bool> connected_{false};
    
    mutable std::mutex connectionMutex_;
    std::condition_variable connectionCondition_;
    
    mutable std::mutex messageMutex_;
    std::condition_variable messageCondition_;
    std::queue<json> messageQueue_;
    
    std::thread readThread_;
};

// WebSocketTransport public interface
WebSocketTransport::WebSocketTransport(const Config& config)
    : pImpl(std::make_unique<Impl>(config)) {
}

WebSocketTransport::~WebSocketTransport() = default;

void WebSocketTransport::send(const json& message) {
    pImpl->send(message);
}

json WebSocketTransport::receive() {
    return pImpl->receive();
}

bool WebSocketTransport::isConnected() const {
    return pImpl->isConnected();
}

void WebSocketTransport::close() {
    pImpl->close();
}

bool WebSocketTransport::connect() {
    return pImpl->connect();
}

void WebSocketTransport::reconnect() {
    close();
    connect();
}

bool WebSocketTransport::waitForConnection(std::chrono::milliseconds timeout) {
    auto start = std::chrono::steady_clock::now();
    while (!isConnected() && 
           std::chrono::steady_clock::now() - start < timeout) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    return isConnected();
}

// MCPServer implementation
MCPServer::MCPServer(std::shared_ptr<api::IContentStore> store,
                     std::shared_ptr<search::SearchExecutor> searchExecutor,
                     std::shared_ptr<metadata::MetadataRepository> metadataRepo,
                     std::unique_ptr<ITransport> transport)
    : store_(std::move(store))
    , searchExecutor_(std::move(searchExecutor))
    , metadataRepo_(std::move(metadataRepo))
    , transport_(std::move(transport)) {
}

MCPServer::~MCPServer() {
    stop();
}

void MCPServer::start() {
    running_ = true;
    spdlog::info("MCP server started");
    
    // Main message loop
    while (running_ && transport_->isConnected()) {
        auto message = transport_->receive();
        
        if (message.is_null() || message.empty()) {
            continue;
        }
        
        auto response = handleRequest(message);
        if (!response.is_null()) {
            transport_->send(response);
        }
    }
}

void MCPServer::stop() {
    running_ = false;
    if (transport_) {
        transport_->close();
    }
}

json MCPServer::handleRequest(const json& request) {
    try {
        // Validate JSON-RPC request
        if (!request.contains("jsonrpc") || request["jsonrpc"] != "2.0") {
            return createError(nullptr, -32600, "Invalid Request");
        }
        
        if (!request.contains("method")) {
            return createError(request.value("id", nullptr), -32600, "Invalid Request");
        }
        
        std::string method = request["method"];
        json params = request.value("params", json::object());
        json id = request.value("id", nullptr);
        
        // Route to appropriate handler
        if (method == "initialize") {
            return createResponse(id, initialize(params));
        } else if (method == "tools/list") {
            return createResponse(id, listTools());
        } else if (method == "resources/list") {
            return createResponse(id, listResources());
        } else if (method == "prompts/list") {
            return createResponse(id, listPrompts());
        } else if (method == "tools/call") {
            std::string name = params["name"];
            json arguments = params.value("arguments", json::object());
            return createResponse(id, callTool(name, arguments));
        } else if (method == "resources/read") {
            std::string uri = params["uri"];
            return createResponse(id, readResource(uri));
        } else {
            return createError(id, -32601, "Method not found");
        }
        
    } catch (const std::exception& e) {
        spdlog::error("Error handling request: {}", e.what());
        return createError(nullptr, -32603, "Internal error");
    }
}

json MCPServer::initialize(const json& params) {
    initialized_ = true;
    
    if (params.contains("clientInfo")) {
        clientInfo_.name = params["clientInfo"].value("name", "unknown");
        clientInfo_.version = params["clientInfo"].value("version", "unknown");
    }
    
    json response;
    response["protocolVersion"] = "2024-11-05";
    response["serverInfo"] = {
        {"name", serverInfo_.name},
        {"version", serverInfo_.version}
    };
    response["capabilities"] = {
        {"tools", json::object()},
        {"resources", json::object()},
        {"prompts", json::object()}
    };
    
    return response;
}

json MCPServer::listTools() {
    json tools = json::array();
    
    // Search documents tool
    tools.push_back({
        {"name", "search_documents"},
        {"description", "Search documents by query"},
        {"inputSchema", {
            {"type", "object"},
            {"properties", {
                {"query", {{"type", "string"}, {"description", "Search query"}}},
                {"limit", {{"type", "integer"}, {"description", "Maximum results"}, {"default", 10}}}
            }},
            {"required", json::array({"query"})}
        }}
    });
    
    // Store document tool
    tools.push_back({
        {"name", "store_document"},
        {"description", "Store a document with metadata"},
        {"inputSchema", {
            {"type", "object"},
            {"properties", {
                {"path", {{"type", "string"}, {"description", "File path"}}},
                {"tags", {{"type", "array"}, {"items", {{"type", "string"}}}}},
                {"metadata", {{"type", "object"}}}
            }},
            {"required", json::array({"path"})}
        }}
    });
    
    // Retrieve document tool
    tools.push_back({
        {"name", "retrieve_document"},
        {"description", "Retrieve a document by hash"},
        {"inputSchema", {
            {"type", "object"},
            {"properties", {
                {"hash", {{"type", "string"}, {"description", "Document hash"}}},
                {"outputPath", {{"type", "string"}, {"description", "Output file path"}}}
            }},
            {"required", json::array({"hash"})}
        }}
    });
    
    // Get stats tool
    tools.push_back({
        {"name", "get_stats"},
        {"description", "Get storage statistics"},
        {"inputSchema", {
            {"type", "object"},
            {"properties", {
                {"detailed", {{"type", "boolean"}, {"default", false}}}
            }}
        }}
    });
    
    // Delete by name tool
    tools.push_back({
        {"name", "delete_by_name"},
        {"description", "Delete documents by name or pattern"},
        {"inputSchema", {
            {"type", "object"},
            {"properties", {
                {"name", {{"type", "string"}, {"description", "Document name to delete"}}},
                {"names", {{"type", "array"}, {"items", {{"type", "string"}}}, {"description", "Multiple document names"}}},
                {"pattern", {{"type", "string"}, {"description", "Glob pattern (e.g., '*.log')"}}},
                {"dry_run", {{"type", "boolean"}, {"default", false}, {"description", "Preview without deleting"}}}
            }},
            {"oneOf", json::array({
                {{"required", json::array({"name"})}},
                {{"required", json::array({"names"})}},
                {{"required", json::array({"pattern"})}}
            })}
        }}
    });
    
    // Get by name tool
    tools.push_back({
        {"name", "get_by_name"},
        {"description", "Retrieve document content by name"},
        {"inputSchema", {
            {"type", "object"},
            {"properties", {
                {"name", {{"type", "string"}, {"description", "Document name"}}},
                {"output_path", {{"type", "string"}, {"description", "Optional output file path"}}}
            }},
            {"required", json::array({"name"})}
        }}
    });
    
    // Cat document tool
    tools.push_back({
        {"name", "cat_document"},
        {"description", "Display document content directly (like cat command)"},
        {"inputSchema", {
            {"type", "object"},
            {"properties", {
                {"hash", {{"type", "string"}, {"description", "Document hash"}}},
                {"name", {{"type", "string"}, {"description", "Document name"}}}
            }},
            {"oneOf", json::array({
                {{"required", json::array({"hash"})}},
                {{"required", json::array({"name"})}}
            })}
        }}
    });
    
    // List documents tool
    tools.push_back({
        {"name", "list_documents"},
        {"description", "List stored documents with metadata"},
        {"inputSchema", {
            {"type", "object"},
            {"properties", {
                {"limit", {{"type", "integer"}, {"default", 50}, {"description", "Maximum number of results"}}},
                {"pattern", {{"type", "string"}, {"description", "Filter by name pattern"}}},
                {"tags", {{"type", "array"}, {"items", {{"type", "string"}}}, {"description", "Filter by tags"}}}
            }}
        }}
    });
    
    return {{"tools", tools}};
}

json MCPServer::listResources() {
    // Could list available documents as resources
    json resources = json::array();
    return {{"resources", resources}};
}

json MCPServer::listPrompts() {
    json prompts = json::array();
    
    prompts.push_back({
        {"name", "summarize_document"},
        {"description", "Generate a summary of a document"},
        {"arguments", json::array({
            {{"name", "document_hash"}, {"description", "Hash of document to summarize"}}
        })}
    });
    
    return {{"prompts", prompts}};
}

json MCPServer::callTool(const std::string& name, const json& arguments) {
    if (name == "search_documents") {
        return searchDocuments(arguments);
    } else if (name == "store_document") {
        return storeDocument(arguments);
    } else if (name == "retrieve_document") {
        return retrieveDocument(arguments);
    } else if (name == "get_stats") {
        return getStats(arguments);
    } else if (name == "delete_by_name") {
        return deleteByName(arguments);
    } else if (name == "get_by_name") {
        return getByName(arguments);
    } else if (name == "cat_document") {
        return catDocument(arguments);
    } else if (name == "list_documents") {
        return listDocuments(arguments);
    } else {
        return {{"error", "Unknown tool: " + name}};
    }
}

json MCPServer::searchDocuments(const json& args) {
    try {
        std::string query = args["query"];
        size_t limit = args.value("limit", 10);
        
        search::SearchRequest request;
        request.query = query;
        request.limit = limit;
        
        auto result = searchExecutor_->search(request);
        if (!result) {
            return {{"error", result.error().message}};
        }
        
        json response;
        response["total"] = result.value().getStatistics().totalResults;
        
        json results = json::array();
        for (const auto& item : result.value().getItems()) {
            results.push_back({
                {"id", item.documentId},
                {"title", item.title},
                {"path", item.path},
                {"score", item.relevanceScore},
                {"snippet", item.contentPreview}
            });
        }
        response["results"] = results;
        
        return response;
        
    } catch (const std::exception& e) {
        return {{"error", std::string("Search failed: ") + e.what()}};
    }
}

json MCPServer::storeDocument(const json& args) {
    try {
        std::string path = args["path"];
        
        api::ContentMetadata metadata;
        if (args.contains("tags")) {
            auto tagsVec = args["tags"].get<std::vector<std::string>>();
            for (const auto& tag : tagsVec) {
                metadata.tags[tag] = ""; // Convert vector to map with empty values
            }
        }
        
        auto result = store_->store(path, metadata);
        if (!result) {
            return {{"error", result.error().message}};
        }
        
        return {
            {"hash", result.value().contentHash},
            {"bytes_stored", result.value().bytesStored},
            {"bytes_deduped", result.value().bytesDeduped}
        };
        
    } catch (const std::exception& e) {
        return {{"error", std::string("Store failed: ") + e.what()}};
    }
}

json MCPServer::retrieveDocument(const json& args) {
    try {
        std::string hash = args["hash"];
        std::string outputPath = args.value("outputPath", hash);
        
        auto result = store_->retrieve(hash, outputPath);
        if (!result) {
            return {{"error", result.error().message}};
        }
        
        return {
            {"found", result.value().found},
            {"size", result.value().size},
            {"path", outputPath}
        };
        
    } catch (const std::exception& e) {
        return {{"error", std::string("Retrieve failed: ") + e.what()}};
    }
}

json MCPServer::getStats(const json& args) {
    try {
        auto stats = store_->getStats();
        
        return {
            {"total_objects", stats.totalObjects},
            {"total_bytes", stats.totalBytes},
            {"unique_blocks", stats.uniqueBlocks},
            {"deduplicated_bytes", stats.deduplicatedBytes},
            {"dedup_ratio", stats.dedupRatio()}
        };
        
    } catch (const std::exception& e) {
        return {{"error", std::string("Stats failed: ") + e.what()}};
    }
}

json MCPServer::deleteByName(const json& args) {
    try {
        bool dry_run = args.value("dry_run", false);
        json result = json::object();
        result["dry_run"] = dry_run;
        
        std::vector<std::pair<std::string, std::string>> toDelete;
        
        if (args.contains("name")) {
            std::string name = args["name"];
            auto resolveResult = resolveNameToHashes(name);
            if (!resolveResult) {
                return {{"error", resolveResult.error().message}};
            }
            toDelete = resolveResult.value();
            
        } else if (args.contains("names")) {
            auto names = args["names"].get<std::vector<std::string>>();
            auto resolveResult = resolveNamesToHashes(names);
            if (!resolveResult) {
                return {{"error", resolveResult.error().message}};
            }
            toDelete = resolveResult.value();
            
        } else if (args.contains("pattern")) {
            std::string pattern = args["pattern"];
            auto resolveResult = resolvePatternToHashes(pattern);
            if (!resolveResult) {
                return {{"error", resolveResult.error().message}};
            }
            toDelete = resolveResult.value();
            result["pattern"] = pattern;
        }
        
        // Build result with actual matches found
        json deleted_items = json::array();
        int successful_deletions = 0;
        json errors = json::array();
        
        for (const auto& [name, hash] : toDelete) {
            if (!dry_run) {
                // Actually delete the document
                auto deleteResult = store_->remove(hash);
                if (deleteResult) {
                    successful_deletions++;
                    deleted_items.push_back({{"name", name}, {"hash", hash}});
                } else {
                    errors.push_back({{"name", name}, {"hash", hash}, {"error", deleteResult.error().message}});
                }
            } else {
                // Dry run - just record what would be deleted
                deleted_items.push_back({{"name", name}, {"hash", hash}});
            }
        }
        
        result["deleted"] = deleted_items;
        result["count"] = dry_run ? toDelete.size() : successful_deletions;
        
        if (!errors.empty()) {
            result["errors"] = errors;
        }
        
        std::string action = dry_run ? "Would delete" : "Deleted";
        result["message"] = action + " " + std::to_string(result["count"].get<int>()) + " document(s)";
        
        return result;
    } catch (const std::exception& e) {
        return {{"error", std::string("Delete failed: ") + e.what()}};
    }
}

json MCPServer::getByName(const json& args) {
    try {
        std::string name = args["name"];
        std::string output_path = args.value("output_path", "");
        
        // Resolve name to hash using existing CLI logic
        auto resolveResult = resolveNameToHash(name);
        if (!resolveResult) {
            return {{"error", resolveResult.error().message}};
        }
        
        std::string hash = resolveResult.value();
        
        // Check if document exists
        auto existsResult = store_->exists(hash);
        if (!existsResult) {
            return {{"error", existsResult.error().message}};
        }
        
        if (!existsResult.value()) {
            return {{"error", "Document not found: " + hash}};
        }
        
        json result;
        result["name"] = name;
        result["hash"] = hash;
        
        if (!output_path.empty()) {
            // Retrieve to specified file path
            auto retrieveResult = store_->retrieve(hash, output_path);
            if (!retrieveResult) {
                return {{"error", retrieveResult.error().message}};
            }
            
            result["output_path"] = output_path;
            result["size"] = retrieveResult.value().size;
            result["message"] = "Document retrieved to: " + output_path;
        } else {
            // Get content directly (similar to cat functionality)
            std::ostringstream oss;
            auto streamResult = store_->retrieveStream(hash, oss, nullptr);
            if (!streamResult) {
                return {{"error", streamResult.error().message}};
            }
            
            result["content"] = oss.str();
            result["size"] = oss.str().size();
            result["message"] = "Document content retrieved";
        }
        
        return result;
    } catch (const std::exception& e) {
        return {{"error", std::string("Get by name failed: ") + e.what()}};
    }
}

json MCPServer::catDocument(const json& args) {
    try {
        std::string hashToDisplay;
        json result = json::object();
        
        if (args.contains("hash")) {
            std::string hash = args["hash"];
            hashToDisplay = hash;
            result["hash"] = hash;
        } else if (args.contains("name")) {
            std::string name = args["name"];
            // Resolve name to hash using existing CLI logic
            auto resolveResult = resolveNameToHash(name);
            if (!resolveResult) {
                return {{"error", resolveResult.error().message}};
            }
            hashToDisplay = resolveResult.value();
            result["name"] = name;
            result["hash"] = hashToDisplay;
        } else {
            return {{"error", "No document specified (hash or name required)"}};
        }
        
        // Check if document exists
        auto existsResult = store_->exists(hashToDisplay);
        if (!existsResult) {
            return {{"error", existsResult.error().message}};
        }
        
        if (!existsResult.value()) {
            return {{"error", "Document not found: " + hashToDisplay}};
        }
        
        // Get content using stream interface (same as CLI cat command)
        std::ostringstream oss;
        auto streamResult = store_->retrieveStream(hashToDisplay, oss, nullptr);
        if (!streamResult) {
            return {{"error", streamResult.error().message}};
        }
        
        result["content"] = oss.str();
        result["size"] = oss.str().size();
        
        return result;
    } catch (const std::exception& e) {
        return {{"error", std::string("Cat document failed: ") + e.what()}};
    }
}

json MCPServer::listDocuments(const json& args) {
    try {
        int limit = args.value("limit", 50);
        std::string pattern = args.value("pattern", "");
        auto tags = args.value("tags", std::vector<std::string>{});
        
        if (!metadataRepo_) {
            return {{"error", "Metadata repository not available"}};
        }
        
        json result = json::object();
        result["limit"] = limit;
        
        // Use existing CLI logic - get all documents from metadata repository
        std::string searchPattern = pattern.empty() ? "%" : pattern;
        
        // Convert glob pattern to SQL LIKE pattern if needed
        if (!pattern.empty()) {
            std::replace(searchPattern.begin(), searchPattern.end(), '*', '%');
            std::replace(searchPattern.begin(), searchPattern.end(), '?', '_');
            if (searchPattern.front() != '%') {
                searchPattern = "%/" + searchPattern;
            }
        }
        
        auto documentsResult = metadataRepo_->findDocumentsByPath(searchPattern);
        if (!documentsResult) {
            return {{"error", "Failed to query documents: " + documentsResult.error().message}};
        }
        
        json documents = json::array();
        
        // Process each document (similar to CLI list command)
        for (const auto& docInfo : documentsResult.value()) {
            if (documents.size() >= static_cast<size_t>(limit)) {
                break;
            }
            
            json doc;
            doc["name"] = docInfo.fileName;
            doc["hash"] = docInfo.sha256Hash;
            doc["path"] = docInfo.filePath;
            doc["extension"] = docInfo.fileExtension;
            doc["size"] = docInfo.fileSize;
            doc["mime_type"] = docInfo.mimeType;
            doc["created"] = std::chrono::duration_cast<std::chrono::seconds>(docInfo.createdTime.time_since_epoch()).count();
            doc["modified"] = std::chrono::duration_cast<std::chrono::seconds>(docInfo.modifiedTime.time_since_epoch()).count();
            doc["indexed"] = std::chrono::duration_cast<std::chrono::seconds>(docInfo.indexedTime.time_since_epoch()).count();
            
            // Get metadata including tags if requested
            if (!tags.empty()) {
                auto metadataResult = metadataRepo_->getAllMetadata(docInfo.id);
                if (metadataResult) {
                    const auto& metadata = metadataResult.value();
                    json docTags = json::array();
                    
                    // Extract tags from metadata
                    for (const auto& [key, value] : metadata) {
                        if (key == "tag" || key.starts_with("tag:")) {
                            docTags.push_back(value.value.empty() ? key : value.value);
                        }
                    }
                    
                    // Filter by tags if specified
                    bool hasMatchingTag = tags.empty();
                    if (!tags.empty()) {
                        for (const auto& requiredTag : tags) {
                            for (const auto& docTag : docTags) {
                                if (docTag.get<std::string>() == requiredTag) {
                                    hasMatchingTag = true;
                                    break;
                                }
                            }
                            if (hasMatchingTag) break;
                        }
                    }
                    
                    if (!hasMatchingTag) {
                        continue; // Skip this document
                    }
                    
                    doc["tags"] = docTags;
                }
            }
            
            documents.push_back(doc);
        }
        
        result["documents"] = documents;
        result["count"] = documents.size();
        result["total_found"] = documentsResult.value().size();
        
        if (!pattern.empty()) {
            result["pattern"] = pattern;
        }
        
        if (!tags.empty()) {
            result["filtered_by_tags"] = tags;
        }
        
        return result;
    } catch (const std::exception& e) {
        return {{"error", std::string("List documents failed: ") + e.what()}};
    }
}

json MCPServer::readResource(const std::string& uri) {
    // Could implement reading document content by URI
    return {{"content", "Resource reading not implemented"}};
}

json MCPServer::createResponse(const json& id, const json& result) {
    json response;
    response["jsonrpc"] = "2.0";
    if (!id.is_null()) {
        response["id"] = id;
    }
    response["result"] = result;
    return response;
}

json MCPServer::createError(const json& id, int code, const std::string& message) {
    json response;
    response["jsonrpc"] = "2.0";
    if (!id.is_null()) {
        response["id"] = id;
    }
    response["error"] = {
        {"code", code},
        {"message", message}
    };
    return response;
}

// Name resolution helper methods (similar to CLI commands)
Result<std::string> MCPServer::resolveNameToHash(const std::string& name) {
    if (!metadataRepo_) {
        return Error{ErrorCode::NotInitialized, "Metadata repository not initialized"};
    }
    
    // Search for documents with matching fileName
    auto documentsResult = metadataRepo_->findDocumentsByPath("%/" + name);
    if (!documentsResult) {
        // Try exact match
        documentsResult = metadataRepo_->findDocumentsByPath(name);
        if (!documentsResult) {
            return Error{ErrorCode::NotFound, "Document not found: " + name};
        }
    }
    
    const auto& documents = documentsResult.value();
    if (documents.empty()) {
        return Error{ErrorCode::NotFound, "Document not found: " + name};
    }
    
    if (documents.size() > 1) {
        return Error{ErrorCode::InvalidOperation, "Ambiguous name: multiple documents match '" + name + "'"};
    }
    
    return documents[0].sha256Hash;
}

Result<std::vector<std::pair<std::string, std::string>>> MCPServer::resolveNameToHashes(const std::string& name) {
    if (!metadataRepo_) {
        return Error{ErrorCode::NotInitialized, "Metadata repository not available"};
    }
    
    // Search for documents with matching fileName
    auto documentsResult = metadataRepo_->findDocumentsByPath("%/" + name);
    if (!documentsResult) {
        // Try exact match
        documentsResult = metadataRepo_->findDocumentsByPath(name);
        if (!documentsResult) {
            return Error{ErrorCode::NotFound, "Failed to query documents: " + documentsResult.error().message};
        }
    }
    
    const auto& documents = documentsResult.value();
    if (documents.empty()) {
        return Error{ErrorCode::NotFound, "No documents found with name: " + name};
    }
    
    std::vector<std::pair<std::string, std::string>> results;
    for (const auto& doc : documents) {
        results.emplace_back(doc.fileName, doc.sha256Hash);
    }
    
    return results;
}

Result<std::vector<std::pair<std::string, std::string>>> MCPServer::resolveNamesToHashes(const std::vector<std::string>& names) {
    std::vector<std::pair<std::string, std::string>> allResults;
    
    // Resolve each name
    for (const auto& name : names) {
        auto result = resolveNameToHashes(name);
        if (result) {
            for (const auto& pair : result.value()) {
                allResults.push_back(pair);
            }
        } else if (result.error().code != ErrorCode::NotFound) {
            // Return early on non-NotFound errors
            return result;
        }
        // Skip NotFound errors for individual names
    }
    
    if (allResults.empty()) {
        return Error{ErrorCode::NotFound, "No documents found for the specified names"};
    }
    
    return allResults;
}

Result<std::vector<std::pair<std::string, std::string>>> MCPServer::resolvePatternToHashes(const std::string& pattern) {
    if (!metadataRepo_) {
        return Error{ErrorCode::NotInitialized, "Metadata repository not available"};
    }
    
    // Convert glob pattern to SQL LIKE pattern
    std::string sqlPattern = pattern;
    // Replace glob wildcards with SQL wildcards
    std::replace(sqlPattern.begin(), sqlPattern.end(), '*', '%');
    std::replace(sqlPattern.begin(), sqlPattern.end(), '?', '_');
    
    // Ensure it matches the end of the path (filename)
    if (sqlPattern.front() != '%') {
        sqlPattern = "%/" + sqlPattern;
    }
    
    auto documentsResult = metadataRepo_->findDocumentsByPath(sqlPattern);
    if (!documentsResult) {
        return Error{ErrorCode::NotFound, "Failed to query documents: " + documentsResult.error().message};
    }
    
    std::vector<std::pair<std::string, std::string>> results;
    for (const auto& doc : documentsResult.value()) {
        results.emplace_back(doc.fileName, doc.sha256Hash);
    }
    
    return results;
}

} // namespace yams::mcp