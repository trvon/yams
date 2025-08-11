#pragma once

#include <yams/api/content_store.h>
#include <yams/api/async_content_store.h>
#include <yams/api/http/auth_filter.h>

#include <drogon/drogon.h>
#include <memory>
#include <filesystem>

namespace yams::api::http {

/**
 * HTTP API Server configuration
 */
struct ServerConfig {
    // Server settings
    std::string address = "0.0.0.0";
    uint16_t port = 8080;
    size_t threadNum = 0;  // 0 = auto-detect
    
    // SSL/TLS settings
    bool useSSL = false;
    std::string certFile;
    std::string keyFile;
    
    // Performance settings
    size_t maxConnectionNum = 100000;
    size_t maxConnectionNumPerIP = 0;  // 0 = unlimited
    size_t clientMaxBodySize = 100 * 1024 * 1024;  // 100MB
    size_t clientMaxMemoryBodySize = 1 * 1024 * 1024;  // 1MB
    size_t clientMaxWebSocketMessageSize = 128 * 1024;  // 128KB
    
    // Timeouts
    size_t idleConnectionTimeout = 60;  // seconds
    size_t requestTimeout = 60;  // seconds
    
    // Logging
    bool enableAccessLog = true;
    std::string accessLogPath = "./access.log";
    bool logWithUtcTime = false;
    
    // Static files
    bool enableStaticFiles = true;
    std::filesystem::path staticFilesPath = "./static";
    
    // Authentication
    AuthFilter::Config authConfig;
    
    // Rate limiting
    RateLimitFilter::Config rateLimitConfig;
    
    // CORS
    CorsFilter::Config corsConfig;
    
    // Logging filter
    LoggingFilter::Config loggingConfig;
};

/**
 * HTTP API Server
 */
class Server {
public:
    explicit Server(std::shared_ptr<IContentStore> store, const ServerConfig& config = {});
    ~Server();
    
    // Start the server
    void start();
    
    // Stop the server
    void stop();
    
    // Wait for server to stop
    void wait();
    
    // Check if server is running
    bool isRunning() const;
    
    // Get server configuration
    const ServerConfig& getConfig() const { return config_; }
    
    // Get content store
    std::shared_ptr<IContentStore> getContentStore() const { return store_; }
    
private:
    std::shared_ptr<IContentStore> store_;
    std::shared_ptr<AsyncContentStore> asyncStore_;
    ServerConfig config_;
    
    void configureServer();
    void registerControllers();
    void registerFilters();
    void setupStaticFiles();
    void setupLogging();
};

/**
 * Create and configure HTTP server with defaults
 */
std::unique_ptr<Server> createHttpServer(
    std::shared_ptr<IContentStore> store,
    uint16_t port = 8080,
    const std::string& address = "0.0.0.0");

/**
 * Create HTTP server from configuration file
 */
std::unique_ptr<Server> createHttpServerFromConfig(
    std::shared_ptr<IContentStore> store,
    const std::filesystem::path& configFile);

} // namespace yams::api::http