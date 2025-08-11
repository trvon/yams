#include <yams/api/http/server.h>
#include <yams/api/http/content_controller.h>

#include <spdlog/spdlog.h>
#include <json/json.h>
#include <fstream>

namespace yams::api::http {

Server::Server(std::shared_ptr<IContentStore> store, const ServerConfig& config)
    : store_(std::move(store))
    , config_(config) {
    
    // Create async wrapper
    asyncStore_ = std::make_unique<AsyncContentStore>(store_);
    asyncStore_->setMaxConcurrentOperations(config_.threadNum > 0 ? config_.threadNum * 2 : 20);
}

Server::~Server() {
    if (isRunning()) {
        stop();
    }
}

void Server::start() {
    spdlog::info("Starting YAMS HTTP API server on {}:{}", config_.address, config_.port);
    
    // Configure server
    configureServer();
    
    // Register controllers and filters
    registerControllers();
    registerFilters();
    
    // Setup static files
    if (config_.enableStaticFiles) {
        setupStaticFiles();
    }
    
    // Setup logging
    setupLogging();
    
    // Start server
    drogon::app().run();
}

void Server::stop() {
    spdlog::info("Stopping YAMS HTTP API server");
    drogon::app().quit();
}

void Server::wait() {
    // Drogon blocks in run(), so nothing to do here
}

bool Server::isRunning() const {
    return drogon::app().isRunning();
}

void Server::configureServer() {
    auto& app = drogon::app();
    
    // Basic configuration
    app.addListener(config_.address, config_.port, config_.useSSL);
    
    if (config_.threadNum > 0) {
        app.setThreadNum(config_.threadNum);
    }
    
    // SSL configuration
    if (config_.useSSL && !config_.certFile.empty() && !config_.keyFile.empty()) {
        app.setSSLFiles(config_.certFile, config_.keyFile);
    }
    
    // Connection settings
    app.setMaxConnectionNum(config_.maxConnectionNum);
    app.setMaxConnectionNumPerIP(config_.maxConnectionNumPerIP);
    
    // Body size limits
    app.setClientMaxBodySize(config_.clientMaxBodySize);
    app.setClientMaxMemoryBodySize(config_.clientMaxMemoryBodySize);
    app.setClientMaxWebSocketMessageSize(config_.clientMaxWebSocketMessageSize);
    
    // Timeouts
    app.setIdleConnectionTimeout(config_.idleConnectionTimeout);
    
    // Custom 404 handler
    app.setCustom404Page(
        Json::FastWriter().write(Json::Value{
            {"success", false},
            {"error", "Not Found"},
            {"message", "The requested endpoint does not exist"},
            {"code", 404}
        }),
        drogon::CT_APPLICATION_JSON
    );
    
    // Enable session (for future use)
    app.enableSession(1200);  // 20 minutes
    
    // Enable compression
    app.enableBrotli(true);
    app.enableGzip(true);
    
    // Server header
    app.setServerHeaderField("YAMS API Server");
}

void Server::registerControllers() {
    // Initialize content controller
    ContentController::initController(asyncStore_);
    
    // Register controllers
    drogon::app().registerController(std::make_shared<ContentController>());
    
    spdlog::info("Registered HTTP controllers");
}

void Server::registerFilters() {
    // Configure filters
    AuthFilter::configure(config_.authConfig);
    RateLimitFilter::configure(config_.rateLimitConfig);
    CorsFilter::configure(config_.corsConfig);
    LoggingFilter::configure(config_.loggingConfig);
    
    // Register global filters
    auto& app = drogon::app();
    
    // Apply filters to API routes
    app.registerFilter(std::make_shared<CorsFilter>());
    app.registerFilter(std::make_shared<LoggingFilter>());
    app.registerFilter(std::make_shared<RateLimitFilter>());
    
    // Apply auth filter to protected routes
    if (config_.authConfig.enableApiKey || config_.authConfig.enableJWT || config_.authConfig.enableBasic) {
        app.registerFilterBefore<AuthFilter>(
            "/api/v1/content.*",
            drogon::Post,
            drogon::Put,
            drogon::Delete
        );
    }
    
    spdlog::info("Registered HTTP filters");
}

void Server::setupStaticFiles() {
    if (std::filesystem::exists(config_.staticFilesPath)) {
        drogon::app().setDocumentRoot(config_.staticFilesPath.string());
        drogon::app().setStaticFileHeaders({
            {"Cache-Control", "public, max-age=3600"},
            {"X-Content-Type-Options", "nosniff"}
        });
        
        spdlog::info("Serving static files from: {}", config_.staticFilesPath.string());
    }
}

void Server::setupLogging() {
    if (config_.enableAccessLog) {
        drogon::app().setLogPath("./")
                     .setLogLevel(trantor::Logger::kInfo)
                     .enableServerHeader(true)
                     .enableDateHeader(true);
        
        if (config_.logWithUtcTime) {
            drogon::app().setLogLocalTime(false);
        }
    }
}

// Factory functions
std::unique_ptr<Server> createHttpServer(
    std::shared_ptr<IContentStore> store,
    uint16_t port,
    const std::string& address) {
    
    ServerConfig config;
    config.port = port;
    config.address = address;
    
    return std::make_unique<Server>(std::move(store), config);
}

std::unique_ptr<Server> createHttpServerFromConfig(
    std::shared_ptr<IContentStore> store,
    const std::filesystem::path& configFile) {
    
    ServerConfig config;
    
    // Load configuration from JSON file
    if (std::filesystem::exists(configFile)) {
        std::ifstream file(configFile);
        Json::Value root;
        Json::Reader reader;
        
        if (reader.parse(file, root)) {
            // Parse server settings
            if (root.isMember("server")) {
                const auto& server = root["server"];
                config.address = server.get("address", config.address).asString();
                config.port = server.get("port", config.port).asUInt();
                config.threadNum = server.get("threads", config.threadNum).asUInt();
                
                // SSL settings
                if (server.isMember("ssl")) {
                    const auto& ssl = server["ssl"];
                    config.useSSL = ssl.get("enabled", false).asBool();
                    config.certFile = ssl.get("cert", "").asString();
                    config.keyFile = ssl.get("key", "").asString();
                }
            }
            
            // Parse limits
            if (root.isMember("limits")) {
                const auto& limits = root["limits"];
                config.maxConnectionNum = limits.get("maxConnections", config.maxConnectionNum).asUInt();
                config.clientMaxBodySize = limits.get("maxBodySize", config.clientMaxBodySize).asUInt();
                config.idleConnectionTimeout = limits.get("idleTimeout", config.idleConnectionTimeout).asUInt();
            }
            
            // Parse auth settings
            if (root.isMember("auth")) {
                const auto& auth = root["auth"];
                config.authConfig.enableApiKey = auth.get("apiKey", true).asBool();
                config.authConfig.enableJWT = auth.get("jwt", false).asBool();
                
                // Load API keys
                if (auth.isMember("validKeys") && auth["validKeys"].isArray()) {
                    for (const auto& key : auth["validKeys"]) {
                        config.authConfig.validApiKeys.insert(key.asString());
                    }
                }
            }
            
            spdlog::info("Loaded server configuration from: {}", configFile.string());
        }
    }
    
    return std::make_unique<Server>(std::move(store), config);
}

} // namespace yams::api::http