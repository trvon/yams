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
    Json::Value errorResponse;
    errorResponse["success"] = false;
    errorResponse["error"] = "Not Found";
    errorResponse["message"] = "The requested endpoint does not exist";
    errorResponse["code"] = 404;
    
    auto resp = drogon::HttpResponse::newHttpResponse();
    resp->setStatusCode(drogon::k404NotFound);
    resp->setContentTypeCode(drogon::CT_APPLICATION_JSON);
    resp->setBody(Json::FastWriter().write(errorResponse));
    app.setCustom404Page(resp);
    
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
    
    // Register pre-routing advice for CORS preflight OPTIONS requests
    app.registerPreRoutingAdvice([this](const drogon::HttpRequestPtr &req,
                                         drogon::FilterCallback &&stop,
                                         drogon::FilterChainCallback &&pass) {
        // Handle CORS preflight OPTIONS requests
        if (req->getMethod() == drogon::Options) {
            auto resp = drogon::HttpResponse::newHttpResponse();
            resp->setStatusCode(drogon::k204NoContent);
            
            // Set CORS headers based on config
            auto origin = req->getHeader("Origin");
            if (config_.corsConfig.allowedOrigins.count("*") > 0 || 
                config_.corsConfig.allowedOrigins.count(origin) > 0) {
                resp->addHeader("Access-Control-Allow-Origin", origin.empty() ? "*" : origin);
                
                if (config_.corsConfig.allowCredentials) {
                    resp->addHeader("Access-Control-Allow-Credentials", "true");
                }
                
                // Allowed methods
                std::stringstream methods;
                for (auto it = config_.corsConfig.allowedMethods.begin(); it != config_.corsConfig.allowedMethods.end(); ++it) {
                    if (it != config_.corsConfig.allowedMethods.begin()) methods << ", ";
                    methods << *it;
                }
                resp->addHeader("Access-Control-Allow-Methods", methods.str());
                
                // Allowed headers
                std::stringstream headers;
                for (auto it = config_.corsConfig.allowedHeaders.begin(); it != config_.corsConfig.allowedHeaders.end(); ++it) {
                    if (it != config_.corsConfig.allowedHeaders.begin()) headers << ", ";
                    headers << *it;
                }
                resp->addHeader("Access-Control-Allow-Headers", headers.str());
                
                resp->addHeader("Access-Control-Max-Age", std::to_string(config_.corsConfig.maxAge));
            }
            
            stop(resp); // Stop processing and return preflight response
            return;
        }
        
        // Not an OPTIONS request, continue to next filter
        pass();
    });
    
    // Apply filters to API routes (CorsFilter now implements middleware pattern)
    app.registerFilter(std::make_shared<CorsFilter>());
    app.registerFilter(std::make_shared<LoggingFilter>());
    app.registerFilter(std::make_shared<RateLimitFilter>());
    
    // Apply auth filter to protected routes
    if (config_.authConfig.enableApiKey || config_.authConfig.enableJWT || config_.authConfig.enableBasic) {
        app.registerFilter(std::make_shared<AuthFilter>());
    }
    
    spdlog::info("Registered HTTP filters with CORS pre-routing advice");
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
                config.threadNum = server.get("threads", static_cast<Json::UInt>(config.threadNum)).asUInt();
                
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
                config.maxConnectionNum = limits.get("maxConnections", static_cast<Json::UInt>(config.maxConnectionNum)).asUInt();
                config.clientMaxBodySize = limits.get("maxBodySize", static_cast<Json::UInt64>(config.clientMaxBodySize)).asUInt64();
                config.idleConnectionTimeout = limits.get("idleTimeout", static_cast<Json::UInt>(config.idleConnectionTimeout)).asUInt();
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