#pragma once

#include <drogon/HttpFilter.h>
#include <string>
#include <set>

namespace yams::api::http {

/**
 * Authentication filter for API endpoints
 * 
 * Supports:
 * - API key authentication (header: X-API-Key)
 * - JWT bearer token authentication (header: Authorization: Bearer <token>)
 * - Basic authentication (header: Authorization: Basic <base64>)
 */
class AuthFilter : public drogon::HttpFilter<AuthFilter> {
public:
    AuthFilter() = default;
    
    static constexpr bool isAutoCreation = false;
    
    void doFilter(const drogon::HttpRequestPtr& req,
                 drogon::FilterCallback&& fcb,
                 drogon::FilterChainCallback&& fccb) override;
    
    // Configuration
    struct Config {
        bool enableApiKey = true;
        bool enableJWT = true;
        bool enableBasic = false;
        
        // API key configuration
        std::set<std::string> validApiKeys;
        
        // JWT configuration
        std::string jwtSecret;
        std::string jwtIssuer;
        
        // Basic auth users (username -> password hash)
        std::unordered_map<std::string, std::string> basicAuthUsers;
        
        // Permissions
        bool requireWritePermission = false;
        std::set<std::string> readOnlyApiKeys;
    };
    
    static void configure(const Config& config);
    
private:
    static Config config_;
    
    // Authentication methods
    bool authenticateApiKey(const std::string& apiKey);
    bool authenticateJWT(const std::string& token);
    bool authenticateBasic(const std::string& credentials);
    
    // Check if request has write permission
    bool hasWritePermission(const drogon::HttpRequestPtr& req);
    
    // Extract authentication credentials from request
    std::optional<std::string> extractApiKey(const drogon::HttpRequestPtr& req);
    std::optional<std::string> extractBearerToken(const drogon::HttpRequestPtr& req);
    std::optional<std::string> extractBasicAuth(const drogon::HttpRequestPtr& req);
};

/**
 * Rate limiting filter
 */
class RateLimitFilter : public drogon::HttpFilter<RateLimitFilter> {
public:
    RateLimitFilter() = default;
    
    static constexpr bool isAutoCreation = false;
    
    void doFilter(const drogon::HttpRequestPtr& req,
                 drogon::FilterCallback&& fcb,
                 drogon::FilterChainCallback&& fccb) override;
    
    struct Config {
        size_t requestsPerMinute = 60;
        size_t requestsPerHour = 1000;
        size_t burstSize = 10;
        
        // Different limits for different operations
        std::unordered_map<std::string, size_t> endpointLimits;
    };
    
    static void configure(const Config& config);
    
private:
    static Config config_;
    
    struct ClientInfo {
        std::atomic<size_t> minuteCount{0};
        std::atomic<size_t> hourCount{0};
        std::chrono::steady_clock::time_point lastReset;
        std::chrono::steady_clock::time_point hourReset;
    };
    
    static std::unordered_map<std::string, ClientInfo> clients_;
    static std::mutex clientsMutex_;
    
    std::string getClientId(const drogon::HttpRequestPtr& req);
    bool checkRateLimit(const std::string& clientId, const std::string& endpoint);
};

/**
 * CORS filter for cross-origin requests
 */
class CorsFilter : public drogon::HttpFilter<CorsFilter> {
public:
    CorsFilter() = default;
    
    static constexpr bool isAutoCreation = false;
    
    void doFilter(const drogon::HttpRequestPtr& req,
                 drogon::FilterCallback&& fcb,
                 drogon::FilterChainCallback&& fccb) override;
    
    struct Config {
        std::set<std::string> allowedOrigins = {"*"};
        std::set<std::string> allowedMethods = {"GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS"};
        std::set<std::string> allowedHeaders = {"Content-Type", "Authorization", "X-API-Key"};
        std::set<std::string> exposedHeaders = {"X-Request-Id", "X-RateLimit-Remaining"};
        bool allowCredentials = true;
        int maxAge = 86400; // 24 hours
    };
    
    static void configure(const Config& config);
    
private:
    static Config config_;
};

/**
 * Request logging filter
 */
class LoggingFilter : public drogon::HttpFilter<LoggingFilter> {
public:
    LoggingFilter() = default;
    
    static constexpr bool isAutoCreation = false;
    
    void doFilter(const drogon::HttpRequestPtr& req,
                 drogon::FilterCallback&& fcb,
                 drogon::FilterChainCallback&& fccb) override;
    
    struct Config {
        bool logHeaders = false;
        bool logBody = false;
        bool logResponse = true;
        std::set<std::string> sensitiveHeaders = {"Authorization", "X-API-Key"};
    };
    
    static void configure(const Config& config);
    
private:
    static Config config_;
    
    std::string generateRequestId();
    void logRequest(const drogon::HttpRequestPtr& req, const std::string& requestId);
};

} // namespace yams::api::http