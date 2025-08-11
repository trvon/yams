#include <yams/api/http/auth_filter.h>
#include <drogon/utils/Utilities.h>
#include <spdlog/spdlog.h>

#include <chrono>
#include <sstream>

namespace yams::api::http {

// Static member initialization
AuthFilter::Config AuthFilter::config_;
RateLimitFilter::Config RateLimitFilter::config_;
CorsFilter::Config CorsFilter::config_;
LoggingFilter::Config LoggingFilter::config_;

std::unordered_map<std::string, RateLimitFilter::ClientInfo> RateLimitFilter::clients_;
std::mutex RateLimitFilter::clientsMutex_;

// AuthFilter implementation
void AuthFilter::configure(const Config& config) {
    config_ = config;
    spdlog::info("AuthFilter configured");
}

void AuthFilter::doFilter(const drogon::HttpRequestPtr& req,
                         drogon::FilterCallback&& fcb,
                         drogon::FilterChainCallback&& fccb) {
    
    // Skip auth for GET/HEAD requests if not required
    auto method = req->getMethod();
    if (!config_.requireWritePermission && 
        (method == drogon::Get || method == drogon::Head)) {
        fccb();
        return;
    }
    
    bool authenticated = false;
    
    // Try API key authentication
    if (config_.enableApiKey) {
        auto apiKey = extractApiKey(req);
        if (apiKey && authenticateApiKey(*apiKey)) {
            authenticated = true;
            req->getAttributes()->insert("auth_method", "api_key");
            req->getAttributes()->insert("api_key", *apiKey);
        }
    }
    
    // Try JWT authentication
    if (!authenticated && config_.enableJWT) {
        auto token = extractBearerToken(req);
        if (token && authenticateJWT(*token)) {
            authenticated = true;
            req->getAttributes()->insert("auth_method", "jwt");
        }
    }
    
    // Try basic authentication
    if (!authenticated && config_.enableBasic) {
        auto credentials = extractBasicAuth(req);
        if (credentials && authenticateBasic(*credentials)) {
            authenticated = true;
            req->getAttributes()->insert("auth_method", "basic");
        }
    }
    
    if (!authenticated) {
        auto resp = drogon::HttpResponse::newHttpResponse();
        resp->setStatusCode(drogon::k401Unauthorized);
        resp->addHeader("WWW-Authenticate", "Bearer realm=\"YAMS API\"");
        resp->setBody("{\"error\":\"Unauthorized\",\"message\":\"Invalid or missing authentication credentials\"}");
        resp->setContentTypeCode(drogon::CT_APPLICATION_JSON);
        fcb(resp);
        return;
    }
    
    // Check write permission for mutating operations
    if (config_.requireWritePermission && 
        (method == drogon::Post || method == drogon::Put || method == drogon::Delete)) {
        if (!hasWritePermission(req)) {
            auto resp = drogon::HttpResponse::newHttpResponse();
            resp->setStatusCode(drogon::k403Forbidden);
            resp->setBody("{\"error\":\"Forbidden\",\"message\":\"Insufficient permissions for this operation\"}");
            resp->setContentTypeCode(drogon::CT_APPLICATION_JSON);
            fcb(resp);
            return;
        }
    }
    
    // Authentication successful, continue to next filter
    fccb();
}

bool AuthFilter::authenticateApiKey(const std::string& apiKey) {
    return config_.validApiKeys.find(apiKey) != config_.validApiKeys.end();
}

bool AuthFilter::authenticateJWT(const std::string& token) {
    // TODO: Implement JWT validation
    // This would involve:
    // 1. Parsing the JWT
    // 2. Verifying the signature with config_.jwtSecret
    // 3. Checking expiration
    // 4. Verifying issuer matches config_.jwtIssuer
    return false;
}

bool AuthFilter::authenticateBasic(const std::string& credentials) {
    // Decode base64 credentials
    auto decoded = drogon::utils::base64Decode(credentials);
    
    // Split username:password
    auto colonPos = decoded.find(':');
    if (colonPos == std::string::npos) {
        return false;
    }
    
    std::string username = decoded.substr(0, colonPos);
    std::string password = decoded.substr(colonPos + 1);
    
    // Look up user
    auto it = config_.basicAuthUsers.find(username);
    if (it == config_.basicAuthUsers.end()) {
        return false;
    }
    
    // TODO: Compare password hash
    // In production, passwords should be hashed with bcrypt or similar
    return password == it->second;
}

bool AuthFilter::hasWritePermission(const drogon::HttpRequestPtr& req) {
    auto attrs = req->getAttributes();
    
    // Check if using read-only API key
    auto apiKeyIt = attrs->find("api_key");
    if (apiKeyIt != attrs->end()) {
        const auto& apiKey = std::any_cast<std::string>(apiKeyIt->second);
        return config_.readOnlyApiKeys.find(apiKey) == config_.readOnlyApiKeys.end();
    }
    
    // JWT and basic auth have full permissions by default
    return true;
}

std::optional<std::string> AuthFilter::extractApiKey(const drogon::HttpRequestPtr& req) {
    auto header = req->getHeader("X-API-Key");
    if (!header.empty()) {
        return header;
    }
    
    // Also check query parameter as fallback
    auto params = req->getParameters();
    auto it = params.find("api_key");
    if (it != params.end()) {
        return it->second;
    }
    
    return std::nullopt;
}

std::optional<std::string> AuthFilter::extractBearerToken(const drogon::HttpRequestPtr& req) {
    auto header = req->getHeader("Authorization");
    if (header.empty()) {
        return std::nullopt;
    }
    
    const std::string bearerPrefix = "Bearer ";
    if (header.substr(0, bearerPrefix.length()) != bearerPrefix) {
        return std::nullopt;
    }
    
    return header.substr(bearerPrefix.length());
}

std::optional<std::string> AuthFilter::extractBasicAuth(const drogon::HttpRequestPtr& req) {
    auto header = req->getHeader("Authorization");
    if (header.empty()) {
        return std::nullopt;
    }
    
    const std::string basicPrefix = "Basic ";
    if (header.substr(0, basicPrefix.length()) != basicPrefix) {
        return std::nullopt;
    }
    
    return header.substr(basicPrefix.length());
}

// RateLimitFilter implementation
void RateLimitFilter::configure(const Config& config) {
    config_ = config;
    spdlog::info("RateLimitFilter configured");
}

void RateLimitFilter::doFilter(const drogon::HttpRequestPtr& req,
                              drogon::FilterCallback&& fcb,
                              drogon::FilterChainCallback&& fccb) {
    
    std::string clientId = getClientId(req);
    std::string endpoint = req->getPath();
    
    if (!checkRateLimit(clientId, endpoint)) {
        auto resp = drogon::HttpResponse::newHttpResponse();
        resp->setStatusCode(drogon::k429TooManyRequests);
        resp->addHeader("Retry-After", "60");
        resp->setBody("{\"error\":\"Too Many Requests\",\"message\":\"Rate limit exceeded\"}");
        resp->setContentTypeCode(drogon::CT_APPLICATION_JSON);
        fcb(resp);
        return;
    }
    
    fccb();
}

std::string RateLimitFilter::getClientId(const drogon::HttpRequestPtr& req) {
    // Try to get authenticated user ID
    auto attrs = req->getAttributes();
    auto apiKeyIt = attrs->find("api_key");
    if (apiKeyIt != attrs->end()) {
        return std::any_cast<std::string>(apiKeyIt->second);
    }
    
    // Fall back to IP address
    return req->getPeerAddr().toIp();
}

bool RateLimitFilter::checkRateLimit(const std::string& clientId, const std::string& endpoint) {
    std::lock_guard lock(clientsMutex_);
    
    auto now = std::chrono::steady_clock::now();
    auto& client = clients_[clientId];
    
    // Reset counters if needed
    auto minuteElapsed = std::chrono::duration_cast<std::chrono::minutes>(
        now - client.lastReset).count();
    if (minuteElapsed >= 1) {
        client.minuteCount = 0;
        client.lastReset = now;
    }
    
    auto hourElapsed = std::chrono::duration_cast<std::chrono::hours>(
        now - client.hourReset).count();
    if (hourElapsed >= 1) {
        client.hourCount = 0;
        client.hourReset = now;
    }
    
    // Check limits
    if (client.minuteCount >= config_.requestsPerMinute) {
        return false;
    }
    if (client.hourCount >= config_.requestsPerHour) {
        return false;
    }
    
    // Check endpoint-specific limits
    auto it = config_.endpointLimits.find(endpoint);
    if (it != config_.endpointLimits.end() && client.minuteCount >= it->second) {
        return false;
    }
    
    // Increment counters
    client.minuteCount++;
    client.hourCount++;
    
    return true;
}

// CorsFilter implementation
void CorsFilter::configure(const Config& config) {
    config_ = config;
    spdlog::info("CorsFilter configured");
}

void CorsFilter::doFilter(const drogon::HttpRequestPtr& req,
                         drogon::FilterCallback&& fcb,
                         drogon::FilterChainCallback&& fccb) {
    
    auto origin = req->getHeader("Origin");
    
    // Handle preflight requests
    if (req->getMethod() == drogon::Options) {
        auto resp = drogon::HttpResponse::newHttpResponse();
        resp->setStatusCode(drogon::k204NoContent);
        
        // Set CORS headers
        if (config_.allowedOrigins.count("*") > 0 || config_.allowedOrigins.count(origin) > 0) {
            resp->addHeader("Access-Control-Allow-Origin", origin.empty() ? "*" : origin);
            
            if (config_.allowCredentials) {
                resp->addHeader("Access-Control-Allow-Credentials", "true");
            }
            
            // Allowed methods
            std::stringstream methods;
            for (auto it = config_.allowedMethods.begin(); it != config_.allowedMethods.end(); ++it) {
                if (it != config_.allowedMethods.begin()) methods << ", ";
                methods << *it;
            }
            resp->addHeader("Access-Control-Allow-Methods", methods.str());
            
            // Allowed headers
            std::stringstream headers;
            for (auto it = config_.allowedHeaders.begin(); it != config_.allowedHeaders.end(); ++it) {
                if (it != config_.allowedHeaders.begin()) headers << ", ";
                headers << *it;
            }
            resp->addHeader("Access-Control-Allow-Headers", headers.str());
            
            resp->addHeader("Access-Control-Max-Age", std::to_string(config_.maxAge));
        }
        
        fcb(resp);
        return;
    }
    
    // Add CORS headers to response
    auto oldCallback = fccb;
    fccb = [origin, this, fcb = std::move(fcb)]() {
        fcb = [origin, this, fcb = std::move(fcb)](const drogon::HttpResponsePtr& resp) {
            if (config_.allowedOrigins.count("*") > 0 || config_.allowedOrigins.count(origin) > 0) {
                resp->addHeader("Access-Control-Allow-Origin", origin.empty() ? "*" : origin);
                
                if (config_.allowCredentials) {
                    resp->addHeader("Access-Control-Allow-Credentials", "true");
                }
                
                // Exposed headers
                if (!config_.exposedHeaders.empty()) {
                    std::stringstream exposed;
                    for (auto it = config_.exposedHeaders.begin(); it != config_.exposedHeaders.end(); ++it) {
                        if (it != config_.exposedHeaders.begin()) exposed << ", ";
                        exposed << *it;
                    }
                    resp->addHeader("Access-Control-Expose-Headers", exposed.str());
                }
            }
            fcb(resp);
        };
    };
    
    oldCallback();
}

// LoggingFilter implementation
void LoggingFilter::configure(const Config& config) {
    config_ = config;
    spdlog::info("LoggingFilter configured");
}

void LoggingFilter::doFilter(const drogon::HttpRequestPtr& req,
                            drogon::FilterCallback&& fcb,
                            drogon::FilterChainCallback&& fccb) {
    
    auto requestId = generateRequestId();
    req->getAttributes()->insert("request_id", requestId);
    
    logRequest(req, requestId);
    
    auto startTime = std::chrono::steady_clock::now();
    
    // Wrap callback to log response
    if (config_.logResponse) {
        auto oldCallback = fcb;
        fcb = [startTime, requestId, oldCallback = std::move(oldCallback)](
            const drogon::HttpResponsePtr& resp) {
            
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - startTime);
            
            resp->addHeader("X-Request-Id", requestId);
            
            spdlog::info("Response: {} {} {} {}ms",
                        requestId,
                        static_cast<int>(resp->getStatusCode()),
                        drogon::statusCodeToString(resp->getStatusCode()),
                        duration.count());
            
            oldCallback(resp);
        };
    }
    
    fccb();
}

std::string LoggingFilter::generateRequestId() {
    return drogon::utils::getUuid();
}

void LoggingFilter::logRequest(const drogon::HttpRequestPtr& req, const std::string& requestId) {
    spdlog::info("Request: {} {} {} from {}",
                requestId,
                drogon::to_string(req->getMethod()),
                req->getPath(),
                req->getPeerAddr().toIpPort());
    
    if (config_.logHeaders) {
        auto headers = req->getHeaders();
        for (const auto& [key, value] : headers) {
            if (config_.sensitiveHeaders.count(key) == 0) {
                spdlog::debug("  Header: {}: {}", key, value);
            }
        }
    }
}

} // namespace yams::api::http