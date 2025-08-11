#pragma once

#include <yams/api/content_metadata.h>
#include <yams/api/content_store.h>

#include <json/json.h>
#include <string>
#include <vector>
#include <chrono>

namespace yams::api::http {

// Convert StoreResult to JSON
inline Json::Value storeResultToJson(const StoreResult& result) {
    Json::Value json;
    json["contentHash"] = result.contentHash;
    json["bytesStored"] = static_cast<Json::UInt64>(result.bytesStored);
    json["bytesDeduped"] = static_cast<Json::UInt64>(result.bytesDeduped);
    json["dedupRatio"] = result.dedupRatio();
    json["duration"] = static_cast<Json::Int64>(result.duration.count());
    return json;
}

// Convert RetrieveResult to JSON
inline Json::Value retrieveResultToJson(const RetrieveResult& result) {
    Json::Value json;
    json["found"] = result.found;
    json["size"] = static_cast<Json::UInt64>(result.size);
    json["duration"] = static_cast<Json::Int64>(result.duration.count());
    return json;
}

// Convert ContentMetadata to JSON
inline Json::Value contentMetadataToJson(const ContentMetadata& metadata) {
    Json::Value json;
    json["mimeType"] = metadata.mimeType;
    json["originalName"] = metadata.name;  // Map name -> originalName
    json["description"] = "";  // Not available in current ContentMetadata
    
    // Convert timestamps to ISO8601
    auto timeToString = [](const auto& tp) {
        auto time = std::chrono::system_clock::to_time_t(tp);
        char buffer[100];
        std::strftime(buffer, sizeof(buffer), "%Y-%m-%dT%H:%M:%SZ", std::gmtime(&time));
        return std::string(buffer);
    };
    
    json["createdAt"] = timeToString(metadata.createdAt);
    json["modifiedAt"] = timeToString(metadata.modifiedAt);
    json["accessedAt"] = timeToString(metadata.accessedAt);
    
    // Tags (ContentMetadata.tags is unordered_map<string, string>, not vector)
    Json::Value tags(Json::arrayValue);
    for (const auto& [key, value] : metadata.tags) {
        Json::Value tag;
        tag[key] = value;
        tags.append(tag);
    }
    json["tags"] = tags;
    
    // These fields don't exist in current ContentMetadata - provide defaults
    json["customFields"] = Json::Value(Json::objectValue);
    json["encoding"] = "";
    json["language"] = "";
    json["version"] = 0;
    json["owner"] = "";
    json["permissions"] = "";
    json["checksum"] = metadata.contentHash;  // Use contentHash as checksum
    
    return json;
}

// Convert JSON to ContentMetadata
inline ContentMetadata jsonToContentMetadata(const Json::Value& json) {
    ContentMetadata metadata;
    
    if (json.isMember("mimeType")) {
        metadata.mimeType = json["mimeType"].asString();
    }
    if (json.isMember("originalName")) {
        metadata.name = json["originalName"].asString();  // Map originalName -> name
    }
    // Skip description - not in ContentMetadata
    
    // Parse timestamps (simplified - would use proper date parsing in production)
    auto parseTime = [](const std::string& /*str*/) {
        // For now, just use current time
        return std::chrono::system_clock::now();
    };
    
    if (json.isMember("createdAt")) {
        metadata.createdAt = parseTime(json["createdAt"].asString());
    }
    if (json.isMember("modifiedAt")) {
        metadata.modifiedAt = parseTime(json["modifiedAt"].asString());
    }
    if (json.isMember("accessedAt")) {
        metadata.accessedAt = parseTime(json["accessedAt"].asString());
    }
    
    // Tags (ContentMetadata.tags is unordered_map<string, string>)
    if (json.isMember("tags") && json["tags"].isArray()) {
        for (const auto& tag : json["tags"]) {
            if (tag.isObject() && !tag.getMemberNames().empty()) {
                for (const auto& key : tag.getMemberNames()) {
                    metadata.tags[key] = tag[key].asString();
                }
            }
        }
    }
    
    // Skip customFields, encoding, language, version, owner, permissions - not in ContentMetadata
    
    if (json.isMember("checksum")) {
        metadata.contentHash = json["checksum"].asString();  // Map checksum -> contentHash
    }
    
    return metadata;
}

// Convert ContentStoreStats to JSON
inline Json::Value contentStoreStatsToJson(const ContentStoreStats& stats) {
    Json::Value json;
    json["totalObjects"] = static_cast<Json::UInt64>(stats.totalObjects);
    json["totalBytes"] = static_cast<Json::UInt64>(stats.totalBytes);
    json["uniqueBlocks"] = static_cast<Json::UInt64>(stats.uniqueBlocks);
    json["deduplicatedBytes"] = static_cast<Json::UInt64>(stats.deduplicatedBytes);
    json["dedupRatio"] = stats.dedupRatio();
    json["storeOperations"] = static_cast<Json::UInt64>(stats.storeOperations);
    json["retrieveOperations"] = static_cast<Json::UInt64>(stats.retrieveOperations);
    json["deleteOperations"] = static_cast<Json::UInt64>(stats.deleteOperations);
    
    auto time = std::chrono::system_clock::to_time_t(stats.lastOperation);
    char buffer[100];
    std::strftime(buffer, sizeof(buffer), "%Y-%m-%dT%H:%M:%SZ", std::gmtime(&time));
    json["lastOperation"] = buffer;
    
    return json;
}

// Convert HealthStatus to JSON
inline Json::Value healthStatusToJson(const HealthStatus& health) {
    Json::Value json;
    json["isHealthy"] = health.isHealthy;
    json["status"] = health.status;
    
    Json::Value warnings(Json::arrayValue);
    for (const auto& warning : health.warnings) {
        warnings.append(warning);
    }
    json["warnings"] = warnings;
    
    Json::Value errors(Json::arrayValue);
    for (const auto& error : health.errors) {
        errors.append(error);
    }
    json["errors"] = errors;
    
    auto time = std::chrono::system_clock::to_time_t(health.lastCheck);
    char buffer[100];
    std::strftime(buffer, sizeof(buffer), "%Y-%m-%dT%H:%M:%SZ", std::gmtime(&time));
    json["lastCheck"] = buffer;
    
    return json;
}

// Error response structure
struct ErrorResponse {
    std::string error;
    std::string message;
    int code;
    std::string requestId;
    
    Json::Value toJson() const {
        Json::Value json;
        json["error"] = error;
        json["message"] = message;
        json["code"] = code;
        json["requestId"] = requestId;
        return json;
    }
};

// Store request structure
struct StoreRequest {
    std::string filename;
    std::string mimeType;
    ContentMetadata metadata;
    
    static StoreRequest fromJson(const Json::Value& json) {
        StoreRequest req;
        if (json.isMember("filename")) {
            req.filename = json["filename"].asString();
        }
        if (json.isMember("mimeType")) {
            req.mimeType = json["mimeType"].asString();
        }
        if (json.isMember("metadata")) {
            req.metadata = jsonToContentMetadata(json["metadata"]);
        }
        return req;
    }
};

// Batch store request
struct BatchStoreRequest {
    struct Item {
        std::string filename;
        std::string base64Content;
        ContentMetadata metadata;
    };
    
    std::vector<Item> items;
    
    static BatchStoreRequest fromJson(const Json::Value& json) {
        BatchStoreRequest req;
        if (json.isMember("items") && json["items"].isArray()) {
            for (const auto& item : json["items"]) {
                Item i;
                if (item.isMember("filename")) {
                    i.filename = item["filename"].asString();
                }
                if (item.isMember("content")) {
                    i.base64Content = item["content"].asString();
                }
                if (item.isMember("metadata")) {
                    i.metadata = jsonToContentMetadata(item["metadata"]);
                }
                req.items.push_back(i);
            }
        }
        return req;
    }
};

// Batch delete request
struct BatchDeleteRequest {
    std::vector<std::string> hashes;
    
    static BatchDeleteRequest fromJson(const Json::Value& json) {
        BatchDeleteRequest req;
        if (json.isMember("hashes") && json["hashes"].isArray()) {
            for (const auto& hash : json["hashes"]) {
                req.hashes.push_back(hash.asString());
            }
        }
        return req;
    }
};

// Search request
struct SearchRequest {
    std::optional<std::string> mimeType;
    std::optional<std::string> namePattern;
    std::vector<std::string> tags;
    std::optional<std::string> createdAfter;
    std::optional<std::string> createdBefore;
    std::unordered_map<std::string, std::string> customFields;
    int limit = 100;
    int offset = 0;
    
    static SearchRequest fromQuery(const std::unordered_map<std::string, std::string>& params) {
        SearchRequest req;
        
        auto it = params.find("mimeType");
        if (it != params.end()) {
            req.mimeType = it->second;
        }
        
        it = params.find("namePattern");
        if (it != params.end()) {
            req.namePattern = it->second;
        }
        
        it = params.find("tags");
        if (it != params.end()) {
            // Parse comma-separated tags
            std::stringstream ss(it->second);
            std::string tag;
            while (std::getline(ss, tag, ',')) {
                req.tags.push_back(tag);
            }
        }
        
        it = params.find("limit");
        if (it != params.end()) {
            req.limit = std::stoi(it->second);
        }
        
        it = params.find("offset");
        if (it != params.end()) {
            req.offset = std::stoi(it->second);
        }
        
        return req;
    }
};

} // namespace yams::api::http