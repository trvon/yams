#include <yams/api/content_metadata.h>

#include <spdlog/spdlog.h>

#include <algorithm>
#include <cstring>
#include <iomanip>
#include <regex>
#include <sstream>

namespace yams::api {

namespace {

// Simple binary serialization format
// Format: [version:4][field_count:4][fields...]
// Field: [type:1][name_len:2][name][data_len:4][data]

enum class FieldType : uint8_t {
    String = 0,
    Timestamp = 1,
    UInt32 = 2,
    StringVector = 3,
    StringMap = 4
};

void writeString(std::vector<std::byte>& buffer, const std::string& str) {
    uint32_t len = static_cast<uint32_t>(str.size());
    buffer.insert(buffer.end(), reinterpret_cast<const std::byte*>(&len),
                  reinterpret_cast<const std::byte*>(&len) + sizeof(len));
    buffer.insert(buffer.end(), reinterpret_cast<const std::byte*>(str.data()),
                  reinterpret_cast<const std::byte*>(str.data()) + str.size());
}

std::string readString(const std::byte*& ptr, const std::byte* end) {
    if (ptr + sizeof(uint32_t) > end) {
        throw std::runtime_error("Buffer underflow reading string length");
    }

    uint32_t len;
    std::memcpy(&len, ptr, sizeof(len));
    ptr += sizeof(len);

    if (ptr + len > end) {
        throw std::runtime_error("Buffer underflow reading string data");
    }

    std::string result(reinterpret_cast<const char*>(ptr), len);
    ptr += len;
    return result;
}

void writeTimestamp(std::vector<std::byte>& buffer,
                    const std::chrono::system_clock::time_point& tp) {
    auto duration = tp.time_since_epoch();
    auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
    buffer.insert(buffer.end(), reinterpret_cast<const std::byte*>(&millis),
                  reinterpret_cast<const std::byte*>(&millis) + sizeof(millis));
}

std::chrono::system_clock::time_point readTimestamp(const std::byte*& ptr, const std::byte* end) {
    if (ptr + sizeof(int64_t) > end) {
        throw std::runtime_error("Buffer underflow reading timestamp");
    }

    int64_t millis;
    std::memcpy(&millis, ptr, sizeof(millis));
    ptr += sizeof(millis);

    return std::chrono::system_clock::time_point(std::chrono::milliseconds(millis));
}

} // anonymous namespace

// Serialization implementation - commented out as not declared in header
// std::vector<std::byte> ContentMetadata::serialize() const {
#if 0
    std::vector<std::byte> buffer;
    
    // Version
    uint32_t version = 1;
    buffer.insert(buffer.end(),
        reinterpret_cast<const std::byte*>(&version),
        reinterpret_cast<const std::byte*>(&version) + sizeof(version));
    
    // Count fields (non-empty only)
    uint32_t fieldCount = 0;
    if (!mimeType.empty()) fieldCount++;
    if (!name.empty()) fieldCount++;
    if (!id.empty()) fieldCount++;
    if (!contentHash.empty()) fieldCount++;
    if (!tags.empty()) fieldCount++;
    fieldCount += 4; // timestamps and size are always included
    
    buffer.insert(buffer.end(),
        reinterpret_cast<const std::byte*>(&fieldCount),
        reinterpret_cast<const std::byte*>(&fieldCount) + sizeof(fieldCount));
    
    // Write fields
    auto writeField = [&buffer](FieldType type, const std::string& name, 
                               const auto& writeFunc) {
        buffer.push_back(static_cast<std::byte>(type));
        uint16_t nameLen = static_cast<uint16_t>(name.size());
        buffer.insert(buffer.end(),
            reinterpret_cast<const std::byte*>(&nameLen),
            reinterpret_cast<const std::byte*>(&nameLen) + sizeof(nameLen));
        buffer.insert(buffer.end(),
            reinterpret_cast<const std::byte*>(name.data()),
            reinterpret_cast<const std::byte*>(name.data()) + name.size());
        writeFunc();
    };
    
    // String fields
    if (!mimeType.empty()) {
        writeField(FieldType::String, "mimeType", [&]() { writeString(buffer, mimeType); });
    }
    if (!name.empty()) {
        writeField(FieldType::String, "name", [&]() { writeString(buffer, name); });
    }
    if (!id.empty()) {
        writeField(FieldType::String, "id", [&]() { writeString(buffer, id); });
    }
    if (!contentHash.empty()) {
        writeField(FieldType::String, "contentHash", [&]() { writeString(buffer, contentHash); });
    }
    
    // Timestamps
    writeField(FieldType::Timestamp, "createdAt", [&]() { writeTimestamp(buffer, createdAt); });
    writeField(FieldType::Timestamp, "modifiedAt", [&]() { writeTimestamp(buffer, modifiedAt); });
    writeField(FieldType::Timestamp, "accessedAt", [&]() { writeTimestamp(buffer, accessedAt); });
    
    // Size
    writeField(FieldType::UInt32, "size", [&]() {
        buffer.insert(buffer.end(),
            reinterpret_cast<const std::byte*>(&this->size),
            reinterpret_cast<const std::byte*>(&this->size) + sizeof(this->size));
    });
    
    
    // Tags are stored as a map
    if (!tags.empty()) {
        writeField(FieldType::StringMap, "tags", [&]() {
            uint32_t mapSize = static_cast<uint32_t>(tags.size());
            buffer.insert(buffer.end(),
                reinterpret_cast<const std::byte*>(&mapSize),
                reinterpret_cast<const std::byte*>(&mapSize) + sizeof(mapSize));
            for (const auto& [key, value] : tags) {
                writeString(buffer, key);
                writeString(buffer, value);
            }
        });
    }
    
    return buffer;
}
#endif

// Deserialization implementation
Result<ContentMetadata> ContentMetadata::deserialize(std::span<const std::byte> data) {
    try {
        ContentMetadata metadata;
        const std::byte* ptr = data.data();
        const std::byte* end = ptr + data.size();

        // Read version
        if (ptr + sizeof(uint32_t) > end) {
            return Result<ContentMetadata>(ErrorCode::CorruptedData);
        }
        uint32_t version;
        std::memcpy(&version, ptr, sizeof(version));
        ptr += sizeof(version);

        if (version != 1) {
            return Result<ContentMetadata>(ErrorCode::InvalidArgument);
        }

        // Read field count
        if (ptr + sizeof(uint32_t) > end) {
            return Result<ContentMetadata>(ErrorCode::CorruptedData);
        }
        uint32_t fieldCount;
        std::memcpy(&fieldCount, ptr, sizeof(fieldCount));
        ptr += sizeof(fieldCount);

        // Read fields
        for (uint32_t i = 0; i < fieldCount; ++i) {
            if (ptr + 1 + sizeof(uint16_t) > end) {
                return Result<ContentMetadata>(ErrorCode::CorruptedData);
            }

            FieldType type = static_cast<FieldType>(*ptr++);

            uint16_t nameLen;
            std::memcpy(&nameLen, ptr, sizeof(nameLen));
            ptr += sizeof(nameLen);

            if (ptr + nameLen > end) {
                return Result<ContentMetadata>(ErrorCode::CorruptedData);
            }

            std::string fieldName(reinterpret_cast<const char*>(ptr), nameLen);
            ptr += nameLen;

            // Read field data based on type
            switch (type) {
                case FieldType::String: {
                    auto value = readString(ptr, end);
                    if (fieldName == "mimeType")
                        metadata.mimeType = value;
                    else if (fieldName == "name")
                        metadata.name = value;
                    else if (fieldName == "id")
                        metadata.id = value;
                    else if (fieldName == "contentHash")
                        metadata.contentHash = value;
                    break;
                }

                case FieldType::Timestamp: {
                    auto value = readTimestamp(ptr, end);
                    if (fieldName == "createdAt")
                        metadata.createdAt = value;
                    else if (fieldName == "modifiedAt")
                        metadata.modifiedAt = value;
                    else if (fieldName == "accessedAt")
                        metadata.accessedAt = value;
                    break;
                }

                case FieldType::UInt32: {
                    if (ptr + sizeof(uint32_t) > end) {
                        return Result<ContentMetadata>(ErrorCode::CorruptedData);
                    }
                    if (fieldName == "size") {
                        uint64_t value;
                        std::memcpy(&value, ptr, sizeof(uint32_t));
                        ptr += sizeof(uint32_t);
                        metadata.size = value;
                    } else {
                        ptr += sizeof(uint32_t);
                    }
                    break;
                }

                case FieldType::StringVector: {
                    if (ptr + sizeof(uint32_t) > end) {
                        return Result<ContentMetadata>(ErrorCode::CorruptedData);
                    }
                    uint32_t count;
                    std::memcpy(&count, ptr, sizeof(count));
                    ptr += sizeof(count);

                    // Skip unknown vector fields
                    for (uint32_t j = 0; j < count; ++j) {
                        readString(ptr, end);
                    }
                    break;
                }

                case FieldType::StringMap: {
                    if (ptr + sizeof(uint32_t) > end) {
                        return Result<ContentMetadata>(ErrorCode::CorruptedData);
                    }
                    uint32_t count;
                    std::memcpy(&count, ptr, sizeof(count));
                    ptr += sizeof(count);

                    if (fieldName == "tags") {
                        metadata.tags.clear();
                        for (uint32_t j = 0; j < count; ++j) {
                            auto key = readString(ptr, end);
                            auto value = readString(ptr, end);
                            metadata.tags[key] = value;
                        }
                    } else {
                        // Skip unknown map fields
                        for (uint32_t j = 0; j < count; ++j) {
                            readString(ptr, end);
                            readString(ptr, end);
                        }
                    }
                    break;
                }
            }
        }

        return metadata;

    } catch (const std::exception& e) {
        spdlog::error("Failed to deserialize metadata: {}", e.what());
        return Result<ContentMetadata>(ErrorCode::CorruptedData);
    }
}

// JSON serialization
std::string ContentMetadata::toJson() const {
    std::ostringstream oss;
    oss << "{\n";

    // Helper to format timestamp
    auto formatTime = [](const auto& tp) {
        auto time = std::chrono::system_clock::to_time_t(tp);
        std::ostringstream timeStr;
        timeStr << std::put_time(std::gmtime(&time), "%Y-%m-%dT%H:%M:%SZ");
        return timeStr.str();
    };

    // Helper to escape JSON string
    auto escapeJson = [](const std::string& str) {
        std::string result;
        for (char c : str) {
            switch (c) {
                case '"':
                    result += "\\\"";
                    break;
                case '\\':
                    result += "\\\\";
                    break;
                case '\b':
                    result += "\\b";
                    break;
                case '\f':
                    result += "\\f";
                    break;
                case '\n':
                    result += "\\n";
                    break;
                case '\r':
                    result += "\\r";
                    break;
                case '\t':
                    result += "\\t";
                    break;
                default:
                    result += c;
            }
        }
        return result;
    };

    // Write fields
    oss << "  \"id\": \"" << escapeJson(id) << "\",\n";
    oss << "  \"name\": \"" << escapeJson(name) << "\",\n";
    oss << "  \"size\": " << size << ",\n";
    oss << "  \"mimeType\": \"" << escapeJson(mimeType) << "\",\n";
    oss << "  \"contentHash\": \"" << escapeJson(contentHash) << "\",\n";
    oss << "  \"createdAt\": \"" << formatTime(createdAt) << "\",\n";
    oss << "  \"modifiedAt\": \"" << formatTime(modifiedAt) << "\",\n";
    oss << "  \"accessedAt\": \"" << formatTime(accessedAt) << "\",\n";

    // Tags (as key-value pairs)
    oss << "  \"tags\": {\n";
    bool first = true;
    for (const auto& [key, value] : tags) {
        if (!first)
            oss << ",\n";
        oss << "    \"" << escapeJson(key) << "\": \"" << escapeJson(value) << "\"";
        first = false;
    }
    oss << "\n  }\n";

    oss << "}";
    return oss.str();
}

// JSON deserialization (simplified - would use a proper JSON parser in production)
Result<ContentMetadata> ContentMetadata::fromJson(const std::string& json) {
    try {
        ContentMetadata metadata;

        // Simple regex-based parsing for test purposes
        // In production, use a proper JSON library

        // Extract string fields
        auto extractString = [&json](const std::string& key) -> std::string {
            std::regex pattern("\"" + key + "\"\\s*:\\s*\"([^\"]+)\"");
            std::smatch match;
            if (std::regex_search(json, match, pattern)) {
                return match[1];
            }
            return "";
        };

        // Extract number fields
        auto extractNumber = [&json](const std::string& key) -> uint64_t {
            std::regex pattern("\"" + key + "\"\\s*:\\s*(\\d+)");
            std::smatch match;
            if (std::regex_search(json, match, pattern)) {
                return std::stoull(match[1]);
            }
            return 0;
        };

        // Extract basic fields
        metadata.id = extractString("id");
        metadata.name = extractString("name");
        metadata.mimeType = extractString("mimeType");
        metadata.contentHash = extractString("contentHash");
        metadata.size = extractNumber("size");

        // Extract timestamps (parse ISO format)
        auto parseTime = [&extractString](const std::string& key) {
            auto timeStr = extractString(key);
            if (timeStr.empty()) {
                return std::chrono::system_clock::now();
            }
            // Simple parsing - assumes format "YYYY-MM-DDTHH:MM:SSZ"
            std::tm tm = {};
            std::istringstream ss(timeStr);
            ss >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%SZ");
            return std::chrono::system_clock::from_time_t(std::mktime(&tm));
        };

        metadata.createdAt = parseTime("createdAt");
        metadata.modifiedAt = parseTime("modifiedAt");
        metadata.accessedAt = parseTime("accessedAt");

        // Extract tags object
        std::regex tagsPattern("\"tags\"\\s*:\\s*\\{([^}]*)\\}");
        std::smatch tagsMatch;
        if (std::regex_search(json, tagsMatch, tagsPattern)) {
            std::string tagsContent = tagsMatch[1];

            // Parse individual tag key-value pairs
            std::regex tagPattern("\"([^\"]+)\"\\s*:\\s*\"([^\"]+)\"");
            std::sregex_iterator it(tagsContent.begin(), tagsContent.end(), tagPattern);
            std::sregex_iterator end;

            for (; it != end; ++it) {
                metadata.tags[(*it)[1]] = (*it)[2];
            }
        }

        return metadata;

    } catch (const std::exception& e) {
        spdlog::error("Failed to parse JSON metadata: {}", e.what());
        return Result<ContentMetadata>(ErrorCode::InvalidArgument);
    }
}

// MetadataQuery implementation
bool MetadataQuery::matches(const ContentMetadata& metadata) const {
    // Check MIME type
    if (mimeType && metadata.mimeType != *mimeType) {
        return false;
    }

    // Check name pattern
    if (namePattern) {
        std::regex pattern(*namePattern);
        if (!std::regex_match(metadata.name, pattern)) {
            return false;
        }
    }

    // Check required tags (tags is now a map)
    for (const auto& tag : requiredTags) {
        if (metadata.tags.find(tag) == metadata.tags.end()) {
            return false;
        }
    }

    // Check any tags
    if (!anyTags.empty()) {
        bool found = false;
        for (const auto& tag : anyTags) {
            if (metadata.tags.find(tag) != metadata.tags.end()) {
                found = true;
                break;
            }
        }
        if (!found) {
            return false;
        }
    }

    // Check exclude tags
    for (const auto& tag : excludeTags) {
        if (metadata.tags.find(tag) != metadata.tags.end()) {
            return false;
        }
    }

    // Check timestamps
    if (createdAfter && metadata.createdAt < *createdAfter) {
        return false;
    }
    if (createdBefore && metadata.createdAt > *createdBefore) {
        return false;
    }
    if (modifiedAfter && metadata.modifiedAt < *modifiedAfter) {
        return false;
    }
    if (modifiedBefore && metadata.modifiedAt > *modifiedBefore) {
        return false;
    }

    // Check custom fields (now checking tags instead)
    for (const auto& [key, value] : customFieldMatches) {
        auto it = metadata.tags.find(key);
        if (it == metadata.tags.end() || it->second != value) {
            return false;
        }
    }

    return true;
}

} // namespace yams::api