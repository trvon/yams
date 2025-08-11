#pragma once

#include <yams/core/types.h>
#include <string>
#include <vector>
#include <unordered_map>
#include <chrono>
#include <optional>

namespace yams::api {

/**
 * @brief Metadata for raw text storage from LLM interactions
 * 
 * This structure captures comprehensive context about the text being stored,
 * enabling intelligent retrieval and search capabilities
 */
struct RawTextMetadata {
    // Core identification
    std::string id;                    // Unique identifier for this text
    std::string source;                // Source system (e.g., "claude", "gpt", "copilot")
    
    // Session context
    std::string chatTitle;             // Title of the chat/conversation
    std::string sessionId;             // Session identifier
    std::string userId;                // User identifier (optional)
    std::chrono::system_clock::time_point timestamp;
    
    // Content context
    std::string contentType;           // Type of content (code, documentation, conversation, etc.)
    std::string language;              // Programming language or natural language
    std::string codebase;              // Associated codebase/project name
    std::string filePath;              // Original file path if applicable
    
    // Interaction context
    std::string userActivity;          // What the user was doing (debugging, implementing, reviewing, etc.)
    std::string llmTask;               // What the LLM was asked to do
    std::vector<std::string> tags;     // User or system-defined tags
    
    // Relationships
    std::string parentId;              // Parent text ID for threaded conversations
    std::vector<std::string> relatedIds; // Related text IDs
    
    // Search optimization
    std::vector<std::string> keywords; // Extracted keywords for search
    float importance = 1.0f;           // Importance/relevance score
    
    // Additional flexible metadata
    std::unordered_map<std::string, std::string> customFields;
    
    // Helper methods
    std::string toXML() const;
    static RawTextMetadata fromXML(const std::string& xml);
    
    // Default constructors
    RawTextMetadata() = default;
    RawTextMetadata(const RawTextMetadata&) = default;
    RawTextMetadata(RawTextMetadata&&) = default;
    RawTextMetadata& operator=(const RawTextMetadata&) = default;
    RawTextMetadata& operator=(RawTextMetadata&&) = default;
};

/**
 * @brief Raw text entry combining content and metadata
 */
struct RawTextEntry {
    std::string content;               // The actual text content
    RawTextMetadata metadata;          // Associated metadata
    
    // Computed fields
    std::string contentHash;           // SHA-256 hash of content
    size_t contentSize = 0;            // Size in bytes
    
    // Serialization
    std::string toXML() const;
    static RawTextEntry fromXML(const std::string& xml);
};

/**
 * @brief Configuration for raw text storage
 */
struct RawTextStorageConfig {
    bool enableCompression = true;     // Compress stored text
    bool enableDeduplication = true;   // Deduplicate identical content
    bool enableFullTextIndex = true;   // Create full-text search index
    bool enableVectorEmbedding = false; // Generate vector embeddings
    
    size_t maxTextSize = 10 * 1024 * 1024; // Max 10MB per text entry
    size_t compressionThreshold = 1024;    // Compress if > 1KB
    
    // Retention policy
    std::chrono::hours retentionPeriod{24 * 365}; // 1 year default
    bool autoCleanup = false;
};

/**
 * @brief Interface for raw text storage operations
 */
class IRawTextStorage {
public:
    virtual ~IRawTextStorage() = default;
    
    // Core operations
    virtual Result<std::string> store(const RawTextEntry& entry) = 0;
    virtual Result<std::string> storeText(const std::string& text, const RawTextMetadata& metadata) = 0;
    virtual Result<RawTextEntry> retrieve(const std::string& id) = 0;
    virtual Result<void> remove(const std::string& id) = 0;
    
    // Batch operations
    virtual Result<std::vector<std::string>> storeBatch(const std::vector<RawTextEntry>& entries) = 0;
    virtual Result<std::vector<RawTextEntry>> retrieveBatch(const std::vector<std::string>& ids) = 0;
    
    // Search operations
    virtual Result<std::vector<RawTextEntry>> searchByKeyword(const std::string& keyword, size_t limit = 100) = 0;
    virtual Result<std::vector<RawTextEntry>> searchByMetadata(const std::unordered_map<std::string, std::string>& criteria) = 0;
    virtual Result<std::vector<RawTextEntry>> fuzzySearch(const std::string& query, float threshold = 0.7f) = 0;
    
    // Session operations
    virtual Result<std::vector<RawTextEntry>> getSessionHistory(const std::string& sessionId) = 0;
    virtual Result<std::vector<RawTextEntry>> getCodebaseHistory(const std::string& codebase) = 0;
    
    // Metadata operations
    virtual Result<void> updateMetadata(const std::string& id, const RawTextMetadata& metadata) = 0;
    virtual Result<std::vector<std::string>> listTags() = 0;
    virtual Result<std::vector<std::string>> listCodebases() = 0;
    
    // Statistics
    virtual Result<size_t> getStorageSize() = 0;
    virtual Result<size_t> getEntryCount() = 0;
};

/**
 * @brief XML serialization utilities for raw text storage
 */
namespace xml {
    std::string escapeXML(const std::string& str);
    std::string unescapeXML(const std::string& str);
    
    std::string serializeMetadata(const RawTextMetadata& metadata);
    RawTextMetadata deserializeMetadata(const std::string& xml);
    
    std::string serializeEntry(const RawTextEntry& entry);
    RawTextEntry deserializeEntry(const std::string& xml);
}

/**
 * @brief Fuzzy search utilities
 */
namespace fuzzy {
    // Calculate Levenshtein distance between two strings
    size_t levenshteinDistance(const std::string& s1, const std::string& s2);
    
    // Calculate similarity score (0.0 to 1.0)
    float similarityScore(const std::string& s1, const std::string& s2);
    
    // Find best matches for a query
    std::vector<std::pair<float, std::string>> findBestMatches(
        const std::string& query,
        const std::vector<std::string>& candidates,
        float threshold = 0.7f,
        size_t maxResults = 10
    );
}

} // namespace yams::api