#include <yams/api/raw_text_storage.h>
#include <yams/compression/compressor_interface.h>
#include <yams/crypto/hasher.h>
#include <yams/storage/storage_engine.h>
// metadata_store.h not needed - using metadata_repository instead
// #include <yams/indexing/text_indexer.h> // File doesn't exist yet

#include <spdlog/spdlog.h>
#include <algorithm>
#include <ctime>
#include <iomanip>
#include <regex>
#include <sstream>

namespace yams::api {

// XML escaping utilities
namespace xml {

std::string escapeXML(const std::string& str) {
    std::string result;
    result.reserve(str.size() * 1.2); // Reserve extra space for escapes

    for (char c : str) {
        switch (c) {
            case '&':
                result += "&amp;";
                break;
            case '<':
                result += "&lt;";
                break;
            case '>':
                result += "&gt;";
                break;
            case '"':
                result += "&quot;";
                break;
            case '\'':
                result += "&apos;";
                break;
            default:
                result += c;
                break;
        }
    }

    return result;
}

std::string unescapeXML(const std::string& str) {
    std::string result = str;

    auto replaceAll = [&result](const std::string& entity, const std::string& replacement) {
        size_t pos = 0;
        while ((pos = result.find(entity, pos)) != std::string::npos) {
            result.replace(pos, entity.size(), replacement);
            pos += replacement.size();
        }
    };

    // Decode ampersands last so literal escaped entities round-trip correctly.
    replaceAll("&lt;", "<");
    replaceAll("&gt;", ">");
    replaceAll("&quot;", "\"");
    replaceAll("&apos;", "'");
    replaceAll("&amp;", "&");

    return result;
}

std::string serializeMetadata(const RawTextMetadata& metadata) {
    std::ostringstream xml;

    xml << "<metadata>\n";
    xml << "  <id>" << escapeXML(metadata.id) << "</id>\n";
    xml << "  <source>" << escapeXML(metadata.source) << "</source>\n";
    xml << "  <chatTitle>" << escapeXML(metadata.chatTitle) << "</chatTitle>\n";
    xml << "  <sessionId>" << escapeXML(metadata.sessionId) << "</sessionId>\n";
    xml << "  <userId>" << escapeXML(metadata.userId) << "</userId>\n";

    // Format timestamp as ISO 8601
    auto timeValue = std::chrono::system_clock::to_time_t(metadata.timestamp);
    std::tm utc{};
#if defined(_WIN32)
    gmtime_s(&utc, &timeValue);
#else
    gmtime_r(&timeValue, &utc);
#endif
    xml << "  <timestamp>" << std::put_time(&utc, "%FT%TZ") << "</timestamp>\n";

    xml << "  <contentType>" << escapeXML(metadata.contentType) << "</contentType>\n";
    xml << "  <language>" << escapeXML(metadata.language) << "</language>\n";
    xml << "  <codebase>" << escapeXML(metadata.codebase) << "</codebase>\n";
    xml << "  <filePath>" << escapeXML(metadata.filePath) << "</filePath>\n";
    xml << "  <userActivity>" << escapeXML(metadata.userActivity) << "</userActivity>\n";
    xml << "  <llmTask>" << escapeXML(metadata.llmTask) << "</llmTask>\n";

    // Tags
    xml << "  <tags>\n";
    for (const auto& tag : metadata.tags) {
        xml << "    <tag>" << escapeXML(tag) << "</tag>\n";
    }
    xml << "  </tags>\n";

    xml << "  <parentId>" << escapeXML(metadata.parentId) << "</parentId>\n";

    // Related IDs
    xml << "  <relatedIds>\n";
    for (const auto& id : metadata.relatedIds) {
        xml << "    <relatedId>" << escapeXML(id) << "</relatedId>\n";
    }
    xml << "  </relatedIds>\n";

    // Keywords
    xml << "  <keywords>\n";
    for (const auto& keyword : metadata.keywords) {
        xml << "    <keyword>" << escapeXML(keyword) << "</keyword>\n";
    }
    xml << "  </keywords>\n";

    xml << "  <importance>" << metadata.importance << "</importance>\n";

    // Custom fields
    xml << "  <customFields>\n";
    for (const auto& [key, value] : metadata.customFields) {
        xml << "    <field name=\"" << escapeXML(key) << "\">" << escapeXML(value) << "</field>\n";
    }
    xml << "  </customFields>\n";

    xml << "</metadata>";

    return xml.str();
}

std::string serializeEntry(const RawTextEntry& entry) {
    std::ostringstream xml;

    xml << "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n";
    xml << "<rawTextEntry>\n";
    xml << "  " << serializeMetadata(entry.metadata) << "\n";
    xml << "  <content><![CDATA[" << entry.content << "]]></content>\n";
    xml << "  <contentHash>" << entry.contentHash << "</contentHash>\n";
    xml << "  <contentSize>" << entry.contentSize << "</contentSize>\n";
    xml << "</rawTextEntry>";

    return xml.str();
}

} // namespace xml

namespace {

std::optional<std::string> extractTagText(const std::string& sourceXml, const std::string& tag) {
    const std::regex tagRegex("<" + tag + ">([\\s\\S]*?)</" + tag + ">");
    std::smatch match;
    if (!std::regex_search(sourceXml, match, tagRegex)) {
        return std::nullopt;
    }
    return xml::unescapeXML(match[1].str());
}

std::vector<std::string> extractRepeatedTagText(const std::string& sourceXml,
                                                const std::string& tag) {
    const std::regex tagRegex("<" + tag + ">([\\s\\S]*?)</" + tag + ">");
    std::vector<std::string> values;
    for (std::sregex_iterator it(sourceXml.begin(), sourceXml.end(), tagRegex), end; it != end;
         ++it) {
        values.push_back(xml::unescapeXML((*it)[1].str()));
    }
    return values;
}

std::unordered_map<std::string, std::string> extractCustomFields(const std::string& sourceXml) {
    const std::regex fieldRegex("<field\\s+name=\"([^\"]*)\">([\\s\\S]*?)</field>");
    std::unordered_map<std::string, std::string> fields;
    for (std::sregex_iterator it(sourceXml.begin(), sourceXml.end(), fieldRegex), end; it != end;
         ++it) {
        fields[xml::unescapeXML((*it)[1].str())] = xml::unescapeXML((*it)[2].str());
    }
    return fields;
}

} // namespace

// RawTextMetadata methods
std::string RawTextMetadata::toXML() const {
    return xml::serializeMetadata(*this);
}

RawTextMetadata RawTextMetadata::fromXML(const std::string& sourceXml) {
    RawTextMetadata metadata;

    auto assignText = [&sourceXml](std::string& target, const std::string& tag) {
        if (auto value = extractTagText(sourceXml, tag)) {
            target = *value;
        }
    };

    assignText(metadata.id, "id");
    assignText(metadata.source, "source");
    assignText(metadata.chatTitle, "chatTitle");
    assignText(metadata.sessionId, "sessionId");
    assignText(metadata.userId, "userId");
    assignText(metadata.contentType, "contentType");
    assignText(metadata.language, "language");
    assignText(metadata.codebase, "codebase");
    assignText(metadata.filePath, "filePath");
    assignText(metadata.userActivity, "userActivity");
    assignText(metadata.llmTask, "llmTask");
    assignText(metadata.parentId, "parentId");

    metadata.tags = extractRepeatedTagText(sourceXml, "tag");
    metadata.relatedIds = extractRepeatedTagText(sourceXml, "relatedId");
    metadata.keywords = extractRepeatedTagText(sourceXml, "keyword");
    metadata.customFields = extractCustomFields(sourceXml);

    if (auto importance = extractTagText(sourceXml, "importance")) {
        try {
            metadata.importance = std::stof(*importance);
        } catch (const std::exception&) {
            // Leave the default importance for malformed legacy metadata.
            metadata.importance = 1.0f;
        }
    }

    return metadata;
}

// RawTextEntry methods
std::string RawTextEntry::toXML() const {
    return xml::serializeEntry(*this);
}

RawTextEntry RawTextEntry::fromXML(const std::string& sourceXml) {
    RawTextEntry entry;

    // Extract metadata section
    std::regex metadataRegex(R"(<metadata>([\s\S]*?)</metadata>)");
    std::smatch match;
    if (std::regex_search(sourceXml, match, metadataRegex)) {
        entry.metadata = RawTextMetadata::fromXML(match[0].str());
    }

    // Extract content from CDATA
    std::regex contentRegex(R"(<content><!\[CDATA\[([\s\S]*?)\]\]></content>)");
    if (std::regex_search(sourceXml, match, contentRegex)) {
        entry.content = match[1].str();
    }

    // Extract hash and size
    std::regex hashRegex("<contentHash>(.*?)</contentHash>");
    if (std::regex_search(sourceXml, match, hashRegex)) {
        entry.contentHash = match[1].str();
    }

    std::regex sizeRegex("<contentSize>(\\d+)</contentSize>");
    if (std::regex_search(sourceXml, match, sizeRegex)) {
        entry.contentSize = std::stoull(match[1].str());
    }

    return entry;
}

// Fuzzy search utilities
namespace fuzzy {

size_t levenshteinDistance(const std::string& s1, const std::string& s2) {
    const size_t m = s1.length();
    const size_t n = s2.length();

    if (m == 0)
        return n;
    if (n == 0)
        return m;

    std::vector<std::vector<size_t>> dp(m + 1, std::vector<size_t>(n + 1));

    for (size_t i = 0; i <= m; ++i) {
        dp[i][0] = i;
    }

    for (size_t j = 0; j <= n; ++j) {
        dp[0][j] = j;
    }

    for (size_t i = 1; i <= m; ++i) {
        for (size_t j = 1; j <= n; ++j) {
            size_t cost = (s1[i - 1] == s2[j - 1]) ? 0 : 1;
            dp[i][j] = std::min({
                dp[i - 1][j] + 1,       // deletion
                dp[i][j - 1] + 1,       // insertion
                dp[i - 1][j - 1] + cost // substitution
            });
        }
    }

    return dp[m][n];
}

float similarityScore(const std::string& s1, const std::string& s2) {
    if (s1.empty() && s2.empty())
        return 1.0f;
    if (s1.empty() || s2.empty())
        return 0.0f;

    size_t distance = levenshteinDistance(s1, s2);
    size_t maxLen = std::max(s1.length(), s2.length());

    return 1.0f - (static_cast<float>(distance) / static_cast<float>(maxLen));
}

std::vector<std::pair<float, std::string>>
findBestMatches(const std::string& query, const std::vector<std::string>& candidates,
                float threshold, size_t maxResults) {
    std::vector<std::pair<float, std::string>> matches;

    for (const auto& candidate : candidates) {
        float score = similarityScore(query, candidate);
        if (score >= threshold) {
            matches.emplace_back(score, candidate);
        }
    }

    // Sort by score (descending)
    std::sort(matches.begin(), matches.end(),
              [](const auto& a, const auto& b) { return a.first > b.first; });

    // Limit results
    if (matches.size() > maxResults) {
        matches.resize(maxResults);
    }

    return matches;
}

} // namespace fuzzy

// Implementation class for RawTextStorage
class RawTextStorage : public IRawTextStorage {
private:
    RawTextStorageConfig config_;
    std::shared_ptr<storage::StorageEngine> storage_;
    // std::shared_ptr<metadata::MetadataStore> metadataStore_;  // TODO: Replace with repository
    // std::shared_ptr<indexing::TextIndexer> textIndexer_;     // TODO: Add when indexing is
    // implemented
    std::unique_ptr<crypto::IContentHasher> hasher_;

public:
    RawTextStorage(const RawTextStorageConfig& config,
                   std::shared_ptr<storage::StorageEngine> storage
                   // TODO: Add metadata and indexer parameters when available
                   )
        : config_(config), storage_(storage), hasher_(crypto::createSHA256Hasher()) {}

    Result<std::string> store(const RawTextEntry& entry) override {
        return storeText(entry.content, entry.metadata);
    }

    Result<std::string> storeText(const std::string& text,
                                  const RawTextMetadata& metadata) override {
        try {
            // Check size limit
            if (text.size() > config_.maxTextSize) {
                return Error{ErrorCode::InvalidArgument, "Text exceeds maximum size limit"};
            }

            // Generate ID if not provided
            std::string id = metadata.id;
            if (id.empty()) {
                // Generate ID from hash of content + timestamp
                hasher_->init();
                hasher_->update(
                    std::span{reinterpret_cast<const std::byte*>(text.data()), text.size()});
                id = hasher_->finalize();
            }

            // Create entry
            RawTextEntry entry;
            entry.content = text;
            entry.metadata = metadata;
            entry.metadata.id = id;
            entry.contentHash = id;
            entry.contentSize = text.size();

            // Serialize to XML
            std::string xmlData = entry.toXML();

            // TODO: Compress xmlData when compressor integration is available.
            const auto* xmlBytes = reinterpret_cast<const std::byte*>(xmlData.data());
            std::vector<std::byte> dataToStore(xmlBytes, xmlBytes + xmlData.size());

            // Store in storage engine
            auto storeResult = storage_->store(id, dataToStore);
            if (!storeResult.has_value()) {
                return Error{storeResult.error().code, storeResult.error().message};
            }

            // TODO: Index for search when indexer is available
            // if (config_.enableFullTextIndex && textIndexer_) {
            //     ...
            // }

            // TODO: Store metadata when metadata store is available
            // if (metadataStore_) {
            //     ...
            // }

            spdlog::info("Stored raw text entry: {} ({} bytes)", id, text.size());
            return id;

        } catch (const std::exception& e) {
            return Error{ErrorCode::Unknown, std::string("Failed to store raw text: ") + e.what()};
        }
    }

    Result<RawTextEntry> retrieve(const std::string& id) override {
        try {
            // Retrieve from storage
            auto retrieveResult = storage_->retrieve(id);
            if (!retrieveResult.has_value()) {
                return Error{retrieveResult.error().code, retrieveResult.error().message};
            }

            // Convert bytes to string
            const auto& data = retrieveResult.value();
            std::string xmlData(reinterpret_cast<const char*>(data.data()), data.size());

            // TODO: Decompress if needed

            // Parse XML
            RawTextEntry entry = RawTextEntry::fromXML(xmlData);

            return entry;

        } catch (const std::exception& e) {
            return Error{ErrorCode::Unknown,
                         std::string("Failed to retrieve raw text: ") + e.what()};
        }
    }

    Result<void> remove(const std::string& id) override {
        // Remove from storage
        auto removeResult = storage_->remove(id);
        if (!removeResult.has_value()) {
            return removeResult;
        }

        // TODO: Remove from index when indexer is available
        // TODO: Remove metadata when metadata store is available

        return Result<void>();
    }

    Result<std::vector<RawTextEntry>> fuzzySearch([[maybe_unused]] const std::string& query,
                                                  [[maybe_unused]] float threshold) override {
        try {
            std::vector<RawTextEntry> results;

            // TODO: Get all document IDs from metadata store when available
            // For now, return empty results
            return results;

        } catch (const std::exception& e) {
            return Error{ErrorCode::Unknown, std::string("Fuzzy search failed: ") + e.what()};
        }
    }

    // Other methods would be implemented similarly...
    Result<std::vector<std::string>> storeBatch(const std::vector<RawTextEntry>& entries) override {
        std::vector<std::string> ids;
        for (const auto& entry : entries) {
            auto result = store(entry);
            if (result.has_value()) {
                ids.push_back(result.value());
            }
        }
        return ids;
    }

    Result<std::vector<RawTextEntry>> retrieveBatch(const std::vector<std::string>& ids) override {
        std::vector<RawTextEntry> entries;
        for (const auto& id : ids) {
            auto result = retrieve(id);
            if (result.has_value()) {
                entries.push_back(result.value());
            }
        }
        return entries;
    }

    Result<std::vector<RawTextEntry>> searchByKeyword([[maybe_unused]] const std::string& keyword,
                                                      [[maybe_unused]] size_t limit) override {
        // Implementation would use the text indexer
        std::vector<RawTextEntry> results;
        // TODO: Implement using textIndexer_
        return results;
    }

    Result<std::vector<RawTextEntry>> searchByMetadata(
        [[maybe_unused]] const std::unordered_map<std::string, std::string>& criteria) override {
        // Implementation would query metadata store
        std::vector<RawTextEntry> results;
        // TODO: Implement using metadataStore_
        return results;
    }

    Result<std::vector<RawTextEntry>> getSessionHistory(const std::string& sessionId) override {
        std::unordered_map<std::string, std::string> criteria;
        criteria["sessionId"] = sessionId;
        return searchByMetadata(criteria);
    }

    Result<std::vector<RawTextEntry>> getCodebaseHistory(const std::string& codebase) override {
        std::unordered_map<std::string, std::string> criteria;
        criteria["codebase"] = codebase;
        return searchByMetadata(criteria);
    }

    Result<void> updateMetadata(const std::string& id, const RawTextMetadata& metadata) override {
        // Retrieve existing entry
        auto entryResult = retrieve(id);
        if (!entryResult.has_value()) {
            return Error{entryResult.error().code, entryResult.error().message};
        }

        // Update metadata
        auto entry = entryResult.value();
        entry.metadata = metadata;
        entry.metadata.id = id; // Preserve ID

        // Re-store
        auto storeResult = store(entry);
        if (!storeResult.has_value()) {
            return Error{ErrorCode::Unknown, "Failed to update metadata"};
        }

        return Result<void>();
    }

    Result<std::vector<std::string>> listTags() override {
        // TODO: Implement tag listing from metadata store
        return std::vector<std::string>{};
    }

    Result<std::vector<std::string>> listCodebases() override {
        // TODO: Implement codebase listing from metadata store
        return std::vector<std::string>{};
    }

    Result<size_t> getStorageSize() override {
        auto stats = storage_->getStorageSize();
        if (stats.has_value()) {
            return stats.value();
        }
        return size_t{0};
    }

    Result<size_t> getEntryCount() override {
        // TODO: Return count from metadata store when available
        return size_t{0};
    }
};

// TODO: Add factory function to create RawTextStorage instances

} // namespace yams::api
