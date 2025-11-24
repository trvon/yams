#include <yams/metadata/path_utils.h>
#include <yams/metadata/tree_builder.h>

#include <spdlog/spdlog.h>
#include <algorithm>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <fmt/format.h>
#include <openssl/sha.h>
#include <yams/storage/storage_engine.h>

namespace fs = std::filesystem;

namespace yams::metadata {

// -----------------------------------------------------------------------------
// TreeEntry
// -----------------------------------------------------------------------------

TreeNode::TreeNode(std::vector<TreeEntry> entries) : entries_(std::move(entries)) {
    // Ensure entries are sorted for canonical representation
    std::sort(entries_.begin(), entries_.end());
}

void TreeNode::addEntry(TreeEntry entry) {
    entries_.push_back(std::move(entry));
    // Invalidate cached hash
    cachedHash_.reset();
    // Keep sorted for canonical representation
    std::sort(entries_.begin(), entries_.end());
}

std::string TreeNode::computeHash() const {
    // Return cached hash if available
    if (cachedHash_) {
        return *cachedHash_;
    }

    // Serialize tree to canonical format
    auto serialized = serialize();

    // Compute SHA-256
    unsigned char hash[SHA256_DIGEST_LENGTH];
    SHA256(serialized.data(), serialized.size(), hash);

    // Convert to hex string
    std::string hexHash;
    hexHash.reserve(SHA256_DIGEST_LENGTH * 2);
    for (int i = 0; i < SHA256_DIGEST_LENGTH; ++i) {
        char buf[3];
        snprintf(buf, sizeof(buf), "%02x", hash[i]);
        hexHash.append(buf);
    }

    cachedHash_ = hexHash;
    return hexHash;
}

std::vector<uint8_t> TreeNode::serialize() const {
    std::vector<uint8_t> result;

    for (const auto& entry : entries_) {
        // Format: "<mode> <name>\0<hash_bytes>"
        // Example: "100644 file.txt\0<32-byte-hash>"

        // Append mode (as ASCII string)
        std::string modeStr = std::to_string(entry.mode);
        result.insert(result.end(), modeStr.begin(), modeStr.end());
        result.push_back(' ');

        // Append name
        result.insert(result.end(), entry.name.begin(), entry.name.end());
        result.push_back('\0');

        // Append hash (convert from hex to binary, 32 bytes)
        if (entry.hash.size() != 64) {
            spdlog::warn("TreeNode::serialize: invalid hash length for entry {}: {}", entry.name,
                         entry.hash.size());
            continue;
        }

        for (size_t i = 0; i < entry.hash.size(); i += 2) {
            unsigned int byte;
            if (sscanf(entry.hash.c_str() + i, "%02x", &byte) != 1) {
                spdlog::warn("TreeNode::serialize: invalid hex in hash for entry {}", entry.name);
                break;
            }
            result.push_back(static_cast<uint8_t>(byte));
        }
    }

    return result;
}

Result<TreeNode> TreeNode::deserialize(std::span<const uint8_t> data) {
    TreeNode node;
    size_t pos = 0;

    while (pos < data.size()) {
        TreeEntry entry;

        // Parse mode (ASCII digits until space)
        std::string modeStr;
        while (pos < data.size() && data[pos] != ' ') {
            modeStr.push_back(static_cast<char>(data[pos++]));
        }
        if (pos >= data.size()) {
            return Error(ErrorCode::InvalidArgument,
                         "Truncated tree data: missing space after mode");
        }
        pos++; // Skip space

        entry.mode = std::stoul(modeStr);
        entry.isDirectory = (entry.mode == 040000);

        // Parse name (until null byte)
        while (pos < data.size() && data[pos] != '\0') {
            entry.name.push_back(static_cast<char>(data[pos++]));
        }
        if (pos >= data.size()) {
            return Error(ErrorCode::InvalidArgument,
                         "Truncated tree data: missing null after name");
        }
        pos++; // Skip null byte

        // Parse hash (32 bytes binary, convert to hex)
        if (pos + 32 > data.size()) {
            return Error(ErrorCode::InvalidArgument, "Truncated tree data: incomplete hash");
        }

        entry.hash.reserve(64);
        for (size_t i = 0; i < 32; ++i) {
            char buf[3];
            snprintf(buf, sizeof(buf), "%02x", data[pos + i]);
            entry.hash.append(buf);
        }
        pos += 32;

        node.addEntry(std::move(entry));
    }

    return node;
}

// -----------------------------------------------------------------------------
// TreeBuilder
// -----------------------------------------------------------------------------

TreeBuilder::TreeBuilder(std::shared_ptr<::yams::storage::IStorageEngine> storageEngine)
    : storageEngine_(std::move(storageEngine)) {
    if (!storageEngine_) {
        throw std::invalid_argument("TreeBuilder: storageEngine cannot be null");
    }
}

Result<std::string>
TreeBuilder::buildFromDirectory(std::string_view directoryPath,
                                const std::vector<std::string>& excludePatterns) {
    if (!fs::exists(directoryPath)) {
        return Error(ErrorCode::NotFound, fmt::format("Directory not found: {}", directoryPath));
    }

    if (!fs::is_directory(directoryPath)) {
        return Error(ErrorCode::InvalidArgument,
                     fmt::format("Path is not a directory: {}", directoryPath));
    }

    std::error_code ec;
    fs::path rootPath = fs::weakly_canonical(fs::path(directoryPath), ec);
    if (ec) {
        rootPath = fs::absolute(fs::path(directoryPath), ec);
        if (ec) {
            rootPath = fs::path(directoryPath);
        }
    }
    std::string normalizedRoot = computePathDerivedValues(rootPath.generic_string()).normalizedPath;

    return buildTreeRecursive(rootPath.generic_string(), excludePatterns, normalizedRoot);
}

Result<std::string> TreeBuilder::buildFromEntries(const std::vector<TreeEntry>& entries) {
    TreeNode node(entries);
    return storeTree(node);
}

Result<TreeNode> TreeBuilder::getTree(std::string_view treeHash) {
    // Retrieve from CAS
    auto dataResult = storageEngine_->retrieve(treeHash);
    if (!dataResult) {
        return Error(dataResult.error());
    }

    // Convert std::byte to uint8_t for deserialize
    const auto& byteData = dataResult.value();
    std::vector<uint8_t> uint8Data(byteData.size());
    std::memcpy(uint8Data.data(), byteData.data(), byteData.size());

    // Deserialize
    return TreeNode::deserialize(uint8Data);
}

Result<bool> TreeBuilder::hasTree(std::string_view treeHash) {
    return storageEngine_->exists(treeHash);
}

Result<std::string> TreeBuilder::buildTreeRecursive(const std::string& dirPath,
                                                    const std::vector<std::string>& excludePatterns,
                                                    const std::string& rootPath) {
    TreeNode node;

    try {
        for (const auto& dirEntry : fs::directory_iterator(dirPath)) {
            const auto& path = dirEntry.path();
            std::string pathStr = path.string();

            // Check exclude patterns
            const bool isDirectory = dirEntry.is_directory();
            if (shouldExclude(pathStr, rootPath, excludePatterns, isDirectory)) {
                spdlog::debug("TreeBuilder: excluding {}", pathStr);
                continue;
            }

            TreeEntry entry;
            entry.name = path.filename().string();

            if (isDirectory) {
                // Recurse into subdirectory
                auto subtreeResult = buildTreeRecursive(pathStr, excludePatterns, rootPath);
                if (!subtreeResult) {
                    return subtreeResult; // Propagate error
                }

                entry.mode = 040000; // Directory mode
                entry.hash = subtreeResult.value();
                entry.isDirectory = true;
                entry.size = 0;

            } else if (dirEntry.is_regular_file()) {
                // Compute file hash
                std::ifstream file(pathStr, std::ios::binary);
                if (!file) {
                    return Error(ErrorCode::IOError,
                                 fmt::format("Failed to open file: {}", pathStr));
                }

                // Read file content
                std::vector<uint8_t> content((std::istreambuf_iterator<char>(file)),
                                             std::istreambuf_iterator<char>());

                // Compute SHA-256
                unsigned char hash[SHA256_DIGEST_LENGTH];
                SHA256(content.data(), content.size(), hash);

                // Convert to hex
                std::string hexHash;
                hexHash.reserve(SHA256_DIGEST_LENGTH * 2);
                for (int i = 0; i < SHA256_DIGEST_LENGTH; ++i) {
                    char buf[3];
                    snprintf(buf, sizeof(buf), "%02x", hash[i]);
                    hexHash.append(buf);
                }

                // Store file content in CAS (convert to std::byte)
                std::span<const std::byte> byteSpan(
                    reinterpret_cast<const std::byte*>(content.data()), content.size());
                auto storeResult = storageEngine_->store(hexHash, byteSpan);
                if (!storeResult) {
                    spdlog::warn("TreeBuilder: failed to store file content for {}: {}", pathStr,
                                 storeResult.error().message);
                }

                entry.mode = static_cast<uint32_t>(dirEntry.status().permissions());
                entry.hash = hexHash;
                entry.isDirectory = false;
                entry.size = static_cast<int64_t>(content.size());

            } else {
                // Skip symlinks, devices, etc.
                spdlog::debug("TreeBuilder: skipping non-regular entry: {}", pathStr);
                continue;
            }

            node.addEntry(std::move(entry));
        }

    } catch (const fs::filesystem_error& e) {
        return Error(ErrorCode::IOError,
                     fmt::format("Filesystem error in {}: {}", dirPath, e.what()));
    }

    // Store tree node and return its hash
    return storeTree(node);
}

Result<std::string> TreeBuilder::storeTree(const TreeNode& node) {
    // Compute tree hash
    std::string treeHash = node.computeHash();

    // Check if already exists (deduplication)
    auto existsResult = storageEngine_->exists(treeHash);
    if (existsResult && existsResult.value()) {
        spdlog::debug("TreeBuilder: tree {} already exists (deduplicated)", treeHash);
        return treeHash;
    }

    // Serialize tree node
    auto serialized = node.serialize();

    // Convert to std::byte span
    std::span<const std::byte> byteSpan(reinterpret_cast<const std::byte*>(serialized.data()),
                                        serialized.size());

    // Store in CAS
    auto storeResult = storageEngine_->store(treeHash, byteSpan);
    if (!storeResult) {
        return Error(storeResult.error());
    }

    spdlog::debug("TreeBuilder: stored tree {} ({} entries, {} bytes)", treeHash, node.size(),
                  serialized.size());

    return treeHash;
}

namespace {
inline bool hasGlobMeta(const std::string& s) {
    return s.find('*') != std::string::npos || s.find('?') != std::string::npos ||
           s.find('[') != std::string::npos;
}

#if !defined(_WIN32)
#include <fnmatch.h>
inline bool fnmatchWrap(const std::string& pat, const std::string& p) {
    // If the pattern uses "**" allow matching across '/'; otherwise respect path boundaries
    int flags = FNM_PERIOD;
    std::string pattern = pat;
    bool hasDoubleStar = pattern.find("**") != std::string::npos;
    if (!hasDoubleStar) {
        flags |= FNM_PATHNAME;
    } else {
        // collapse '**' to '*' for libc fnmatch semantics where '*' may cross '/'
        std::string collapsed;
        collapsed.reserve(pattern.size());
        for (size_t i = 0; i < pattern.size(); ++i) {
            if (pattern[i] == '*' && i + 1 < pattern.size() && pattern[i + 1] == '*') {
                collapsed.push_back('*');
                ++i;
            } else {
                collapsed.push_back(pattern[i]);
            }
        }
        pattern.swap(collapsed);
    }
    return fnmatch(pattern.c_str(), p.c_str(), flags) == 0;
}
#else
// Minimal wildcard matcher for Windows: '*' matches any, '?' matches single char; no '[]'
inline bool simpleGlob(const char* pat, const char* str) {
    if (!pat || !str)
        return false;
    while (*pat) {
        if (*pat == '*') {
            while (*pat == '*')
                ++pat;
            if (!*pat)
                return true;
            for (const char* s = str; *s; ++s) {
                if (simpleGlob(pat, s))
                    return true;
            }
            return false;
        } else if (*pat == '?') {
            if (!*str)
                return false;
            ++pat;
            ++str;
        } else {
            if (*pat != *str)
                return false;
            ++pat;
            ++str;
        }
    }
    return *str == '\0';
}
#endif
} // namespace

bool TreeBuilder::shouldExclude(std::string_view path, std::string_view root,
                                const std::vector<std::string>& patterns, bool isDirectory) const {
    auto derived = computePathDerivedValues(std::string(path));
    const std::string normPath = derived.normalizedPath;

    std::string relativePath;
    if (!root.empty()) {
        std::error_code ec;
        fs::path normFs(normPath);
        fs::path rootFs{std::string(root)};
        auto rel = normFs.lexically_relative(rootFs);
        if (!ec && !rel.empty() && rel != fs::path(".")) {
            relativePath = rel.generic_string();
        } else if (!rel.empty() && rel == fs::path(".")) {
            relativePath.clear();
        } else {
            const std::string rootString = rootFs.generic_string();
            if (normPath.rfind(rootString, 0) == 0) {
                relativePath = normPath.substr(rootString.size());
                if (!relativePath.empty() &&
                    (relativePath.front() == '/' || relativePath.front() == '\\')) {
                    relativePath.erase(relativePath.begin());
                }
            } else {
                relativePath = normPath;
            }
        }
    } else {
        relativePath = normPath;
    }

    auto normalizeSlashes = [](std::string s) {
        std::replace(s.begin(), s.end(), '\\', '/');
        return s;
    };
    auto trimLeadingDot = [](std::string& s) {
        while (s.rfind("./", 0) == 0) {
            s.erase(0, 2);
        }
        if (s == ".") {
            s.clear();
        }
    };

    std::vector<std::string> candidates;
    std::string relNormalized = normalizeSlashes(relativePath);
    trimLeadingDot(relNormalized);
    if (!relNormalized.empty()) {
        candidates.push_back(relNormalized);
    }

    std::string absNormalized = normalizeSlashes(normPath);
    if (candidates.empty() ||
        std::find(candidates.begin(), candidates.end(), absNormalized) == candidates.end()) {
        candidates.push_back(absNormalized);
    }

    if (isDirectory) {
        const std::size_t original = candidates.size();
        for (std::size_t i = 0; i < original; ++i) {
            std::string withSlash = candidates[i];
            if (!withSlash.empty() && withSlash.back() != '/') {
                withSlash.push_back('/');
                if (std::find(candidates.begin(), candidates.end(), withSlash) ==
                    candidates.end()) {
                    candidates.push_back(std::move(withSlash));
                }
            }
        }
    }

    for (const auto& rawPattern : patterns) {
        if (rawPattern.empty())
            continue;
        std::string pattern = normalizeSlashes(rawPattern);
        const bool hasMeta = hasGlobMeta(pattern);
        if (!hasMeta) {
            for (const auto& candidate : candidates) {
                if (candidate.find(pattern) != std::string::npos)
                    return true;
            }
            continue;
        }

        for (const auto& candidate : candidates) {
            if (candidate.empty())
                continue;
#if !defined(_WIN32)
            if (fnmatchWrap(pattern, candidate))
                return true;
#else
            if (simpleGlob(pattern.c_str(), candidate.c_str()))
                return true;
#endif
        }
    }

    return false;
}

} // namespace yams::metadata
