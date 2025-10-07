#include <yams/metadata/path_utils.h>

#include <algorithm>
#include <filesystem>
#include <string_view>
#include <yams/crypto/hasher.h>

namespace yams::metadata {

namespace {
std::string normalizeSlashes(const std::string& path) {
    std::string result = path;
    std::replace(result.begin(), result.end(), '\\', '/');
    // Remove redundant './' segments using filesystem lexically_normal when available.
    try {
        std::filesystem::path p(result);
        auto norm = p.lexically_normal().generic_string();
        if (!norm.empty())
            result = norm;
    } catch (...) {
        // Fall back to slash normalized string if lexically_normal fails (e.g., on non-UTF8).
    }
    return result;
}

std::string computeHashHex(const std::string& value) {
    auto hasher = yams::crypto::createSHA256Hasher();
    return hasher->hash(value);
}

int computeDepth(const std::filesystem::path& path) {
    int depth = 0;
    for (const auto& part : path) {
        if (part.empty())
            continue;
        ++depth;
    }
    return depth;
}
} // namespace

PathDerivedValues computePathDerivedValues(const std::string& filePath) {
    PathDerivedValues out;
    out.normalizedPath = normalizeSlashes(filePath);

    std::filesystem::path fsPath(out.normalizedPath);
    std::string prefix;
    try {
        prefix = fsPath.parent_path().generic_string();
    } catch (...) {
        prefix = {};
    }

    out.pathPrefix = prefix;

    std::string parentForHash = prefix.empty() ? std::string{} : prefix;
    out.parentHash = parentForHash.empty() ? std::string{} : computeHashHex(parentForHash);
    out.reversePath = std::string(out.normalizedPath.rbegin(), out.normalizedPath.rend());
    out.pathHash = computeHashHex(out.normalizedPath);
    out.pathDepth = computeDepth(fsPath);
    return out;
}

} // namespace yams::metadata
