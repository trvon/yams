#include <yams/detection/file_type_detector.h>

#include <nlohmann/json.hpp>

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <mutex>
#include <sstream>

#ifdef __linux__
#include <unistd.h>
#include <linux/limits.h>
#endif

#ifdef __APPLE__
#include <mach-o/dyld.h>
#endif

#ifdef YAMS_HAS_LIBMAGIC
#include <magic.h>
#endif

namespace yams::detection {

namespace {
// Extension to MIME type mapping
const std::unordered_map<std::string, std::string> EXTENSION_MIME_MAP = {
    // Text formats
    {".txt", "text/plain"},
    {".html", "text/html"},
    {".htm", "text/html"},
    {".css", "text/css"},
    {".js", "application/javascript"},
    {".json", "application/json"},
    {".xml", "application/xml"},
    {".yaml", "application/x-yaml"},
    {".yml", "application/x-yaml"},
    {".md", "text/markdown"},
    {".csv", "text/csv"},

    // Documents
    {".pdf", "application/pdf"},
    {".doc", "application/msword"},
    {".docx", "application/vnd.openxmlformats-officedocument.wordprocessingml.document"},
    {".xls", "application/vnd.ms-excel"},
    {".xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"},
    {".ppt", "application/vnd.ms-powerpoint"},
    {".pptx", "application/vnd.openxmlformats-officedocument.presentationml.presentation"},

    // Images
    {".jpg", "image/jpeg"},
    {".jpeg", "image/jpeg"},
    {".png", "image/png"},
    {".gif", "image/gif"},
    {".bmp", "image/bmp"},
    {".svg", "image/svg+xml"},
    {".webp", "image/webp"},
    {".ico", "image/x-icon"},

    // Audio/Video
    {".mp3", "audio/mpeg"},
    {".wav", "audio/wav"},
    {".ogg", "audio/ogg"},
    {".m4a", "audio/mp4"},
    {".mp4", "video/mp4"},
    {".avi", "video/x-msvideo"},
    {".mkv", "video/x-matroska"},
    {".webm", "video/webm"},
    {".mov", "video/quicktime"},

    // Archives
    {".zip", "application/zip"},
    {".tar", "application/x-tar"},
    {".gz", "application/gzip"},
    {".bz2", "application/x-bzip2"},
    {".7z", "application/x-7z-compressed"},
    {".rar", "application/x-rar-compressed"},
    {".xz", "application/x-xz"},

    // Programming
    {".cpp", "text/x-c++"},
    {".c", "text/x-c"},
    {".h", "text/x-c"},
    {".hpp", "text/x-c++"},
    {".py", "text/x-python"},
    {".java", "text/x-java"},
    {".rs", "text/x-rust"},
    {".go", "text/x-go"},
    {".sh", "application/x-sh"},
    {".bat", "application/x-msdos-program"},

    // Executables
    {".exe", "application/x-msdownload"},
    {".dll", "application/x-msdownload"},
    {".so", "application/x-sharedlib"},
    {".dylib", "application/x-sharedlib"},
    {".class", "application/java-vm"},
    {".jar", "application/java-archive"}};
} // namespace

class FileTypeDetector::Impl {
public:
    FileTypeDetectorConfig config;
    std::vector<FilePattern> patterns;
    mutable std::mutex patternsMutex;

    // Cache for detection results
    struct CacheEntry {
        std::string hash;
        FileSignature signature;
    };
    mutable std::unordered_map<std::string, FileSignature> cache;
    mutable std::mutex cacheMutex;
    mutable CacheStats cacheStats;

    // Classification maps built from patterns
    std::unordered_map<std::string, std::string> mimeToFileType;
    std::unordered_map<std::string, bool> mimeToIsBinary;

#ifdef YAMS_HAS_LIBMAGIC
    magic_t magicCookie = nullptr;
    mutable std::mutex magicMutex; // Protect libmagic operations (not thread-safe)
#endif

    Impl() = default;

    ~Impl() {
#ifdef YAMS_HAS_LIBMAGIC
        if (magicCookie) {
            std::lock_guard<std::mutex> lock(magicMutex);
            magic_close(magicCookie);
        }
#endif
    }

    Result<void> initializeLibMagic() {
#ifdef YAMS_HAS_LIBMAGIC
        std::lock_guard<std::mutex> lock(magicMutex); // Protect initialization

        magicCookie = magic_open(MAGIC_MIME_TYPE | MAGIC_ERROR);
        if (!magicCookie) {
            return Error{ErrorCode::InternalError, "Failed to initialize libmagic"};
        }

        if (magic_load(magicCookie, nullptr) != 0) {
            std::string error = magic_error(magicCookie);
            magic_close(magicCookie);
            magicCookie = nullptr;
            return Error{ErrorCode::InternalError, "Failed to load magic database: " + error};
        }

        // Debug: Initialized libmagic successfully
#endif
        return {};
    }

    Result<FileSignature> detectWithLibMagic(std::span<const std::byte> data [[maybe_unused]]) {
#ifdef YAMS_HAS_LIBMAGIC
        std::lock_guard<std::mutex> lock(magicMutex); // Protect libmagic calls

        if (!magicCookie) {
            return Error{ErrorCode::NotInitialized, "libmagic not initialized"};
        }

        const char* mimeType = magic_buffer(magicCookie, data.data(), data.size());
        if (!mimeType) {
            return Error{ErrorCode::InternalError,
                         "libmagic detection failed: " + std::string(magic_error(magicCookie))};
        }

        FileSignature sig;
        sig.mimeType = mimeType;
        sig.magicNumber = FileTypeDetector::bytesToHex(data, 16);
        // Use the instance methods to ensure consistency
        sig.isBinary = FileTypeDetector::instance().isBinaryMimeType(mimeType);
        sig.fileType = FileTypeDetector::instance().getFileTypeCategory(mimeType);
        sig.confidence = 0.95f; // High confidence from libmagic

        // Set description based on file type
        if (sig.fileType == "image") {
            sig.description = "Image file";
        } else if (sig.fileType == "video") {
            sig.description = "Video file";
        } else if (sig.fileType == "audio") {
            sig.description = "Audio file";
        } else if (sig.fileType == "text" || sig.fileType == "code") {
            sig.description = "Text file";
        } else if (sig.fileType == "archive") {
            sig.description = "Archive file";
        } else if (sig.fileType == "document") {
            sig.description = "Document file";
        } else if (sig.fileType == "application") {
            sig.description = "Application file";
        } else {
            sig.description = "Unknown file type";
        }

        return sig;
#else
        // Silence unused parameter warning when libmagic support is not compiled in
        (void)data;
        return Error{ErrorCode::NotSupported, "libmagic not available"};
#endif
    }

    Result<FileSignature> detectWithPatterns(std::span<const std::byte> data) {
        std::lock_guard<std::mutex> lock(patternsMutex);

        auto match_pass = [&](bool exclude_generic) -> std::optional<FileSignature> {
            FileSignature bestMatch;
            float bestConfidence = 0.0f;

            for (const auto& pattern : patterns) {
                // Optionally exclude overly generic patterns (like debug/octet-stream)
                if (exclude_generic) {
                    if (pattern.mimeType == "application/octet-stream" ||
                        pattern.fileType == "debug" || pattern.fileType == "binary") {
                        continue;
                    }
                }

                // Check if we have enough data for this pattern
                if (data.size() < pattern.offset + pattern.pattern.size()) {
                    continue;
                }

                // Compare pattern at specified offset
                bool matches = true;
                for (size_t i = 0; i < pattern.pattern.size(); ++i) {
                    if (data[pattern.offset + i] != pattern.pattern[i]) {
                        matches = false;
                        break;
                    }
                }

                if (matches && pattern.confidence > bestConfidence) {
                    bestMatch.mimeType = pattern.mimeType;
                    bestMatch.fileType = pattern.fileType;
                    bestMatch.description = pattern.description;
                    bestMatch.magicNumber = pattern.patternHex;
                    bestMatch.confidence = pattern.confidence;
                    bestMatch.isBinary = true;
                    bestConfidence = pattern.confidence;
                }
            }

            if (bestConfidence > 0.0f) {
                return bestMatch;
            }
            return std::nullopt;
        };

        // First try matching with generic patterns excluded to prefer specific signatures
        if (auto res = match_pass(true)) {
            return *res;
        }
        // Fallback to full pass including generic patterns
        if (auto res = match_pass(false)) {
            return *res;
        }

        return Error{ErrorCode::NotFound, "No matching pattern found"};
    }
};

FileTypeDetector::FileTypeDetector() : pImpl(std::make_unique<Impl>()) {}

FileTypeDetector::~FileTypeDetector() = default;

FileTypeDetector& FileTypeDetector::instance() {
    static FileTypeDetector instance;
    return instance;
}

Result<void> FileTypeDetector::initialize(const FileTypeDetectorConfig& config) {
    // Reset internal state to ensure clean initialization between runs/tests
    pImpl->config = config;
    {
        // Clear pattern database and classification maps
        std::lock_guard<std::mutex> lock_patterns(pImpl->patternsMutex);
        pImpl->patterns.clear();
        pImpl->mimeToFileType.clear();
        pImpl->mimeToIsBinary.clear();
    }
    {
        // Clear cache to avoid stale detections carrying over between configurations
        std::lock_guard<std::mutex> lock_cache(pImpl->cacheMutex);
        pImpl->cache.clear();
        pImpl->cacheStats = {};
    }
    pImpl->cacheStats.maxSize = config.cacheSize;

    // Initialize libmagic if requested
    if (config.useLibMagic) {
        auto result = pImpl->initializeLibMagic();
        if (!result && result.error().code != ErrorCode::NotSupported) {
            // Warning: Failed to initialize libmagic
        }
    }

    // Load built-in patterns
    if (config.useBuiltinPatterns) {
        // Load built-in patterns into a fresh vector
        pImpl->patterns = getDefaultPatterns();
        // Debug: Loaded built-in file patterns

        // Build classification maps from built-in patterns
        for (const auto& pattern : pImpl->patterns) {
            if (!pattern.mimeType.empty()) {
                pImpl->mimeToFileType[pattern.mimeType] = pattern.fileType;

                bool isBinary = true;
                if (pattern.fileType == "text" || pattern.mimeType.find("text/") == 0 ||
                    pattern.mimeType == "application/json" ||
                    pattern.mimeType == "application/xml" ||
                    pattern.mimeType == "application/javascript" ||
                    pattern.mimeType == "application/x-yaml" ||
                    pattern.mimeType == "application/yaml" ||
                    pattern.mimeType == "application/x-sh") {
                    isBinary = false;
                }
                pImpl->mimeToIsBinary[pattern.mimeType] = isBinary;
            }
        }
    }

    // Load custom patterns from file if specified
    if (config.useCustomPatterns && !config.patternsFile.empty()) {
        auto result = loadPatternsFromFile(config.patternsFile);
        if (!result) {
            // Warning: Failed to load custom patterns
        }
    }

    // Always populate extension-based MIME mappings
    for (const auto& [ext, mime] : EXTENSION_MIME_MAP) {
        if (pImpl->mimeToFileType.find(mime) == pImpl->mimeToFileType.end()) {
            std::string fileType = "binary";

            // Check for code/programming file types
            if (mime.find("text/x-c") == 0 || mime.find("text/x-python") == 0 ||
                mime.find("text/x-java") == 0 || mime.find("text/x-rust") == 0 ||
                mime.find("text/x-go") == 0 || mime.find("text/x-ruby") == 0 ||
                mime.find("text/x-perl") == 0 || mime.find("text/x-php") == 0 ||
                mime == "application/javascript" || mime == "application/x-sh" ||
                mime == "application/x-perl" || mime == "application/x-python" ||
                mime == "application/x-ruby") {
                fileType = "code";
            } else if (mime.find("text/") == 0)
                fileType = "text";
            else if (mime.find("image/") == 0)
                fileType = "image";
            else if (mime.find("video/") == 0)
                fileType = "video";
            else if (mime.find("audio/") == 0)
                fileType = "audio";
            else if (mime.find("application/pdf") == 0 || mime.find("application/msword") == 0 ||
                     mime.find("application/vnd.") == 0)
                fileType = "document";
            else if (mime.find("zip") != std::string::npos ||
                     mime.find("tar") != std::string::npos ||
                     mime.find("compressed") != std::string::npos ||
                     mime.find("archive") != std::string::npos)
                fileType = "archive";
            else if (mime == "application/json" || mime == "application/xml" ||
                     mime == "application/x-yaml" || mime == "application/yaml")
                fileType = "text";

            pImpl->mimeToFileType[mime] = fileType;
            pImpl->mimeToIsBinary[mime] = (fileType != "text" && fileType != "code");
        }
    }

    return {};
}

Result<void> FileTypeDetector::initializeWithMagicNumbers() {
    // Serialize initialization across threads and make it idempotent on success.
    // We want to avoid concurrent initialize() calls (which reset internal state and may
    // touch libmagic) while still allowing retries if a previous attempt failed.
    static std::mutex initMutex;
    static bool initialized = false;
    std::lock_guard<std::mutex> lk(initMutex);
    if (initialized) {
        return Result<void>();
    }

    // Find the magic_numbers.json file
    auto magicNumbersPath = findMagicNumbersFile();

    // Create configuration
    FileTypeDetectorConfig config;

    if (!magicNumbersPath.empty()) {
        config.patternsFile = magicNumbersPath;
        config.useCustomPatterns = true;
    } else {
        config.useCustomPatterns = false;
    }

    // Always use built-in patterns as fallback
    config.useBuiltinPatterns = true;
    config.cacheResults = true;
    config.cacheSize = 1000;

    // Initialize the singleton instance
    auto res = instance().initialize(config);
    if (res) {
        initialized = true;
    }
    return res;
}

std::filesystem::path FileTypeDetector::findMagicNumbersFile() {
    namespace fs = std::filesystem;

    std::vector<fs::path> searchPaths;

    // 1. Check environment variable
    if (const char* dataDir = std::getenv("YAMS_DATA_DIR")) {
        searchPaths.push_back(fs::path(dataDir) / "magic_numbers.json");
    }

    // 2. Check relative to executable location (for installed binaries)
    try {
        // Get the path to the current executable
#ifdef __linux__
        char result[PATH_MAX];
        ssize_t count = readlink("/proc/self/exe", result, PATH_MAX);
        if (count != -1) {
            fs::path exePath(std::string(result, count));
            // Check ../share/yams/data relative to binary
            searchPaths.push_back(exePath.parent_path().parent_path() / "share" / "yams" / "data" /
                                  "magic_numbers.json");
        }
#elif defined(__APPLE__)
        char path[1024];
        uint32_t size = sizeof(path);
        if (_NSGetExecutablePath(path, &size) == 0) {
            fs::path exePath(path);
            // Check ../share/yams/data relative to binary
            searchPaths.push_back(exePath.parent_path().parent_path() / "share" / "yams" / "data" /
                                  "magic_numbers.json");
        }
#endif
    } catch (...) {
        // Ignore errors in getting executable path
    }

    // 3. Check common installation paths
    searchPaths.push_back("/usr/local/share/yams/data/magic_numbers.json");
    searchPaths.push_back("/usr/share/yams/data/magic_numbers.json");
    searchPaths.push_back("/opt/yams/share/data/magic_numbers.json");

    // 4. Check relative to current working directory (for development)
    searchPaths.push_back("data/magic_numbers.json");
    searchPaths.push_back("../data/magic_numbers.json");
    searchPaths.push_back("../../data/magic_numbers.json");
    searchPaths.push_back("../../../data/magic_numbers.json");
    searchPaths.push_back("../../../../data/magic_numbers.json");

    // 5. Check in home directory
    if (const char* home = std::getenv("HOME")) {
        searchPaths.push_back(fs::path(home) / ".local" / "share" / "yams" / "data" /
                              "magic_numbers.json");
    }

    // Find the first existing file
    for (const auto& path : searchPaths) {
        if (fs::exists(path) && fs::is_regular_file(path)) {
            // Found magic_numbers.json at this path
            return path;
        }
    }

    // magic_numbers.json not found in any standard location
    return {};
}

Result<FileSignature> FileTypeDetector::detectFromBuffer(std::span<const std::byte> data) {
    if (data.empty()) {
        return Error{ErrorCode::InvalidArgument, "Empty buffer"};
    }

    // Check cache if enabled
    if (pImpl->config.cacheResults) {
        std::string cacheKey = bytesToHex(data, std::min(data.size(), size_t(32)));

        std::lock_guard<std::mutex> lock(pImpl->cacheMutex);
        auto it = pImpl->cache.find(cacheKey);
        if (it != pImpl->cache.end()) {
            pImpl->cacheStats.hits++;
            return it->second;
        }
        pImpl->cacheStats.misses++;
    }

    FileSignature result;
    bool detected = false;

    // Try libmagic first if available
#ifdef YAMS_HAS_LIBMAGIC
    if (pImpl->config.useLibMagic && pImpl->magicCookie) {
        auto magicResult = pImpl->detectWithLibMagic(data);
        if (magicResult) {
            // Treat overly generic MIME types from libmagic as provisional only
            const auto& m = magicResult.value().mimeType;
            bool isGeneric = (m == "application/octet-stream");
            if (!isGeneric) {
                result = magicResult.value();
                detected = true;
            }
        }
    }
#endif

    // Try pattern matching if libmagic failed or not available
    if (!detected && pImpl->config.useBuiltinPatterns) {
        auto patternResult = pImpl->detectWithPatterns(data);
        if (patternResult) {
            result = patternResult.value();
            detected = true;
        }
    }

    // Fallback to binary/text detection
    if (!detected) {
        result.isBinary = isBinaryData(data);
        result.mimeType = result.isBinary ? "application/octet-stream" : "text/plain";
        result.fileType = result.isBinary ? "binary" : "text";
        result.description = result.isBinary ? "Binary file" : "Text file";
        result.magicNumber = extractMagicNumber(data);
        result.confidence = 0.3f; // Low confidence for fallback
    }

    // Cache result if enabled
    if (pImpl->config.cacheResults && detected) {
        std::string cacheKey = bytesToHex(data, std::min(data.size(), size_t(32)));

        std::lock_guard<std::mutex> lock(pImpl->cacheMutex);
        if (pImpl->cache.size() >= pImpl->config.cacheSize) {
            // Simple LRU: remove first element
            pImpl->cache.erase(pImpl->cache.begin());
        }
        pImpl->cache[cacheKey] = result;
        pImpl->cacheStats.entries = pImpl->cache.size();
    }

    return result;
}

Result<FileSignature> FileTypeDetector::detectFromFile(const std::filesystem::path& path) {
    if (!std::filesystem::exists(path)) {
        return Error{ErrorCode::FileNotFound, "File not found: " + path.string()};
    }

    // Read first bytes of file
    std::ifstream file(path, std::ios::binary);
    if (!file) {
        return Error{ErrorCode::PermissionDenied, "Cannot open file: " + path.string()};
    }

    std::vector<std::byte> buffer(pImpl->config.maxBytesToRead);
    file.read(reinterpret_cast<char*>(buffer.data()), buffer.size());
    size_t bytesRead = file.gcount();
    buffer.resize(bytesRead);

    auto result = detectFromBuffer(buffer);

    // If detection failed or has low confidence, try extension-based detection
    if (!result || result.value().confidence < 0.5f) {
        std::string extension = path.extension().string();
        std::string mimeType = getMimeTypeFromExtension(extension);

        if (mimeType != "application/octet-stream") {
            FileSignature sig;
            sig.mimeType = mimeType;
            sig.confidence = 0.6f; // Medium confidence for extension-based
            sig.magicNumber = extractMagicNumber(buffer);
            // Use the instance methods to ensure consistency
            sig.isBinary = FileTypeDetector::instance().isBinaryMimeType(mimeType);
            sig.fileType = FileTypeDetector::instance().getFileTypeCategory(mimeType);

            return sig;
        }
    }

    return result;
}

Result<void> FileTypeDetector::loadPatternsFromFile(const std::filesystem::path& patternsFile) {
    if (!std::filesystem::exists(patternsFile)) {
        return Error{ErrorCode::FileNotFound, "Patterns file not found: " + patternsFile.string()};
    }

    try {
        std::ifstream file(patternsFile);
        nlohmann::json j;
        file >> j;

        std::lock_guard<std::mutex> lock(pImpl->patternsMutex);

        for (const auto& item : j["patterns"]) {
            FilePattern pattern;
            pattern.patternHex = item["hex"];
            pattern.offset = item.value("offset", 0);
            pattern.fileType = item["type"];
            pattern.mimeType = item.value("mime", "application/octet-stream");
            pattern.description = item.value("description", "");
            pattern.confidence = item.value("confidence", 1.0f);

            // Convert hex to bytes
            auto bytesResult = hexToBytes(pattern.patternHex);
            if (bytesResult) {
                pattern.pattern = bytesResult.value();
                pImpl->patterns.push_back(pattern);
            }

            // Build classification maps
            if (!pattern.mimeType.empty()) {
                pImpl->mimeToFileType[pattern.mimeType] = pattern.fileType;

                // Determine if binary based on file type and MIME type
                bool isBinary = true;
                if (pattern.fileType == "text" || pattern.mimeType.find("text/") == 0 ||
                    pattern.mimeType == "application/json" ||
                    pattern.mimeType == "application/xml" ||
                    pattern.mimeType == "application/javascript" ||
                    pattern.mimeType == "application/x-yaml" ||
                    pattern.mimeType == "application/yaml" ||
                    pattern.mimeType == "application/x-sh") {
                    isBinary = false;
                }
                pImpl->mimeToIsBinary[pattern.mimeType] = isBinary;
            }
        }

        // Also add extension-based MIME types to maps
        for (const auto& [ext, mime] : EXTENSION_MIME_MAP) {
            // Only add if not already present from patterns
            if (pImpl->mimeToFileType.find(mime) == pImpl->mimeToFileType.end()) {
                // Determine file type from MIME
                std::string fileType = "binary";

                // Check for code/programming file types
                if (mime.find("text/x-c") == 0 || mime.find("text/x-python") == 0 ||
                    mime.find("text/x-java") == 0 || mime.find("text/x-rust") == 0 ||
                    mime.find("text/x-go") == 0 || mime.find("text/x-ruby") == 0 ||
                    mime.find("text/x-perl") == 0 || mime.find("text/x-php") == 0 ||
                    mime == "application/javascript" || mime == "application/x-sh" ||
                    mime == "application/x-perl" || mime == "application/x-python" ||
                    mime == "application/x-ruby") {
                    fileType = "code";
                } else if (mime.find("text/") == 0)
                    fileType = "text";
                else if (mime.find("image/") == 0)
                    fileType = "image";
                else if (mime.find("video/") == 0)
                    fileType = "video";
                else if (mime.find("audio/") == 0)
                    fileType = "audio";
                else if (mime.find("application/pdf") == 0 ||
                         mime.find("application/msword") == 0 || mime.find("application/vnd.") == 0)
                    fileType = "document";
                else if (mime.find("zip") != std::string::npos ||
                         mime.find("tar") != std::string::npos ||
                         mime.find("compressed") != std::string::npos ||
                         mime.find("archive") != std::string::npos)
                    fileType = "archive";
                else if (mime == "application/json" || mime == "application/xml" ||
                         mime == "application/x-yaml" || mime == "application/yaml")
                    fileType = "text";

                pImpl->mimeToFileType[mime] = fileType;
                pImpl->mimeToIsBinary[mime] = (fileType != "text" && fileType != "code");
            }
        }

        // Info: Loaded patterns from file
        return {};

    } catch (const std::exception& e) {
        return Error{ErrorCode::InvalidArgument,
                     "Failed to parse patterns file: " + std::string(e.what())};
    }
}

Result<size_t> FileTypeDetector::loadPatternsFromDatabase(metadata::Database& db) {
    auto stmtResult =
        db.prepare("SELECT pattern_hex, offset, file_type, mime_type, description, confidence "
                   "FROM file_patterns ORDER BY confidence DESC");
    if (!stmtResult)
        return stmtResult.error();

    auto stmt = std::move(stmtResult).value();
    std::lock_guard<std::mutex> lock(pImpl->patternsMutex);
    size_t count = 0;

    while (true) {
        auto stepResult = stmt.step();
        if (!stepResult)
            return stepResult.error();
        if (!stepResult.value())
            break;

        FilePattern pattern;
        pattern.patternHex = stmt.getString(0);
        pattern.offset = stmt.getInt(1);
        pattern.fileType = stmt.getString(2);
        pattern.mimeType = stmt.getString(3);
        pattern.description = stmt.getString(4);
        pattern.confidence = stmt.getDouble(5);

        auto bytesResult = hexToBytes(pattern.patternHex);
        if (bytesResult) {
            pattern.pattern = bytesResult.value();
            pImpl->patterns.push_back(pattern);
            count++;
        }
    }

    return count;
}

Result<size_t> FileTypeDetector::savePatternsToDatabase(metadata::Database& db) {
    size_t count = 0;

    auto transactionResult = db.transaction([this, &db, &count]() -> Result<void> {
        // Clear existing patterns
        auto clearResult = db.execute("DELETE FROM file_patterns");
        if (!clearResult)
            return clearResult.error();

        // Insert new patterns
        auto stmtResult =
            db.prepare("INSERT INTO file_patterns (pattern, pattern_hex, offset, file_type, "
                       "mime_type, description, confidence) VALUES (?, ?, ?, ?, ?, ?, ?)");
        if (!stmtResult)
            return stmtResult.error();

        auto stmt = std::move(stmtResult).value();
        std::lock_guard<std::mutex> lock(pImpl->patternsMutex);

        for (const auto& pattern : pImpl->patterns) {
            auto bindResult =
                stmt.bindAll(std::span<const std::byte>(pattern.pattern), pattern.patternHex,
                             static_cast<int>(pattern.offset), pattern.fileType, pattern.mimeType,
                             pattern.description, pattern.confidence);
            if (!bindResult)
                return bindResult.error();

            auto execResult = stmt.execute();
            if (!execResult)
                return execResult.error();

            stmt.reset();
            count++;
        }

        // Update version info
        auto versionResult = db.execute(
            "INSERT OR REPLACE INTO file_patterns_version (id, version, updated_time, source) "
            "VALUES (1, '1.0', strftime('%s', 'now'), 'FileTypeDetector')");
        if (!versionResult)
            return versionResult.error();

        return Result<void>();
    });

    if (!transactionResult) {
        return transactionResult.error();
    }

    return count;
}

Result<void> FileTypeDetector::addPattern(const FilePattern& pattern) {
    if (pattern.pattern.empty() || pattern.fileType.empty()) {
        return Error{ErrorCode::InvalidArgument, "Invalid pattern"};
    }

    std::lock_guard<std::mutex> lock(pImpl->patternsMutex);
    pImpl->patterns.push_back(pattern);
    return {};
}

std::vector<FilePattern> FileTypeDetector::getPatterns() const {
    std::lock_guard<std::mutex> lock(pImpl->patternsMutex);
    return pImpl->patterns;
}

void FileTypeDetector::clearCache() {
    std::lock_guard<std::mutex> lock(pImpl->cacheMutex);
    pImpl->cache.clear();
    pImpl->cacheStats.entries = 0;
    pImpl->cacheStats.hits = 0;
    pImpl->cacheStats.misses = 0;
}

FileTypeDetector::CacheStats FileTypeDetector::getCacheStats() const {
    std::lock_guard<std::mutex> lock(pImpl->cacheMutex);
    return pImpl->cacheStats;
}

bool FileTypeDetector::hasLibMagic() const {
#ifdef YAMS_HAS_LIBMAGIC
    return pImpl->magicCookie != nullptr;
#else
    return false;
#endif
}

std::string FileTypeDetector::getMimeTypeFromExtension(const std::string& extension) {
    std::string ext = extension;
    if (!ext.empty() && ext[0] != '.') {
        ext = "." + ext;
    }

    // Convert to lowercase
    std::transform(ext.begin(), ext.end(), ext.begin(), ::tolower);

    auto it = EXTENSION_MIME_MAP.find(ext);
    return it != EXTENSION_MIME_MAP.end() ? it->second : "application/octet-stream";
}

bool FileTypeDetector::isTextMimeType(const std::string& mimeType) const {
    // First check our loaded patterns
    auto it = pImpl->mimeToIsBinary.find(mimeType);
    if (it != pImpl->mimeToIsBinary.end()) {
        return !it->second; // Return true if NOT binary
    }

    // Fall back to common text MIME types
    if (mimeType.find("text/") == 0)
        return true;

    // Known text application types
    return mimeType == "application/json" || mimeType == "application/xml" ||
           mimeType == "application/javascript" || mimeType == "application/x-yaml" ||
           mimeType == "application/yaml" || mimeType == "application/x-sh";
}

bool FileTypeDetector::isBinaryMimeType(const std::string& mimeType) const {
    return !isTextMimeType(mimeType);
}

std::string FileTypeDetector::getFileTypeCategory(const std::string& mimeType) const {
    // First check our loaded patterns
    auto it = pImpl->mimeToFileType.find(mimeType);
    if (it != pImpl->mimeToFileType.end()) {
        return it->second;
    }

    // Check for code/programming file types
    if (mimeType.find("text/x-c") == 0 ||          // C/C++ source
        mimeType.find("text/x-python") == 0 ||     // Python
        mimeType.find("text/x-java") == 0 ||       // Java
        mimeType.find("text/x-rust") == 0 ||       // Rust
        mimeType.find("text/x-go") == 0 ||         // Go
        mimeType.find("text/x-ruby") == 0 ||       // Ruby
        mimeType.find("text/x-perl") == 0 ||       // Perl
        mimeType.find("text/x-php") == 0 ||        // PHP
        mimeType.find("text/x-swift") == 0 ||      // Swift
        mimeType.find("text/x-kotlin") == 0 ||     // Kotlin
        mimeType.find("text/x-scala") == 0 ||      // Scala
        mimeType.find("text/x-typescript") == 0 || // TypeScript
        mimeType.find("text/x-csharp") == 0 ||     // C#
        mimeType.find("text/x-objc") == 0 ||       // Objective-C
        mimeType.find("text/x-asm") == 0 ||        // Assembly
        mimeType.find("text/x-makefile") == 0 ||   // Makefile
        mimeType.find("text/x-cmake") == 0 ||      // CMake
        mimeType == "application/javascript" ||    // JavaScript
        mimeType == "application/x-sh" ||          // Shell script
        mimeType == "application/x-perl" ||        // Perl
        mimeType == "application/x-python" ||      // Python
        mimeType == "application/x-ruby") {        // Ruby
        return "code";
    }

    // Fall back to MIME type analysis
    if (mimeType.find("text/") == 0)
        return "text";
    if (mimeType.find("image/") == 0)
        return "image";
    if (mimeType.find("video/") == 0)
        return "video";
    if (mimeType.find("audio/") == 0)
        return "audio";
    if (mimeType.find("application/pdf") == 0 || mimeType.find("application/msword") == 0 ||
        mimeType.find("application/vnd.") == 0)
        return "document";
    if (mimeType.find("zip") != std::string::npos || mimeType.find("tar") != std::string::npos ||
        mimeType.find("compressed") != std::string::npos ||
        mimeType.find("archive") != std::string::npos)
        return "archive";
    if (mimeType == "application/json" || mimeType == "application/xml" ||
        mimeType == "application/x-yaml" || mimeType == "application/yaml")
        return "text";

    return "binary";
}

std::string FileTypeDetector::bytesToHex(std::span<const std::byte> data, size_t maxLength) {
    std::stringstream ss;
    size_t length = std::min(data.size(), maxLength);

    for (size_t i = 0; i < length; ++i) {
        ss << std::setfill('0') << std::setw(2) << std::hex << static_cast<int>(data[i]);
    }

    return ss.str();
}

Result<std::vector<std::byte>> FileTypeDetector::hexToBytes(const std::string& hex) {
    if (hex.length() % 2 != 0) {
        return Error{ErrorCode::InvalidArgument, "Hex string must have even length"};
    }

    std::vector<std::byte> bytes;
    bytes.reserve(hex.length() / 2);

    for (size_t i = 0; i < hex.length(); i += 2) {
        try {
            int value = std::stoi(hex.substr(i, 2), nullptr, 16);
            bytes.push_back(static_cast<std::byte>(value));
        } catch (...) {
            return Error{ErrorCode::InvalidArgument, "Invalid hex string"};
        }
    }

    return bytes;
}

std::vector<FilePattern> getDefaultPatterns() {
    std::vector<FilePattern> patterns;

    // Define common file signatures
    struct PatternDef {
        const char* hex;
        size_t offset;
        const char* type;
        const char* mime;
        const char* desc;
        float confidence;
    };

    static const PatternDef patternDefs[] = {
        // Images
        {"FFD8FF", 0, "image", "image/jpeg", "JPEG image", 1.0f},
        {"89504E470D0A1A0A", 0, "image", "image/png", "PNG image", 1.0f},
        {"474946383761", 0, "image", "image/gif", "GIF87a image", 1.0f},
        {"474946383961", 0, "image", "image/gif", "GIF89a image", 1.0f},
        {"424D", 0, "image", "image/bmp", "BMP image", 1.0f},
        {"52494646", 0, "image", "image/webp", "WebP image",
         0.8f}, // RIFF header, needs more checking

        // Documents
        {"255044462D", 0, "document", "application/pdf", "PDF document", 1.0f},
        {"D0CF11E0A1B11AE1", 0, "document", "application/vnd.ms-office",
         "Microsoft Office document", 1.0f},
        {"504B0304", 0, "document", "application/vnd.openxmlformats", "Office Open XML", 0.9f},

        // Archives
        {"504B0304", 0, "archive", "application/zip", "ZIP archive", 1.0f},
        {"504B0506", 0, "archive", "application/zip", "ZIP archive (empty)", 1.0f},
        {"504B0708", 0, "archive", "application/zip", "ZIP archive (spanned)", 1.0f},
        {"1F8B", 0, "archive", "application/gzip", "GZIP archive", 1.0f},
        {"425A68", 0, "archive", "application/x-bzip2", "BZIP2 archive", 1.0f},
        {"377ABCAF271C", 0, "archive", "application/x-7z-compressed", "7-Zip archive", 1.0f},
        {"52617221", 0, "archive", "application/x-rar-compressed", "RAR archive", 1.0f},
        {"7573746172", 257, "archive", "application/x-tar", "TAR archive", 1.0f},

        // Executables
        {"4D5A", 0, "executable", "application/x-msdownload", "Windows/DOS executable", 1.0f},
        {"7F454C46", 0, "executable", "application/x-executable", "ELF executable", 1.0f},
        {"CAFEBABE", 0, "executable", "application/java-vm", "Java class file", 1.0f},
        {"FEEDFACE", 0, "executable", "application/x-mach-binary", "Mach-O binary (32-bit)", 1.0f},
        {"FEEDFACF", 0, "executable", "application/x-mach-binary", "Mach-O binary (64-bit)", 1.0f},
        {"CEFAEDFE", 0, "executable", "application/x-mach-binary", "Mach-O binary (reverse)", 1.0f},

        // Audio/Video
        {"494433", 0, "audio", "audio/mpeg", "MP3 with ID3v2", 1.0f},
        {"FFFB", 0, "audio", "audio/mpeg", "MP3 audio", 0.9f},
        {"52494646", 0, "multimedia", "audio/wav", "WAV audio", 0.8f}, // RIFF header
        {"4F676753", 0, "multimedia", "application/ogg", "OGG container", 1.0f},
        {"664C6143", 0, "audio", "audio/flac", "FLAC audio", 1.0f},
        {"0000001466747970", 4, "video", "video/mp4", "MP4 video", 1.0f},
        {"1A45DFA3", 0, "video", "video/x-matroska", "Matroska video", 1.0f},

        // Text formats (usually don't have magic numbers, but some do)
        {"3C3F786D6C20", 0, "text", "application/xml", "XML document", 0.9f},
        {"EFBBBF", 0, "text", "text/plain", "UTF-8 BOM text", 0.8f},
        {"FFFE", 0, "text", "text/plain", "UTF-16 LE BOM text", 0.8f},
        {"FEFF", 0, "text", "text/plain", "UTF-16 BE BOM text", 0.8f}};

    for (const auto& def : patternDefs) {
        FilePattern pattern;
        pattern.patternHex = def.hex;
        pattern.offset = def.offset;
        pattern.fileType = def.type;
        pattern.mimeType = def.mime;
        pattern.description = def.desc;
        pattern.confidence = def.confidence;

        auto bytesResult = FileTypeDetector::hexToBytes(pattern.patternHex);
        if (bytesResult) {
            pattern.pattern = bytesResult.value();
            patterns.push_back(pattern);
        }
    }

    return patterns;
}

bool isBinaryData(std::span<const std::byte> data) {
    if (data.empty())
        return false;

    size_t checkLength = std::min(data.size(), size_t(512));
    size_t nullBytes = 0;
    size_t controlChars = 0;
    size_t printableChars = 0;

    for (size_t i = 0; i < checkLength; ++i) {
        unsigned char c = static_cast<unsigned char>(data[i]);

        if (c == 0) {
            nullBytes++;
        } else if (c < 32 && c != '\t' && c != '\n' && c != '\r') {
            controlChars++;
        } else if (c >= 32 && c < 127) {
            printableChars++;
        }
    }

    // If we have null bytes, it's likely binary
    if (nullBytes > 0)
        return true;

    // If control characters exceed 10% of checked bytes, likely binary
    if (controlChars > checkLength / 10)
        return true;

    // If printable characters are less than 70%, likely binary
    if (printableChars < checkLength * 0.7)
        return true;

    return false;
}

std::string extractMagicNumber(std::span<const std::byte> data, size_t length) {
    return FileTypeDetector::bytesToHex(data, std::min(length, data.size()));
}

} // namespace yams::detection
