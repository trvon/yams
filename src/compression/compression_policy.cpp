#include <yams/compression/compression_policy.h>
#include <spdlog/fmt/fmt.h>
#include <yams/compression/compression_utils.h>
#include <spdlog/spdlog.h>
#include <algorithm>
#include <cctype>
#include <filesystem>
#include <thread>

namespace yams::compression {

namespace {
    /**
     * @brief Convert string to lowercase
     */
    std::string toLowerCase(std::string str) {
        std::transform(str.begin(), str.end(), str.begin(), 
            [](unsigned char c) { return std::tolower(c); });
        return str;
    }
    
    /**
     * @brief Get current CPU usage (simplified)
     */
    double getCurrentCPUUsage() {
        // Simplified implementation - in production would use platform-specific APIs
        static auto lastCheck = std::chrono::steady_clock::now();
        auto now = std::chrono::steady_clock::now();
        
        // Simulate varying CPU usage
        auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - lastCheck);
        lastCheck = now;
        
        // Return a value between 0.1 and 0.4 for testing
        return 0.1 + (elapsed.count() % 30) / 100.0;
    }
    
    /**
     * @brief Get free disk space in bytes
     */
    uint64_t getFreeDiskSpace(const std::string& path = ".") {
        std::error_code ec;
        auto spaceInfo = std::filesystem::space(path, ec);
        if (ec) {
            spdlog::warn("Failed to get disk space: {}", ec.message());
            return 0;
        }
        return spaceInfo.free;
    }
    
    /**
     * @brief Get number of active compression operations
     */
    size_t getActiveCompressionCount() {
        // In a real implementation, this would track actual operations
        static std::atomic<size_t> activeCount{0};
        return activeCount.load();
    }
}

//-----------------------------------------------------------------------------
// CompressionPolicy
//-----------------------------------------------------------------------------

CompressionPolicy::CompressionPolicy(Rules rules) 
    : rules_(std::move(rules)) {
    spdlog::debug("CompressionPolicy initialized with custom rules");
}

CompressionDecision CompressionPolicy::shouldCompress(
    const api::ContentMetadata& metadata,
    const AccessPattern& pattern) const {
    
    std::lock_guard lock(rulesMutex_);
    
    // Step 1: Check if already compressed
    if (isAlreadyCompressed(metadata)) {
        return CompressionDecision::dontCompress(
            "Content appears to be already compressed");
    }
    
    // Step 2: Check file age
    auto age = pattern.ageSinceAccess();
    if (age < rules_.neverCompressBefore) {
        return CompressionDecision::dontCompress(
            fmt::format("File too new ({} hours)", age.count()));
    }
    
    // Step 3: Check file size
    if (metadata.size < rules_.neverCompressBelow) {
        return CompressionDecision::dontCompress(
            fmt::format("File too small ({} bytes)", metadata.size));
    }
    
    // Step 4: Check file type
    if (!isCompressibleType(metadata.mimeType, metadata.name)) {
        return CompressionDecision::dontCompress(
            "File type is not compressible");
    }
    
    // Step 5: Check system resources
    if (!hasSystemResources()) {
        return CompressionDecision::dontCompress(
            "Insufficient system resources");
    }
    
    // Step 6: Determine algorithm and level
    auto algo = selectAlgorithm(metadata, pattern);
    auto level = selectLevel(algo, metadata, pattern);
    
    // Step 7: Build reason
    std::string reason;
    if (metadata.size >= rules_.alwaysCompressAbove) {
        reason = fmt::format("Large file ({:.1f} MB)", 
            static_cast<double>(metadata.size) / (1024*1024));
    } else if (age >= rules_.archiveAfterAge) {
        reason = fmt::format("Old file ({} days)", age.count() / 24);
    } else {
        auto temp = classifyTemperature(pattern);
        reason = fmt::format("{} file with {:.1f} accesses/day", 
            temp == FileTemperature::Cold ? "Cold" : 
            temp == FileTemperature::Hot ? "Hot" : "Warm",
            pattern.accessFrequency());
    }
    
    return CompressionDecision::compress(algo, level, reason);
}

CompressionAlgorithm CompressionPolicy::selectAlgorithm(
    const api::ContentMetadata& metadata,
    const AccessPattern& pattern) const {
    
    auto age = pattern.totalAge();
    auto temp = classifyTemperature(pattern);
    
    // Use LZMA for cold/archival data
    if (age >= rules_.archiveAfterAge || temp == FileTemperature::Cold) {
        spdlog::debug("Selecting LZMA for cold/archival data: {} days old, {} accesses/day",
            age.count() / 24, pattern.accessFrequency());
        return CompressionAlgorithm::LZMA;
    }
    
    // Use LZMA for large files that aren't accessed frequently
    if (metadata.size >= rules_.preferZstdBelow && 
        pattern.accessFrequency() < rules_.coldFileAccessesPerDay) {
        spdlog::debug("Selecting LZMA for large inactive file: {} MB, {} accesses/day",
            metadata.size / (1024*1024), pattern.accessFrequency());
        return CompressionAlgorithm::LZMA;
    }
    
    // Default to Zstandard for active data
    spdlog::debug("Selecting Zstandard for active data");
    return CompressionAlgorithm::Zstandard;
}

uint8_t CompressionPolicy::selectLevel(
    CompressionAlgorithm algo,
    const api::ContentMetadata& metadata,
    const AccessPattern& pattern) const {
    
    switch (algo) {
        case CompressionAlgorithm::Zstandard:
            if (pattern.accessFrequency() > rules_.hotFileAccessesPerDay) {
                return 1;  // Fast compression for hot files
            } else if (pattern.totalAge() >= rules_.archiveAfterAge) {
                return rules_.archiveZstdLevel;
            } else {
                return rules_.defaultZstdLevel;
            }
            
        case CompressionAlgorithm::LZMA:
            // For LZMA, balance size vs time based on file size
            if (metadata.size < 10 * 1024 * 1024) {  // < 10MB
                return std::min(rules_.defaultLzmaLevel, uint8_t(5));
            } else {
                return rules_.defaultLzmaLevel;
            }
            
        default:
            return 0;
    }
}

bool CompressionPolicy::isCompressibleType(
    const std::string& mimeType,
    const std::string& filename) const {
    
    // First check excluded types
    if (rules_.excludedTypes.count(toLowerCase(mimeType)) > 0) {
        return false;
    }
    
    // Check excluded extensions
    auto ext = getExtension(filename);
    if (!ext.empty() && rules_.excludedExtensions.count(ext) > 0) {
        return false;
    }
    
    // Check if explicitly compressible
    if (rules_.compressibleTypes.count(toLowerCase(mimeType)) > 0) {
        return true;
    }
    
    // Check compressible extensions
    if (!ext.empty() && rules_.compressibleExtensions.count(ext) > 0) {
        return true;
    }
    
    // Default: check if it's a text-based MIME type
    auto lowerMime = toLowerCase(mimeType);
    return lowerMime.find("text/") == 0 || 
           lowerMime.find("application/") == 0;
}

bool CompressionPolicy::hasSystemResources() const {
    // Check CPU usage
    double cpuUsage = getCurrentCPUUsage();
    if (cpuUsage > rules_.maxCpuUsage) {
        spdlog::debug("CPU usage too high: {:.1f}% > {:.1f}%", 
            cpuUsage * 100, rules_.maxCpuUsage * 100);
        return false;
    }
    
    // Check free disk space
    auto freeSpace = getFreeDiskSpace();
    if (freeSpace < rules_.minFreeSpaceBytes) {
        spdlog::debug("Insufficient disk space: {} MB < {} MB",
            freeSpace / (1024*1024), 
            rules_.minFreeSpaceBytes / (1024*1024));
        return false;
    }
    
    // Check concurrent compressions
    auto activeCompressions = getActiveCompressionCount();
    if (activeCompressions >= rules_.maxConcurrentCompressions) {
        spdlog::debug("Too many concurrent compressions: {} >= {}",
            activeCompressions, rules_.maxConcurrentCompressions);
        return false;
    }
    
    return true;
}

std::string CompressionPolicy::getExtension(const std::string& filename) {
    auto pos = filename.rfind('.');
    if (pos != std::string::npos && pos > 0 && pos < filename.length() - 1) {
        return toLowerCase(filename.substr(pos));
    }
    return "";
}

bool CompressionPolicy::isAlreadyCompressed(
    const api::ContentMetadata& metadata) const {
    
    // Check MIME type
    auto lowerMime = toLowerCase(metadata.mimeType);
    if (lowerMime.find("compressed") != std::string::npos ||
        lowerMime.find("zip") != std::string::npos ||
        lowerMime.find("gzip") != std::string::npos) {
        return true;
    }
    
    // Check extension
    auto ext = getExtension(metadata.name);
    if (rules_.excludedExtensions.count(ext) > 0) {
        return true;
    }
    
    // Could also check file headers, but that requires data access
    return false;
}

CompressionPolicy::FileTemperature CompressionPolicy::classifyTemperature(
    const AccessPattern& pattern) const {
    
    double freq = pattern.accessFrequency();
    
    if (freq >= rules_.hotFileAccessesPerDay) {
        return FileTemperature::Hot;
    } else if (freq <= rules_.coldFileAccessesPerDay) {
        return FileTemperature::Cold;
    } else {
        return FileTemperature::Warm;
    }
}

//-----------------------------------------------------------------------------
// CompressionScheduler::Impl
//-----------------------------------------------------------------------------

class CompressionScheduler::Impl {
public:
    explicit Impl(Config cfg) : config_(std::move(cfg)) {}
    
    ~Impl() {
        stop();
    }
    
    [[nodiscard]] Result<void> start() {
        bool expected = false;
        if (!running_.compare_exchange_strong(expected, true)) {
            return Error{ErrorCode::OperationInProgress, 
                "Scheduler is already running"};
        }
        
        workerThread_ = std::thread(&Impl::workerLoop, this);
        spdlog::info("Compression scheduler started");
        return {};
    }
    
    void stop() {
        if (running_.exchange(false)) {
            cv_.notify_all();
            if (workerThread_.joinable()) {
                workerThread_.join();
            }
            spdlog::info("Compression scheduler stopped");
        }
    }
    
    [[nodiscard]] bool isRunning() const noexcept {
        return running_.load();
    }
    
    [[nodiscard]] Result<size_t> scanNow() {
        if (!running_) {
            return Error{ErrorCode::InvalidState, "Scheduler is not running"};
        }
        
        // Trigger immediate scan
        cv_.notify_all();
        
        // In a real implementation, this would return actual count
        return 0;
    }
    
    void onCompression(std::function<void(const std::string&, CompressionResult)> callback) {
        std::lock_guard lock(callbackMutex_);
        compressionCallback_ = std::move(callback);
    }
    
private:
    Config config_;
    std::atomic<bool> running_{false};
    std::thread workerThread_;
    std::condition_variable cv_;
    std::mutex mutex_;
    std::mutex callbackMutex_;
    std::function<void(const std::string&, CompressionResult)> compressionCallback_;
    
    void workerLoop() {
        while (running_) {
            // Wait for next scan interval
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait_for(lock, config_.scanInterval,
                [this] { return !running_.load(); });
            
            if (!running_) break;
            
            // Scan for candidates
            spdlog::debug("Compression scheduler scanning for candidates");
            auto candidates = findCompressionCandidates();
            
            if (!candidates.empty()) {
                spdlog::info("Found {} compression candidates", candidates.size());
            }
            
            // Process in batches
            for (size_t i = 0; i < candidates.size() && running_; i += config_.batchSize) {
                size_t batchEnd = std::min(i + config_.batchSize, candidates.size());
                processBatch(candidates, i, batchEnd);
                
                // Delay between batches
                if (running_ && batchEnd < candidates.size()) {
                    std::this_thread::sleep_for(config_.batchDelay);
                }
            }
        }
    }
    
    std::vector<std::string> findCompressionCandidates() {
        // In a real implementation, this would scan the storage
        // and apply the policy to find candidates
        return {};
    }
    
    void processBatch(const std::vector<std::string>& candidates,
                     size_t start, size_t end) {
        for (size_t i = start; i < end && running_; ++i) {
            processCandidate(candidates[i]);
        }
    }
    
    void processCandidate(const std::string& id) {
        // In a real implementation, this would:
        // 1. Load metadata and access pattern
        // 2. Apply policy
        // 3. Compress if needed
        // 4. Call callback
        spdlog::debug("Processing compression candidate: {}", id);
    }
};

//-----------------------------------------------------------------------------
// CompressionScheduler
//-----------------------------------------------------------------------------

CompressionScheduler::CompressionScheduler(Config config)
    : pImpl(std::make_unique<Impl>(std::move(config))) {
}

CompressionScheduler::~CompressionScheduler() = default;

Result<void> CompressionScheduler::start() {
    return pImpl->start();
}

void CompressionScheduler::stop() {
    pImpl->stop();
}

bool CompressionScheduler::isRunning() const noexcept {
    return pImpl->isRunning();
}

Result<size_t> CompressionScheduler::scanNow() {
    return pImpl->scanNow();
}

void CompressionScheduler::onCompression(
    std::function<void(const std::string&, CompressionResult)> callback) {
    pImpl->onCompression(std::move(callback));
}

} // namespace yams::compression