#include <yams/search/search_cache.h>
#include <algorithm>
#include <fstream>
#include <sstream>
#include <chrono>

namespace yams::search {

SearchCache::SearchCache(const SearchCacheConfig& config)
    : config_(config) {
    
    stats_.maxSize = config.maxEntries;
    stats_.maxMemory = config.maxMemoryMB * 1024 * 1024; // Convert to bytes
}

std::optional<SearchResults> SearchCache::get(const CacheKey& key) {
    auto startTime = std::chrono::high_resolution_clock::now();
    
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    auto it = cache_.find(key);
    if (it == cache_.end()) {
        // Cache miss
        if (config_.enableStatistics) {
            stats_.misses.fetch_add(1);
            
            auto endTime = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                endTime - startTime);
            stats_.totalAccessTime.fetch_add(duration.count());
            stats_.totalMissTime.fetch_add(duration.count());
        }
        return std::nullopt;
    }
    
    auto& entry = it->second;
    
    // Check if expired
    if (entry->isExpired()) {
        lock.unlock();
        
        // Remove expired entry
        std::unique_lock<std::shared_mutex> writeLock(mutex_);
        cache_.erase(key);
        
        auto listIt = keyToListIter_.find(key);
        if (listIt != keyToListIter_.end()) {
            lruList_.erase(listIt->second);
            keyToListIter_.erase(listIt);
        }
        
        if (config_.enableStatistics) {
            stats_.ttlExpirations.fetch_add(1);
            stats_.currentSize.fetch_sub(1);
            currentMemoryUsage_.fetch_sub(entry->memorySize);
            stats_.memoryUsage.store(currentMemoryUsage_.load());
        }
        
        return std::nullopt;
    }
    
    // Cache hit
    entry->lastAccessTime = std::chrono::system_clock::now();
    entry->accessCount++;
    
    // Move to front for LRU
    if (config_.evictionPolicy == SearchCacheConfig::EvictionPolicy::LRU) {
        lock.unlock();
        std::unique_lock<std::shared_mutex> writeLock(mutex_);
        moveToFront(key);
    }
    
    if (config_.enableStatistics) {
        stats_.hits.fetch_add(1);
        
        auto endTime = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
            endTime - startTime);
        stats_.totalAccessTime.fetch_add(duration.count());
        stats_.totalHitTime.fetch_add(duration.count());
        
        if (config_.enablePatternTracking) {
            stats_.recordQueryPattern(key.getComponents().queryText);
        }
    }
    
    return entry->data;
}

void SearchCache::put(const CacheKey& key, 
                     const SearchResults& results,
                     std::chrono::seconds ttl) {
    
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    // Check if key already exists
    auto it = cache_.find(key);
    if (it != cache_.end()) {
        // Update existing entry
        auto& entry = it->second;
        
        size_t oldSize = entry->memorySize;
        size_t newSize = calculateMemorySize(results);
        
        entry->data = results;
        entry->insertTime = std::chrono::system_clock::now();
        entry->lastAccessTime = entry->insertTime;
        entry->ttl = (ttl.count() > 0) ? ttl : config_.defaultTTL;
        entry->memorySize = newSize;
        
        currentMemoryUsage_.fetch_add(newSize - oldSize);
        
        moveToFront(key);
        
        if (config_.enableStatistics) {
            stats_.memoryUsage.store(currentMemoryUsage_.load());
        }
        
        return;
    }
    
    // Check size limits
    while (cache_.size() >= config_.maxEntries) {
        evictByPolicy();
    }
    
    // Check memory limits
    enforceMemoryLimit();
    
    // Create new entry
    auto entry = std::make_shared<CacheEntry>();
    entry->data = results;
    entry->insertTime = std::chrono::system_clock::now();
    entry->lastAccessTime = entry->insertTime;
    entry->ttl = (ttl.count() > 0) ? ttl : config_.defaultTTL;
    entry->accessCount = 0;
    entry->memorySize = calculateMemorySize(results);
    
    // Add to cache
    cache_[key] = entry;
    
    // Add to LRU list
    lruList_.push_front(key);
    keyToListIter_[key] = lruList_.begin();
    
    // Update statistics
    currentMemoryUsage_.fetch_add(entry->memorySize);
    
    if (config_.enableStatistics) {
        stats_.insertions.fetch_add(1);
        stats_.currentSize.fetch_add(1);
        stats_.memoryUsage.store(currentMemoryUsage_.load());
    }
}

bool SearchCache::contains(const CacheKey& key) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    auto it = cache_.find(key);
    if (it == cache_.end()) {
        return false;
    }
    
    return !it->second->isExpired();
}

void SearchCache::invalidate(const CacheKey& key) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    auto it = cache_.find(key);
    if (it != cache_.end()) {
        size_t memSize = it->second->memorySize;
        cache_.erase(it);
        
        auto listIt = keyToListIter_.find(key);
        if (listIt != keyToListIter_.end()) {
            lruList_.erase(listIt->second);
            keyToListIter_.erase(listIt);
        }
        
        currentMemoryUsage_.fetch_sub(memSize);
        
        if (config_.enableStatistics) {
            stats_.invalidations.fetch_add(1);
            stats_.currentSize.fetch_sub(1);
            stats_.memoryUsage.store(currentMemoryUsage_.load());
        }
    }
}

size_t SearchCache::invalidatePattern(const std::string& pattern) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    size_t count = 0;
    std::vector<CacheKey> keysToRemove;
    
    // Find all keys matching the pattern
    for (const auto& [key, entry] : cache_) {
        if (key.matchesPattern(pattern)) {
            keysToRemove.push_back(key);
        }
    }
    
    // Remove matching entries
    for (const auto& key : keysToRemove) {
        auto it = cache_.find(key);
        if (it != cache_.end()) {
            size_t memSize = it->second->memorySize;
            cache_.erase(it);
            
            auto listIt = keyToListIter_.find(key);
            if (listIt != keyToListIter_.end()) {
                lruList_.erase(listIt->second);
                keyToListIter_.erase(listIt);
            }
            
            currentMemoryUsage_.fetch_sub(memSize);
            count++;
        }
    }
    
    if (config_.enableStatistics && count > 0) {
        stats_.invalidations.fetch_add(count);
        stats_.currentSize.fetch_sub(count);
        stats_.memoryUsage.store(currentMemoryUsage_.load());
    }
    
    return count;
}

void SearchCache::clear() {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    cache_.clear();
    lruList_.clear();
    keyToListIter_.clear();
    currentMemoryUsage_.store(0);
    
    if (config_.enableStatistics) {
        stats_.currentSize.store(0);
        stats_.memoryUsage.store(0);
    }
}

size_t SearchCache::size() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return cache_.size();
}

size_t SearchCache::memoryUsage() const {
    return currentMemoryUsage_.load();
}

void SearchCache::evict(size_t count) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    for (size_t i = 0; i < count && !cache_.empty(); ++i) {
        evictByPolicy();
    }
}

size_t SearchCache::removeExpired() {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    size_t count = 0;
    std::vector<CacheKey> expiredKeys;
    
    // Find expired entries
    for (const auto& [key, entry] : cache_) {
        if (entry->isExpired()) {
            expiredKeys.push_back(key);
        }
    }
    
    // Remove expired entries
    for (const auto& key : expiredKeys) {
        auto it = cache_.find(key);
        if (it != cache_.end()) {
            size_t memSize = it->second->memorySize;
            cache_.erase(it);
            
            auto listIt = keyToListIter_.find(key);
            if (listIt != keyToListIter_.end()) {
                lruList_.erase(listIt->second);
                keyToListIter_.erase(listIt);
            }
            
            currentMemoryUsage_.fetch_sub(memSize);
            count++;
        }
    }
    
    if (config_.enableStatistics && count > 0) {
        stats_.ttlExpirations.fetch_add(count);
        stats_.currentSize.fetch_sub(count);
        stats_.memoryUsage.store(currentMemoryUsage_.load());
    }
    
    return count;
}

CacheStats SearchCache::getStats() const {
    // Create a snapshot of the stats (atomics can't be copied directly)
    CacheStats snapshot;
    snapshot.hits.store(stats_.hits.load());
    snapshot.misses.store(stats_.misses.load());
    snapshot.evictions.store(stats_.evictions.load());
    snapshot.invalidations.store(stats_.invalidations.load());
    snapshot.insertions.store(stats_.insertions.load());
    snapshot.currentSize.store(stats_.currentSize.load());
    snapshot.memoryUsage.store(stats_.memoryUsage.load());
    snapshot.maxSize = stats_.maxSize;
    snapshot.maxMemory = stats_.maxMemory;
    snapshot.totalAccessTime.store(stats_.totalAccessTime.load());
    snapshot.totalHitTime.store(stats_.totalHitTime.load());
    snapshot.totalMissTime.store(stats_.totalMissTime.load());
    snapshot.ttlExpirations.store(stats_.ttlExpirations.load());
    return snapshot;
}

void SearchCache::resetStats() {
    stats_.reset();
    stats_.maxSize = config_.maxEntries;
    stats_.maxMemory = config_.maxMemoryMB * 1024 * 1024;
}

void SearchCache::warmUp(const QueryExecutor& executor) {
    if (!config_.enableWarmup || config_.warmupQueries.empty()) {
        return;
    }
    
    for (const auto& query : config_.warmupQueries) {
        try {
            auto response = executor(query);
            auto key = CacheKey::fromQuery(query, nullptr, 0, 10);
            put(key, response);
        } catch (...) {
            // Ignore warmup failures
        }
    }
}

void SearchCache::addWarmupQuery(const std::string& query) {
    config_.warmupQueries.push_back(query);
}

void SearchCache::setConfig(const SearchCacheConfig& config) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    config_ = config;
    
    // Update statistics limits
    stats_.maxSize = config.maxEntries;
    stats_.maxMemory = config.maxMemoryMB * 1024 * 1024;
    
    // Enforce new limits
    while (cache_.size() > config_.maxEntries) {
        evictByPolicy();
    }
    
    enforceMemoryLimit();
}

void SearchCache::moveToFront(const KeyType& key) {
    auto it = keyToListIter_.find(key);
    if (it != keyToListIter_.end()) {
        lruList_.erase(it->second);
        lruList_.push_front(key);
        keyToListIter_[key] = lruList_.begin();
    }
}

void SearchCache::evictLRU() {
    if (lruList_.empty()) {
        return;
    }
    
    auto key = lruList_.back();
    lruList_.pop_back();
    keyToListIter_.erase(key);
    
    auto it = cache_.find(key);
    if (it != cache_.end()) {
        size_t memSize = it->second->memorySize;
        cache_.erase(it);
        currentMemoryUsage_.fetch_sub(memSize);
        
        if (config_.enableStatistics) {
            stats_.evictions.fetch_add(1);
            stats_.currentSize.fetch_sub(1);
            stats_.memoryUsage.store(currentMemoryUsage_.load());
        }
    }
}

void SearchCache::evictByPolicy() {
    switch (config_.evictionPolicy) {
        case SearchCacheConfig::EvictionPolicy::LRU:
            evictLRU();
            break;
            
        case SearchCacheConfig::EvictionPolicy::LFU:
            // Find least frequently used
            if (!cache_.empty()) {
                auto minIt = std::min_element(cache_.begin(), cache_.end(),
                    [](const auto& a, const auto& b) {
                        return a.second->accessCount < b.second->accessCount;
                    });
                
                if (minIt != cache_.end()) {
                    auto key = minIt->first;
                    size_t memSize = minIt->second->memorySize;
                    cache_.erase(minIt);
                    
                    auto listIt = keyToListIter_.find(key);
                    if (listIt != keyToListIter_.end()) {
                        lruList_.erase(listIt->second);
                        keyToListIter_.erase(listIt);
                    }
                    
                    currentMemoryUsage_.fetch_sub(memSize);
                    
                    if (config_.enableStatistics) {
                        stats_.evictions.fetch_add(1);
                        stats_.currentSize.fetch_sub(1);
                        stats_.memoryUsage.store(currentMemoryUsage_.load());
                    }
                }
            }
            break;
            
        case SearchCacheConfig::EvictionPolicy::FIFO:
            // Same as LRU for this implementation
            evictLRU();
            break;
            
        case SearchCacheConfig::EvictionPolicy::TTL:
            // Remove the entry with shortest remaining TTL
            if (!cache_.empty()) {
                auto minIt = std::min_element(cache_.begin(), cache_.end(),
                    [](const auto& a, const auto& b) {
                        auto now = std::chrono::system_clock::now();
                        auto remainingA = a.second->ttl - (now - a.second->insertTime);
                        auto remainingB = b.second->ttl - (now - b.second->insertTime);
                        return remainingA < remainingB;
                    });
                
                if (minIt != cache_.end()) {
                    auto key = minIt->first;
                    size_t memSize = minIt->second->memorySize;
                    cache_.erase(minIt);
                    
                    auto listIt = keyToListIter_.find(key);
                    if (listIt != keyToListIter_.end()) {
                        lruList_.erase(listIt->second);
                        keyToListIter_.erase(listIt);
                    }
                    
                    currentMemoryUsage_.fetch_sub(memSize);
                    
                    if (config_.enableStatistics) {
                        stats_.evictions.fetch_add(1);
                        stats_.currentSize.fetch_sub(1);
                        stats_.memoryUsage.store(currentMemoryUsage_.load());
                    }
                }
            }
            break;
    }
}

size_t SearchCache::calculateMemorySize(const SearchResults& response) {
    size_t size = sizeof(SearchResults);
    
    // Add size of result items
    for (const auto& item : response.getItems()) {
        size += sizeof(SearchResultItem);
        size += item.title.size();
        size += item.contentPreview.size();
        size += item.path.size();
        
        // Add highlight sizes
        for (const auto& highlight : item.highlights) {
            size += sizeof(highlight);
            size += highlight.snippet.size();
        }
    }
    
    // Add facet sizes
    for (const auto& facet : response.getFacets()) {
        size += sizeof(facet);
        size += facet.name.size();
        
        for (const auto& value : facet.values) {
            size += value.value.size();
            size += sizeof(value.count);
        }
    }
    
    // Add spell correction size
    // TODO: Add spell correction support when implemented
    // for (const auto& correction : results.spellCorrections) {
    //     size += correction.original.size();
    //     size += correction.suggestion.size();
    //     size += sizeof(correction.confidence);
    // }
    
    return size;
}

void SearchCache::enforceMemoryLimit() {
    size_t maxMemory = config_.maxMemoryMB * 1024 * 1024;
    
    while (currentMemoryUsage_.load() > maxMemory && !cache_.empty()) {
        evictByPolicy();
    }
}

Result<void> SearchCache::saveToDisk(const std::string& path) const {
    try {
        std::ofstream file(path, std::ios::binary);
        if (!file) {
            return Error{ErrorCode::FileNotFound, "Cannot open file for writing"};
        }
        
        std::shared_lock<std::shared_mutex> lock(mutex_);
        
        // Write header
        size_t cacheSize = cache_.size();
        file.write(reinterpret_cast<const char*>(&cacheSize), sizeof(cacheSize));
        
        // Write each entry
        for (const auto& [key, entry] : cache_) {
            // Write key
            std::string keyStr = key.toString();
            size_t keyLen = keyStr.size();
            file.write(reinterpret_cast<const char*>(&keyLen), sizeof(keyLen));
            file.write(keyStr.data(), keyLen);
            
            // Write entry
            std::string entryData = serializeEntry(*entry);
            size_t entryLen = entryData.size();
            file.write(reinterpret_cast<const char*>(&entryLen), sizeof(entryLen));
            file.write(entryData.data(), entryLen);
        }
        
        return Result<void>();
        
    } catch (const std::exception& e) {
        return Error{ErrorCode::InternalError, e.what()};
    }
}

Result<void> SearchCache::loadFromDisk(const std::string& path) {
    try {
        std::ifstream file(path, std::ios::binary);
        if (!file) {
            return Error{ErrorCode::FileNotFound, "Cannot open file for reading"};
        }
        
        std::unique_lock<std::shared_mutex> lock(mutex_);
        
        // Clear existing cache
        clear();
        
        // Read header
        size_t cacheSize;
        file.read(reinterpret_cast<char*>(&cacheSize), sizeof(cacheSize));
        
        // Read each entry
        for (size_t i = 0; i < cacheSize; ++i) {
            // Read key
            size_t keyLen;
            file.read(reinterpret_cast<char*>(&keyLen), sizeof(keyLen));
            
            std::string keyStr(keyLen, '\0');
            file.read(keyStr.data(), keyLen);
            
            // Read entry
            size_t entryLen;
            file.read(reinterpret_cast<char*>(&entryLen), sizeof(entryLen));
            
            std::string entryData(entryLen, '\0');
            file.read(entryData.data(), entryLen);
            
            // Deserialize and add to cache
            auto entry = deserializeEntry(entryData);
            if (entry) {
                // Parse key components from string
                // This is simplified - in production, we'd store components
                CacheKey key = CacheKey::fromQuery(keyStr, nullptr, 0, 10);
                
                cache_[key] = std::move(entry);
                lruList_.push_back(key);
                keyToListIter_[key] = std::prev(lruList_.end());
                
                currentMemoryUsage_.fetch_add(cache_[key]->memorySize);
            }
        }
        
        if (config_.enableStatistics) {
            stats_.currentSize.store(cache_.size());
            stats_.memoryUsage.store(currentMemoryUsage_.load());
        }
        
        return Result<void>();
        
    } catch (const std::exception& e) {
        return Error{ErrorCode::InternalError, e.what()};
    }
}

std::string SearchCache::serializeEntry(const CacheEntry& entry) const {
    // Simplified serialization - in production, use a proper serialization library
    std::stringstream ss;
    
    // Serialize timestamps
    auto insertTime = entry.insertTime.time_since_epoch().count();
    auto lastAccessTime = entry.lastAccessTime.time_since_epoch().count();
    auto ttl = entry.ttl.count();
    
    ss << insertTime << "|" << lastAccessTime << "|" << ttl << "|";
    ss << entry.accessCount << "|" << entry.memorySize << "|";
    
    // Serialize SearchResults (simplified)
    ss << entry.data.getStatistics().totalResults << "|";
    ss << entry.data.getItems().size() << "|";
    
    // Add more serialization as needed
    
    return ss.str();
}

std::unique_ptr<SearchCache::CacheEntry> SearchCache::deserializeEntry(
    const std::string& data) const {
    
    // Simplified deserialization
    auto entry = std::make_unique<CacheEntry>();
    
    std::stringstream ss(data);
    std::string token;
    
    // Parse timestamps
    if (std::getline(ss, token, '|')) {
        auto count = std::stoull(token);
        entry->insertTime = std::chrono::system_clock::time_point(
            std::chrono::system_clock::duration(count));
    }
    
    if (std::getline(ss, token, '|')) {
        auto count = std::stoull(token);
        entry->lastAccessTime = std::chrono::system_clock::time_point(
            std::chrono::system_clock::duration(count));
    }
    
    if (std::getline(ss, token, '|')) {
        entry->ttl = std::chrono::seconds(std::stoull(token));
    }
    
    if (std::getline(ss, token, '|')) {
        entry->accessCount = std::stoull(token);
    }
    
    if (std::getline(ss, token, '|')) {
        entry->memorySize = std::stoull(token);
    }
    
    // Parse SearchResponse (simplified)
    if (std::getline(ss, token, '|')) {
        entry->data.getStatistics().totalResults = std::stoull(token);
    }
    
    // Add more deserialization as needed
    
    return entry;
}

// ===== Factory Implementation =====

std::unique_ptr<SearchCache> SearchCacheFactory::createDefault() {
    return std::make_unique<SearchCache>();
}

std::unique_ptr<SearchCache> SearchCacheFactory::create(
    const SearchCacheConfig& config) {
    return std::make_unique<SearchCache>(config);
}

std::unique_ptr<SearchCache> SearchCacheFactory::createDistributed(
    const SearchCacheConfig& config,
    const std::string& redisUrl) {
    // Future enhancement: Create Redis-backed distributed cache
    // For now, return regular cache
    return std::make_unique<SearchCache>(config);
}

} // namespace yams::search