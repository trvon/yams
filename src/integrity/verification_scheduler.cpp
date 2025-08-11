#include <yams/integrity/verifier.h>

#include <spdlog/spdlog.h>

#include <algorithm>
#include <mutex>
#include <queue>
#include <unordered_map>

namespace yams::integrity {

/**
 * Implementation details for VerificationScheduler
 */
struct VerificationScheduler::Impl {
    SchedulingStrategy strategy;
    std::priority_queue<BlockInfo> verificationQueue;
    std::unordered_map<std::string, BlockInfo> blockInfoMap;
    mutable std::mutex mutex;
    
    explicit Impl(SchedulingStrategy strategy)
        : strategy(strategy)
    {}
    
    void addBlockToQueue(const BlockInfo& block) {
        std::lock_guard lock(mutex);
        
        // Update or add block info
        blockInfoMap[block.hash] = block;
        
        // Add to priority queue
        verificationQueue.push(block);
        
        spdlog::debug("Added block {} to verification queue (priority: {})", 
                     block.hash.substr(0, 8), block.getPriority());
    }
    
    std::optional<BlockInfo> getNextBlockFromQueue() {
        std::lock_guard lock(mutex);
        
        if (verificationQueue.empty()) {
            return std::nullopt;
        }
        
        auto block = verificationQueue.top();
        verificationQueue.pop();
        
        // Remove from map if it exists
        auto it = blockInfoMap.find(block.hash);
        if (it != blockInfoMap.end()) {
            blockInfoMap.erase(it);
        }
        
        spdlog::debug("Retrieved block {} from verification queue", 
                     block.hash.substr(0, 8));
        
        return block;
    }
    
    void updateBlockInfoInMap(const std::string& hash, const VerificationResult& result) {
        std::lock_guard lock(mutex);
        
        auto it = blockInfoMap.find(hash);
        if (it != blockInfoMap.end()) {
            auto& block = it->second;
            
            // Update based on verification result
            block.lastVerified = result.timestamp;
            
            if (!result.isSuccess()) {
                block.failureCount++;
                spdlog::debug("Incremented failure count for block {} to {}", 
                             hash.substr(0, 8), block.failureCount);
            } else {
                // Reset failure count on success
                block.failureCount = 0;
            }
            
            // Re-add to queue with updated priority if strategy requires it
            if (strategy == SchedulingStrategy::ByFailures && !result.isSuccess()) {
                verificationQueue.push(block);
                spdlog::debug("Re-queued failed block {} with higher priority", 
                             hash.substr(0, 8));
            }
        }
    }
    
    void clearQueueInternal() {
        std::lock_guard lock(mutex);
        
        // Clear priority queue
        std::priority_queue<BlockInfo> empty;
        verificationQueue.swap(empty);
        
        // Clear block info map
        blockInfoMap.clear();
        
        spdlog::info("Cleared verification queue");
    }
    
    size_t getQueueSizeInternal() const {
        std::lock_guard lock(mutex);
        return verificationQueue.size();
    }
    
    void setStrategyInternal(SchedulingStrategy newStrategy) {
        std::lock_guard lock(mutex);
        
        if (strategy == newStrategy) {
            return;
        }
        
        strategy = newStrategy;
        
        // Rebuild queue with new strategy
        std::vector<BlockInfo> blocks;
        while (!verificationQueue.empty()) {
            blocks.push_back(verificationQueue.top());
            verificationQueue.pop();
        }
        
        // Re-add all blocks (priority will be recalculated based on new strategy)
        for (const auto& block : blocks) {
            verificationQueue.push(block);
        }
        
        spdlog::info("Changed verification scheduling strategy");
    }
};

// VerificationScheduler implementation

VerificationScheduler::VerificationScheduler(SchedulingStrategy strategy)
    : pImpl(std::make_unique<Impl>(strategy))
{
}

VerificationScheduler::~VerificationScheduler() = default;

void VerificationScheduler::addBlock(const BlockInfo& block) {
    pImpl->addBlockToQueue(block);
}

void VerificationScheduler::addBlocks(const std::vector<BlockInfo>& blocks) {
    for (const auto& block : blocks) {
        addBlock(block);
    }
    
    spdlog::info("Added {} blocks to verification queue", blocks.size());
}

std::optional<BlockInfo> VerificationScheduler::getNextBlock() {
    return pImpl->getNextBlockFromQueue();
}

size_t VerificationScheduler::getQueueSize() const {
    return pImpl->getQueueSizeInternal();
}

void VerificationScheduler::clearQueue() {
    pImpl->clearQueueInternal();
}

void VerificationScheduler::setStrategy(SchedulingStrategy strategy) {
    pImpl->setStrategyInternal(strategy);
}

void VerificationScheduler::updateBlockInfo(const std::string& hash, const VerificationResult& result) {
    pImpl->updateBlockInfoInMap(hash, result);
}

} // namespace yams::integrity