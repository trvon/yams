#pragma once

#include <yams/core/types.h>

#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <unordered_map>
#include <vector>

namespace yams::daemon {

// ============================================================================
// Resource Pool Configuration
// ============================================================================

template <typename ResourceType> struct PoolConfig {
    size_t minSize = 1;  // Minimum resources to keep
    size_t maxSize = 10; // Maximum resources allowed
    size_t maxIdle = 5;  // Maximum idle resources

    std::chrono::seconds idleTimeout{300};      // Idle resource timeout
    std::chrono::seconds acquisitionTimeout{5}; // Max wait for resource

    bool preCreateResources = false; // Pre-create min resources on init
    bool validateOnAcquire = true;   // Validate resource before giving to client
    bool validateOnReturn = false;   // Validate resource on return
};

// ============================================================================
// Resource Wrapper with Metadata
// ============================================================================

template <typename T> class PooledResource {
public:
    using ResourcePtr = std::shared_ptr<T>;

    PooledResource(ResourcePtr resource, const std::string& id) : resource_(resource), id_(id) {
        lastUsed_ = std::chrono::steady_clock::now();
        createdAt_ = lastUsed_;
    }

    // Resource access
    T* operator->() { return resource_.get(); }
    const T* operator->() const { return resource_.get(); }
    T& operator*() { return *resource_; }
    const T& operator*() const { return *resource_; }

    // Metadata
    const std::string& getId() const { return id_; }
    size_t getUseCount() const { return useCount_; }

    void markUsed() {
        lastUsed_ = std::chrono::steady_clock::now();
        useCount_++;
    }

    bool isExpired(std::chrono::seconds timeout) const {
        auto now = std::chrono::steady_clock::now();
        return (now - lastUsed_) > timeout;
    }

    std::chrono::steady_clock::time_point getLastUsed() const { return lastUsed_; }
    std::chrono::steady_clock::time_point getCreatedAt() const { return createdAt_; }

private:
    ResourcePtr resource_;
    std::string id_;
    std::chrono::steady_clock::time_point createdAt_;
    std::chrono::steady_clock::time_point lastUsed_;
    size_t useCount_ = 0;
};

// ============================================================================
// Generic Resource Pool Template
// ============================================================================

template <typename ResourceType> class ResourcePool {
public:
    using ResourcePtr = std::shared_ptr<ResourceType>;
    using PooledResourcePtr = std::shared_ptr<PooledResource<ResourceType>>;
    using ResourceFactory = std::function<Result<ResourcePtr>(const std::string& id)>;
    using ResourceValidator = std::function<bool(const ResourceType&)>;
    using ResourceDestructor = std::function<void(ResourceType*)>;

    // ========================================================================
    // RAII Handle for Automatic Resource Return
    // ========================================================================

    class Handle {
    public:
        Handle() = default;

        Handle(PooledResourcePtr resource, ResourcePool* pool) : resource_(resource), pool_(pool) {}

        ~Handle() {
            if (resource_ && pool_) {
                pool_->returnResource(resource_);
            }
        }

        // Move-only semantics
        Handle(Handle&& other) noexcept
            : resource_(std::move(other.resource_)), pool_(other.pool_) {
            other.pool_ = nullptr;
        }

        Handle& operator=(Handle&& other) noexcept {
            if (this != &other) {
                if (resource_ && pool_) {
                    pool_->returnResource(resource_);
                }
                resource_ = std::move(other.resource_);
                pool_ = other.pool_;
                other.pool_ = nullptr;
            }
            return *this;
        }

        // Delete copy operations
        Handle(const Handle&) = delete;
        Handle& operator=(const Handle&) = delete;

        // Resource access
        ResourceType* operator->() { return resource_ ? &(**resource_) : nullptr; }
        const ResourceType* operator->() const { return resource_ ? &(**resource_) : nullptr; }
        ResourceType& operator*() { return **resource_; }
        const ResourceType& operator*() const { return **resource_; }

        bool isValid() const { return resource_ != nullptr; }
        explicit operator bool() const { return isValid(); }

        PooledResourcePtr get() const { return resource_; }

    private:
        PooledResourcePtr resource_;
        ResourcePool* pool_ = nullptr;
    };

    // ========================================================================
    // Constructor and Destructor
    // ========================================================================

    explicit ResourcePool(const PoolConfig<ResourceType>& config, ResourceFactory factory,
                          ResourceValidator validator = nullptr)
        : config_(config), factory_(factory), validator_(validator) {
        if (config_.preCreateResources) {
            for (size_t i = 0; i < config_.minSize; ++i) {
                auto id = generateResourceId();
                if (auto result = factory_(id); result) {
                    auto pooled =
                        std::make_shared<PooledResource<ResourceType>>(result.value(), id);
                    available_.push(pooled);
                    totalResources_++;
                }
            }
        }
    }

    ~ResourcePool() { shutdown(); }

    // ========================================================================
    // Resource Acquisition
    // ========================================================================

    Result<Handle> acquire(std::chrono::milliseconds timeout = std::chrono::milliseconds(0)) {
        std::unique_lock<std::mutex> lock(mutex_);

        // Use provided timeout or default from config
        if (timeout == std::chrono::milliseconds(0)) {
            timeout =
                std::chrono::duration_cast<std::chrono::milliseconds>(config_.acquisitionTimeout);
        }

        // Try to get an available resource
        if (cv_.wait_for(lock, timeout, [this] {
                return !available_.empty() || totalResources_ < config_.maxSize || shutdown_;
            })) {
            if (shutdown_) {
                return Error{ErrorCode::SystemShutdown, "Resource pool is shutting down"};
            }

            PooledResourcePtr resource;

            // Get existing resource if available
            if (!available_.empty()) {
                resource = available_.front();
                available_.pop();

                // Validate if configured
                if (config_.validateOnAcquire && validator_) {
                    if (!validator_(**resource)) {
                        // Resource is invalid, create a new one
                        totalResources_--;
                        resource = nullptr;
                    }
                }
            }

            // Create new resource if needed and allowed.
            // Reserve a slot, then create outside the lock to avoid blocking the pool.
            if (!resource && totalResources_ < config_.maxSize) {
                auto id = generateResourceId();
                // Reserve capacity so other threads don't oversubscribe
                totalResources_++;
                lock.unlock();
                auto result = factory_(id);
                lock.lock();
                if (!result) {
                    // Creation failed; release reservation
                    totalResources_--;
                    cv_.notify_one();
                    return result.error();
                }

                resource = std::make_shared<PooledResource<ResourceType>>(result.value(), id);
            }

            if (resource) {
                resource->markUsed();
                inUse_.insert({resource->getId(), resource});
                return Handle(resource, this);
            }
        }

        return Error{ErrorCode::Timeout, "Failed to acquire resource within timeout"};
    }

    // ========================================================================
    // Resource Management
    // ========================================================================

    void returnResource(PooledResourcePtr resource) {
        if (!resource)
            return;

        std::unique_lock<std::mutex> lock(mutex_);

        // Remove from in-use set
        inUse_.erase(resource->getId());

        // Validate on return if configured
        if (config_.validateOnReturn && validator_) {
            if (!validator_(**resource)) {
                // Resource is invalid, don't return to pool
                totalResources_--;
                cv_.notify_one();
                return;
            }
        }

        // Check if resource is expired
        if (resource->isExpired(config_.idleTimeout)) {
            totalResources_--;
            cv_.notify_one();
            return;
        }

        // Return to available pool if not at max idle
        if (available_.size() < config_.maxIdle) {
            available_.push(resource);
        } else {
            totalResources_--;
        }

        cv_.notify_one();
    }

    // Clean up expired resources
    void evictExpired() {
        std::unique_lock<std::mutex> lock(mutex_);

        std::queue<PooledResourcePtr> active;
        while (!available_.empty()) {
            auto resource = available_.front();
            available_.pop();

            if (!resource->isExpired(config_.idleTimeout)) {
                active.push(resource);
            } else {
                totalResources_--;
            }
        }

        available_ = std::move(active);
    }

    // ========================================================================
    // Pool Statistics
    // ========================================================================

    struct Stats {
        size_t totalResources;
        size_t availableResources;
        size_t inUseResources;
        size_t maxSize;
        size_t minSize;
    };

    Stats getStats() const {
        std::unique_lock<std::mutex> lock(mutex_);
        return Stats{.totalResources = totalResources_,
                     .availableResources = available_.size(),
                     .inUseResources = inUse_.size(),
                     .maxSize = config_.maxSize,
                     .minSize = config_.minSize};
    }

    // ========================================================================
    // Lifecycle
    // ========================================================================

    void shutdown() {
        std::unique_lock<std::mutex> lock(mutex_);
        shutdown_ = true;

        // Clear all resources
        while (!available_.empty()) {
            available_.pop();
        }
        inUse_.clear();
        totalResources_ = 0;

        cv_.notify_all();
    }

private:
    std::string generateResourceId() { return "resource_" + std::to_string(nextId_++); }

    PoolConfig<ResourceType> config_;
    ResourceFactory factory_;
    ResourceValidator validator_;
    ResourceDestructor destructor_;

    mutable std::mutex mutex_;
    std::condition_variable cv_;

    std::queue<PooledResourcePtr> available_;
    std::unordered_map<std::string, PooledResourcePtr> inUse_;

    size_t totalResources_ = 0;
    size_t nextId_ = 0;
    bool shutdown_ = false;
};

} // namespace yams::daemon
