#include <atomic>
#include <chrono>
#include <set>
#include <thread>
#include <gtest/gtest.h>
#include <yams/daemon/resource/resource_pool.h>

namespace yams::daemon::test {

using namespace std::chrono_literals;

// Test resource for pooling
class TestResource {
public:
    explicit TestResource(int id) : id_(id), valid_(true) { creationCount_++; }

    ~TestResource() { destructionCount_++; }

    int getId() const { return id_; }
    bool isValid() const { return valid_; }
    void invalidate() { valid_ = false; }

    void doWork() const {
        workCount_++;
        std::this_thread::sleep_for(1ms);
    }

    static void resetCounters() {
        creationCount_ = 0;
        destructionCount_ = 0;
        workCount_ = 0;
    }

    static int getCreationCount() { return creationCount_; }
    static int getDestructionCount() { return destructionCount_; }
    static int getWorkCount() { return workCount_; }

private:
    int id_;
    bool valid_;
    static std::atomic<int> creationCount_;
    static std::atomic<int> destructionCount_;
    static std::atomic<int> workCount_;
};

std::atomic<int> TestResource::creationCount_{0};
std::atomic<int> TestResource::destructionCount_{0};
std::atomic<int> TestResource::workCount_{0};

class ResourcePoolTest : public ::testing::Test {
protected:
    void SetUp() override {
        TestResource::resetCounters();
        resourceIdCounter_ = 0;
    }

    void TearDown() override {
        pool_.reset();
        // Allow time for resources to be destroyed
        std::this_thread::sleep_for(10ms);
    }

    PoolConfig<TestResource> createDefaultConfig() {
        PoolConfig<TestResource> config;
        config.minSize = 2;
        config.maxSize = 5;
        config.maxIdle = 3;
        config.idleTimeout = std::chrono::seconds(1);
        config.acquisitionTimeout = std::chrono::seconds(1);
        config.validateOnAcquire = true;
        config.preCreateResources = true;
        return config;
    }

    std::shared_ptr<TestResource> createResource(const std::string& /*id*/) {
        return std::make_shared<TestResource>(++resourceIdCounter_);
    }

    bool validateResource(const TestResource& resource) { return resource.isValid(); }

    std::unique_ptr<ResourcePool<TestResource>> pool_;
    std::atomic<int> resourceIdCounter_{0};
};

// Test basic pool creation and resource pre-creation
TEST_F(ResourcePoolTest, PoolCreation) {
    auto config = createDefaultConfig();
    config.minSize = 3;
    config.preCreateResources = true;

    pool_ = std::make_unique<ResourcePool<TestResource>>(
        config,
        [this](const std::string& id) {
            return Result<std::shared_ptr<TestResource>>(createResource(id));
        },
        [this](const TestResource& r) { return validateResource(r); });

    // Should have pre-created minSize resources
    std::this_thread::sleep_for(10ms);
    EXPECT_EQ(TestResource::getCreationCount(), 3);

    auto stats = pool_->getStats();
    EXPECT_EQ(stats.totalResources, 3);
    EXPECT_EQ(stats.availableResources, 3);
    EXPECT_EQ(stats.inUseResources, 0);
}

// Test lazy loading (no pre-creation)
TEST_F(ResourcePoolTest, LazyLoading) {
    auto config = createDefaultConfig();
    config.minSize = 3;
    config.preCreateResources = false; // Lazy loading

    pool_ = std::make_unique<ResourcePool<TestResource>>(config, [this](const std::string& id) {
        return Result<std::shared_ptr<TestResource>>(createResource(id));
    });

    // Should not have created any resources yet
    EXPECT_EQ(TestResource::getCreationCount(), 0);

    // Acquire a resource
    auto handleResult = pool_->acquire();
    ASSERT_TRUE(handleResult) << "Failed to acquire resource: " << handleResult.error().message;

    // Now one resource should be created
    EXPECT_EQ(TestResource::getCreationCount(), 1);
}

// Test resource acquisition and release
TEST_F(ResourcePoolTest, AcquireRelease) {
    auto config = createDefaultConfig();
    pool_ = std::make_unique<ResourcePool<TestResource>>(config, [this](const std::string& id) {
        return Result<std::shared_ptr<TestResource>>(createResource(id));
    });

    // Acquire a resource
    auto handleResult = pool_->acquire();
    ASSERT_TRUE(handleResult);
    auto handle = std::move(handleResult).value();

    EXPECT_TRUE(handle.isValid());
    EXPECT_NE(handle.get(), nullptr);

    // Do some work
    handle->doWork();
    EXPECT_EQ(TestResource::getWorkCount(), 1);

    // Resource should be returned when handle goes out of scope
    {
        auto stats = pool_->getStats();
        EXPECT_EQ(stats.inUseResources, 1);
    }

    handle = ResourcePool<TestResource>::Handle(); // Release

    // Resource should be back in pool
    auto stats = pool_->getStats();
    EXPECT_EQ(stats.inUseResources, 0);
    EXPECT_GT(stats.availableResources, 0);
}

// Test maximum pool size enforcement
TEST_F(ResourcePoolTest, MaxSizeEnforcement) {
    auto config = createDefaultConfig();
    config.maxSize = 2;
    config.acquisitionTimeout = std::chrono::seconds(1);

    pool_ = std::make_unique<ResourcePool<TestResource>>(config, [this](const std::string& id) {
        return Result<std::shared_ptr<TestResource>>(createResource(id));
    });

    // Acquire max resources
    std::vector<ResourcePool<TestResource>::Handle> handles;
    for (int i = 0; i < 2; ++i) {
        auto result = pool_->acquire();
        ASSERT_TRUE(result);
        handles.push_back(std::move(result).value());
    }

    // Try to acquire one more - should timeout
    auto result = pool_->acquire(10ms);
    EXPECT_FALSE(result) << "Should not be able to acquire beyond max size";
    if (!result) {
        EXPECT_EQ(result.error().code, ErrorCode::Timeout);
    }

    // Release one
    handles.pop_back();

    // Now should be able to acquire
    result = pool_->acquire();
    EXPECT_TRUE(result) << "Should be able to acquire after release";
}

// Test resource validation
TEST_F(ResourcePoolTest, ResourceValidation) {
    auto config = createDefaultConfig();
    config.validateOnAcquire = true;

    pool_ = std::make_unique<ResourcePool<TestResource>>(
        config,
        [this](const std::string& id) {
            return Result<std::shared_ptr<TestResource>>(createResource(id));
        },
        [](const TestResource& r) { return r.isValid(); });

    // Acquire and invalidate a resource
    auto handle1Result = pool_->acquire();
    ASSERT_TRUE(handle1Result);
    auto handle1 = std::move(handle1Result).value();

    auto resourceId = handle1->getId();
    handle1->invalidate();

    // Release the invalid resource
    handle1 = ResourcePool<TestResource>::Handle();

    // Acquire again - should get a new resource (invalid one should be discarded)
    auto handle2Result = pool_->acquire();
    ASSERT_TRUE(handle2Result);
    auto handle2 = std::move(handle2Result).value();

    EXPECT_NE(handle2->getId(), resourceId) << "Should get a new resource, not the invalid one";
    EXPECT_TRUE(handle2->isValid());
}

// Test concurrent acquisition
TEST_F(ResourcePoolTest, ConcurrentAcquisition) {
    auto config = createDefaultConfig();
    config.maxSize = 10;

    pool_ = std::make_unique<ResourcePool<TestResource>>(config, [this](const std::string& id) {
        return Result<std::shared_ptr<TestResource>>(createResource(id));
    });

    const int numThreads = 20;
    const int acquisitionsPerThread = 50;
    std::atomic<int> successCount{0};
    std::atomic<int> failCount{0};

    std::vector<std::thread> threads;
    for (int t = 0; t < numThreads; ++t) {
        threads.emplace_back([this, &successCount, &failCount, acquisitionsPerThread]() {
            for (int i = 0; i < acquisitionsPerThread; ++i) {
                auto result = pool_->acquire(100ms);
                if (result) {
                    result.value()->doWork();
                    successCount++;
                    // Brief hold before release
                    std::this_thread::sleep_for(std::chrono::microseconds(100));
                } else {
                    failCount++;
                }
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    // Most should succeed
    EXPECT_GT(successCount, numThreads * acquisitionsPerThread * 0.8);
    EXPECT_EQ(TestResource::getWorkCount(), successCount);
}

// Test idle timeout and eviction
TEST_F(ResourcePoolTest, IdleEviction) {
    auto config = createDefaultConfig();
    config.maxIdle = 2;
    config.idleTimeout = std::chrono::seconds(1);
    config.minSize = 0; // Allow full eviction

    pool_ = std::make_unique<ResourcePool<TestResource>>(config, [this](const std::string& id) {
        return Result<std::shared_ptr<TestResource>>(createResource(id));
    });

    // Create some resources
    std::vector<ResourcePool<TestResource>::Handle> handles;
    for (int i = 0; i < 3; ++i) {
        auto result = pool_->acquire();
        ASSERT_TRUE(result);
        handles.push_back(std::move(result).value());
    }

    auto createdCount = TestResource::getCreationCount();
    EXPECT_EQ(createdCount, 3);

    // Release all
    handles.clear();

    // Wait for idle timeout
    std::this_thread::sleep_for(100ms);

    // Trigger eviction
    pool_->evictExpired();

    // Should have evicted idle resources
    auto stats = pool_->getStats();
    EXPECT_LE(stats.availableResources, config.maxIdle);
}

// Test pool shutdown
TEST_F(ResourcePoolTest, PoolShutdown) {
    auto config = createDefaultConfig();
    pool_ = std::make_unique<ResourcePool<TestResource>>(config, [this](const std::string& id) {
        return Result<std::shared_ptr<TestResource>>(createResource(id));
    });

    // Acquire some resources
    auto handle1 = pool_->acquire();
    ASSERT_TRUE(handle1);

    // Shutdown pool
    pool_->shutdown();

    // Should not be able to acquire after shutdown
    auto handle2 = pool_->acquire();
    EXPECT_FALSE(handle2);
    if (!handle2) {
        EXPECT_EQ(handle2.error().code, ErrorCode::SystemShutdown);
    }
}

// Test resource creation failure handling
TEST_F(ResourcePoolTest, CreationFailure) {
    auto config = createDefaultConfig();
    config.preCreateResources = false;

    int creationAttempts = 0;
    pool_ = std::make_unique<ResourcePool<TestResource>>(
        config,
        [&creationAttempts](const std::string& /*id*/) -> Result<std::shared_ptr<TestResource>> {
            creationAttempts++;
            if (creationAttempts % 2 == 0) {
                return Error{ErrorCode::InternalError, "Simulated creation failure"};
            }
            return std::make_shared<TestResource>(creationAttempts);
        });

    // First acquisition should succeed (attempt 1)
    auto result1 = pool_->acquire();
    EXPECT_TRUE(result1);

    result1 = ResourcePool<TestResource>::Handle(); // Release

    // Second acquisition might fail initially but retry (attempt 2 fails, attempt 3 succeeds)
    auto result2 = pool_->acquire();
    EXPECT_TRUE(result2) << "Should eventually succeed with retry";
}

// Test statistics tracking
TEST_F(ResourcePoolTest, Statistics) {
    auto config = createDefaultConfig();
    pool_ = std::make_unique<ResourcePool<TestResource>>(config, [this](const std::string& id) {
        return Result<std::shared_ptr<TestResource>>(createResource(id));
    });

    auto stats1 = pool_->getStats();
    // Check initial stats

    // Acquire and release
    {
        auto handle = pool_->acquire();
        ASSERT_TRUE(handle);
        handle.value()->doWork();
    }

    auto stats2 = pool_->getStats();
    // Check stats after acquire/release
    EXPECT_GT(stats2.totalResources, 0);
}

// Test move semantics for handles
TEST_F(ResourcePoolTest, HandleMoveSemantics) {
    auto config = createDefaultConfig();
    pool_ = std::make_unique<ResourcePool<TestResource>>(config, [this](const std::string& id) {
        return Result<std::shared_ptr<TestResource>>(createResource(id));
    });

    auto result = pool_->acquire();
    ASSERT_TRUE(result);
    auto handle1 = std::move(result).value();

    EXPECT_TRUE(handle1.isValid());

    // Move construct
    auto handle2 = std::move(handle1);
    EXPECT_FALSE(handle1.isValid()); // handle1 should be empty
    EXPECT_TRUE(handle2.isValid());

    // Move assign
    ResourcePool<TestResource>::Handle handle3;
    handle3 = std::move(handle2);
    EXPECT_FALSE(handle2.isValid());
    EXPECT_TRUE(handle3.isValid());
}

} // namespace yams::daemon::test