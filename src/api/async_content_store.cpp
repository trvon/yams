#include <yams/api/async_content_store.h>
#include <yams/api/content_store_error.h>

#include <spdlog/spdlog.h>

#include <condition_variable>
#include <future>
#include <queue>
#include <thread>

namespace yams::api {

// Implementation details
struct AsyncContentStore::Impl {
    std::shared_ptr<IContentStore> store;
    size_t maxConcurrentOps;

    // Operation tracking
    mutable std::mutex opMutex;
    std::condition_variable opCv;
    std::atomic<size_t> pendingOps{0};
    std::atomic<bool> cancelRequested{false};

    // Future tracking for cancellation
    std::vector<std::future<void>> pendingFutures;

    Impl(std::shared_ptr<IContentStore> s, size_t maxOps)
        : store(std::move(s)), maxConcurrentOps(maxOps) {}

    void waitForSlot() {
        std::unique_lock lock(opMutex);
        opCv.wait(lock, [this] { return pendingOps < maxConcurrentOps || cancelRequested; });

        if (cancelRequested) {
            throw OperationCancelledException("Async operations cancelled");
        }

        ++pendingOps;
    }

    void releaseSlot() {
        {
            std::lock_guard lock(opMutex);
            --pendingOps;
        }
        opCv.notify_one();
    }

    template <typename F> auto runAsync(F&& func) -> std::future<decltype(func())> {
        waitForSlot();

        return std::async(std::launch::async, [this, func = std::forward<F>(func)]() {
            // Ensure slot is released even if exception occurs
            struct SlotGuard {
                Impl* impl;
                ~SlotGuard() { impl->releaseSlot(); }
            } guard{this};

            return func();
        });
    }
};

// Constructor and destructor
AsyncContentStore::AsyncContentStore(std::shared_ptr<IContentStore> store)
    : pImpl(std::make_unique<Impl>(std::move(store), 10)), store_(pImpl->store) {}

AsyncContentStore::~AsyncContentStore() {
    // Wait for all pending operations
    waitAll();
}

AsyncContentStore::AsyncContentStore(AsyncContentStore&&) noexcept = default;
AsyncContentStore& AsyncContentStore::operator=(AsyncContentStore&&) noexcept = default;

// Future-based async operations
std::future<Result<StoreResult>> AsyncContentStore::storeAsync(const std::filesystem::path& path,
                                                               const ContentMetadata& metadata,
                                                               ProgressCallback progress) {
    return pImpl->runAsync([store = pImpl->store, path, metadata, progress]() {
        return store->store(path, metadata, progress);
    });
}

std::future<Result<RetrieveResult>>
AsyncContentStore::retrieveAsync(const std::string& hash, const std::filesystem::path& outputPath,
                                 ProgressCallback progress) {
    return pImpl->runAsync([store = pImpl->store, hash, outputPath, progress]() {
        return store->retrieve(hash, outputPath, progress);
    });
}

std::future<Result<StoreResult>>
AsyncContentStore::storeStreamAsync(std::istream& stream, const ContentMetadata& metadata,
                                    ProgressCallback progress) {
    // Note: Stream reference capture requires careful handling
    // In production, would need to ensure stream lifetime
    return pImpl->runAsync([store = pImpl->store, &stream, metadata, progress]() {
        return store->storeStream(stream, metadata, progress);
    });
}

std::future<Result<RetrieveResult>>
AsyncContentStore::retrieveStreamAsync(const std::string& hash, std::ostream& output,
                                       ProgressCallback progress) {
    return pImpl->runAsync([store = pImpl->store, hash, &output, progress]() {
        return store->retrieveStream(hash, output, progress);
    });
}

std::future<Result<bool>> AsyncContentStore::existsAsync(const std::string& hash) {
    return pImpl->runAsync([store = pImpl->store, hash]() { return store->exists(hash); });
}

std::future<Result<bool>> AsyncContentStore::removeAsync(const std::string& hash) {
    return pImpl->runAsync([store = pImpl->store, hash]() { return store->remove(hash); });
}

std::future<Result<ContentMetadata>> AsyncContentStore::getMetadataAsync(const std::string& hash) {
    return pImpl->runAsync([store = pImpl->store, hash]() { return store->getMetadata(hash); });
}

std::future<Result<void>> AsyncContentStore::updateMetadataAsync(const std::string& hash,
                                                                 const ContentMetadata& metadata) {
    return pImpl->runAsync(
        [store = pImpl->store, hash, metadata]() { return store->updateMetadata(hash, metadata); });
}

// Callback-based async operations
void AsyncContentStore::storeAsync(const std::filesystem::path& path, StoreCallback callback,
                                   const ContentMetadata& metadata, ProgressCallback progress) {
    auto future = storeAsync(path, metadata, progress);

    // Launch a task to wait for result and invoke callback
    std::thread([future = std::move(future), callback = std::move(callback)]() mutable {
        try {
            auto result = future.get();
            callback(result);
        } catch (const std::exception& e) {
            callback(Result<StoreResult>(ErrorCode::Unknown));
        }
    }).detach();
}

void AsyncContentStore::retrieveAsync(const std::string& hash,
                                      const std::filesystem::path& outputPath,
                                      RetrieveCallback callback, ProgressCallback progress) {
    auto future = retrieveAsync(hash, outputPath, progress);

    std::thread([future = std::move(future), callback = std::move(callback)]() mutable {
        try {
            auto result = future.get();
            callback(result);
        } catch (const std::exception& e) {
            callback(Result<RetrieveResult>(ErrorCode::Unknown));
        }
    }).detach();
}

void AsyncContentStore::existsAsync(const std::string& hash, BoolCallback callback) {
    auto future = existsAsync(hash);

    std::thread([future = std::move(future), callback = std::move(callback)]() mutable {
        try {
            auto result = future.get();
            callback(result);
        } catch (const std::exception& e) {
            callback(Result<bool>(ErrorCode::Unknown));
        }
    }).detach();
}

void AsyncContentStore::removeAsync(const std::string& hash, BoolCallback callback) {
    auto future = removeAsync(hash);

    std::thread([future = std::move(future), callback = std::move(callback)]() mutable {
        try {
            auto result = future.get();
            callback(result);
        } catch (const std::exception& e) {
            callback(Result<bool>(ErrorCode::Unknown));
        }
    }).detach();
}

void AsyncContentStore::getMetadataAsync(const std::string& hash, MetadataCallback callback) {
    auto future = getMetadataAsync(hash);

    std::thread([future = std::move(future), callback = std::move(callback)]() mutable {
        try {
            auto result = future.get();
            callback(result);
        } catch (const std::exception& e) {
            callback(Result<ContentMetadata>(ErrorCode::Unknown));
        }
    }).detach();
}

void AsyncContentStore::updateMetadataAsync(const std::string& hash,
                                            const ContentMetadata& metadata,
                                            VoidCallback callback) {
    auto future = updateMetadataAsync(hash, metadata);

    std::thread([future = std::move(future), callback = std::move(callback)]() mutable {
        try {
            auto result = future.get();
            callback(result);
        } catch (const std::exception& e) {
            callback(Result<void>(ErrorCode::Unknown));
        }
    }).detach();
}

// Batch operations
std::future<std::vector<Result<StoreResult>>>
AsyncContentStore::storeBatchAsync(const std::vector<std::filesystem::path>& paths,
                                   const std::vector<ContentMetadata>& metadata) {
    return pImpl->runAsync(
        [store = pImpl->store, paths, metadata]() { return store->storeBatch(paths, metadata); });
}

std::future<std::vector<Result<bool>>>
AsyncContentStore::removeBatchAsync(const std::vector<std::string>& hashes) {
    return pImpl->runAsync([store = pImpl->store, hashes]() { return store->removeBatch(hashes); });
}

// Maintenance operations
std::future<Result<void>> AsyncContentStore::verifyAsync(ProgressCallback progress) {
    return pImpl->runAsync([store = pImpl->store, progress]() { return store->verify(progress); });
}

std::future<Result<void>> AsyncContentStore::compactAsync(ProgressCallback progress) {
    return pImpl->runAsync([store = pImpl->store, progress]() { return store->compact(progress); });
}

std::future<Result<void>> AsyncContentStore::garbageCollectAsync(ProgressCallback progress) {
    return pImpl->runAsync(
        [store = pImpl->store, progress]() { return store->garbageCollect(progress); });
}

// Concurrency control
void AsyncContentStore::setMaxConcurrentOperations(size_t max) {
    if (max == 0) {
        throw std::invalid_argument("Max concurrent operations must be > 0");
    }

    std::lock_guard lock(pImpl->opMutex);
    pImpl->maxConcurrentOps = max;
    pImpl->opCv.notify_all();
}

size_t AsyncContentStore::getMaxConcurrentOperations() const {
    std::lock_guard lock(pImpl->opMutex);
    return pImpl->maxConcurrentOps;
}

size_t AsyncContentStore::getPendingOperations() const {
    return pImpl->pendingOps.load();
}

void AsyncContentStore::waitAll() {
    std::unique_lock lock(pImpl->opMutex);
    pImpl->opCv.wait(lock, [this] { return pImpl->pendingOps == 0; });
}

void AsyncContentStore::cancelAll() {
    {
        std::lock_guard lock(pImpl->opMutex);
        pImpl->cancelRequested = true;
    }
    pImpl->opCv.notify_all();

    // Note: This doesn't actually cancel running operations,
    // just prevents new ones from starting
    spdlog::warn("Cancel requested for async operations");
}

// Factory function
std::unique_ptr<AsyncContentStore> createAsyncContentStore(std::shared_ptr<IContentStore> store,
                                                           size_t maxConcurrentOps) {
    auto asyncStore = std::make_unique<AsyncContentStore>(std::move(store));
    asyncStore->setMaxConcurrentOperations(maxConcurrentOps);
    return asyncStore;
}

} // namespace yams::api