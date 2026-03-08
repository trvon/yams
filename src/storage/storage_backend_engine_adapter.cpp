#include <yams/storage/storage_backend_engine_adapter.h>

#include <future>
#include <mutex>

namespace yams::storage {

namespace {

class StorageBackendEngineAdapter final : public IStorageEngine {
public:
    explicit StorageBackendEngineAdapter(std::shared_ptr<IStorageBackend> backend)
        : backend_(std::move(backend)) {}

    Result<void> store(std::string_view hash, std::span<const std::byte> data) override {
        auto backend = withBackend();
        if (!backend) {
            return backend.error();
        }

        auto result = backend.value()->store(hash, data);
        if (result) {
            stats_.writeOperations.fetch_add(1, std::memory_order_relaxed);
            stats_.totalObjects.fetch_add(1, std::memory_order_relaxed);
            stats_.totalBytes.fetch_add(static_cast<uint64_t>(data.size()),
                                        std::memory_order_relaxed);
        } else {
            stats_.failedOperations.fetch_add(1, std::memory_order_relaxed);
        }
        return result;
    }

    Result<std::vector<std::byte>> retrieve(std::string_view hash) const override {
        auto backend = withBackend();
        if (!backend) {
            return backend.error();
        }

        auto result = backend.value()->retrieve(hash);
        if (result) {
            stats_.readOperations.fetch_add(1, std::memory_order_relaxed);
        } else {
            stats_.failedOperations.fetch_add(1, std::memory_order_relaxed);
        }
        return result;
    }

    Result<RawObject> retrieveRaw(std::string_view hash) const override {
        auto retrieved = retrieve(hash);
        if (!retrieved) {
            return retrieved.error();
        }
        RawObject out;
        out.data = std::move(retrieved.value());
        return out;
    }

    Result<bool> exists(std::string_view hash) const noexcept override {
        try {
            auto backend = withBackend();
            if (!backend) {
                return backend.error();
            }
            return backend.value()->exists(hash);
        } catch (const std::exception& e) {
            return Error{ErrorCode::InternalError, std::string("exists failed: ") + e.what()};
        } catch (...) {
            return Error{ErrorCode::InternalError, "exists failed"};
        }
    }

    Result<void> remove(std::string_view hash) override {
        auto backend = withBackend();
        if (!backend) {
            return backend.error();
        }

        auto result = backend.value()->remove(hash);
        if (result) {
            stats_.deleteOperations.fetch_add(1, std::memory_order_relaxed);
        } else {
            stats_.failedOperations.fetch_add(1, std::memory_order_relaxed);
        }
        return result;
    }

    Result<uint64_t> getBlockSize(std::string_view hash) const override {
        auto retrieved = retrieve(hash);
        if (!retrieved) {
            return retrieved.error();
        }
        return static_cast<uint64_t>(retrieved.value().size());
    }

    std::future<Result<void>> storeAsync(std::string_view hash,
                                         std::span<const std::byte> data) override {
        return std::async(std::launch::async,
                          [this, key = std::string(hash),
                           copy = std::vector<std::byte>(data.begin(), data.end())]() {
                              return store(key, copy);
                          });
    }

    std::future<Result<std::vector<std::byte>>>
    retrieveAsync(std::string_view hash) const override {
        return std::async(std::launch::async,
                          [this, key = std::string(hash)]() { return retrieve(key); });
    }

    std::future<Result<RawObject>> retrieveRawAsync(std::string_view hash) const override {
        return std::async(std::launch::async,
                          [this, key = std::string(hash)]() { return retrieveRaw(key); });
    }

    std::vector<Result<void>>
    storeBatch(const std::vector<std::pair<std::string, std::vector<std::byte>>>& items) override {
        std::vector<Result<void>> out;
        out.reserve(items.size());
        for (const auto& [hash, data] : items) {
            out.push_back(store(hash, data));
        }
        return out;
    }

    StorageStats getStats() const noexcept override { return stats_; }

    Result<uint64_t> getStorageSize() const override {
        auto backend = withBackend();
        if (!backend) {
            return backend.error();
        }
        auto keys = backend.value()->list();
        if (!keys) {
            return keys.error();
        }

        uint64_t total = 0;
        for (const auto& key : keys.value()) {
            auto object = backend.value()->retrieve(key);
            if (!object) {
                return object.error();
            }
            total += static_cast<uint64_t>(object.value().size());
        }
        return total;
    }

private:
    Result<std::shared_ptr<IStorageBackend>> withBackend() const {
        std::lock_guard<std::mutex> lock(backendMutex_);
        if (!backend_) {
            return Error{ErrorCode::NotInitialized, "Storage backend adapter is not initialized"};
        }
        return backend_;
    }

    mutable std::mutex backendMutex_;
    std::shared_ptr<IStorageBackend> backend_;
    mutable StorageStats stats_;
};

} // namespace

std::shared_ptr<IStorageEngine>
createStorageEngineFromBackend(std::unique_ptr<IStorageBackend> backend) {
    if (!backend) {
        return nullptr;
    }
    auto sharedBackend = std::shared_ptr<IStorageBackend>(std::move(backend));
    return std::make_shared<StorageBackendEngineAdapter>(std::move(sharedBackend));
}

} // namespace yams::storage
