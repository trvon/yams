#include <gtest/gtest.h>

#include <yams/crypto/hasher.h>
#include <yams/integrity/repair_manager.h>

#include <deque>
#include <future>
#include <unordered_map>

using namespace yams;
using namespace yams::integrity;

namespace {

class MemoryStorageEngine final : public storage::IStorageEngine {
public:
    Result<void> store(std::string_view hash, std::span<const std::byte> data) override {
        data_[std::string(hash)] = std::vector<std::byte>(data.begin(), data.end());
        return {};
    }

    Result<std::vector<std::byte>> retrieve(std::string_view hash) const override {
        auto it = data_.find(std::string(hash));
        if (it == data_.end()) {
            return Error{ErrorCode::NotFound, "missing"};
        }
        return it->second;
    }

    Result<IStorageEngine::RawObject> retrieveRaw(std::string_view hash) const override {
        auto it = data_.find(std::string(hash));
        if (it == data_.end()) {
            return Error{ErrorCode::NotFound, "missing"};
        }
        IStorageEngine::RawObject obj;
        obj.data = it->second;
        obj.header = std::nullopt;
        return obj;
    }

    Result<bool> exists(std::string_view hash) const noexcept override {
        return data_.contains(std::string(hash));
    }

    Result<void> remove(std::string_view hash) override {
        data_.erase(std::string(hash));
        return {};
    }

    std::future<Result<void>> storeAsync(std::string_view hash,
                                         std::span<const std::byte> data) override {
        return std::async(std::launch::deferred,
                          [this, hash = std::string(hash),
                           buffer = std::vector<std::byte>(data.begin(), data.end())]() {
                              data_[hash] = buffer;
                              return Result<void>();
                          });
    }

    std::future<Result<std::vector<std::byte>>>
    retrieveAsync(std::string_view hash) const override {
        return std::async(std::launch::deferred,
                          [this, hash = std::string(hash)]() { return retrieve(hash); });
    }

    std::future<Result<IStorageEngine::RawObject>>
    retrieveRawAsync(std::string_view hash) const override {
        return std::async(std::launch::deferred,
                          [this, hash = std::string(hash)]() { return retrieveRaw(hash); });
    }

    std::vector<Result<void>>
    storeBatch(const std::vector<std::pair<std::string, std::vector<std::byte>>>& items) override {
        std::vector<Result<void>> out;
        out.reserve(items.size());
        for (const auto& [hash, bytes] : items) {
            data_[hash] = bytes;
            out.emplace_back();
        }
        return out;
    }

    storage::StorageStats getStats() const noexcept override { return {}; }
    Result<uint64_t> getStorageSize() const override {
        return Result<uint64_t>(static_cast<uint64_t>(0));
    }

private:
    mutable std::unordered_map<std::string, std::vector<std::byte>> data_;
};

std::vector<std::byte> to_bytes(const std::string& s) {
    return std::vector<std::byte>(reinterpret_cast<const std::byte*>(s.data()),
                                  reinterpret_cast<const std::byte*>(s.data()) + s.size());
}

std::string computeHash(std::span<const std::byte> data) {
    auto hasher = crypto::createSHA256Hasher();
    hasher->init();
    hasher->update(data);
    return hasher->finalize();
}

} // namespace

TEST(RepairManager, AttemptsStrategiesInOrder) {
    MemoryStorageEngine storage;
    RepairManagerConfig config;

    const std::string payload = "hello-world";
    const auto bytes = to_bytes(payload);
    const auto hash = computeHash(std::span<const std::byte>(bytes.data(), bytes.size()));

    bool backupCalled = false;
    config.backupFetcher = [&](const std::string&) -> Result<std::vector<std::byte>> {
        backupCalled = true;
        return bytes;
    };

    RepairManager manager(storage, config);
    EXPECT_TRUE(manager.attemptRepair(hash));
    EXPECT_TRUE(backupCalled);

    auto retrieved = storage.retrieve(hash);
    ASSERT_TRUE(retrieved.has_value());
    EXPECT_EQ(retrieved.value(), bytes);
}

TEST(RepairManager, SkipsMissingFetchers) {
    MemoryStorageEngine storage;
    RepairManagerConfig config;
    const std::string payload = "hello";
    const auto bytes = to_bytes(payload);
    const auto hash = computeHash(std::span<const std::byte>(bytes.data(), bytes.size()));

    bool p2pCalled = false;
    config.p2pFetcher = [&](const std::string&) -> Result<std::vector<std::byte>> {
        p2pCalled = true;
        return Error{ErrorCode::NetworkError, "unreachable"};
    };

    RepairManager manager(storage, config);
    EXPECT_FALSE(manager.attemptRepair(hash, {RepairStrategy::FromP2P}));
    EXPECT_TRUE(p2pCalled);
    EXPECT_FALSE(storage.exists(hash).value());
}
