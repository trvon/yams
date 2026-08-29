// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

// pi-lens-ignore: fatal error
#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <cstring>
#include <filesystem>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <yams/compat/thread_stop_compat.h>
#include <yams/memory_sync/memory_sync_service.h>
#include <yams/storage/storage_backend.h>

using namespace yams::memory_sync;

// Catch2 assertion decomposition intentionally resembles chained comparisons to clang-tidy.
// NOLINTBEGIN(bugprone-chained-comparison)

namespace {

std::vector<std::byte> bytes(std::string_view text) {
    std::vector<std::byte> out(text.size());
    std::memcpy(out.data(), text.data(), text.size());
    return out;
}

std::string text(const std::vector<std::byte>& data) {
    return std::string(reinterpret_cast<const char*>(data.data()), data.size());
}

class CountingBackend : public yams::storage::IStorageBackend {
public:
    yams::Result<void> initialize(const yams::storage::BackendConfig& config) override {
        return backend_.initialize(config);
    }
    yams::Result<void> store(std::string_view key, std::span<const std::byte> data) override {
        return backend_.store(key, data);
    }
    yams::Result<std::vector<std::byte>> retrieve(std::string_view key) const override {
        return backend_.retrieve(key);
    }
    yams::Result<bool> exists(std::string_view key) const override { return backend_.exists(key); }
    yams::Result<void> remove(std::string_view key) override { return backend_.remove(key); }
    yams::Result<std::vector<std::string>> list(std::string_view prefix) const override {
        return backend_.list(prefix);
    }
    yams::Result<yams::storage::ObjectListPage> listPage(std::string_view prefix,
                                                         std::optional<std::string_view> cursor,
                                                         std::size_t limit) const override {
        if (prefix == "index/") {
            listPageCalls.fetch_add(1, std::memory_order_relaxed);
        }
        return backend_.listPage(prefix, cursor, limit);
    }
    yams::Result<::yams::StorageStats> getStats() const override { return backend_.getStats(); }
    std::future<yams::Result<void>> storeAsync(std::string_view key,
                                               std::span<const std::byte> data) override {
        return backend_.storeAsync(key, data);
    }
    std::future<yams::Result<std::vector<std::byte>>>
    retrieveAsync(std::string_view key) const override {
        return backend_.retrieveAsync(key);
    }
    std::string getType() const override { return backend_.getType(); }
    bool isRemote() const override { return false; }
    yams::Result<void> flush() override { return backend_.flush(); }

    mutable std::atomic<std::size_t> listPageCalls{0};

private:
    yams::storage::FilesystemBackend backend_;
};

class FailPrefixOnceBackend final : public CountingBackend {
public:
    void failNextStore(std::string prefix) {
        failPrefix_ = std::move(prefix);
        armed_.store(true, std::memory_order_release);
    }

    yams::Result<void> store(std::string_view key, std::span<const std::byte> data) override {
        if (armed_.load(std::memory_order_acquire) && key.starts_with(failPrefix_) &&
            armed_.exchange(false, std::memory_order_acq_rel)) {
            return yams::Error{yams::ErrorCode::IOError, "injected prefix store failure"};
        }
        return CountingBackend::store(key, data);
    }

private:
    std::atomic<bool> armed_{false};
    std::string failPrefix_;
};

class ThrowOnceListBackend final : public CountingBackend {
public:
    yams::Result<yams::storage::ObjectListPage> listPage(std::string_view prefix,
                                                         std::optional<std::string_view> cursor,
                                                         std::size_t limit) const override {
        if (!thrown_.exchange(true, std::memory_order_acq_rel)) {
            throw std::runtime_error("injected list failure");
        }
        return CountingBackend::listPage(prefix, cursor, limit);
    }

private:
    mutable std::atomic<bool> thrown_{false};
};

class ThrowOnStoreBackend final : public CountingBackend {
public:
    explicit ThrowOnStoreBackend(std::size_t throwOnCall) : throwOnCall_(throwOnCall) {}

    yams::Result<void> store(std::string_view key, std::span<const std::byte> data) override {
        const auto call = storeCalls.fetch_add(1, std::memory_order_acq_rel) + 1;
        if (call == throwOnCall_) {
            throw std::runtime_error("injected lease store failure");
        }
        return CountingBackend::store(key, data);
    }

    std::atomic<std::size_t> storeCalls{0};

private:
    std::size_t throwOnCall_;
};

class BlockingListBackend final : public CountingBackend {
public:
    std::future<void> enteredFuture() { return entered_.get_future(); }

    yams::Result<yams::storage::ObjectListPage>
    listPage(std::string_view, std::optional<std::string_view>, std::size_t) const override {
        if (!enteredOnce_.exchange(true, std::memory_order_acq_rel)) {
            entered_.set_value();
        }
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this] { return cancelled_.load(std::memory_order_acquire); });
        return yams::Error{yams::ErrorCode::OperationCancelled, "blocking list cancelled"};
    }

    void requestCancel() noexcept override {
        cancelCalls.fetch_add(1, std::memory_order_relaxed);
        cancelled_.store(true, std::memory_order_release);
        cv_.notify_all();
    }

    void resetCancel() noexcept override {
        cancelled_.store(false, std::memory_order_release);
        enteredOnce_.store(false, std::memory_order_release);
    }

    std::atomic<std::uint32_t> cancelCalls{0};

private:
    mutable std::promise<void> entered_;
    mutable std::atomic<bool> enteredOnce_{false};
    mutable std::mutex mutex_;
    mutable std::condition_variable cv_;
    std::atomic<bool> cancelled_{false};
};

/// Blocks `listPage` while armed, releasing when unarmed. Used to prove IPC
/// readers never wait on a slow reconciliation: publish first (unarmed), arm,
/// start the worker, then assert readers return promptly from the committed
/// snapshot while the worker is stuck mid-sync.
class SlowListBackend final : public CountingBackend {
public:
    void arm() { armed_.store(true, std::memory_order_release); }

    void releaseList() {
        armed_.store(false, std::memory_order_release);
        cv_.notify_all();
    }

    void waitUntilBlocked() const {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this] { return blocked_.load(std::memory_order_acquire); });
    }

    yams::Result<yams::storage::ObjectListPage> listPage(std::string_view prefix,
                                                         std::optional<std::string_view> cursor,
                                                         std::size_t limit) const override {
        if (armed_.load(std::memory_order_acquire)) {
            {
                std::lock_guard<std::mutex> lock(mutex_);
                blocked_.store(true, std::memory_order_release);
            }
            cv_.notify_all();
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait(lock, [this] { return !armed_.load(std::memory_order_acquire); });
            blocked_.store(false, std::memory_order_release);
        }
        return CountingBackend::listPage(prefix, cursor, limit);
    }

private:
    mutable std::atomic<bool> armed_{false};
    mutable std::atomic<bool> blocked_{false};
    mutable std::mutex mutex_;
    mutable std::condition_variable cv_;
};

std::unique_ptr<yams::storage::FilesystemBackend> makeBackend(const std::filesystem::path& dir) {
    yams::storage::BackendConfig config;
    config.type = "filesystem";
    config.localPath = dir;
    auto backend = std::make_unique<yams::storage::FilesystemBackend>();
    REQUIRE(backend->initialize(config).has_value());
    return backend;
}

struct TempDirGuard {
    TempDirGuard() {
        path = std::filesystem::temp_directory_path() /
               ("yams-memory-sync-service-" +
                std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        std::filesystem::create_directories(path);
    }
    ~TempDirGuard() {
        std::error_code ec;
        std::filesystem::remove_all(path, ec);
    }
    std::filesystem::path path;
};

} // namespace

TEST_CASE("MemorySyncService start/stop is idempotent and clean", "[memory-sync][service]") {
    TempDirGuard tmp;
    MemorySyncService service{makeBackend(tmp.path), MemorySyncConfig{"A", 50}};

    REQUIRE(service.start().has_value());
    CHECK(service.started());
    REQUIRE(service.start().has_value()); // idempotent

    service.stop();
    CHECK_FALSE(service.started());
    service.stop(); // idempotent, noexcept
    CHECK_FALSE(service.started());
}

TEST_CASE("MemorySyncService publish and read round-trips", "[memory-sync][service]") {
    TempDirGuard tmp;
    MemorySyncService service{makeBackend(tmp.path), MemorySyncConfig{"A", 50}};

    REQUIRE(service.publish("slot", bytes("hello service")).has_value());
    const auto value = service.read("slot");
    REQUIRE(value.has_value());
    CHECK(text(value.value()) == "hello service");
}

TEST_CASE("MemorySyncService replicates user-record tombstones", "[memory-sync][service]") {
    TempDirGuard tmp;
    MemorySyncService writer{makeBackend(tmp.path), MemorySyncConfig{"A", 50}};
    MemorySyncService reader{makeBackend(tmp.path), MemorySyncConfig{"B", 50}};

    REQUIRE(writer.publish("user/key", bytes("value")).has_value());
    REQUIRE(reader.syncOnce().has_value());
    REQUIRE(reader.readCached("user/key").has_value());
    REQUIRE(writer.erase("user/key", "key").has_value());
    const auto merged = reader.syncOnce();
    REQUIRE(merged.has_value());
    CHECK(merged.value().at("user/key").isTombstone());
    CHECK_FALSE(reader.readCached("user/key").has_value());
}

TEST_CASE("MemorySyncService worker replays a retained ready erase",
          "[memory-sync][service][tombstone][outbox]") {
    TempDirGuard dir;
    yams::storage::BackendConfig backendConfig;
    backendConfig.type = "filesystem";
    backendConfig.localPath = dir.path;
    auto backend = std::make_unique<FailPrefixOnceBackend>();
    auto* observed = backend.get();
    REQUIRE(backend->initialize(backendConfig).has_value());
    MemorySyncService service{std::move(backend),
                              MemorySyncConfig{"writer", 10, "outbox-service-corpus", 1}};
    REQUIRE(service.publish("user/key", bytes("value")).has_value());
    observed->failNextStore("index/");
    auto failed = service.erase("user/key", "key");
    REQUIRE_FALSE(failed.has_value());
    auto pending = service.pendingErases();
    REQUIRE(pending.has_value());
    REQUIRE(pending.value().size() == 1);
    CHECK(pending.value().front().ready);
    CHECK(pending.value().front().prepared);

    REQUIRE(service.start().has_value());
    bool drained = false;
    for (int attempt = 0; attempt < 100; ++attempt) {
        auto retained = service.pendingErases();
        REQUIRE(retained.has_value());
        if (retained.value().empty()) {
            drained = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{5});
    }
    service.stop();
    REQUIRE(drained);
    CHECK_FALSE(service.readCached("user/key").has_value());
    CHECK(service.replicationState().commitments.at("writer").counter == 2);
}

TEST_CASE("MemorySyncService two nodes converge", "[memory-sync][service]") {
    TempDirGuard tmp;
    MemorySyncService a{makeBackend(tmp.path), MemorySyncConfig{"A", 50}};
    MemorySyncService b{makeBackend(tmp.path), MemorySyncConfig{"B", 50}};

    REQUIRE(a.publish("a-key", bytes("from-a")).has_value());
    REQUIRE(b.publish("b-key", bytes("from-b")).has_value());

    const auto mergedA = a.syncOnce();
    const auto mergedB = b.syncOnce();
    REQUIRE(mergedA.has_value());
    REQUIRE(mergedB.has_value());

    CHECK(mergedA.value().count("a-key") == 1);
    CHECK(mergedB.value().count("b-key") == 1);
    CHECK(mergedA.value().at("a-key").entryHash == mergedB.value().at("a-key").entryHash);
}

TEST_CASE("MemorySyncService publishIfChanged suppresses unchanged causal envelopes",
          "[memory-sync][service]") {
    TempDirGuard tmp;
    MemorySyncService service{makeBackend(tmp.path), MemorySyncConfig{"A", 25}};

    auto first = service.publishIfChanged("stable", bytes("same"));
    REQUIRE(first.has_value());
    CHECK(first.value());
    auto before = service.syncOnce();
    REQUIRE(before.has_value());

    auto second = service.publishIfChanged("stable", bytes("same"));
    REQUIRE(second.has_value());
    CHECK_FALSE(second.value());
    auto after = service.syncOnce();
    REQUIRE(after.has_value());
    const auto& beforeRecord = before.value().at("stable");
    const auto& afterRecord = after.value().at("stable");
    CHECK(afterRecord.entryHash == beforeRecord.entryHash);
    CHECK(afterRecord.ts == beforeRecord.ts);
    CHECK(afterRecord.origin == beforeRecord.origin);
    CHECK(afterRecord.vv.counters() == beforeRecord.vv.counters());
}

TEST_CASE("MemorySyncService invokes apply callback after periodic reconciliation",
          "[memory-sync][service]") {
    TempDirGuard tmp;
    MemorySyncService service{makeBackend(tmp.path), MemorySyncConfig{"A", 25}};
    std::atomic<std::uint32_t> callbacks{0};
    service.setAfterSyncCallback([&service, &callbacks] {
        if (service.syncOnce().has_value()) {
            callbacks.fetch_add(1, std::memory_order_relaxed);
        }
    });

    REQUIRE(service.start().has_value());
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (callbacks.load(std::memory_order_relaxed) == 0 &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    CHECK(callbacks.load(std::memory_order_relaxed) > 0);
    CHECK(service.successfulSyncCycles() >= 1);
    CHECK(service.failedSyncCycles() == 0);
    CHECK(service.lastSuccessfulSyncAgeMs() < 2000);
    service.stop();
}

TEST_CASE("MemorySyncService contains reconciliation exceptions and retries",
          "[memory-sync][service][lifecycle][exception]") {
    TempDirGuard tmp;
    auto backend = std::make_unique<ThrowOnceListBackend>();
    yams::storage::BackendConfig config;
    config.type = "filesystem";
    config.localPath = tmp.path;
    REQUIRE(backend->initialize(config).has_value());
    MemorySyncService service{std::move(backend), MemorySyncConfig{"A", 25}};

    REQUIRE(service.start().has_value());
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (service.successfulSyncCycles() == 0 && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    CHECK(service.failedSyncCycles() >= 1);
    CHECK(service.successfulSyncCycles() >= 1);
    service.stop();
    CHECK_FALSE(service.started());
}

TEST_CASE("MemorySyncService converts initial lease exceptions to errors",
          "[memory-sync][service][lifecycle][exception][temporary]") {
    TempDirGuard tmp;
    auto backend = std::make_unique<CountingBackend>();
    auto maintenance = std::make_unique<ThrowOnStoreBackend>(1);
    yams::storage::BackendConfig backendConfig;
    backendConfig.type = "filesystem";
    backendConfig.localPath = tmp.path / "session";
    yams::storage::BackendConfig maintenanceConfig;
    maintenanceConfig.type = "filesystem";
    maintenanceConfig.localPath = tmp.path / "maintenance";
    REQUIRE(backend->initialize(backendConfig).has_value());
    REQUIRE(maintenance->initialize(maintenanceConfig).has_value());
    MemorySyncService service{std::move(backend),     MemorySyncConfig{"A", 25}, {}, false,
                              std::move(maintenance), "sessions/test/lease"};

    std::optional<yams::Result<void>> started;
    CHECK_NOTHROW(started.emplace(service.start()));
    REQUIRE(started.has_value());
    CHECK_FALSE(started->has_value());
    CHECK_FALSE(service.started());
}

TEST_CASE("MemorySyncService contains lease refresh exceptions and retries",
          "[memory-sync][service][lifecycle][exception][temporary]") {
    TempDirGuard tmp;
    auto backend = std::make_unique<CountingBackend>();
    auto maintenance = std::make_unique<ThrowOnStoreBackend>(2);
    auto* observedMaintenance = maintenance.get();
    yams::storage::BackendConfig backendConfig;
    backendConfig.type = "filesystem";
    backendConfig.localPath = tmp.path / "session";
    yams::storage::BackendConfig maintenanceConfig;
    maintenanceConfig.type = "filesystem";
    maintenanceConfig.localPath = tmp.path / "maintenance";
    REQUIRE(backend->initialize(backendConfig).has_value());
    REQUIRE(maintenance->initialize(maintenanceConfig).has_value());
    MemorySyncService service{std::move(backend),     MemorySyncConfig{"A", 25}, {}, false,
                              std::move(maintenance), "sessions/test/lease"};
    std::atomic<std::size_t> callbacks{0};
    std::promise<void> firstCallback;
    auto firstCallbackDone = firstCallback.get_future();
    service.setAfterSyncCallback([&] {
        if (callbacks.fetch_add(1, std::memory_order_relaxed) == 0) {
            firstCallback.set_value();
        }
    });

    REQUIRE(service.start().has_value());
    REQUIRE(firstCallbackDone.wait_for(std::chrono::seconds(2)) == std::future_status::ready);
    CHECK(observedMaintenance->storeCalls.load(std::memory_order_acquire) >= 3);
    CHECK(service.failedSyncCycles() >= 1);
    CHECK(service.successfulSyncCycles() >= 1);
    CHECK(callbacks.load(std::memory_order_relaxed) >= 1);
    service.stop();
    CHECK_FALSE(service.started());
}

TEST_CASE("MemorySyncService periodic callback reuses one reconciliation snapshot",
          "[memory-sync][service][lifecycle]") {
    TempDirGuard tmp;
    auto backend = std::make_unique<CountingBackend>();
    yams::storage::BackendConfig backendConfig;
    backendConfig.type = "filesystem";
    backendConfig.localPath = tmp.path;
    REQUIRE(backend->initialize(backendConfig).has_value());
    auto* observed = backend.get();
    MemorySyncService service{std::move(backend), MemorySyncConfig{"A", 5000}};
    std::promise<void> callbackDone;
    auto done = callbackDone.get_future();
    service.setAfterSyncCallback([&] {
        REQUIRE(service.syncOnce().has_value());
        REQUIRE(service.syncOnce().has_value());
        REQUIRE(service.syncOnce().has_value());
        callbackDone.set_value();
    });

    REQUIRE(service.start().has_value());
    REQUIRE(done.wait_for(std::chrono::seconds(2)) == std::future_status::ready);
    service.stop();
    CHECK(observed->listPageCalls.load(std::memory_order_relaxed) == 1);
}

TEST_CASE("MemorySyncService stop interrupts a slow backend reconciliation",
          "[memory-sync][service][lifecycle][cancellation]") {
    TempDirGuard tmp;
    auto backend = std::make_unique<BlockingListBackend>();
    yams::storage::BackendConfig config;
    config.type = "filesystem";
    config.localPath = tmp.path;
    REQUIRE(backend->initialize(config).has_value());
    auto entered = backend->enteredFuture();
    auto* observed = backend.get();
    MemorySyncService service{std::move(backend), MemorySyncConfig{"A", 5000}};

    REQUIRE(service.start().has_value());
    REQUIRE(entered.wait_for(std::chrono::seconds(2)) == std::future_status::ready);
    auto stopped = std::async(std::launch::async, [&] { service.stop(); });
    REQUIRE(stopped.wait_for(std::chrono::seconds(2)) == std::future_status::ready);
    CHECK(observed->cancelCalls.load(std::memory_order_relaxed) > 0);
    CHECK_FALSE(service.started());
}

TEST_CASE("MemorySyncService stop waits for an active callback and joins cleanly",
          "[memory-sync][service][lifecycle]") {
    TempDirGuard tmp;
    MemorySyncService service{makeBackend(tmp.path), MemorySyncConfig{"A", 5000}};
    std::promise<void> entered;
    auto enteredFuture = entered.get_future();
    std::promise<void> release;
    auto releaseFuture = release.get_future().share();
    service.setAfterSyncCallback([&] {
        entered.set_value();
        releaseFuture.wait();
    });
    REQUIRE(service.start().has_value());
    REQUIRE(enteredFuture.wait_for(std::chrono::seconds(2)) == std::future_status::ready);

    auto stopped = std::async(std::launch::async, [&] { service.stop(); });
    CHECK(stopped.wait_for(std::chrono::milliseconds(20)) == std::future_status::timeout);
    release.set_value();
    REQUIRE(stopped.wait_for(std::chrono::seconds(2)) == std::future_status::ready);
    CHECK_FALSE(service.started());
}

TEST_CASE("MemorySyncService callback can request stop without self-join",
          "[memory-sync][service][lifecycle]") {
    TempDirGuard tmp;
    MemorySyncService service{makeBackend(tmp.path), MemorySyncConfig{"A", 5000}};
    std::promise<void> returned;
    auto returnedFuture = returned.get_future();
    service.setAfterSyncCallback([&] {
        service.stop();
        returned.set_value();
    });
    REQUIRE(service.start().has_value());
    REQUIRE(returnedFuture.wait_for(std::chrono::seconds(2)) == std::future_status::ready);
    service.stop();
    CHECK_FALSE(service.started());
}

TEST_CASE("MemorySyncService concurrent lifecycle and status calls remain race-free",
          "[memory-sync][service][lifecycle][stress]") {
    TempDirGuard tmp;
    MemorySyncService service{makeBackend(tmp.path), MemorySyncConfig{"A", 10}};
    std::atomic<std::uint32_t> lifecycleFailures{0};
    std::vector<yams::compat::jthread> callers;
    for (int thread = 0; thread < 6; ++thread) {
        callers.emplace_back([&, thread] {
            for (int iteration = 0; iteration < 50; ++iteration) {
                if ((thread + iteration) % 3 == 0) {
                    service.stop();
                } else if (!service.start().has_value()) {
                    lifecycleFailures.fetch_add(1, std::memory_order_relaxed);
                }
                (void)service.started();
                (void)service.mergedRecordCount();
            }
        });
    }
    callers.clear();
    service.stop();
    CHECK(lifecycleFailures.load(std::memory_order_relaxed) == 0);
    CHECK_FALSE(service.started());
}

TEST_CASE("MemorySyncService periodic worker syncs without hanging", "[memory-sync][service]") {
    TempDirGuard tmp;
    MemorySyncService a{makeBackend(tmp.path), MemorySyncConfig{"A", 50}};
    MemorySyncService b{makeBackend(tmp.path), MemorySyncConfig{"B", 50}};

    REQUIRE(a.start().has_value());
    REQUIRE(b.start().has_value());

    REQUIRE(a.publish("periodic-key", bytes("via-periodic-sync")).has_value());

    // B's periodic worker should converge within a bounded deadline.
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
    bool converged = false;
    while (std::chrono::steady_clock::now() < deadline) {
        auto value = b.read("periodic-key");
        if (value.has_value() && text(value.value()) == "via-periodic-sync") {
            converged = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(25));
    }
    CHECK(converged);

    a.stop();
    b.stop();
    CHECK_FALSE(a.started());
    CHECK_FALSE(b.started());
}

// NOLINTEND(bugprone-chained-comparison)

TEST_CASE("MemorySyncService direct delta apply invokes adapter callback after lock release",
          "[memory-sync][service][direct-delta]") {
    TempDirGuard writerDir;
    TempDirGuard readerDir;
    MemorySyncService writer{makeBackend(writerDir.path),
                             MemorySyncConfig{"writer", 60'000, "direct-service-corpus", 1}};
    MemorySyncService reader{makeBackend(readerDir.path),
                             MemorySyncConfig{"reader", 60'000, "direct-service-corpus", 1}};
    REQUIRE(writer.publish("user/key", bytes("value")).has_value());
    auto deltas = writer.exportLocalDeltasAfter({});
    REQUIRE(deltas.has_value());
    REQUIRE(deltas.value().deltas.size() == 1);

    std::atomic<std::size_t> callbacks{0};
    reader.setAfterSyncCallback([&] { callbacks.fetch_add(1, std::memory_order_relaxed); });
    auto applied = reader.applyDeltas(deltas.value().deltas);
    REQUIRE(applied.has_value());
    CHECK(applied.value().merged == 1);
    CHECK(callbacks.load(std::memory_order_relaxed) == 1);
    CHECK(reader.readCached("user/key").has_value());
}

TEST_CASE("MemorySyncService serializes concurrent post-sync callbacks",
          "[memory-sync][service][direct-delta][concurrency]") {
    TempDirGuard writerADir;
    TempDirGuard writerBDir;
    TempDirGuard readerDir;
    MemorySyncService writerA{makeBackend(writerADir.path),
                              MemorySyncConfig{"writer-a", 60'000, "callback-corpus", 1}};
    MemorySyncService writerB{makeBackend(writerBDir.path),
                              MemorySyncConfig{"writer-b", 60'000, "callback-corpus", 1}};
    MemorySyncService reader{makeBackend(readerDir.path),
                             MemorySyncConfig{"reader", 60'000, "callback-corpus", 1}};
    REQUIRE(writerA.publish("user/a", bytes("a")).has_value());
    REQUIRE(writerB.publish("user/b", bytes("b")).has_value());
    auto deltaA = writerA.exportLocalDeltasAfter({});
    auto deltaB = writerB.exportLocalDeltasAfter({});
    REQUIRE(deltaA.has_value());
    REQUIRE(deltaB.has_value());

    std::atomic<std::size_t> active{0};
    std::atomic<std::size_t> maximumActive{0};
    std::atomic<std::size_t> callbacks{0};
    std::promise<void> firstEnteredPromise;
    auto firstEntered = firstEnteredPromise.get_future();
    std::promise<void> releaseFirstPromise;
    auto releaseFirst = releaseFirstPromise.get_future().share();
    reader.setAfterSyncCallback([&] {
        const auto nowActive = active.fetch_add(1, std::memory_order_acq_rel) + 1;
        auto observed = maximumActive.load(std::memory_order_acquire);
        while (observed < nowActive && !maximumActive.compare_exchange_weak(
                                           observed, nowActive, std::memory_order_acq_rel)) {
        }
        if (callbacks.fetch_add(1, std::memory_order_acq_rel) == 0) {
            firstEnteredPromise.set_value();
            releaseFirst.wait();
        }
        active.fetch_sub(1, std::memory_order_acq_rel);
    });

    auto first =
        std::async(std::launch::async, [&] { return reader.applyDeltas(deltaA.value().deltas); });
    REQUIRE(firstEntered.wait_for(std::chrono::seconds(2)) == std::future_status::ready);
    auto second =
        std::async(std::launch::async, [&] { return reader.applyDeltas(deltaB.value().deltas); });

    const auto mergedDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (reader.replicationState().version.get("writer-b") == 0 &&
           std::chrono::steady_clock::now() < mergedDeadline) {
        std::this_thread::yield();
    }
    REQUIRE(reader.replicationState().version.get("writer-b") == 1);
    CHECK(second.wait_for(std::chrono::milliseconds(0)) == std::future_status::timeout);
    CHECK(maximumActive.load(std::memory_order_acquire) == 1);

    releaseFirstPromise.set_value();
    REQUIRE(first.get().has_value());
    REQUIRE(second.get().has_value());
    CHECK(callbacks.load(std::memory_order_acquire) == 2);
    CHECK(maximumActive.load(std::memory_order_acquire) == 1);
}

TEST_CASE("MemorySyncService callback stop does not join a worker waiting for serialization",
          "[memory-sync][service][direct-delta][concurrency][stop]") {
    TempDirGuard writerDir;
    TempDirGuard readerDir;
    MemorySyncService writer{makeBackend(writerDir.path),
                             MemorySyncConfig{"writer", 60'000, "callback-stop-corpus", 1}};
    MemorySyncService reader{makeBackend(readerDir.path),
                             MemorySyncConfig{"reader", 1, "callback-stop-corpus", 1}};
    REQUIRE(writer.publish("user/key", bytes("value")).has_value());
    auto deltas = writer.exportLocalDeltasAfter({});
    REQUIRE(deltas.has_value());

    std::promise<void> callbackEnteredPromise;
    auto callbackEntered = callbackEnteredPromise.get_future();
    std::promise<void> allowStopPromise;
    auto allowStop = allowStopPromise.get_future().share();
    std::atomic<bool> first{true};
    reader.setAfterSyncCallback([&] {
        if (first.exchange(false, std::memory_order_acq_rel)) {
            callbackEnteredPromise.set_value();
            allowStop.wait();
            reader.stop();
        }
    });

    auto directApply =
        std::async(std::launch::async, [&] { return reader.applyDeltas(deltas.value().deltas); });
    REQUIRE(callbackEntered.wait_for(std::chrono::seconds(2)) == std::future_status::ready);
    REQUIRE(reader.start().has_value());
    const auto workerDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (reader.successfulSyncCycles() == 0 &&
           std::chrono::steady_clock::now() < workerDeadline) {
        std::this_thread::yield();
    }
    REQUIRE(reader.successfulSyncCycles() > 0);

    allowStopPromise.set_value();
    REQUIRE(directApply.wait_for(std::chrono::seconds(2)) == std::future_status::ready);
    REQUIRE(directApply.get().has_value());
    reader.stop();
    CHECK_FALSE(reader.started());
    REQUIRE(reader.start().has_value());
    CHECK(reader.started());
    reader.stop();
    CHECK_FALSE(reader.started());
}

TEST_CASE("MemorySyncService serializes callbacks across service instances",
          "[memory-sync][service][direct-delta][concurrency][cross-service]") {
    TempDirGuard writerADir;
    TempDirGuard writerBDir;
    TempDirGuard readerADir;
    TempDirGuard readerBDir;
    MemorySyncService writerA{makeBackend(writerADir.path),
                              MemorySyncConfig{"writer-a", 60'000, "global-callback-a", 1}};
    MemorySyncService writerB{makeBackend(writerBDir.path),
                              MemorySyncConfig{"writer-b", 60'000, "global-callback-b", 1}};
    MemorySyncService readerA{makeBackend(readerADir.path),
                              MemorySyncConfig{"reader-a", 60'000, "global-callback-a", 1}};
    MemorySyncService readerB{makeBackend(readerBDir.path),
                              MemorySyncConfig{"reader-b", 60'000, "global-callback-b", 1}};
    REQUIRE(writerA.publish("user/a", bytes("a")).has_value());
    REQUIRE(writerB.publish("user/b", bytes("b")).has_value());
    auto deltaA = writerA.exportLocalDeltasAfter({});
    auto deltaB = writerB.exportLocalDeltasAfter({});
    REQUIRE(deltaA.has_value());
    REQUIRE(deltaB.has_value());

    std::atomic<std::size_t> active{0};
    std::atomic<std::size_t> maximumActive{0};
    std::atomic<std::size_t> callbacks{0};
    std::promise<void> firstEnteredPromise;
    auto firstEntered = firstEnteredPromise.get_future();
    std::promise<void> releaseFirstPromise;
    auto releaseFirst = releaseFirstPromise.get_future().share();
    const auto callback = [&] {
        const auto nowActive = active.fetch_add(1, std::memory_order_acq_rel) + 1;
        auto observed = maximumActive.load(std::memory_order_acquire);
        while (observed < nowActive && !maximumActive.compare_exchange_weak(
                                           observed, nowActive, std::memory_order_acq_rel)) {
        }
        if (callbacks.fetch_add(1, std::memory_order_acq_rel) == 0) {
            firstEnteredPromise.set_value();
            releaseFirst.wait();
        }
        active.fetch_sub(1, std::memory_order_acq_rel);
    };
    readerA.setAfterSyncCallback(callback);
    readerB.setAfterSyncCallback(callback);

    auto first =
        std::async(std::launch::async, [&] { return readerA.applyDeltas(deltaA.value().deltas); });
    REQUIRE(firstEntered.wait_for(std::chrono::seconds(2)) == std::future_status::ready);
    auto second =
        std::async(std::launch::async, [&] { return readerB.applyDeltas(deltaB.value().deltas); });
    const auto mergedDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (readerB.replicationState().version.get("writer-b") == 0 &&
           std::chrono::steady_clock::now() < mergedDeadline) {
        std::this_thread::yield();
    }
    REQUIRE(readerB.replicationState().version.get("writer-b") == 1);
    CHECK(second.wait_for(std::chrono::milliseconds(0)) == std::future_status::timeout);
    CHECK(maximumActive.load(std::memory_order_acquire) == 1);

    releaseFirstPromise.set_value();
    REQUIRE(first.get().has_value());
    REQUIRE(second.get().has_value());
    CHECK(callbacks.load(std::memory_order_acquire) == 2);
    CHECK(maximumActive.load(std::memory_order_acquire) == 1);
}

TEST_CASE("MemorySyncService refreshes queued callback snapshots after serialization",
          "[memory-sync][service][direct-delta][concurrency][snapshot]") {
    TempDirGuard writerADir;
    TempDirGuard writerBDir;
    TempDirGuard readerDir;
    MemorySyncService writerA{makeBackend(writerADir.path),
                              MemorySyncConfig{"writer-a", 60'000, "callback-snapshot", 1}};
    MemorySyncService writerB{makeBackend(writerBDir.path),
                              MemorySyncConfig{"writer-b", 60'000, "callback-snapshot", 1}};
    MemorySyncService reader{makeBackend(readerDir.path),
                             MemorySyncConfig{"reader", 1, "callback-snapshot", 1}};
    REQUIRE(writerA.publish("user/a", bytes("a")).has_value());
    REQUIRE(writerB.publish("user/b", bytes("b")).has_value());
    auto deltaA = writerA.exportLocalDeltasAfter({});
    auto deltaB = writerB.exportLocalDeltasAfter({});
    REQUIRE(deltaA.has_value());
    REQUIRE(deltaB.has_value());

    std::atomic<std::size_t> callbacks{0};
    std::atomic<bool> staleSnapshot{false};
    std::promise<void> firstEnteredPromise;
    auto firstEntered = firstEnteredPromise.get_future();
    std::promise<void> releaseFirstPromise;
    auto releaseFirst = releaseFirstPromise.get_future().share();
    reader.setAfterSyncCallback([&] {
        if (callbacks.fetch_add(1, std::memory_order_acq_rel) == 0) {
            firstEnteredPromise.set_value();
            releaseFirst.wait();
            return;
        }
        auto snapshot = reader.syncOnce();
        if (!snapshot || !snapshot.value().contains("user/b")) {
            staleSnapshot.store(true, std::memory_order_release);
        }
    });

    auto first =
        std::async(std::launch::async, [&] { return reader.applyDeltas(deltaA.value().deltas); });
    REQUIRE(firstEntered.wait_for(std::chrono::seconds(2)) == std::future_status::ready);
    REQUIRE(reader.start().has_value());
    const auto workerDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (reader.successfulSyncCycles() == 0 &&
           std::chrono::steady_clock::now() < workerDeadline) {
        std::this_thread::yield();
    }
    REQUIRE(reader.successfulSyncCycles() > 0);

    auto second =
        std::async(std::launch::async, [&] { return reader.applyDeltas(deltaB.value().deltas); });
    const auto mergeDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (reader.replicationState().version.get("writer-b") == 0 &&
           std::chrono::steady_clock::now() < mergeDeadline) {
        std::this_thread::yield();
    }
    REQUIRE(reader.replicationState().version.get("writer-b") == 1);
    releaseFirstPromise.set_value();
    REQUIRE(first.get().has_value());
    REQUIRE(second.get().has_value());
    const auto callbackDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (callbacks.load(std::memory_order_acquire) < 3 &&
           std::chrono::steady_clock::now() < callbackDeadline) {
        std::this_thread::yield();
    }
    REQUIRE(callbacks.load(std::memory_order_acquire) >= 3);
    reader.stop();

    CHECK(callbacks.load(std::memory_order_acquire) >= 3);
    CHECK_FALSE(staleSnapshot.load(std::memory_order_acquire));
}

TEST_CASE("MemorySyncService stop drains an active direct-session callback",
          "[memory-sync][service][direct-delta][concurrency][stop]") {
    TempDirGuard writerDir;
    TempDirGuard readerDir;
    MemorySyncService writer{makeBackend(writerDir.path),
                             MemorySyncConfig{"writer", 60'000, "callback-drain-corpus", 1}};
    MemorySyncService reader{makeBackend(readerDir.path),
                             MemorySyncConfig{"reader", 60'000, "callback-drain-corpus", 1}};
    REQUIRE(writer.publish("user/key", bytes("value")).has_value());
    auto deltas = writer.exportLocalDeltasAfter({});
    REQUIRE(deltas.has_value());

    std::promise<void> callbackEnteredPromise;
    auto callbackEntered = callbackEnteredPromise.get_future();
    std::promise<void> releaseCallbackPromise;
    auto releaseCallback = releaseCallbackPromise.get_future().share();
    std::atomic<bool> callbackObservedStopped{false};
    reader.setAfterSyncCallback([&] {
        callbackEnteredPromise.set_value();
        releaseCallback.wait();
        callbackObservedStopped.store(!reader.started(), std::memory_order_release);
    });
    auto directApply =
        std::async(std::launch::async, [&] { return reader.applyDeltas(deltas.value().deltas); });
    REQUIRE(callbackEntered.wait_for(std::chrono::seconds(2)) == std::future_status::ready);
    std::promise<void> stopStartedPromise;
    auto stopStarted = stopStartedPromise.get_future();
    auto stopped = std::async(std::launch::async, [&] {
        stopStartedPromise.set_value();
        reader.stop();
    });
    REQUIRE(stopStarted.wait_for(std::chrono::seconds(2)) == std::future_status::ready);
    CHECK(stopped.wait_for(std::chrono::milliseconds(100)) == std::future_status::timeout);

    releaseCallbackPromise.set_value();
    REQUIRE(directApply.get().has_value());
    stopped.get();
    CHECK(callbackObservedStopped.load(std::memory_order_acquire));
    CHECK_FALSE(reader.started());
}

TEST_CASE("MemorySyncService can destroy another service from a callback",
          "[memory-sync][service][direct-delta][concurrency][destruction]") {
    TempDirGuard writerDir;
    TempDirGuard readerADir;
    TempDirGuard readerBDir;
    MemorySyncService writer{makeBackend(writerDir.path),
                             MemorySyncConfig{"writer", 60'000, "callback-destroy-a", 1}};
    MemorySyncService readerA{makeBackend(readerADir.path),
                              MemorySyncConfig{"reader-a", 60'000, "callback-destroy-a", 1}};
    auto readerB = std::make_unique<MemorySyncService>(
        makeBackend(readerBDir.path), MemorySyncConfig{"reader-b", 1, "callback-destroy-b", 1});
    REQUIRE(writer.publish("user/key", bytes("value")).has_value());
    auto deltas = writer.exportLocalDeltasAfter({});
    REQUIRE(deltas.has_value());

    std::promise<void> callbackEnteredPromise;
    auto callbackEntered = callbackEnteredPromise.get_future();
    std::promise<void> allowDestroyPromise;
    auto allowDestroy = allowDestroyPromise.get_future().share();
    readerA.setAfterSyncCallback([&] {
        callbackEnteredPromise.set_value();
        allowDestroy.wait();
        readerB.reset();
    });
    auto apply =
        std::async(std::launch::async, [&] { return readerA.applyDeltas(deltas.value().deltas); });
    REQUIRE(callbackEntered.wait_for(std::chrono::seconds(2)) == std::future_status::ready);
    REQUIRE(readerB->start().has_value());
    const auto workerDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (readerB->successfulSyncCycles() == 0 &&
           std::chrono::steady_clock::now() < workerDeadline) {
        std::this_thread::yield();
    }
    REQUIRE(readerB->successfulSyncCycles() > 0);

    allowDestroyPromise.set_value();
    REQUIRE(apply.wait_for(std::chrono::seconds(2)) == std::future_status::ready);
    REQUIRE(apply.get().has_value());
    CHECK(readerB == nullptr);
}

TEST_CASE("MemorySyncService coalesces recursive post-sync callbacks",
          "[memory-sync][service][direct-delta][concurrency][recursive]") {
    TempDirGuard writerADir;
    TempDirGuard writerBDir;
    TempDirGuard readerDir;
    MemorySyncService writerA{makeBackend(writerADir.path),
                              MemorySyncConfig{"writer-a", 60'000, "recursive-corpus", 1}};
    MemorySyncService writerB{makeBackend(writerBDir.path),
                              MemorySyncConfig{"writer-b", 60'000, "recursive-corpus", 1}};
    MemorySyncService reader{makeBackend(readerDir.path),
                             MemorySyncConfig{"reader", 60'000, "recursive-corpus", 1}};
    REQUIRE(writerA.publish("user/a", bytes("a")).has_value());
    REQUIRE(writerB.publish("user/b", bytes("b")).has_value());
    auto deltaA = writerA.exportLocalDeltasAfter({});
    auto deltaB = writerB.exportLocalDeltasAfter({});
    REQUIRE(deltaA.has_value());
    REQUIRE(deltaB.has_value());
    REQUIRE(reader.applyDeltas(deltaA.value().deltas).has_value());
    REQUIRE(reader.applyDeltas(deltaB.value().deltas).has_value());

    std::atomic<std::size_t> callbacks{0};
    std::atomic<bool> nestedQuarantineSucceeded{false};
    reader.setAfterSyncCallback([&] {
        if (callbacks.fetch_add(1, std::memory_order_acq_rel) == 0) {
            auto nested = reader.quarantineWriter("writer-b", "reader");
            nestedQuarantineSucceeded.store(nested && nested.value(), std::memory_order_release);
        }
    });
    auto quarantined = reader.quarantineWriter("writer-a", "reader");
    REQUIRE(quarantined.has_value());
    CHECK(quarantined.value());
    CHECK(nestedQuarantineSucceeded.load(std::memory_order_acquire));
    CHECK(callbacks.load(std::memory_order_acquire) == 2);
    CHECK(reader.replicationState().quarantinedWriters.contains("writer-a"));
    CHECK(reader.replicationState().quarantinedWriters.contains("writer-b"));
}

TEST_CASE("MemorySyncService preserves callback ownership across services",
          "[memory-sync][service][direct-delta][concurrency][recursive]") {
    TempDirGuard writerA1Dir;
    TempDirGuard writerA2Dir;
    TempDirGuard writerBDir;
    TempDirGuard readerADir;
    TempDirGuard readerBDir;
    MemorySyncService writerA1{makeBackend(writerA1Dir.path),
                               MemorySyncConfig{"writer-a1", 60'000, "cross-callback-a", 1}};
    MemorySyncService writerA2{makeBackend(writerA2Dir.path),
                               MemorySyncConfig{"writer-a2", 60'000, "cross-callback-a", 1}};
    MemorySyncService writerB{makeBackend(writerBDir.path),
                              MemorySyncConfig{"writer-b", 60'000, "cross-callback-b", 1}};
    MemorySyncService readerA{makeBackend(readerADir.path),
                              MemorySyncConfig{"reader-a", 60'000, "cross-callback-a", 1}};
    MemorySyncService readerB{makeBackend(readerBDir.path),
                              MemorySyncConfig{"reader-b", 60'000, "cross-callback-b", 1}};
    REQUIRE(writerA1.publish("user/a1", bytes("a1")).has_value());
    REQUIRE(writerA2.publish("user/a2", bytes("a2")).has_value());
    REQUIRE(writerB.publish("user/b", bytes("b")).has_value());
    auto deltaA1 = writerA1.exportLocalDeltasAfter({});
    auto deltaA2 = writerA2.exportLocalDeltasAfter({});
    auto deltaB = writerB.exportLocalDeltasAfter({});
    REQUIRE(deltaA1.has_value());
    REQUIRE(deltaA2.has_value());
    REQUIRE(deltaB.has_value());
    REQUIRE(readerA.applyDeltas(deltaA1.value().deltas).has_value());
    REQUIRE(readerA.applyDeltas(deltaA2.value().deltas).has_value());
    REQUIRE(readerB.applyDeltas(deltaB.value().deltas).has_value());

    std::atomic<std::size_t> callbacksA{0};
    std::atomic<std::size_t> callbacksB{0};
    std::atomic<bool> nestedB{false};
    std::atomic<bool> nestedA{false};
    readerB.setAfterSyncCallback([&] {
        if (callbacksB.fetch_add(1, std::memory_order_acq_rel) == 0) {
            auto quarantineA = readerA.quarantineWriter("writer-a2", "reader-a");
            nestedA.store(quarantineA && quarantineA.value(), std::memory_order_release);
        }
    });
    readerA.setAfterSyncCallback([&] {
        if (callbacksA.fetch_add(1, std::memory_order_acq_rel) == 0) {
            auto quarantineB = readerB.quarantineWriter("writer-b", "reader-b");
            nestedB.store(quarantineB && quarantineB.value(), std::memory_order_release);
        }
    });

    auto quarantineA1 = readerA.quarantineWriter("writer-a1", "reader-a");
    REQUIRE(quarantineA1.has_value());
    CHECK(quarantineA1.value());
    CHECK(nestedA.load(std::memory_order_acquire));
    CHECK(nestedB.load(std::memory_order_acquire));
    CHECK(callbacksA.load(std::memory_order_acquire) == 2);
    CHECK(callbacksB.load(std::memory_order_acquire) == 1);
}

TEST_CASE("MemorySyncService publishes quarantine snapshot before adapter invalidation",
          "[memory-sync][service][direct-delta][quarantine]") {
    TempDirGuard writerDir;
    TempDirGuard readerDir;
    MemorySyncService writer{makeBackend(writerDir.path),
                             MemorySyncConfig{"writer", 60'000, "quarantine-service", 1}};
    MemorySyncService reader{makeBackend(readerDir.path),
                             MemorySyncConfig{"reader", 60'000, "quarantine-service", 1}};
    REQUIRE(writer.publish("user/key", bytes("value")).has_value());
    auto deltas = writer.exportLocalDeltasAfter({});
    REQUIRE(deltas.has_value());

    REQUIRE(reader.applyDeltas(deltas.value().deltas).has_value());
    std::atomic<std::size_t> callbacks{0};
    reader.setAfterSyncCallback([&] {
        CHECK(reader.replicationState().quarantinedWriters.contains("writer"));
        CHECK_FALSE(reader.readCached("user/key").has_value());
        auto adapterState = reader.syncOnce();
        REQUIRE(adapterState.has_value());
        REQUIRE(adapterState.value().contains("user/key"));
        CHECK(adapterState.value().at("user/key").isTombstone());
        callbacks.fetch_add(1, std::memory_order_relaxed);
    });

    auto quarantined = reader.quarantineWriter("writer", "reader");
    REQUIRE(quarantined.has_value());
    CHECK(quarantined.value());
    CHECK(callbacks.load(std::memory_order_relaxed) == 1);
    CHECK(reader.replicationState().quarantinedWriters.contains("writer"));
    CHECK_FALSE(reader.readCached("user/key").has_value());

    auto repeated = reader.quarantineWriter("writer", "reader");
    REQUIRE(repeated.has_value());
    CHECK_FALSE(repeated.value());
    CHECK(callbacks.load(std::memory_order_relaxed) == 1);
}

TEST_CASE("MemorySyncService readers do not block on a slow reconciliation",
          "[memory-sync][service]") {
    TempDirGuard tmp;
    auto backend = std::make_unique<SlowListBackend>();
    yams::storage::BackendConfig config;
    config.type = "filesystem";
    config.localPath = tmp.path;
    REQUIRE(backend->initialize(config).has_value());
    auto* observed = backend.get();
    MemorySyncService service{std::move(backend), MemorySyncConfig{"A", 5000}};

    // Publish a value before arming the block so a committed snapshot exists.
    // syncOnce commits the page so the key (and its hydrated blob) are in the
    // reconciled state the snapshot is refreshed from.
    REQUIRE(service.publish("user/k", bytes("v1")).has_value());
    REQUIRE(service.syncOnce().has_value());

    observed->arm();
    REQUIRE(service.start().has_value());
    observed->waitUntilBlocked();

    // The worker is now stuck inside listPage (mid-reconciliation). IPC readers
    // (status counts, cached reads) must return promptly from the last committed
    // snapshot instead of waiting on the reconciliation lock.
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
    auto reader = std::async(std::launch::async, [&] {
        return service.readCached("user/k").has_value() && service.mergedRecordCount() == 1 &&
               service.quarantinedRecordCount() == 0;
    });
    CHECK(reader.wait_for(std::chrono::seconds(1)) == std::future_status::ready);
    if (reader.wait_for(std::chrono::seconds(0)) == std::future_status::ready) {
        CHECK(reader.get());
    }
    CHECK(std::chrono::steady_clock::now() < deadline);

    observed->releaseList();
    service.stop();
    CHECK_FALSE(service.started());
}
