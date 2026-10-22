// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

// pi-lens-ignore: fatal error
#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <condition_variable>
#include <cstring>
#include <filesystem>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include <yams/compat/thread_stop_compat.h>
#include <yams/config/config_helpers.h>
#include <yams/memory_sync/lifecycle_drain.h>
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

class CompletionSignal {
public:
    void signal() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            signalled_ = true;
        }
        cv_.notify_all();
    }

    template <typename Rep, typename Period>
    bool waitFor(const std::chrono::duration<Rep, Period>& timeout) const {
        std::unique_lock<std::mutex> lock(mutex_);
        return cv_.wait_for(lock, timeout, [this] { return signalled_; });
    }

private:
    mutable std::mutex mutex_;
    mutable std::condition_variable cv_;
    bool signalled_{false};
};

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

class SecondPageGateBackend final : public CountingBackend {
public:
    std::future<void> arm() {
        armed_.store(true);
        return entered_.get_future();
    }
    void release() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            released_ = true;
        }
        cv_.notify_all();
    }
    void requestCancel() noexcept override { release(); }
    yams::Result<yams::storage::ObjectListPage> listPage(std::string_view prefix,
                                                         std::optional<std::string_view> cursor,
                                                         std::size_t limit) const override {
        if (armed_.load() && prefix == "index/" && calls_.fetch_add(1) == 1) {
            entered_.set_value();
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait(lock, [&] { return released_; });
        }
        return CountingBackend::listPage(prefix, cursor, limit);
    }

private:
    std::atomic<bool> armed_{false};
    mutable std::atomic<std::size_t> calls_{0};
    mutable std::promise<void> entered_;
    mutable std::mutex mutex_;
    mutable std::condition_variable cv_;
    bool released_{false};
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
        cv_.wait(lock, [this] { return cancelled_; });
        return yams::Error{yams::ErrorCode::OperationCancelled, "blocking list cancelled"};
    }

    void requestCancel() noexcept override {
        cancelCalls.fetch_add(1, std::memory_order_relaxed);
        {
            std::lock_guard<std::mutex> lock(mutex_);
            cancelled_ = true;
        }
        cv_.notify_all();
    }

    void resetCancel() noexcept override {
        std::lock_guard<std::mutex> lock(mutex_);
        cancelled_ = false;
    }

    std::atomic<std::uint32_t> cancelCalls{0};

private:
    mutable std::promise<void> entered_;
    mutable std::atomic<bool> enteredOnce_{false};
    mutable std::mutex mutex_;
    mutable std::condition_variable cv_;
    bool cancelled_{false};
};

/// Provides a generation-aware cancellation barrier for the stop/start race test.
/// The first requestCancel() wakes the old worker, then pauses before returning so
/// a concurrent start can reset cancellation and launch the next generation.
class RestartCancellationBackend final : public CountingBackend {
public:
    template <typename Rep, typename Period>
    bool waitForListCalls(std::size_t count,
                          const std::chrono::duration<Rep, Period>& timeout) const {
        std::unique_lock<std::mutex> lock(stateMutex_);
        return stateCv_.wait_for(lock, timeout,
                                 [this, count] { return listGenerations_.size() >= count; });
    }

    template <typename Rep, typename Period>
    bool waitForFirstCancel(const std::chrono::duration<Rep, Period>& timeout) const {
        std::unique_lock<std::mutex> lock(barrierMutex_);
        return barrierCv_.wait_for(lock, timeout, [this] { return firstCancelApplied_; });
    }

    template <typename Rep, typename Period>
    bool waitForGenerationCancel(std::uint64_t generation,
                                 const std::chrono::duration<Rep, Period>& timeout) const {
        std::unique_lock<std::mutex> lock(stateMutex_);
        return stateCv_.wait_for(lock, timeout,
                                 [this, generation] { return cancelledGeneration_ == generation; });
    }

    std::optional<std::uint64_t> listGeneration(std::size_t oneBasedCall) const {
        std::lock_guard<std::mutex> lock(stateMutex_);
        if (oneBasedCall == 0 || listGenerations_.size() < oneBasedCall) {
            return std::nullopt;
        }
        return listGenerations_[oneBasedCall - 1];
    }

    void releaseFirstCancel() {
        {
            std::lock_guard<std::mutex> lock(barrierMutex_);
            releaseFirstCancel_ = true;
        }
        barrierCv_.notify_all();
    }

    // Failure cleanup must also cover a reset that races after cleanup begins.
    void abortWaits() {
        {
            std::lock_guard<std::mutex> lock(barrierMutex_);
            aborting_ = true;
            releaseFirstCancel_ = true;
        }
        barrierCv_.notify_all();
        {
            std::lock_guard<std::mutex> lock(stateMutex_);
            cancelledGeneration_ = generation_;
        }
        stateCv_.notify_all();
    }

    yams::Result<yams::storage::ObjectListPage>
    listPage(std::string_view, std::optional<std::string_view>, std::size_t) const override {
        std::unique_lock<std::mutex> lock(stateMutex_);
        const auto generation = generation_;
        listGenerations_.push_back(generation);
        stateCv_.notify_all();
        stateCv_.wait(lock, [this, generation] { return cancelledGeneration_ == generation; });
        return yams::Error{yams::ErrorCode::OperationCancelled, "generation list cancelled"};
    }

    void requestCancel() noexcept override {
        const auto call = cancelCalls_.fetch_add(1, std::memory_order_acq_rel) + 1;
        {
            std::lock_guard<std::mutex> lock(stateMutex_);
            cancelledGeneration_ = generation_;
        }
        stateCv_.notify_all();
        if (call != 1) {
            return;
        }
        std::unique_lock<std::mutex> lock(barrierMutex_);
        firstCancelApplied_ = true;
        barrierCv_.notify_all();
        barrierCv_.wait(lock, [this] { return releaseFirstCancel_ || aborting_; });
    }

    void resetCancel() noexcept override {
        {
            // Serialize the abort decision with installing the new generation.
            // Otherwise cleanup can cancel the old generation between these updates.
            std::scoped_lock lock(barrierMutex_, stateMutex_);
            ++generation_;
            cancelledGeneration_ = aborting_ ? generation_ : 0;
        }
        stateCv_.notify_all();
    }

private:
    mutable std::mutex stateMutex_;
    mutable std::condition_variable stateCv_;
    mutable std::uint64_t generation_{0};
    mutable std::uint64_t cancelledGeneration_{0};
    mutable std::vector<std::uint64_t> listGenerations_;
    std::atomic<std::uint32_t> cancelCalls_{0};

    mutable std::mutex barrierMutex_;
    mutable std::condition_variable barrierCv_;
    bool firstCancelApplied_{false};
    bool releaseFirstCancel_{false};
    bool aborting_{false};
};

/// Blocks `listPage` while armed, releasing when unarmed. Used to prove IPC
/// readers never wait on a slow reconciliation: publish first (unarmed), arm,
/// start the worker, then assert readers return promptly from the committed
/// snapshot while the worker is stuck mid-sync.
class SlowListBackend final : public CountingBackend {
public:
    void arm() {
        std::lock_guard<std::mutex> lock(mutex_);
        armed_ = true;
    }

    void releaseList() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            armed_ = false;
        }
        cv_.notify_all();
    }

    void waitUntilBlocked() const {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this] { return blocked_; });
    }

    yams::Result<yams::storage::ObjectListPage> listPage(std::string_view prefix,
                                                         std::optional<std::string_view> cursor,
                                                         std::size_t limit) const override {
        std::unique_lock<std::mutex> lock(mutex_);
        if (armed_) {
            blocked_ = true;
            cv_.notify_all();
            cv_.wait(lock, [this] { return !armed_; });
            blocked_ = false;
        }
        lock.unlock();
        return CountingBackend::listPage(prefix, cursor, limit);
    }

private:
    mutable bool armed_{false};
    mutable bool blocked_{false};
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

class FilesystemReadObserverGuard {
public:
    explicit FilesystemReadObserverGuard(std::function<void(std::string_view)>& observer)
        : previous_(yams::storage::testing_setFilesystemReadObserver(&observer)) {}
    ~FilesystemReadObserverGuard() { yams::storage::testing_setFilesystemReadObserver(previous_); }

private:
    std::function<void(std::string_view)>* previous_;
};

class PressurePauseObserverGuard {
public:
    explicit PressurePauseObserverGuard(std::function<void()>& observer)
        : previous_(MemorySyncService::testingSetPressurePauseObserver(&observer)) {}
    ~PressurePauseObserverGuard() { MemorySyncService::testingSetPressurePauseObserver(previous_); }

private:
    std::function<void()>* previous_;
};

} // namespace

TEST_CASE("MemorySyncService adapter indexes have independent bounded bookkeeping",
          "[memory-sync][service][adapter-index]") {
    TempDirGuard tmp;
    MemorySyncConfig config{"reader"};
    config.exclusiveBackend = true;
    config.limits.maxMergedKeys = 2;
    config.limits.maxCacheBytes = 8;
    config.limits.maxEnvelopeBytes = 1024;
    MemorySyncService service{makeBackend(tmp.path), config};
    REQUIRE(service.publish("document/a", bytes("a")).has_value());
    auto view = service.captureAdapterView();
    REQUIRE(view.has_value());
    auto index = service.createAdapterIndex(view.value());
    REQUIRE(index.has_value());
    for (int i = 0; i < 6; ++i) {
        REQUIRE(index.value()
                    ->put("chunk/" + std::to_string(i), "model/" + std::to_string(i))
                    .has_value());
    }
    for (int i = 5; i >= 0; --i) {
        auto row = index.value()->lookup("chunk/" + std::to_string(i));
        REQUIRE(row.has_value());
        REQUIRE(row.value().has_value());
        CHECK(*row.value() == "model/" + std::to_string(i));
    }
    auto absent = index.value()->lookup("absent");
    REQUIRE(absent.has_value());
    CHECK_FALSE(absent.value().has_value());
    auto cached = service.readCached("document/a");
    REQUIRE(cached.has_value());
    CHECK(text(cached.value()) == "a");
    CHECK(service.mergedRecordCount() == 1);

    SECTION("input row limit rejects before mutation and leaves earlier rows readable") {
        // maxEnvelopeBytes*4, independently of the eight-byte verified-value cache.
        auto rejected = index.value()->put("oversized", std::string(4097, 'x'));
        REQUIRE_FALSE(rejected.has_value());
        CHECK(rejected.error().code == yams::ErrorCode::ResourceExhausted);
        auto retained = index.value()->lookup("chunk/0");
        REQUIRE(retained.has_value());
        REQUIRE(retained.value().has_value());
        CHECK(*retained.value() == "model/0");
    }
    SECTION("SQLite encoded-row overhead also preserves earlier rows on rejection") {
        const std::string key = "boundary";
        // Raw bytes fit; SQLite's record header crosses the 4096-byte encoded-row limit.
        auto rejected = index.value()->put(key, std::string(4095 - key.size(), 'x'));
        REQUIRE_FALSE(rejected.has_value());
        CHECK(rejected.error().code == yams::ErrorCode::ResourceExhausted);
        auto retained = index.value()->lookup("chunk/0");
        REQUIRE(retained.has_value());
        REQUIRE(retained.value().has_value());
        CHECK(*retained.value() == "model/0");
    }
    SECTION("updates and binary-safe identities do not change source visibility") {
        const std::string key{"chunk\0model", 11};
        const std::string value{"value\0bytes", 11};
        REQUIRE(index.value()->put(key, value).has_value());
        auto row = index.value()->lookup(key);
        REQUIRE(row.has_value());
        REQUIRE(row.value().has_value());
        CHECK(*row.value() == value);
        REQUIRE(index.value()->put("chunk/0", "replacement").has_value());
        auto updated = index.value()->lookup("chunk/0");
        REQUIRE(updated.has_value());
        REQUIRE(updated.value().has_value());
        CHECK(*updated.value() == "replacement");
        TempDirGuard otherTmp;
        MemorySyncService other{makeBackend(otherTmp.path), MemorySyncConfig{"other"}};
        auto foreign = other.createAdapterIndex(view.value());
        REQUIRE_FALSE(foreign.has_value());
        CHECK(foreign.error().code == yams::ErrorCode::InvalidArgument);
    }
}

TEST_CASE("MemorySyncService concurrent adapter allocation respects the shared lease quota",
          "[memory-sync][service][adapter-index][concurrency]") {
    TempDirGuard tmp;
    MemorySyncConfig config{"reader"};
    config.exclusiveBackend = true;
    MemorySyncService service{makeBackend(tmp.path), config};
    REQUIRE(service.publish("document/a", bytes("a")).has_value());
    auto view = service.captureAdapterView();
    REQUIRE(view.has_value());
    auto one = service.createAdapterIndex(view.value());
    auto two = service.createAdapterIndex(view.value());
    REQUIRE(one.has_value());
    REQUIRE(two.has_value());
    const auto activeDirectories = [&]() {
        std::size_t count = 0;
        for (const auto& entry : std::filesystem::directory_iterator(tmp.path / "temp")) {
            if (entry.is_directory() && entry.path().filename().string().starts_with("sync-sweep-"))
                ++count;
        }
        return count;
    };
    REQUIRE(activeDirectories() == 3); // one committed generation and two scratch indexes
    CompletionSignal entered;
    CompletionSignal release;
    std::atomic<bool> observerTimedOut{false};
    auto allocating = std::async(std::launch::async, [&]() {
        struct Reset {
            ~Reset() { MemorySyncLoop::testingSetScratchAllocationObserver({}); }
        } reset;
        MemorySyncLoop::testingSetScratchAllocationObserver([&]() {
            entered.signal();
            if (!release.waitFor(std::chrono::seconds(5)))
                observerTimedOut.store(true);
        });
        return service.createAdapterIndex(view.value());
    });
    const bool reachedBoundary = entered.waitFor(std::chrono::seconds(2));
    auto competing =
        std::async(std::launch::async, [&]() { return service.createAdapterIndex(view.value()); });
    const bool completedBeforeRelease =
        competing.wait_for(std::chrono::milliseconds(500)) == std::future_status::ready;
    release.signal();
    auto first = allocating.get();
    auto second = competing.get();
    // Drain every worker and release the gate before any fatal assertion.
    REQUIRE(reachedBoundary);
    REQUIRE_FALSE(observerTimedOut.load());
    CHECK(completedBeforeRelease);
    REQUIRE(first.has_value());
    CHECK_FALSE(second.has_value());
    if (!second)
        CHECK(second.error().code == yams::ErrorCode::ResourceExhausted);
    CHECK(activeDirectories() <= 4);
}

TEST_CASE("MemorySyncService cold adapter I/O does not serialize independent mutations",
          "[memory-sync][service][adapter-concurrency][disk-backed-serving]") {
    TempDirGuard tmp;
    {
        MemorySyncService writer{makeBackend(tmp.path), MemorySyncConfig{"writer"}};
        REQUIRE(writer.publish("user/cold", bytes("colddata")).has_value());
        REQUIRE(writer.publish("user/other", bytes("other---")).has_value());
        REQUIRE(writer.publish("user/warm", bytes("warmdata")).has_value());
    }
    MemorySyncConfig config{"reader"};
    config.exclusiveBackend = true;
    config.limits.maxMergedKeys = 1;
    config.limits.maxCacheBytes = 8;
    config.limits.maxIndexObjectsPerSync = 1;
    MemorySyncService service{makeBackend(tmp.path), config};
    REQUIRE(service.syncFully().has_value());
    REQUIRE(service.mergedRecordCount() == 3);
    REQUIRE(service.readCommitted("user/warm").has_value());
    auto view = service.captureAdapterView();
    REQUIRE(view.has_value());
    REQUIRE_FALSE(service.readCached("user/cold").has_value());
    const auto cold = service.readCommittedWinnersAfter("user/cold", "", 1);
    REQUIRE(cold.has_value());
    REQUIRE(cold.value().size() == 1);
    REQUIRE(cold.value().front().first == "user/cold");
    const auto blobKey = "blob/" + cold.value().front().second.entryHash;

    bool quarantineDuringFetch = false;
    SECTION("an independent publisher completes while the cold read is gated") {}
    SECTION("a mid-fetch quarantine completes and rejects the pinned payload") {
        quarantineDuringFetch = true;
    }

    std::promise<void> enteredPromise;
    auto entered = enteredPromise.get_future();
    std::promise<void> releasePromise;
    auto release = releasePromise.get_future().share();
    std::atomic<bool> gated{false};
    auto reading = std::async(std::launch::async, [&] {
        std::function<void(std::string_view)> observer = [&](std::string_view key) {
            if (key == blobKey && !gated.exchange(true)) {
                enteredPromise.set_value();
                release.wait();
            }
        };
        FilesystemReadObserverGuard observed{observer};
        return service.readAdapterPayload(view.value(), "user/cold");
    });
    const bool readEntered = entered.wait_for(std::chrono::seconds(2)) == std::future_status::ready;
    CompletionSignal mutationStarted;
    auto mutation = std::async(std::launch::async, [&]() -> yams::Result<void> {
        mutationStarted.signal();
        if (!quarantineDuringFetch) {
            return service.publish("user/new", bytes("nextdata"));
        }
        const auto banned = service.quarantineWriter("writer", "reader", "mid-fetch test");
        if (!banned) {
            return banned.error();
        }
        if (!banned.value()) {
            return yams::Error{yams::ErrorCode::InvalidState, "writer was not quarantined"};
        }
        return {};
    });
    const bool started = mutationStarted.waitFor(std::chrono::seconds(2));
    // A generous bound for a tiny local write under TSAN; no time is spent sleeping to
    // manufacture the interleaving. The old implementation cannot finish until release.
    const bool mutationBeforeRelease =
        mutation.wait_for(std::chrono::milliseconds(500)) == std::future_status::ready;
    releasePromise.set_value();
    const auto payload = reading.get();
    const auto changed = mutation.get();
    // Release/join both tasks before fatal assertions, including fixture failures.
    REQUIRE(readEntered);
    REQUIRE(started);
    REQUIRE(changed.has_value());
    CHECK(mutationBeforeRelease);
    if (quarantineDuringFetch) {
        CHECK_FALSE(payload.has_value());
        if (!payload) {
            CHECK(payload.error().code == yams::ErrorCode::Unauthorized);
        }
    } else {
        REQUIRE(payload.has_value());
        CHECK(text(payload.value()) == "colddata");
        CHECK(service.mergedRecordCount() == 4);
    }
}

TEST_CASE("MemorySyncService adapter pressure pauses do not hold the loop lock",
          "[memory-sync][service][adapter-concurrency][governor]") {
    TempDirGuard tmp;
    MemorySyncConfig config{"reader"};
    config.exclusiveBackend = true;
    config.limits.maxIndexObjectsPerSync = 1;
    std::atomic<bool> pressure{false};
    MemorySyncService service{makeBackend(tmp.path),          config, {}, false, {}, "",
                              [&] { return pressure.load(); }};
    REQUIRE(service.publish("user/a", bytes("alpha")).has_value());
    auto view = service.captureAdapterView();
    REQUIRE(view.has_value());
    REQUIRE(service.mergedRecordCount() == 1);
    pressure.store(true);
    service.testingExpireWorkQuantum();

    bool cancelDuringPause = false;
    SECTION("publish can proceed while the visitor is pressure paused") {}
    SECTION("stop cancels the paused visitor before it visits a row") {
        cancelDuringPause = true;
    }

    std::promise<void> enteredPromise;
    auto entered = enteredPromise.get_future();
    std::promise<void> releasePromise;
    auto release = releasePromise.get_future().share();
    std::atomic<bool> gated{false};
    std::size_t visited = 0;
    auto visiting = std::async(std::launch::async, [&] {
        std::function<void()> observer = [&] {
            if (!gated.exchange(true)) {
                enteredPromise.set_value();
                release.wait();
            }
        };
        PressurePauseObserverGuard observed{observer};
        return service.visitAdapterWinners(view.value(), "user/",
                                           [&](const auto&, const auto&) -> yams::Result<void> {
                                               ++visited;
                                               return {};
                                           });
    });
    const bool pauseEntered =
        entered.wait_for(std::chrono::seconds(2)) == std::future_status::ready;
    CompletionSignal mutationStarted;
    auto mutation = std::async(std::launch::async, [&]() -> yams::Result<void> {
        mutationStarted.signal();
        if (cancelDuringPause) {
            service.stop();
            return {};
        }
        return service.publish("user/b", bytes("beta"));
    });
    const bool started = mutationStarted.waitFor(std::chrono::seconds(2));
    const bool mutationBeforeRelease =
        mutation.wait_for(std::chrono::milliseconds(500)) == std::future_status::ready;
    releasePromise.set_value();
    const auto progress = visiting.get();
    const auto changed = mutation.get();
    REQUIRE(pauseEntered);
    REQUIRE(started);
    REQUIRE(changed.has_value());
    CHECK(mutationBeforeRelease);
    if (cancelDuringPause) {
        REQUIRE_FALSE(progress.has_value());
        CHECK(progress.error().code == yams::ErrorCode::OperationCancelled);
        CHECK(visited == 0);
    } else {
        REQUIRE(progress.has_value());
        CHECK(visited == 1); // The concurrent publication is outside the pinned generation.
        CHECK(service.mergedRecordCount() == 2);
    }
}

TEST_CASE("MemorySyncService pinned transaction work defers pressure without waiting",
          "[memory-sync][service][adapter-pressure-defer][governor]") {
    TempDirGuard tmp;
    MemorySyncConfig config{"reader"};
    config.exclusiveBackend = true;
    config.limits.maxMergedKeys = 2;
    config.limits.maxCacheBytes = 8;
    config.limits.maxIndexObjectsPerSync = 1;
    std::atomic<bool> pressure{false};
    MemorySyncService service{makeBackend(tmp.path),          config, {}, false, {}, "",
                              [&] { return pressure.load(); }};
    REQUIRE(service.publish("user/a", bytes("aaaaaaaa")).has_value());
    REQUIRE(service.publish("user/b", bytes("bbbbbbbb")).has_value());
    auto view = service.captureAdapterView(MemorySyncService::AdapterPressureMode::Defer);
    REQUIRE(view.has_value());
    REQUIRE(service.mergedRecordCount() == 2);
    const std::string warm = service.readCached("user/a") ? "user/a" : "user/b";
    const std::string cold = warm == "user/a" ? "user/b" : "user/a";
    REQUIRE(service.readCached(warm).has_value());
    REQUIRE_FALSE(service.readCached(cold).has_value());
    auto index = service.createAdapterIndex(view.value());
    REQUIRE(index.has_value());
    REQUIRE(index.value()->put("old", "kept").has_value());
    std::size_t pauses = 0;
    std::size_t blobReads = 0;
    std::function<void()> pauseObserver = [&] { ++pauses; };
    std::function<void(std::string_view)> readObserver = [&](std::string_view key) {
        if (key.starts_with("blob/")) {
            ++blobReads;
        }
    };
    PressurePauseObserverGuard paused{pauseObserver};
    FilesystemReadObserverGuard retrieved{readObserver};
    // Temporary local admission is distinct from scratch capacity or remote-admission errors.
    auto checkDeferred = [](const auto& result) {
        CHECK_FALSE(result.has_value());
        if (!result) {
            CHECK(result.error().code == yams::ErrorCode::ResourceBusy);
        }
    };
    pressure.store(true);
    service.testingExpireWorkQuantum();
    checkDeferred(service.captureAdapterView(MemorySyncService::AdapterPressureMode::Defer));
    service.testingExpireWorkQuantum();
    std::size_t visited = 0;
    auto visit = service.visitAdapterWinners(view.value(), "user/",
                                             [&](const auto&, const auto&) -> yams::Result<void> {
                                                 ++visited;
                                                 return {};
                                             });
    checkDeferred(visit);
    CHECK(visited == 0);
    service.testingExpireWorkQuantum();
    checkDeferred(service.readAdapterPayload(view.value(), cold));
    service.testingExpireWorkQuantum();
    checkDeferred(service.readAdapterPayload(view.value(), warm));
    service.testingExpireWorkQuantum();
    auto created = service.createAdapterIndex(view.value());
    checkDeferred(created);
    service.testingExpireWorkQuantum();
    checkDeferred(index.value()->put("blocked", "not committed"));
    service.testingExpireWorkQuantum();
    checkDeferred(index.value()->lookup("old"));
    CHECK(pauses == 0);
    CHECK(blobReads == 0);
    // The explicitly cache-only API stays available and never becomes a pressure wait.
    CHECK(service.readCached(warm).has_value());
    pressure.store(false);
    auto retained = index.value()->lookup("old");
    auto rejected = index.value()->lookup("blocked");
    REQUIRE(retained.has_value());
    REQUIRE(retained.value().has_value());
    CHECK(*retained.value() == "kept");
    REQUIRE(rejected.has_value());
    CHECK_FALSE(rejected.value().has_value());
    auto retry = service.readAdapterPayload(view.value(), cold);
    REQUIRE(retry.has_value());
    CHECK(text(retry.value()) == (cold == "user/a" ? "aaaaaaaa" : "bbbbbbbb"));
    visited = 0;
    REQUIRE(service
                .visitAdapterWinners(view.value(), "user/",
                                     [&](const auto&, const auto&) -> yams::Result<void> {
                                         ++visited;
                                         return {};
                                     })
                .has_value());
    CHECK(visited == 2);
    service.stop();
    auto cancelled = service.readAdapterPayload(view.value(), warm);
    REQUIRE_FALSE(cancelled.has_value());
    CHECK(cancelled.error().code == yams::ErrorCode::OperationCancelled);
}

TEST_CASE("MemorySyncService deferred adapter work still checks remote admission",
          "[memory-sync][service][adapter-pressure-defer][governor]") {
    class RemoteBackend final : public CountingBackend {
    public:
        bool isRemote() const override { return true; }
    };
    TempDirGuard tmp;
    auto backend = std::make_unique<RemoteBackend>();
    yams::storage::BackendConfig backendConfig;
    backendConfig.type = "filesystem";
    backendConfig.localPath = tmp.path;
    REQUIRE(backend->initialize(backendConfig).has_value());
    std::atomic<bool> admitted{true};
    MemorySyncService service{std::move(backend), MemorySyncConfig{"reader"},
                              [&] { return admitted.load(); }};
    REQUIRE(service.publish("user/a", bytes("value")).has_value());
    auto view = service.captureAdapterView(MemorySyncService::AdapterPressureMode::Defer);
    REQUIRE(view.has_value());
    auto index = service.createAdapterIndex(view.value());
    REQUIRE(index.has_value());
    REQUIRE(index.value()->put("old", "kept").has_value());
    admitted.store(false);
    auto checkDenied = [](const auto& result) {
        CHECK_FALSE(result.has_value());
        if (!result) {
            CHECK(result.error().code == yams::ErrorCode::ResourceExhausted);
        }
    };
    checkDenied(service.readAdapterPayload(view.value(), "user/a"));
    checkDenied(service.createAdapterIndex(view.value()));
    checkDenied(index.value()->lookup("old"));
    checkDenied(index.value()->put("blocked", "not committed"));
    admitted.store(true);
    auto retained = index.value()->lookup("old");
    auto rejected = index.value()->lookup("blocked");
    REQUIRE(retained.has_value());
    REQUIRE(retained.value().has_value());
    CHECK(*retained.value() == "kept");
    REQUIRE(rejected.has_value());
    CHECK_FALSE(rejected.value().has_value());
    service.stop();
    auto cancelled = service.readAdapterPayload(view.value(), "user/a");
    REQUIRE_FALSE(cancelled.has_value());
    CHECK(cancelled.error().code == yams::ErrorCode::OperationCancelled);
}

TEST_CASE("MemorySyncService escaped adapter indexes cancel after their owner is destroyed",
          "[memory-sync][service][adapter-pressure-defer][adapter-index-lifetime]") {
    auto mode = MemorySyncService::AdapterPressureMode::Wait;
    SECTION("ordinary pressure policy") {}
    SECTION("immediate pressure deferral") {
        mode = MemorySyncService::AdapterPressureMode::Defer;
    }
    TempDirGuard tmp;
    std::shared_ptr<MemorySyncLoop::AdapterIndex> escaped;
    {
        MemorySyncConfig config{"reader"};
        config.exclusiveBackend = true;
        MemorySyncService service{makeBackend(tmp.path), config};
        REQUIRE(service.publish("user/a", bytes("a")).has_value());
        auto view = service.captureAdapterView(mode);
        REQUIRE(view.has_value());
        auto index = service.createAdapterIndex(view.value());
        REQUIRE(index.has_value());
        REQUIRE(index.value()->put("old", "kept").has_value());
        escaped = std::move(index.value());
    }
    auto read = escaped->lookup("old");
    auto write = escaped->put("new", "blocked");
    CHECK_FALSE(read.has_value());
    if (!read) {
        CHECK(read.error().code == yams::ErrorCode::OperationCancelled);
    }
    CHECK_FALSE(write.has_value());
    if (!write) {
        CHECK(write.error().code == yams::ErrorCode::OperationCancelled);
    }
}

TEST_CASE("MemorySyncService deferred payload reads do not wait for busy mutable state",
          "[memory-sync][service][adapter-pressure-defer][adapter-state-busy]") {
    TempDirGuard tmp;
    auto backend = std::make_unique<SecondPageGateBackend>();
    auto* gate = backend.get();
    yams::storage::BackendConfig backendConfig;
    backendConfig.type = "filesystem";
    backendConfig.localPath = tmp.path;
    REQUIRE(backend->initialize(backendConfig).has_value());
    MemorySyncConfig config{"reader"};
    config.limits.maxIndexObjectsPerSync = 1;
    MemorySyncService service{std::move(backend), config};
    REQUIRE(service.publish("user/a", bytes("alpha")).has_value());
    REQUIRE(service.publish("user/b", bytes("beta")).has_value());
    auto view = service.captureAdapterView(MemorySyncService::AdapterPressureMode::Defer);
    REQUIRE(view.has_value());
    auto entered = gate->arm();
    auto syncing = std::async(std::launch::async, [&] { return service.syncFully(); });
    const bool stateHeld = entered.wait_for(std::chrono::seconds(2)) == std::future_status::ready;
    CompletionSignal readStarted;
    auto reading = std::async(std::launch::async, [&] {
        readStarted.signal();
        return service.readAdapterPayload(view.value(), "user/a");
    });
    const bool started = readStarted.waitFor(std::chrono::seconds(2));
    const bool rejectedBeforeRelease =
        reading.wait_for(std::chrono::milliseconds(500)) == std::future_status::ready;
    const auto cached = service.readCached("user/a");
    // Release and join every background operation before any fatal assertion.
    gate->release();
    const auto reconciled = syncing.get();
    const auto read = reading.get();
    REQUIRE(stateHeld);
    REQUIRE(started);
    REQUIRE(reconciled.has_value());
    REQUIRE(cached.has_value());
    CHECK(text(cached.value()) == "alpha");
    CHECK(rejectedBeforeRelease);
    CHECK_FALSE(read.has_value());
    if (!read) {
        CHECK(read.error().code == yams::ErrorCode::ResourceBusy);
    }
    const auto retried = service.readAdapterPayload(view.value(), "user/a");
    REQUIRE(retried.has_value());
    CHECK(text(retried.value()) == "alpha");
}

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

TEST_CASE("MemorySyncService does not republish unchanged evicted winners",
          "[memory-sync][service][disk-backed-writes][backfill]") {
    TempDirGuard tmp;
    {
        auto backend = makeBackend(tmp.path);
        MemorySyncLoop writer{*backend, "writer"};
        for (int i = 0; i < 6; ++i) {
            REQUIRE(
                writer
                    .publish("user/key-" + std::to_string(i), bytes("payload-" + std::to_string(i)))
                    .has_value());
        }
    }
    MemorySyncConfig config{"writer", 5000};
    config.exclusiveBackend = true;
    config.limits.maxMergedKeys = 2;
    config.limits.maxCacheBytes = 18;
    MemorySyncService service{makeBackend(tmp.path), config};
    REQUIRE(service.syncFully().has_value());
    for (int i = 0; i < 6; ++i) {
        CAPTURE(i);
        const auto published = service.publishIfChanged("user/key-" + std::to_string(i),
                                                        bytes("payload-" + std::to_string(i)));
        REQUIRE(published.has_value());
        CHECK_FALSE(published.value());
    }
}

TEST_CASE("MemorySyncService pages every exclusive winner beyond resident budgets",
          "[memory-sync][service][disk-backed-serving][pagination]") {
    TempDirGuard tmp;
    constexpr std::size_t kRecords = 6;
    constexpr std::size_t kPageSize = 2;
    std::vector<std::pair<std::string, std::string>> expected;
    {
        auto backend = makeBackend(tmp.path);
        MemorySyncLoop writer{*backend, "writer"};
        for (std::size_t i = 0; i < kRecords; ++i) {
            expected.emplace_back("user/key-" + std::to_string(i), "payload-" + std::to_string(i));
            REQUIRE(
                writer.publish(expected.back().first, bytes(expected.back().second)).has_value());
        }
    }
    MemorySyncConfig config{"reader", 5000};
    config.exclusiveBackend = true;
    config.limits.maxIndexObjectsPerSync = kPageSize;
    config.limits.maxMergedKeys = kPageSize;
    config.limits.maxCacheBytes = kPageSize * expected.front().second.size();

    for (int recovery = 0; recovery < 2; ++recovery) {
        CAPTURE(recovery);
        MemorySyncService service{makeBackend(tmp.path), config};
        const auto recovered = service.syncFully();
        INFO("recovery result: " << (recovered ? std::string{"success"}
                                               : recovered.error().message));
        REQUIRE(recovered.has_value());
        CHECK(service.mergedRecordCount() == kRecords);
        CHECK(service.committedWinnersAfter("user/", "", 0).empty());
        std::size_t seen = 0;
        std::string cursor;
        bool drained = false;
        // Bound the test even if a broken implementation never drains its cursor.
        for (std::size_t pageIndex = 0; pageIndex <= kRecords; ++pageIndex) {
            const auto page = service.committedWinnersAfter("user/", cursor, kPageSize);
            REQUIRE(page.size() <= kPageSize);
            if (page.empty()) {
                drained = true;
                break;
            }
            for (const auto& [key, record] : page) {
                REQUIRE(seen < expected.size());
                REQUIRE(key > cursor);
                CHECK(key == expected[seen].first);
                CHECK(record.logicalKey == key);
                const auto value = service.read(key);
                REQUIRE(value.has_value());
                CHECK(text(value.value()) == expected[seen].second);
                cursor = key;
                ++seen;
            }
        }
        CHECK(drained);
        CHECK(seen == kRecords);
    }
}

TEST_CASE("MemorySyncService counts complete sweeps rather than successful pages",
          "[memory-sync][service][sweep-progress]") {
    TempDirGuard tmp;
    auto backend = std::make_unique<SecondPageGateBackend>();
    yams::storage::BackendConfig backendConfig;
    backendConfig.localPath = tmp.path;
    REQUIRE(backend->initialize(backendConfig).has_value());
    MemorySyncLoop writer{*backend, "writer"};
    REQUIRE(writer.publish("a", bytes("one")).has_value());
    REQUIRE(writer.publish("b", bytes("two")).has_value());
    auto* gate = backend.get();
    auto secondPage = gate->arm();
    MemorySyncConfig config{"reader", 1};
    config.limits.maxIndexObjectsPerSync = 1;
    std::atomic<std::size_t> callbacks{0};
    CompletionSignal complete;
    MemorySyncService service{std::move(backend), config};
    service.setAfterSyncCallback([&] {
        callbacks.fetch_add(1);
        complete.signal();
    });
    REQUIRE(service.start().has_value());
    REQUIRE(secondPage.wait_for(std::chrono::seconds(2)) == std::future_status::ready);
    CHECK(service.successfulSyncCycles() == 0);
    CHECK(callbacks.load() == 0);
    gate->release();
    REQUIRE(complete.waitFor(std::chrono::seconds(2)));
    service.stop();
    CHECK(service.successfulSyncCycles() >= 1);
}

TEST_CASE("MemorySyncService makes paged progress under sustained local pressure",
          "[memory-sync][service][governor][sweep-progress]") {
    TempDirGuard tmp;
    auto backend = std::make_unique<CountingBackend>();
    yams::storage::BackendConfig backendConfig;
    backendConfig.localPath = tmp.path;
    REQUIRE(backend->initialize(backendConfig).has_value());
    MemorySyncLoop writer{*backend, "writer"};
    REQUIRE(writer.publish("a", bytes("one")).has_value());
    REQUIRE(writer.publish("b", bytes("two")).has_value());
    auto* observed = backend.get();
    const auto before = observed->listPageCalls.load();
    MemorySyncConfig config{"reader", 5000};
    config.limits.maxIndexObjectsPerSync = 1;
    std::atomic<std::size_t> pressureChecks{0};
    MemorySyncService service{std::move(backend), config, {}, false, {}, "", [&] {
                                  pressureChecks.fetch_add(1);
                                  return true;
                              }};
    // syncFully drives all pages without wall-clock waits between worker cycles.
    REQUIRE(service.syncFully().has_value());
    CHECK(pressureChecks.load() > 0);
    CHECK(observed->listPageCalls.load() - before == 2);
    CHECK(service.mergedRecordCount() == 2);
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

TEST_CASE("MemorySyncService stop re-cancels a worker restarted during shutdown",
          "[memory-sync][service][lifecycle][cancellation][race]") {
    TempDirGuard tmp;
    auto backend = std::make_unique<RestartCancellationBackend>();
    yams::storage::BackendConfig config;
    config.type = "filesystem";
    config.localPath = tmp.path;
    REQUIRE(backend->initialize(config).has_value());
    auto* observed = backend.get();
    MemorySyncService service{std::move(backend), MemorySyncConfig{"A", 5000}};

    REQUIRE(service.start().has_value());
    const bool firstWorkerEntered = observed->waitForListCalls(1, std::chrono::seconds(2));
    if (!firstWorkerEntered) {
        observed->abortWaits();
        service.stop();
        REQUIRE(firstWorkerEntered);
    }

    CompletionSignal stopFinished;
    std::thread stopper([&] {
        service.stop();
        stopFinished.signal();
    });
    const bool firstCancelApplied = observed->waitForFirstCancel(std::chrono::seconds(2));
    if (!firstCancelApplied) {
        observed->abortWaits();
        stopper.join();
        service.stop();
        REQUIRE(firstCancelApplied);
    }

    CompletionSignal restartFinished;
    std::atomic<bool> restartSucceeded{false};
    std::thread restarter([&] {
        restartSucceeded.store(service.start().has_value(), std::memory_order_release);
        restartFinished.signal();
    });
    const bool restartReturned = restartFinished.waitFor(std::chrono::seconds(2));
    if (!restartReturned) {
        observed->abortWaits();
        restarter.join();
        stopper.join();
        service.stop();
        REQUIRE(restartReturned);
    }
    restarter.join();
    const bool restarted = restartSucceeded.load(std::memory_order_acquire);
    if (!restarted) {
        observed->abortWaits();
        stopper.join();
        service.stop();
        REQUIRE(restarted);
    }

    const bool restartedWorkerEntered = observed->waitForListCalls(2, std::chrono::seconds(2));
    const auto restartedGeneration = observed->listGeneration(2);
    if (!restartedWorkerEntered || !restartedGeneration.has_value()) {
        observed->abortWaits();
        stopper.join();
        service.stop();
        REQUIRE(restartedWorkerEntered);
        REQUIRE(restartedGeneration.has_value());
    }

    // Let the original, pre-lock cancellation return. stop() must cancel again
    // while holding lifecycleMutex_, because start() reset cancellation and the
    // worker now blocked belongs to a new generation.
    observed->releaseFirstCancel();
    const bool newGenerationCancelled =
        observed->waitForGenerationCancel(*restartedGeneration, std::chrono::seconds(2));
    const bool stopReturned = stopFinished.waitFor(std::chrono::seconds(2));

    // Preserve a bounded red failure: unblock every generation before joining
    // test threads, even when production omitted the second cancellation.
    observed->abortWaits();
    stopper.join();
    service.stop();

    CHECK(newGenerationCancelled);
    CHECK(stopReturned);
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
    if (!yams::config::getenv_optional("YAMS_RUN_STRESS_TESTS").has_value()) {
        SKIP("Stress tests disabled. Set YAMS_RUN_STRESS_TESTS=1 to enable.");
    }
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
            auto nested = reader.quarantineWriter("writer-b", "reader", "test request");
            nestedQuarantineSucceeded.store(nested && nested.value(), std::memory_order_release);
        }
    });
    auto quarantined = reader.quarantineWriter("writer-a", "reader", "test request");
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
            auto quarantineA = readerA.quarantineWriter("writer-a2", "reader-a", "test request");
            nestedA.store(quarantineA && quarantineA.value(), std::memory_order_release);
        }
    });
    readerA.setAfterSyncCallback([&] {
        if (callbacksA.fetch_add(1, std::memory_order_acq_rel) == 0) {
            auto quarantineB = readerB.quarantineWriter("writer-b", "reader-b", "test request");
            nestedB.store(quarantineB && quarantineB.value(), std::memory_order_release);
        }
    });

    auto quarantineA1 = readerA.quarantineWriter("writer-a1", "reader-a", "test request");
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

    auto quarantined = reader.quarantineWriter("writer", "reader", "test request");
    REQUIRE(quarantined.has_value());
    CHECK(quarantined.value());
    CHECK(callbacks.load(std::memory_order_relaxed) == 1);
    CHECK(reader.replicationState().quarantinedWriters.contains("writer"));
    CHECK_FALSE(reader.readCached("user/key").has_value());

    auto repeated = reader.quarantineWriter("writer", "reader", "test request");
    REQUIRE(repeated.has_value());
    CHECK_FALSE(repeated.value());
    CHECK(callbacks.load(std::memory_order_relaxed) == 1);
}

TEST_CASE("MemorySync callback drain releases lifecycle before waiting",
          "[memory-sync][service][lifecycle][callback-drain-order]") {
    std::mutex lifecycle;
    std::recursive_mutex callbacks;
    std::unique_lock<std::mutex> lifecycleGate(lifecycle);
    bool lifecycleHeldAtBoundary = true;
    auto callbackDrain = yams::memory_sync::detail::acquireCallbackDrain(
        lifecycleGate, callbacks, [&] { lifecycleHeldAtBoundary = lifecycleGate.owns_lock(); });

    CHECK_FALSE(lifecycleHeldAtBoundary);
    CHECK(lifecycleGate.owns_lock());
    CHECK(callbackDrain.owns_lock());
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
