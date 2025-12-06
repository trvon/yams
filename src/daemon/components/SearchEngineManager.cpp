#include <yams/daemon/components/SearchEngineManager.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/search/search_engine.h>
#include <yams/search/search_engine_builder.h>
#include <yams/vector/embedding_generator.h>
#include <yams/vector/vector_database.h>
#include <yams/vector/vector_index_manager.h>

#include <spdlog/spdlog.h>
#include <boost/asio/as_tuple.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/experimental/awaitable_operators.hpp>
#include <boost/asio/experimental/channel.hpp>
#include <boost/asio/experimental/concurrent_channel.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>

namespace yams::daemon {

yams::search::SearchEngine* SearchEngineManager::getCachedEngine() const {
    std::shared_lock lock(snapshotMutex_);
    return cachedEngine_;
}

SearchEngineSnapshot SearchEngineManager::getSnapshot() const {
    return fsm_.snapshot();
}

std::shared_ptr<yams::search::SearchEngine> SearchEngineManager::getEngine() const {
    std::shared_lock lock(engineMutex_);
    return engine_;
}

void SearchEngineManager::setEngine(const std::shared_ptr<yams::search::SearchEngine>& engine,
                                    bool vectorEnabled) {
    {
        std::unique_lock lock(engineMutex_);
        engine_ = engine;
    }

    // Update FSM and snapshot
    if (engine) {
        SearchEngineRebuildCompletedEvent ev;
        ev.vectorEnabled = vectorEnabled;
        ev.durationMs = 0;
        fsm_.dispatch(ev);
    }
    refreshSnapshot();
}

void SearchEngineManager::refreshSnapshot() {
    std::shared_ptr<yams::search::SearchEngine> eng;
    {
        std::shared_lock lock(engineMutex_);
        eng = engine_;
    }

    std::unique_lock snapLock(snapshotMutex_);
    cachedEngine_ = eng.get();
}

boost::asio::awaitable<Result<std::shared_ptr<yams::search::SearchEngine>>>
SearchEngineManager::buildEngine(std::shared_ptr<yams::metadata::MetadataRepository> metadataRepo,
                                 std::shared_ptr<yams::vector::VectorDatabase> vectorDatabase,
                                 std::shared_ptr<yams::vector::VectorIndexManager> vectorManager,
                                 std::shared_ptr<yams::vector::EmbeddingGenerator> embeddingGen,
                                 const std::string& reason, int timeoutMs,
                                 boost::asio::any_io_executor workerExecutor) {
    using namespace boost::asio::experimental::awaitable_operators;

    auto ex = co_await boost::asio::this_coro::executor;

    // Enable vector search only when a manager is provided; otherwise build a text-only engine
    // that can be upgraded later when vectors become available.
    bool vectorEnabled = (vectorManager != nullptr && vectorDatabase != nullptr);

    if (vectorEnabled) {
        spdlog::info("[SearchEngineManager] Vector search enabled: vectorManager provided");
    } else {
        spdlog::info(
            "[SearchEngineManager] Vector search disabled: building text-only engine (will "
            "rebuild when vectors become available)");
    }

    // Dispatch FSM event: build started
    try {
        SearchEngineRebuildStartedEvent ev;
        ev.reason = reason;
        ev.includeVectorSearch = vectorEnabled;
        fsm_.dispatch(ev);
    } catch (...) {
    }

    spdlog::info("[SearchEngineManager] Build started: reason={} vector={} timeout={}ms", reason,
                 vectorEnabled, timeoutMs);

    // Create builder and configure
    auto builder = std::make_shared<yams::search::SearchEngineBuilder>();
    builder->withMetadataRepo(metadataRepo);
    if (vectorEnabled && vectorDatabase)
        builder->withVectorDatabase(vectorDatabase);
    if (vectorEnabled && vectorManager)
        builder->withVectorIndex(vectorManager);
    if (vectorEnabled && embeddingGen)
        builder->withEmbeddingGenerator(embeddingGen);

    auto opts = yams::search::SearchEngineBuilder::BuildOptions::makeDefault();
    if (!vectorEnabled) {
        opts.config.vectorWeight = 0.0f;
        opts.config.vectorMaxResults = 0;
    }

    using RetT = Result<std::shared_ptr<yams::search::SearchEngine>>;
    boost::asio::experimental::concurrent_channel<
        boost::asio::any_io_executor, void(boost::system::error_code, std::shared_ptr<RetT>)>
        ch(ex, 1);

    // Post build work to worker executor (blocking operations)
    try {
        boost::asio::post(workerExecutor, [builder, opts, &ch]() mutable {
            try {
                auto r = builder->buildEmbedded(opts);
                ch.try_send(boost::system::error_code{}, std::make_shared<RetT>(std::move(r)));
            } catch (...) {
                ch.try_send(
                    boost::asio::error::make_error_code(boost::asio::error::operation_aborted),
                    std::make_shared<RetT>(
                        Error{ErrorCode::InternalError, "Engine build exception"}));
            }
        });
    } catch (...) {
        // Fallback to synchronous build
        auto r = builder->buildEmbedded(opts);
        if (r) {
            const auto& newEngine = r.value();
            {
                std::unique_lock lock(engineMutex_);
                engine_ = newEngine;
            }
            try {
                SearchEngineRebuildCompletedEvent ev;
                ev.vectorEnabled = vectorEnabled;
                ev.durationMs = 0;
                fsm_.dispatch(ev);
                refreshSnapshot();
            } catch (...) {
            }
            spdlog::info("[SearchEngineManager] Build completed synchronously (fallback)");
            co_return Result<std::shared_ptr<yams::search::SearchEngine>>(newEngine);
        }
        spdlog::warn("[SearchEngineManager] Build failed synchronously (fallback)");
        try {
            SearchEngineRebuildFailedEvent ev;
            ev.error = "sync_build_failed";
            fsm_.dispatch(ev);
        } catch (...) {
        }
        co_return Result<std::shared_ptr<yams::search::SearchEngine>>(
            Error{ErrorCode::InternalError, "sync_build_failed"});
    }

    // Wait with timeout
    boost::asio::steady_timer timer(ex);
    timer.expires_after(std::chrono::milliseconds(timeoutMs));

    auto which = co_await (ch.async_receive(boost::asio::as_tuple(boost::asio::use_awaitable)) ||
                           timer.async_wait(boost::asio::as_tuple(boost::asio::use_awaitable)));

    // Timeout case
    if (which.index() == 1) {
        spdlog::warn("[SearchEngineManager] Build timeout after {}ms", timeoutMs);
        try {
            SearchEngineRebuildFailedEvent ev;
            ev.error = "build_timeout";
            fsm_.dispatch(ev);
        } catch (...) {
        }
        co_return Result<std::shared_ptr<yams::search::SearchEngine>>(
            Error{ErrorCode::InternalError, "build_timeout"});
    }

    // Extract result from channel
    auto tup = std::move(std::get<0>(which));
    auto ec = std::get<0>(tup);
    auto resultOpt = std::get<1>(tup);

    if (ec || !resultOpt || !resultOpt->has_value()) {
        std::string errMsg = "unknown_error";
        if (resultOpt && !resultOpt->has_value())
            errMsg = resultOpt->error().message;

        spdlog::error("[SearchEngineManager] Build failed: {}", errMsg);
        try {
            SearchEngineRebuildFailedEvent ev;
            ev.error = errMsg;
            fsm_.dispatch(ev);
        } catch (...) {
        }
        co_return Result<std::shared_ptr<yams::search::SearchEngine>>(
            Error{ErrorCode::InternalError, errMsg});
    }

    // Success: store engine and update FSM
    auto newEngine = resultOpt->value();
    {
        std::unique_lock lock(engineMutex_);
        engine_ = newEngine;
    }

    try {
        SearchEngineRebuildCompletedEvent ev;
        ev.vectorEnabled = vectorEnabled;
        ev.durationMs = 0; // TODO: track actual duration
        fsm_.dispatch(ev);
        refreshSnapshot();
    } catch (...) {
    }

    spdlog::info("[SearchEngineManager] Build completed: vector={}", vectorEnabled);
    co_return Result<std::shared_ptr<yams::search::SearchEngine>>(newEngine);
}

} // namespace yams::daemon
