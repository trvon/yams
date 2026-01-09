#include <yams/daemon/components/SearchEngineManager.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/search/search_engine.h>
#include <yams/search/search_engine_builder.h>
#include <yams/vector/embedding_generator.h>
#include <yams/vector/vector_database.h>
#include <yams/vector/vector_index_manager.h>

#include <spdlog/spdlog.h>
#include <boost/asio/as_tuple.hpp>
#include <boost/asio/associated_executor.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
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
                                 std::shared_ptr<yams::vector::EmbeddingGenerator> embeddingGen,
                                 const std::string& reason, int timeoutMs,
                                 const boost::asio::any_io_executor& workerExecutor) {
    auto ex = co_await boost::asio::this_coro::executor;

    // Enable vector search only when vector database is provided
    bool vectorEnabled = (vectorDatabase != nullptr);

    if (vectorEnabled) {
        spdlog::info("[SearchEngineManager] Vector search enabled: vectorDatabase provided");
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
    } catch (...) { // NOLINT(bugprone-empty-catch): FSM failures must not interrupt build
    }

    spdlog::info("[SearchEngineManager] Build started: reason={} vector={} timeout={}ms", reason,
                 vectorEnabled, timeoutMs);

    // Create builder and configure
    auto builder = std::make_shared<yams::search::SearchEngineBuilder>();
    builder->withMetadataRepo(metadataRepo);
    if (vectorEnabled && vectorDatabase)
        builder->withVectorDatabase(vectorDatabase);
    if (vectorEnabled && embeddingGen)
        builder->withEmbeddingGenerator(embeddingGen);

    auto opts = yams::search::SearchEngineBuilder::BuildOptions::makeDefault();
    if (!vectorEnabled) {
        opts.config.vectorWeight = 0.0f;
        opts.config.vectorMaxResults = 0;
    }

    using RetT = Result<std::shared_ptr<yams::search::SearchEngine>>;

    // Use async_initiate pattern with timeout racing (no experimental APIs)
    co_return co_await boost::asio::async_initiate<decltype(boost::asio::use_awaitable),
                                                   void(std::exception_ptr, RetT)>(
        [this, builder, opts, workerExecutor, ex, timeoutMs, vectorEnabled](auto handler) mutable {
            // Shared state for race coordination
            auto completed = std::make_shared<std::atomic<bool>>(false);
            auto timer = std::make_shared<boost::asio::steady_timer>(ex);
            timer->expires_after(std::chrono::milliseconds(timeoutMs));

            // Capture handler in shared_ptr for safe sharing between timer and work
            using HandlerT = std::decay_t<decltype(handler)>;
            auto handlerPtr = std::make_shared<HandlerT>(std::move(handler));
            auto completion_exec = boost::asio::get_associated_executor(*handlerPtr, ex);

            // Set up timeout
            timer->async_wait([this, completed, handlerPtr, completion_exec,
                               timeoutMs](const boost::system::error_code& ec) mutable {
                if (ec)
                    return; // Timer cancelled
                if (!completed->exchange(true, std::memory_order_acq_rel)) {
                    // Timeout won
                    spdlog::warn("[SearchEngineManager] Build timeout after {}ms", timeoutMs);
                    try {
                        SearchEngineRebuildFailedEvent ev;
                        ev.error = "build_timeout";
                        fsm_.dispatch(ev);
                    } catch (...) {
                    }
                    boost::asio::post(completion_exec, [h = std::move(*handlerPtr)]() mutable {
                        std::move(h)(std::exception_ptr{},
                                     RetT(Error{ErrorCode::InternalError, "build_timeout"}));
                    });
                }
            });

            // Post build work to worker executor (blocking operations)
            boost::asio::post(workerExecutor, [this, builder, opts, timer, completed, handlerPtr,
                                               completion_exec, vectorEnabled]() mutable {
                RetT result(Error{ErrorCode::InternalError, "unknown_error"});
                try {
                    auto r = builder->buildEmbedded(opts);
                    if (r) {
                        auto newEngine = r.value();
                        {
                            std::unique_lock lock(engineMutex_);
                            engine_ = newEngine;
                        }
                        result = RetT(newEngine);
                    } else {
                        result = RetT(Error{ErrorCode::InternalError, r.error().message});
                    }
                } catch (const std::exception& e) {
                    result = RetT(Error{ErrorCode::InternalError, e.what()});
                } catch (...) {
                    result = RetT(Error{ErrorCode::InternalError, "Engine build exception"});
                }

                if (!completed->exchange(true, std::memory_order_acq_rel)) {
                    // Work won
                    timer->cancel();

                    // Update FSM based on result
                    if (result.has_value()) {
                        try {
                            SearchEngineRebuildCompletedEvent ev;
                            ev.vectorEnabled = vectorEnabled;
                            ev.durationMs = 0;
                            fsm_.dispatch(ev);
                            refreshSnapshot();
                        } catch (...) {
                        }
                        spdlog::info("[SearchEngineManager] Build completed: vector={}",
                                     vectorEnabled);
                    } else {
                        try {
                            SearchEngineRebuildFailedEvent ev;
                            ev.error = result.error().message;
                            fsm_.dispatch(ev);
                        } catch (...) {
                        }
                        spdlog::error("[SearchEngineManager] Build failed: {}",
                                      result.error().message);
                    }

                    boost::asio::post(completion_exec, [h = std::move(*handlerPtr),
                                                        r = std::move(result)]() mutable {
                        std::move(h)(std::exception_ptr{}, std::move(r));
                    });
                }
            });
        },
        boost::asio::use_awaitable);
}

} // namespace yams::daemon
