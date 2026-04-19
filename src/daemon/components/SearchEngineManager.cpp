#include <yams/daemon/components/SearchEngineManager.h>
#include <yams/daemon/components/ConfigResolver.h>
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

namespace {
std::uint64_t nextLexicalDeltaEpoch(std::atomic<std::uint64_t>& counter) {
    return counter.fetch_add(1, std::memory_order_acq_rel) + 1;
}

constexpr std::size_t kMaxRecentLexicalDeltaDocs = 256;
} // namespace

yams::search::SearchEngine* SearchEngineManager::getCachedEngine() const {
    std::shared_lock lock(snapshotMutex_);
    return cachedEngine_;
}

SearchEngineSnapshot SearchEngineManager::getSnapshot() const {
    return fsm_.snapshot();
}

void SearchEngineManager::noteLexicalDeltaQueued(std::size_t documentCount) {
    if (documentCount == 0) {
        return;
    }
    lexicalDeltaPendingDocs_.fetch_add(static_cast<std::uint64_t>(documentCount),
                                       std::memory_order_relaxed);
    nextLexicalDeltaEpoch(lexicalDeltaQueuedEpoch_);
}

void SearchEngineManager::noteLexicalDeltaQueuedHash(std::string hash) {
    if (hash.empty()) {
        return;
    }
    std::lock_guard<std::mutex> lock(lexicalDeltaMutex_);
    pendingLexicalDeltaHashes_.push_back(std::move(hash));
}

void SearchEngineManager::noteLexicalDeltaQueuedHashes(const std::vector<std::string>& hashes) {
    if (hashes.empty()) {
        return;
    }
    std::lock_guard<std::mutex> lock(lexicalDeltaMutex_);
    for (const auto& hash : hashes) {
        if (!hash.empty()) {
            pendingLexicalDeltaHashes_.push_back(hash);
        }
    }
}

void SearchEngineManager::noteLexicalDeltaPublished(std::size_t documentCount) {
    if (documentCount == 0) {
        return;
    }

    const auto published = static_cast<std::uint64_t>(documentCount);
    lexicalDeltaPublishedDocs_.fetch_add(published, std::memory_order_relaxed);

    auto pending = lexicalDeltaPendingDocs_.load(std::memory_order_relaxed);
    while (true) {
        const auto nextPending = pending > published ? pending - published : 0;
        if (lexicalDeltaPendingDocs_.compare_exchange_weak(
                pending, nextPending, std::memory_order_acq_rel, std::memory_order_relaxed)) {
            break;
        }
    }
    nextLexicalDeltaEpoch(lexicalDeltaPublishedEpoch_);

    std::lock_guard<std::mutex> lock(lexicalDeltaMutex_);
    for (const auto& hash : pendingLexicalDeltaHashes_) {
        if (hash.empty() || recentLexicalDeltaSet_.contains(hash)) {
            continue;
        }
        recentLexicalDeltaOrder_.push_back(hash);
        recentLexicalDeltaSet_.insert(hash);
        while (recentLexicalDeltaOrder_.size() > kMaxRecentLexicalDeltaDocs) {
            auto evicted = std::move(recentLexicalDeltaOrder_.front());
            recentLexicalDeltaOrder_.pop_front();
            recentLexicalDeltaSet_.erase(evicted);
        }
    }
    pendingLexicalDeltaHashes_.clear();
}

SearchEngineManager::LexicalDeltaSnapshot
SearchEngineManager::getLexicalDeltaSnapshot() const noexcept {
    return {
        lexicalDeltaQueuedEpoch_.load(std::memory_order_relaxed),
        lexicalDeltaPublishedEpoch_.load(std::memory_order_relaxed),
        lexicalDeltaPendingDocs_.load(std::memory_order_relaxed),
        lexicalDeltaPublishedDocs_.load(std::memory_order_relaxed),
        static_cast<std::uint64_t>(recentLexicalDeltaSet_.size()),
    };
}

std::vector<std::string> SearchEngineManager::getRecentLexicalDeltaHashes() const {
    std::lock_guard<std::mutex> lock(lexicalDeltaMutex_);
    return std::vector<std::string>(recentLexicalDeltaOrder_.begin(),
                                    recentLexicalDeltaOrder_.end());
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

void SearchEngineManager::clearEngine() {
    {
        std::unique_lock lock(engineMutex_);
        engine_.reset();
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
                                 std::shared_ptr<yams::metadata::KnowledgeGraphStore> kgStore,
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
    builder->withMetadataRepo(std::move(metadataRepo));
    if (kgStore)
        builder->withKGStore(std::move(kgStore));
    if (vectorEnabled && vectorDatabase)
        builder->withVectorDatabase(std::move(vectorDatabase));
    if (vectorEnabled && embeddingGen)
        builder->withEmbeddingGenerator(std::move(embeddingGen));

    auto opts = yams::search::SearchEngineBuilder::BuildOptions::makeDefault();
    if (!tunerStatePath_.empty()) {
        opts.tunerStatePath = tunerStatePath_;
    }
    // Default daemon policy: lexical-first with adaptive semantic fallback.
    // Keep weak-query vector fanout disabled here so hybrid search stays precision-biased
    // instead of broadening semantic candidates whenever lexical evidence is sparse.
    // SearchEngine::Impl computes auto-threshold as max(maxResults*2, 50) when this is 0.
    opts.config.enableAdaptiveVectorFallback = true;
    opts.config.enableWeakQueryFanoutBoost = false;
    opts.config.adaptiveVectorSkipMinTier1Hits = 0;

    // Apply topology routing policy from config file. Env vars
    // (YAMS_SEARCH_TOPOLOGY_*) still override inside SearchEngineBuilder.
    {
        auto tp = ConfigResolver::resolveTopologyRoutingPolicy();
        if (tp.enableWeakQueryRouting) {
            opts.config.enableTopologyWeakQueryRouting = *tp.enableWeakQueryRouting;
            spdlog::info("SearchEngine enableTopologyWeakQueryRouting applied via config: {}",
                         *tp.enableWeakQueryRouting);
        }
        if (tp.maxClusters) {
            opts.config.topologyWeakQueryMaxClusters = *tp.maxClusters;
            spdlog::info("SearchEngine topologyWeakQueryMaxClusters applied via config: {}",
                         *tp.maxClusters);
        }
        if (tp.maxDocs) {
            opts.config.topologyWeakQueryMaxDocs = *tp.maxDocs;
            spdlog::info("SearchEngine topologyWeakQueryMaxDocs applied via config: {}",
                         *tp.maxDocs);
        }
        if (tp.medoidBoost) {
            opts.config.topologyMedoidBoost = std::max(0.0f, *tp.medoidBoost);
            spdlog::info("SearchEngine topologyMedoidBoost applied via config: {:.3f}",
                         opts.config.topologyMedoidBoost);
        }
        if (tp.bridgeBoost) {
            opts.config.topologyBridgeBoost = std::max(0.0f, *tp.bridgeBoost);
            spdlog::info("SearchEngine topologyBridgeBoost applied via config: {:.3f}",
                         opts.config.topologyBridgeBoost);
        }
        if (tp.routedBaseMultiplier) {
            opts.config.topologyRoutedBaseMultiplier = std::max(0.0f, *tp.routedBaseMultiplier);
            spdlog::info("SearchEngine topologyRoutedBaseMultiplier applied via config: {:.3f}",
                         opts.config.topologyRoutedBaseMultiplier);
        }
        if (tp.routingVariant) {
            using V = yams::search::SearchEngineConfig::TopologyRoutingVariant;
            const auto& raw = *tp.routingVariant;
            if (raw == "vector_seed")
                opts.config.topologyRoutingVariant = V::VectorSeed;
            else if (raw == "kg_walk")
                opts.config.topologyRoutingVariant = V::KgWalk;
            else if (raw == "score_replace")
                opts.config.topologyRoutingVariant = V::ScoreReplace;
            else if (raw == "medoid_promote")
                opts.config.topologyRoutingVariant = V::MedoidPromote;
            else
                opts.config.topologyRoutingVariant = V::Baseline;
            spdlog::info("SearchEngine topologyRoutingVariant applied via config: {}", raw);
        }
    }

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
                                               completion_exec, vectorEnabled,
                                               workerExecutor]() mutable {
                RetT result(Error{ErrorCode::InternalError, "unknown_error"});
                try {
                    auto r = builder->buildEmbedded(opts);
                    if (r) {
                        auto newEngine = r.value();
                        newEngine->setExecutor(workerExecutor);
                        {
                            std::unique_lock lock(engineMutex_);
                            engine_ = newEngine;
                        }
                        refreshSnapshot();
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
