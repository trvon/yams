#include <yams/daemon/components/EmbeddingService.h>

#include <spdlog/spdlog.h>

#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <cstdlib>
#include <future>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include <fmt/format.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <yams/api/content_store.h>
#include <yams/core/types.h>
#include <yams/daemon/components/ConfigResolver.h>
#include <yams/daemon/components/InternalEventBus.h>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/daemon/components/TuningSnapshot.h>
#include <yams/daemon/components/WorkCoordinator.h>
#include <yams/daemon/resource/gpu_info.h>
#include <yams/daemon/resource/model_provider.h>
#include <yams/daemon/resource/OnnxConcurrencyRegistry.h>
#include <yams/ingest/ingest_helpers.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/vector/document_chunker.h>
#include <yams/vector/vector_database.h>

namespace yams {
namespace daemon {

EmbeddingService::EmbeddingService(std::shared_ptr<api::IContentStore> store,
                                   std::shared_ptr<metadata::MetadataRepository> meta,
                                   WorkCoordinator* coordinator)
    : store_(std::move(store)), meta_(std::move(meta)), coordinator_(coordinator),
      strand_(coordinator_->makeStrand()) {}

EmbeddingService::~EmbeddingService() {
    shutdown();
}

Result<void> EmbeddingService::initialize() {
    // Use configurable channel capacity from TuneAdvisor
    std::size_t capacity = static_cast<std::size_t>(TuneAdvisor::embedChannelCapacity());
    std::size_t postIngestCap = static_cast<std::size_t>(TuneAdvisor::postIngestQueueMax());
    if (postIngestCap > 0) {
        capacity = std::min(capacity, postIngestCap);
    }
    capacity = std::max<std::size_t>(256u, capacity);
    embedChannel_ = InternalEventBus::instance().get_or_create_channel<InternalEventBus::EmbedJob>(
        "embed_jobs", capacity);

    if (!embedChannel_) {
        return Error{ErrorCode::InvalidOperation,
                     "Failed to create embedding channel on InternalBus"};
    }

    spdlog::info("EmbeddingService: initialized with channel capacity {}", capacity);
    return Result<void>();
}

void EmbeddingService::start() {
    stop_.store(false);
    pollerRunning_.store(false, std::memory_order_release);
    {
        std::lock_guard<std::mutex> lock(inferTrackerMutex_);
        activeInferSubBatches_.clear();
    }
    TuneAdvisor::setPostIngestStageActive(TuneAdvisor::PostIngestStage::Embed, true);
    boost::asio::co_spawn(strand_, channelPoller(), boost::asio::detached);
    spdlog::info("EmbeddingService: started parallel channel poller");
}

void EmbeddingService::shutdown() {
    if (stop_.exchange(true)) {
        return;
    }
    TuneAdvisor::setPostIngestStageActive(TuneAdvisor::PostIngestStage::Embed, false);
    spdlog::info("EmbeddingService: shutting down (processed={}, failed={}, inFlight={})",
                 processed_.load(), failed_.load(), inFlight_.load());

    // Best-effort: clear queued jobs promptly so shutdown focuses on already-running work.
    // This runs on the service strand to avoid races with channelPoller's pendingJobs_ access.
    try {
        std::promise<void> drainDone;
        auto drainFuture = drainDone.get_future();
        boost::asio::post(strand_, [this, done = std::move(drainDone)]() mutable {
            std::size_t droppedDocs = 0;
            std::size_t droppedJobs = 0;
            try {
                for (const auto& pending : pendingJobs_) {
                    droppedDocs += pending.hashes.size();
                    ++droppedJobs;
                }
                pendingJobs_.clear();

                auto channel = std::atomic_load_explicit(&embedChannel_, std::memory_order_acquire);
                InternalEventBus::EmbedJob queued;
                while (channel && channel->try_pop(queued)) {
                    droppedDocs += queued.hashes.size();
                    ++droppedJobs;
                }

                pendingApprox_.store(0, std::memory_order_relaxed);
                if (droppedDocs > 0) {
                    failed_.fetch_add(droppedDocs, std::memory_order_relaxed);
                    InternalEventBus::instance().incEmbedDropped(droppedDocs);
                    spdlog::info("EmbeddingService: dropped queued embed jobs={} "
                                 "docs={} during shutdown",
                                 droppedJobs, droppedDocs);
                }
            } catch (...) {
            }
            lifecycleCv_.notify_all();
            try {
                done.set_value();
            } catch (...) {
            }
        });
        (void)drainFuture.wait_for(std::chrono::milliseconds(1500));
    } catch (...) {
    }

    std::chrono::milliseconds maxWait{30000};
    if (const char* env = std::getenv("YAMS_EMBED_SHUTDOWN_WAIT_MS")) {
        try {
            const auto parsed = static_cast<std::chrono::milliseconds::rep>(std::stoll(env));
            if (parsed > 0) {
                maxWait = std::chrono::milliseconds(parsed);
            }
        } catch (...) {
        }
    }

    const auto deadline = std::chrono::steady_clock::now() + maxWait;
    {
        std::unique_lock<std::mutex> lock(lifecycleMutex_);
        lifecycleCv_.wait_until(lock, deadline, [&]() {
            return !pollerRunning_.load(std::memory_order_acquire) &&
                   inFlight_.load(std::memory_order_acquire) == 0;
        });
    }

    const auto channel = std::atomic_load_explicit(&embedChannel_, std::memory_order_acquire);
    const auto channelQueued = channel ? channel->size_approx() : 0;
    const auto pendingApprox = pendingApprox_.load(std::memory_order_relaxed);
    const auto inFlight = inFlight_.load(std::memory_order_relaxed);
    const auto pollerRunning = pollerRunning_.load(std::memory_order_relaxed);
    const auto inferActive = activeInferSubBatches();
    const auto inferOldestMs = inferOldestActiveMs();
    std::string lastModel;
    {
        std::lock_guard<std::mutex> lock(lifecycleMutex_);
        lastModel = lastDispatchedModel_;
    }

    if (pollerRunning || inFlight > 0) {
        spdlog::warn("EmbeddingService: shutdown timeout (poller_running={} inFlight={} pending={} "
                     "channel_queued={} infer_active={} infer_oldest_ms={} last_model='{}')",
                     pollerRunning ? 1 : 0, inFlight, pendingApprox, channelQueued, inferActive,
                     inferOldestMs, lastModel.empty() ? "<default>" : lastModel);
    } else {
        spdlog::info("EmbeddingService: shutdown complete (pending={} channel_queued={} "
                     "infer_active={} infer_oldest_ms={} last_model='{}')",
                     pendingApprox, channelQueued, inferActive, inferOldestMs,
                     lastModel.empty() ? "<default>" : lastModel);
    }
}

void EmbeddingService::setProviders(
    std::function<std::shared_ptr<IModelProvider>()> providerGetter,
    std::function<std::string()> modelNameGetter,
    std::function<std::shared_ptr<yams::vector::VectorDatabase>()> dbGetter) {
    getModelProvider_ = std::move(providerGetter);
    getPreferredModel_ = std::move(modelNameGetter);
    getVectorDatabase_ = std::move(dbGetter);
}

std::size_t EmbeddingService::queuedJobs() const {
    const auto chPtr = std::atomic_load_explicit(&embedChannel_, std::memory_order_acquire);
    const std::size_t ch = chPtr ? chPtr->size_approx() : 0;
    const std::size_t pending = this->pendingApprox_.load(std::memory_order_relaxed);
    return ch + pending;
}

std::size_t EmbeddingService::inFlightJobs() const {
    return inFlight_.load();
}

std::size_t EmbeddingService::activeInferSubBatches() const {
    std::lock_guard<std::mutex> lock(inferTrackerMutex_);
    return activeInferSubBatches_.size();
}

uint64_t EmbeddingService::inferSubBatchStartedCount() const {
    return inferSubBatchStarted_.load(std::memory_order_relaxed);
}

uint64_t EmbeddingService::inferSubBatchCompletedCount() const {
    return inferSubBatchCompleted_.load(std::memory_order_relaxed);
}

uint64_t EmbeddingService::inferSubBatchLastDurationMs() const {
    return inferSubBatchLastDurationMs_.load(std::memory_order_relaxed);
}

uint64_t EmbeddingService::inferSubBatchMaxDurationMs() const {
    return inferSubBatchMaxDurationMs_.load(std::memory_order_relaxed);
}

uint64_t EmbeddingService::inferSubBatchWarnCount() const {
    return inferSubBatchWarnCount_.load(std::memory_order_relaxed);
}

uint64_t EmbeddingService::inferOldestActiveMs() const {
    std::lock_guard<std::mutex> lock(inferTrackerMutex_);
    if (activeInferSubBatches_.empty()) {
        return 0;
    }
    const auto now = std::chrono::steady_clock::now();
    auto oldest = now;
    for (const auto& [_, started] : activeInferSubBatches_) {
        if (started < oldest) {
            oldest = started;
        }
    }
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(now - oldest).count());
}

boost::asio::awaitable<void> EmbeddingService::channelPoller() {
    boost::asio::steady_timer timer(co_await boost::asio::this_coro::executor);

    pollerRunning_.store(true, std::memory_order_release);
    struct PollerExitGuard {
        EmbeddingService* self;
        ~PollerExitGuard() {
            self->pendingApprox_.store(0, std::memory_order_relaxed);
            self->pollerRunning_.store(false, std::memory_order_release);
            self->lifecycleCv_.notify_all();
        }
    } pollerExit{this};

    auto idleDelay = std::chrono::milliseconds(5);
    auto maxIdleDelay = []() {
        auto snap = TuningSnapshotRegistry::instance().get();
        uint32_t pollMs = snap ? snap->workerPollMs : TuneAdvisor::workerPollMs();
        return std::chrono::milliseconds(std::max<uint32_t>(50, pollMs));
    };

    spdlog::info("[EmbeddingService] Parallel poller started");

    const bool poolDebug = []() {
        if (const char* s = std::getenv("YAMS_EMBED_DEBUG_POOL")) {
            return std::string{s} == "1" || std::string{s} == "true" || std::string{s} == "yes";
        }
        return false;
    }();

    const resource::GpuInfo gpuInfo = resource::detectGpu();
    const bool coremlUnified =
        gpuInfo.detected && gpuInfo.provider == "coreml" && gpuInfo.unifiedMemory;
    std::size_t coremlUnifiedCap = 1;
    std::string embedProfile;
    if (const char* s = std::getenv("YAMS_BENCH_EMBED_PROFILE")) {
        embedProfile = s;
        std::transform(embedProfile.begin(), embedProfile.end(), embedProfile.begin(),
                       [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        if (embedProfile == "safe") {
            coremlUnifiedCap = 1;
        } else if (embedProfile == "balanced") {
            coremlUnifiedCap = 2;
        }
    }
    if (const char* s = std::getenv("YAMS_EMBED_COREML_SAFE_CONCURRENCY")) {
        try {
            const auto parsed = static_cast<std::size_t>(std::stoull(s));
            if (parsed > 0) {
                coremlUnifiedCap = parsed;
            }
        } catch (...) {
        }
    }
    if (coremlUnified) {
        spdlog::info(
            "[EmbeddingService] CoreML unified-memory safety mode enabled: max concurrent embed "
            "jobs capped at {} (profile='{}')",
            coremlUnifiedCap, embedProfile.empty() ? "default" : embedProfile);
    }

    std::size_t coremlDynamicCap = coremlUnifiedCap;
    uint64_t lastWarnCountObserved = 0;

    std::size_t lastEffectiveConcurrent = 0;
    while (!stop_.load()) {
        bool didWork = false;
        InternalEventBus::EmbedJob job;

        auto channel = std::atomic_load_explicit(&embedChannel_, std::memory_order_acquire);
        const std::size_t baseConcurrent =
            std::max<std::size_t>(1, TuneAdvisor::postEmbedConcurrent());
        const std::size_t hardConcurrentCap =
            std::max<std::size_t>(baseConcurrent, TuneAdvisor::getEmbedMaxConcurrency());
        const std::size_t maxJobDocCap = TuneAdvisor::resolvedEmbedJobDocCap();
        const std::size_t channelBacklog = channel ? channel->size_approx() : 0;
        const std::size_t bufferedBacklog = channelBacklog + this->pendingJobs_.size();

        const bool allowRamp = []() {
            if (const char* s = std::getenv("YAMS_EMBED_ALLOW_RAMP")) {
                return std::string{s} == "1" || std::string{s} == "true" || std::string{s} == "yes";
            }
            return false;
        }();

        std::size_t effectiveMaxConcurrent = baseConcurrent;
        if (allowRamp && hardConcurrentCap > baseConcurrent) {
            const std::size_t rampThreshold =
                std::max<std::size_t>(baseConcurrent * maxJobDocCap, 32u);
            const std::size_t rampStep = std::max<std::size_t>(maxJobDocCap, 16u);
            if (bufferedBacklog > rampThreshold) {
                const std::size_t extra =
                    (bufferedBacklog - rampThreshold + (rampStep - 1)) / rampStep;
                effectiveMaxConcurrent =
                    std::min<std::size_t>(hardConcurrentCap, baseConcurrent + extra);
            }
        }

        try {
            auto snap = OnnxConcurrencyRegistry::instance().snapshot();
            const std::size_t embedLane = static_cast<std::size_t>(OnnxLane::Embedding);
            const std::size_t embedReserved = snap.lanes[embedLane].reserved;
            const std::size_t sharedAvailable = snap.availableSlots;
            const std::size_t onnxBudget =
                std::max<std::size_t>(1u, embedReserved + sharedAvailable);
            effectiveMaxConcurrent = std::min(effectiveMaxConcurrent, onnxBudget);
        } catch (...) {
        }
        if (coremlUnified) {
            const auto lastInferMs = inferSubBatchLastDurationMs_.load(std::memory_order_relaxed);
            const auto oldestInferMs = inferOldestActiveMs();
            const auto warnCount = inferSubBatchWarnCount_.load(std::memory_order_relaxed);

            // Downshift aggressively on signs of pressure/stalls.
            if (warnCount > lastWarnCountObserved || oldestInferMs > 20000 || lastInferMs > 12000) {
                coremlDynamicCap = 1;
            } else {
                // Gradually recover toward configured cap when latency is healthy and backlog
                // exists.
                const std::size_t rampThreshold =
                    std::max<std::size_t>(maxJobDocCap * coremlUnifiedCap, 64u);
                if (bufferedBacklog > rampThreshold && lastInferMs > 0 && lastInferMs < 6000 &&
                    oldestInferMs < 8000 && coremlDynamicCap < coremlUnifiedCap) {
                    coremlDynamicCap++;
                }

                // Reduce cap when work is nearly drained to minimize unified-memory pressure.
                if (bufferedBacklog < maxJobDocCap && coremlDynamicCap > 1) {
                    coremlDynamicCap--;
                }
            }

            lastWarnCountObserved = warnCount;
            effectiveMaxConcurrent = std::min(effectiveMaxConcurrent, coremlDynamicCap);
        }
        const std::size_t maxPendingJobs =
            std::max<std::size_t>(maxJobDocCap, effectiveMaxConcurrent * 2);
        if (effectiveMaxConcurrent != lastEffectiveConcurrent) {
            spdlog::debug(
                "[EmbeddingService] adaptive concurrency: base={} effective={} hard_cap={} "
                "allow_ramp={} backlog={} channel={} pending={}",
                baseConcurrent, effectiveMaxConcurrent, hardConcurrentCap, allowRamp ? 1 : 0,
                bufferedBacklog, channelBacklog, this->pendingJobs_.size());
            lastEffectiveConcurrent = effectiveMaxConcurrent;
        }

        // Pull jobs into pending buffer to allow model-based grouping
        while (channel && channel->try_pop(job)) {
            didWork = true;
            this->pendingJobs_.push_back(std::move(job));
            if (this->pendingJobs_.size() >= maxPendingJobs) {
                break;
            }
        }

        // Keep an approximate pending backlog for status/benchmarks.
        this->pendingApprox_.store(this->pendingJobs_.size(), std::memory_order_relaxed);

        if (inFlight_.load() < effectiveMaxConcurrent && !this->pendingJobs_.empty()) {
            std::string defaultModel;
            if (getPreferredModel_) {
                defaultModel = getPreferredModel_();
            }
            // Group pending jobs by model name to reduce model switching
            std::unordered_map<std::string, InternalEventBus::EmbedJob> grouped;
            grouped.reserve(this->pendingJobs_.size());

            std::vector<InternalEventBus::EmbedJob> deferred;
            deferred.reserve(this->pendingJobs_.size());

            for (auto& pending : this->pendingJobs_) {
                if (pending.hashes.empty()) {
                    continue;
                }
                if (pending.modelName.empty() && !defaultModel.empty()) {
                    pending.modelName = defaultModel;
                }
                auto& bucket = grouped[pending.modelName];
                if (bucket.hashes.empty()) {
                    bucket.modelName = pending.modelName;
                    bucket.skipExisting = pending.skipExisting;
                }
                if (bucket.skipExisting != pending.skipExisting) {
                    deferred.push_back(std::move(pending));
                    continue;
                }
                bucket.hashes.insert(bucket.hashes.end(), pending.hashes.begin(),
                                     pending.hashes.end());
            }

            this->pendingJobs_.clear();
            if (!deferred.empty()) {
                this->pendingJobs_.insert(this->pendingJobs_.end(),
                                          std::make_move_iterator(deferred.begin()),
                                          std::make_move_iterator(deferred.end()));
            }

            this->pendingApprox_.store(this->pendingJobs_.size(), std::memory_order_relaxed);

            auto dispatchJob = [this, poolDebug](InternalEventBus::EmbedJob&& dispatch) {
                if (dispatch.hashes.empty()) {
                    return;
                }

                if (stop_.load(std::memory_order_acquire)) {
                    failed_.fetch_add(dispatch.hashes.size(), std::memory_order_relaxed);
                    if (poolDebug) {
                        spdlog::info("[EmbeddingService] drop dispatch during shutdown model='{}' "
                                     "hashes={}",
                                     dispatch.modelName.empty() ? "<default>" : dispatch.modelName,
                                     dispatch.hashes.size());
                    }
                    return;
                }

                dispatch.batchSize = static_cast<uint32_t>(dispatch.hashes.size());
                inFlight_.fetch_add(1, std::memory_order_acq_rel);
                InternalEventBus::instance().incEmbedConsumed(dispatch.batchSize);

                if (poolDebug) {
                    spdlog::info("[EmbeddingService] dispatch model='{}' hashes={} in_flight={}",
                                 dispatch.modelName.empty() ? "<default>" : dispatch.modelName,
                                 dispatch.hashes.size(), inFlight_.load(std::memory_order_relaxed));
                }

                if (!dispatch.modelName.empty()) {
                    std::lock_guard<std::mutex> lock(lifecycleMutex_);
                    lastDispatchedModel_ = dispatch.modelName;
                }

                try {
                    boost::asio::post(coordinator_->getExecutor(), [this, job = std::move(dispatch),
                                                                    poolDebug]() mutable {
                        const std::size_t jobSize = job.hashes.size();
                        const std::string jobModel =
                            job.modelName.empty() ? std::string{"<default>"} : job.modelName;
                        const auto started = std::chrono::steady_clock::now();

                        struct ScopeGuard {
                            EmbeddingService* self;
                            bool poolDebug;
                            std::size_t jobSize;
                            std::string jobModel;
                            std::chrono::steady_clock::time_point started;
                            ~ScopeGuard() {
                                auto previous =
                                    self->inFlight_.fetch_sub(1, std::memory_order_acq_rel);
                                std::size_t remaining = (previous > 0) ? (previous - 1) : 0;
                                self->lifecycleCv_.notify_all();
                                if (poolDebug) {
                                    const auto durMs =
                                        std::chrono::duration_cast<std::chrono::milliseconds>(
                                            std::chrono::steady_clock::now() - started)
                                            .count();
                                    spdlog::info("[EmbeddingService] complete model='{}' "
                                                 "hashes={} dur_ms={} in_flight={}",
                                                 jobModel, jobSize, durMs, remaining);
                                }
                            }
                        } guard{this, poolDebug, jobSize, std::move(jobModel), started};

                        try {
                            processEmbedJob(std::move(job));
                        } catch (const std::exception& e) {
                            spdlog::error("[EmbeddingService] Uncaught exception in embed job: "
                                          "{}",
                                          e.what());
                        } catch (...) {
                            spdlog::error("[EmbeddingService] Unknown exception in embed job");
                        }
                    });
                } catch (const std::exception& e) {
                    inFlight_.fetch_sub(1, std::memory_order_acq_rel);
                    failed_.fetch_add(dispatch.batchSize, std::memory_order_relaxed);
                    lifecycleCv_.notify_all();
                    spdlog::error("[EmbeddingService] Failed to post embed job to executor: {}",
                                  e.what());
                } catch (...) {
                    inFlight_.fetch_sub(1, std::memory_order_acq_rel);
                    failed_.fetch_add(dispatch.batchSize, std::memory_order_relaxed);
                    lifecycleCv_.notify_all();
                    spdlog::error("[EmbeddingService] Failed to post embed job to executor");
                }
            };

            std::string lastModel;
            {
                std::lock_guard<std::mutex> lock(lifecycleMutex_);
                lastModel = lastDispatchedModel_;
            }

            struct BucketRef {
                std::string key;
                InternalEventBus::EmbedJob* bucket;
            };
            std::vector<BucketRef> orderedBuckets;
            orderedBuckets.reserve(grouped.size());
            for (auto& [key, bucket] : grouped) {
                orderedBuckets.push_back(BucketRef{key, &bucket});
            }

            std::sort(orderedBuckets.begin(), orderedBuckets.end(),
                      [&](const BucketRef& a, const BucketRef& b) {
                          const bool aNamed = !a.key.empty();
                          const bool bNamed = !b.key.empty();
                          if (aNamed != bNamed) {
                              return aNamed; // Named models first
                          }
                          const bool aSticky = !lastModel.empty() && a.key == lastModel;
                          const bool bSticky = !lastModel.empty() && b.key == lastModel;
                          if (aSticky != bSticky) {
                              return aSticky; // Keep model affinity when possible
                          }
                          return a.bucket->hashes.size() > b.bucket->hashes.size();
                      });

            if (poolDebug && !orderedBuckets.empty()) {
                std::string order;
                for (const auto& item : orderedBuckets) {
                    if (!order.empty()) {
                        order += ",";
                    }
                    order += (item.key.empty() ? std::string{"<default>"} : item.key);
                    order += ":" + std::to_string(item.bucket->hashes.size());
                }
                spdlog::info("[EmbeddingService] dispatch order [{}]", order);
            }

            for (auto& entry : orderedBuckets) {
                auto& bucket = *entry.bucket;
                if (stop_.load(std::memory_order_acquire)) {
                    break;
                }
                std::size_t offset = 0;
                while (offset < bucket.hashes.size() && inFlight_.load() < effectiveMaxConcurrent) {
                    std::size_t take = std::min(maxJobDocCap, bucket.hashes.size() - offset);
                    InternalEventBus::EmbedJob split;
                    split.modelName = bucket.modelName;
                    split.skipExisting = bucket.skipExisting;
                    split.hashes.assign(bucket.hashes.begin() + static_cast<std::ptrdiff_t>(offset),
                                        bucket.hashes.begin() +
                                            static_cast<std::ptrdiff_t>(offset + take));
                    dispatchJob(std::move(split));
                    offset += take;
                }

                if (offset < bucket.hashes.size()) {
                    InternalEventBus::EmbedJob remainder;
                    remainder.modelName = bucket.modelName;
                    remainder.skipExisting = bucket.skipExisting;
                    remainder.hashes.assign(bucket.hashes.begin() +
                                                static_cast<std::ptrdiff_t>(offset),
                                            bucket.hashes.end());
                    this->pendingJobs_.push_back(std::move(remainder));
                }
            }

            this->pendingApprox_.store(this->pendingJobs_.size(), std::memory_order_relaxed);
        }

        if (didWork) {
            idleDelay = std::chrono::milliseconds(5);
            continue; // Check for more work immediately
        }

        // Idle - wait before polling again
        timer.expires_after(idleDelay);
        co_await timer.async_wait(boost::asio::use_awaitable);
        const auto maxIdle = maxIdleDelay();
        if (idleDelay < maxIdle) {
            idleDelay *= 2;
            if (idleDelay > maxIdle) {
                idleDelay = maxIdle;
            }
        }
    }

    spdlog::info("[EmbeddingService] Parallel poller exited");
}

void EmbeddingService::processEmbedJob(InternalEventBus::EmbedJob job) {
    const bool timingEnabled = []() {
        if (const char* s = std::getenv("YAMS_EMBED_DEBUG_TIMINGS")) {
            return std::string{s} == "1" || std::string{s} == "true" || std::string{s} == "yes";
        }
        return false;
    }();
    const bool poolDebug = []() {
        if (const char* s = std::getenv("YAMS_EMBED_DEBUG_POOL")) {
            return std::string{s} == "1" || std::string{s} == "true" || std::string{s} == "yes";
        }
        return false;
    }();
    uint64_t warnMs = 5000;
    if (const char* s = std::getenv("YAMS_EMBED_TIMING_WARN_MS")) {
        try {
            warnMs = static_cast<uint64_t>(std::stoull(s));
        } catch (...) {
        }
    }

    const auto jobTag = [&]() -> std::string {
        if (!job.hashes.empty()) {
            const auto& h = job.hashes.front();
            return h.size() > 12 ? h.substr(0, 12) : h;
        }
        return std::string{"empty"};
    }();

    auto logPhase = [&](const char* phase, std::chrono::steady_clock::time_point start,
                        const std::string& detail) {
        const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::steady_clock::now() - start)
                            .count();
        if (timingEnabled || ms >= static_cast<long long>(warnMs)) {
            spdlog::info("[EmbeddingService] job={} phase={} dur_ms={} {}", jobTag, phase, ms,
                         detail);
        }
    };

    spdlog::debug(
        "[EmbeddingService] processEmbedJob job={} hashes={} skipExisting={} modelHint='{}'",
        jobTag, job.hashes.size(), job.skipExisting ? "true" : "false", job.modelName);
    std::shared_ptr<IModelProvider> provider;
    std::string modelName;
    std::shared_ptr<yams::vector::VectorDatabase> vdb;

    if (getModelProvider_)
        provider = getModelProvider_();
    if (getPreferredModel_)
        modelName = getPreferredModel_();
    if (getVectorDatabase_)
        vdb = getVectorDatabase_();

    spdlog::debug("[EmbeddingService] Callbacks: provider={} model='{}' vdb={}",
                  provider ? "yes" : "no", modelName, vdb ? "yes" : "no");

    if (!job.modelName.empty()) {
        modelName = job.modelName;
    }

    if (!provider || modelName.empty() || !vdb) {
        // Common case during ingestion runs with embeddings disabled: PostIngestQueue may still
        // enqueue jobs, but the provider/model/vdb are intentionally unset.
        spdlog::debug("EmbeddingService: providers unavailable for batch of {} documents "
                      "(provider={}, model='{}', vdb={})",
                      job.hashes.size(), provider ? "available" : "null", modelName,
                      vdb ? "available" : "null");
        failed_.fetch_add(job.hashes.size());
        return;
    }

    if (stop_.load(std::memory_order_acquire)) {
        failed_.fetch_add(job.hashes.size(), std::memory_order_relaxed);
        InternalEventBus::instance().incEmbedDropped(job.hashes.size());
        spdlog::info("EmbeddingService: aborting job={} model='{}' docs={} due to shutdown", jobTag,
                     modelName, job.hashes.size());
        return;
    }

    auto logPoolState = [&](const char* phase) {
        if (!poolDebug || !provider) {
            return;
        }
        std::size_t loadedCount = 0;
        std::vector<std::string> loadedModels;
        try {
            loadedCount = provider->getLoadedModelCount();
            loadedModels = provider->getLoadedModels();
        } catch (...) {
        }
        std::string preview;
        const std::size_t limit = std::min<std::size_t>(loadedModels.size(), 4u);
        for (std::size_t i = 0; i < limit; ++i) {
            if (!preview.empty()) {
                preview += ",";
            }
            preview += loadedModels[i];
        }
        if (loadedModels.size() > limit) {
            if (!preview.empty()) {
                preview += ",";
            }
            preview += "...";
        }

        spdlog::info("[EmbeddingService] job={} pool phase={} model='{}' loaded_count={} "
                     "loaded_preview=[{}]",
                     jobTag, phase, modelName, loadedCount, preview);
    };

    spdlog::debug("EmbeddingService: processing batch of {} documents with model '{}'",
                  job.hashes.size(), modelName);
    logPoolState("job_start");

    // ============================================================
    // Phase 1: Gather all document content and metadata
    // ============================================================
    const auto tGather = std::chrono::steady_clock::now();
    struct DocData {
        std::string hash;
        std::string text;
        std::string fileName;
        std::string filePath;
        std::string mimeType;
    };
    std::vector<DocData> docsToEmbed;
    docsToEmbed.reserve(job.hashes.size());

    // Phase 2: optional prepared payload from post-ingest.
    std::vector<const InternalEventBus::EmbedPreparedDoc*> preparedDocPtr;
    preparedDocPtr.reserve(job.hashes.size());
    std::vector<bool> docHasPreparedChunks;
    docHasPreparedChunks.reserve(job.hashes.size());

    std::size_t skipped = 0;
    std::size_t failedGather = 0;

    std::unordered_set<std::string> preparedHashes;
    preparedHashes.reserve(job.preparedDocs.size());
    for (const auto& pd : job.preparedDocs) {
        preparedHashes.insert(pd.hash);
        if (job.skipExisting) {
            auto hasEmbedRes = meta_->hasDocumentEmbeddingByHash(pd.hash);
            if (hasEmbedRes && hasEmbedRes.value()) {
                spdlog::debug(
                    "EmbeddingService: skipExisting=true, already embedded (prepared): {}",
                    pd.hash);
                skipped++;
                continue;
            }
        }

        if (pd.chunks.empty()) {
            // Malformed prepared payload; fall back to DB gather via hashes.
            continue;
        }

        docsToEmbed.push_back({pd.hash, std::string{}, pd.fileName, pd.filePath, pd.mimeType});
        preparedDocPtr.push_back(&pd);
        docHasPreparedChunks.push_back(true);
    }

    for (const auto& hash : job.hashes) {
        if (!job.preparedDocs.empty() && preparedHashes.find(hash) != preparedHashes.end()) {
            continue;
        }
        try {
            auto docInfoRes = meta_->getDocumentByHash(hash);
            if (!docInfoRes || !docInfoRes.value().has_value()) {
                spdlog::warn("EmbeddingService: document not found: {}", hash);
                failedGather++;
                continue;
            }

            const auto& docInfo = *docInfoRes.value();

            // Check embedding status via metadata repository (separate DB, no VectorDatabase lock)
            // This avoids mutex contention with EntityGraphService's insertEntityVectorsBatch
            if (job.skipExisting) {
                auto hasEmbedRes = meta_->hasDocumentEmbeddingByHash(hash);
                if (hasEmbedRes && hasEmbedRes.value()) {
                    spdlog::debug("EmbeddingService: skipExisting=true, already embedded: {}",
                                  hash);
                    skipped++;
                    continue;
                }
            }

            auto contentOpt = meta_->getContent(docInfo.id);
            if (!contentOpt || !contentOpt.value().has_value()) {
                spdlog::debug("EmbeddingService: no content for document {}", hash);
                failedGather++;
                continue;
            }

            const auto& text = contentOpt.value().value().contentText;
            if (text.empty()) {
                spdlog::debug("EmbeddingService: empty content for document {}", hash);
                skipped++;
                continue;
            }

            docsToEmbed.push_back(
                {hash, text, docInfo.fileName, docInfo.filePath, docInfo.mimeType});
            preparedDocPtr.push_back(nullptr);
            docHasPreparedChunks.push_back(false);
        } catch (const std::exception& e) {
            spdlog::error("EmbeddingService: exception gathering {}: {}", hash, e.what());
            failedGather++;
        }
    }

    failed_.fetch_add(failedGather);

    if (docsToEmbed.empty()) {
        logPhase("gather", tGather,
                 fmt::format("docs_to_embed=0 skipped={} failed_gather={}", skipped, failedGather));
        spdlog::debug("EmbeddingService: no documents to embed after gathering");
        return;
    }

    if (stop_.load(std::memory_order_acquire)) {
        spdlog::info("EmbeddingService: aborting job={} before chunking (docs={}) due to shutdown",
                     jobTag, docsToEmbed.size());
        failed_.fetch_add(docsToEmbed.size(), std::memory_order_relaxed);
        InternalEventBus::instance().incEmbedDropped(docsToEmbed.size());
        std::vector<std::string> failedHashes;
        failedHashes.reserve(docsToEmbed.size());
        for (const auto& doc : docsToEmbed) {
            failedHashes.push_back(doc.hash);
        }
        (void)meta_->batchUpdateDocumentRepairStatuses(failedHashes,
                                                       metadata::RepairStatus::Failed);
        return;
    }

    logPhase("gather", tGather,
             fmt::format("docs_to_embed={} skipped={} failed_gather={} hashes_in_job={} model='{}'",
                         docsToEmbed.size(), skipped, failedGather, job.hashes.size(), modelName));

    spdlog::debug("EmbeddingService: gathered {} documents for embedding", docsToEmbed.size());

    // ============================================================
    // Phase 2: Chunk all documents
    // ============================================================
    const auto tChunk = std::chrono::steady_clock::now();
    struct ChunkInfo {
        size_t docIdx;       // Index into docsToEmbed
        std::string chunkId; // Unique chunk ID
        std::string content; // Chunk text
        size_t startOffset;
        size_t endOffset;
        bool prepared;
    };
    std::vector<ChunkInfo> allChunks;
    std::vector<std::string> docPreviews;
    docPreviews.resize(docsToEmbed.size());

    const auto chunkPolicy = ConfigResolver::resolveEmbeddingChunkingPolicy();
    auto strategy = chunkPolicy.strategy;
    auto ccfg = chunkPolicy.config;
    const bool chunkCfgOverridden = chunkPolicy.overridden;

    if (timingEnabled && chunkCfgOverridden) {
        spdlog::info("[EmbeddingService] job={} chunk_cfg strategy={} target={} min={} max={} "
                     "overlap_size={} "
                     "overlap_pct={:.3f} preserve_sentences={} use_tokens={}",
                     jobTag, static_cast<int>(strategy), ccfg.target_chunk_size,
                     ccfg.min_chunk_size, ccfg.max_chunk_size, ccfg.overlap_size,
                     ccfg.overlap_percentage, ccfg.preserve_sentences,
                     ccfg.use_token_count ? 1 : 0);
    }
    uint64_t totalDocChars = 0;
    uint64_t totalChunkChars = 0;

    // Pre-fill previews + chunks from prepared payload when available.
    for (size_t docIdx = 0; docIdx < docsToEmbed.size(); ++docIdx) {
        const auto* pd = preparedDocPtr[docIdx];
        if (!pd) {
            continue;
        }
        if (!pd->chunks.empty()) {
            const auto& first = pd->chunks.front().content;
            docPreviews[docIdx] = first.size() <= 1000 ? first : first.substr(0, 1000);
        } else {
            docPreviews[docIdx].clear();
        }

        for (const auto& c : pd->chunks) {
            totalChunkChars += static_cast<uint64_t>(c.content.size());
            allChunks.push_back({docIdx, c.chunkId, c.content, c.startOffset, c.endOffset, true});
        }
    }

    // Chunk any remaining docs the legacy way.
    const bool needChunker = std::any_of(docHasPreparedChunks.begin(), docHasPreparedChunks.end(),
                                         [](bool v) { return !v; });

    std::unique_ptr<yams::vector::DocumentChunker> chunker;
    if (needChunker) {
        chunker = yams::vector::createChunker(strategy, ccfg, nullptr);
    }

    for (size_t docIdx = 0; docIdx < docsToEmbed.size(); ++docIdx) {
        if (preparedDocPtr[docIdx]) {
            continue;
        }
        if (stop_.load(std::memory_order_acquire)) {
            spdlog::info("EmbeddingService: aborting job={} during chunking (docs={}) due to "
                         "shutdown",
                         jobTag, docsToEmbed.size());
            failed_.fetch_add(docsToEmbed.size(), std::memory_order_relaxed);
            InternalEventBus::instance().incEmbedDropped(docsToEmbed.size());
            std::vector<std::string> failedHashes;
            failedHashes.reserve(docsToEmbed.size());
            for (const auto& doc : docsToEmbed) {
                failedHashes.push_back(doc.hash);
            }
            (void)meta_->batchUpdateDocumentRepairStatuses(failedHashes,
                                                           metadata::RepairStatus::Failed);
            return;
        }
        const auto& doc = docsToEmbed[docIdx];
        totalDocChars += static_cast<uint64_t>(doc.text.size());
        docPreviews[docIdx] = doc.text.size() <= 1000 ? doc.text : doc.text.substr(0, 1000);

        if (!chunker) {
            continue;
        }
        auto chunks = chunker->chunkDocument(doc.text, doc.hash);

        if (chunks.empty()) {
            std::string chunkId = yams::vector::utils::generateChunkId(doc.hash, 0);
            allChunks.push_back({docIdx, chunkId, doc.text, 0, doc.text.size(), false});
            totalChunkChars += static_cast<uint64_t>(doc.text.size());
        } else {
            for (size_t i = 0; i < chunks.size(); ++i) {
                auto& c = chunks[i];
                std::string chunkId = c.chunk_id.empty()
                                          ? yams::vector::utils::generateChunkId(doc.hash, i)
                                          : c.chunk_id;
                allChunks.push_back(
                    {docIdx, chunkId, std::move(c.content), c.start_offset, c.end_offset, false});
                totalChunkChars += static_cast<uint64_t>(allChunks.back().content.size());
            }
        }

        docsToEmbed[docIdx].text.clear();
        docsToEmbed[docIdx].text.shrink_to_fit();
    }

    spdlog::debug("EmbeddingService: chunked {} documents into {} chunks", docsToEmbed.size(),
                  allChunks.size());

    const auto selectionCfg = ConfigResolver::resolveEmbeddingSelectionPolicy();

    if (selectionCfg.mode != ConfigResolver::EmbeddingSelectionPolicy::Mode::Full &&
        !allChunks.empty()) {
        auto looksLikeHeading = [](const std::string& text) {
            if (text.empty()) {
                return false;
            }
            std::size_t lineEnd = text.find('\n');
            if (lineEnd == std::string::npos) {
                lineEnd = text.size();
            }
            std::string firstLine = text.substr(0, lineEnd);
            firstLine.erase(firstLine.begin(),
                            std::find_if(firstLine.begin(), firstLine.end(),
                                         [](unsigned char ch) { return !std::isspace(ch); }));
            firstLine.erase(std::find_if(firstLine.rbegin(), firstLine.rend(),
                                         [](unsigned char ch) { return !std::isspace(ch); })
                                .base(),
                            firstLine.end());
            if (firstLine.empty()) {
                return false;
            }
            if (firstLine.rfind("#", 0) == 0 || firstLine.rfind("##", 0) == 0) {
                return true;
            }
            if (firstLine.size() <= 120 && firstLine.back() == ':') {
                return true;
            }
            int alpha = 0;
            int upper = 0;
            for (unsigned char ch : firstLine) {
                if (std::isalpha(ch)) {
                    ++alpha;
                    if (std::isupper(ch)) {
                        ++upper;
                    }
                }
            }
            return alpha >= 6 && upper >= (alpha * 8) / 10;
        };

        std::vector<std::vector<std::size_t>> perDocChunkIdx(docsToEmbed.size());
        for (std::size_t idx = 0; idx < allChunks.size(); ++idx) {
            auto docIdx = allChunks[idx].docIdx;
            if (docIdx >= perDocChunkIdx.size()) {
                continue;
            }
            if (docHasPreparedChunks[docIdx]) {
                continue;
            }
            perDocChunkIdx[docIdx].push_back(idx);
        }

        std::vector<ChunkInfo> selectedChunks;
        selectedChunks.reserve(allChunks.size());
        std::size_t droppedChunks = 0;
        std::size_t totalSelectedChars = 0;

        // Keep prepared chunks untouched.
        for (const auto& c : allChunks) {
            if (c.prepared) {
                totalSelectedChars += c.content.size();
                selectedChunks.push_back(c);
            }
        }

        for (const auto& chunkIndexes : perDocChunkIdx) {
            if (chunkIndexes.empty()) {
                continue;
            }

            std::size_t effectiveMaxChunks = selectionCfg.maxChunksPerDoc;
            std::size_t effectiveMaxChars = selectionCfg.maxCharsPerDoc;
            if (selectionCfg.mode == ConfigResolver::EmbeddingSelectionPolicy::Mode::Adaptive) {
                const std::size_t localChunkCount = chunkIndexes.size();
                if (effectiveMaxChunks > 0) {
                    effectiveMaxChunks = std::min<std::size_t>(
                        std::max<std::size_t>(effectiveMaxChunks, 4u),
                        std::max<std::size_t>(effectiveMaxChunks, 4u + (localChunkCount / 12u)));
                }
                if (effectiveMaxChars > 0) {
                    effectiveMaxChars =
                        std::max<std::size_t>(effectiveMaxChars, 16000u + localChunkCount * 256u);
                }
            }

            if (selectionCfg.strategy ==
                ConfigResolver::EmbeddingSelectionPolicy::Strategy::IntroHeadings) {
                std::size_t selectedCount = 0;
                std::size_t selectedChars = 0;
                bool pickedAny = false;

                for (std::size_t rank = 0; rank < chunkIndexes.size(); ++rank) {
                    if (effectiveMaxChunks > 0 && selectedCount >= effectiveMaxChunks) {
                        break;
                    }
                    const auto idx = chunkIndexes[rank];
                    const auto& chunk = allChunks[idx];
                    if (rank != 0 && !looksLikeHeading(chunk.content)) {
                        continue;
                    }
                    const auto chunkSize = chunk.content.size();
                    if (effectiveMaxChars > 0 && selectedChars > 0 &&
                        (selectedChars + chunkSize) > effectiveMaxChars) {
                        continue;
                    }
                    selectedChunks.push_back(std::move(allChunks[idx]));
                    pickedAny = true;
                    selectedCount += 1;
                    selectedChars += chunkSize;
                    totalSelectedChars += chunkSize;
                }

                if (!pickedAny) {
                    const auto fallbackIdx = chunkIndexes.front();
                    totalSelectedChars += allChunks[fallbackIdx].content.size();
                    selectedChunks.push_back(std::move(allChunks[fallbackIdx]));
                    selectedCount = 1;
                }
                droppedChunks += (chunkIndexes.size() - selectedCount);
                continue;
            }

            struct ScoredChunk {
                double score;
                std::size_t idx;
            };
            std::vector<ScoredChunk> scored;
            scored.reserve(chunkIndexes.size());

            for (std::size_t rank = 0; rank < chunkIndexes.size(); ++rank) {
                const auto idx = chunkIndexes[rank];
                const auto& chunk = allChunks[idx];
                const auto chunkSize = chunk.content.size();
                double score = 1.0 / (1.0 + static_cast<double>(rank));
                if (rank == 0) {
                    score += selectionCfg.introBoost;
                }
                if (looksLikeHeading(chunk.content)) {
                    score += selectionCfg.headingBoost;
                }
                if (chunkSize >= 200 && chunkSize <= 1600) {
                    score += 0.25;
                } else if (chunkSize < 80) {
                    score -= 0.15;
                }
                scored.push_back(ScoredChunk{score, idx});
            }

            std::stable_sort(
                scored.begin(), scored.end(),
                [](const ScoredChunk& a, const ScoredChunk& b) { return a.score > b.score; });

            std::size_t selectedCount = 0;
            std::size_t selectedChars = 0;
            std::unordered_set<std::size_t> picked;
            picked.reserve(scored.size());

            for (const auto& item : scored) {
                if (effectiveMaxChunks > 0 && selectedCount >= effectiveMaxChunks) {
                    break;
                }
                const auto chunkSize = allChunks[item.idx].content.size();
                if (effectiveMaxChars > 0 && selectedChars > 0 &&
                    (selectedChars + chunkSize) > effectiveMaxChars) {
                    continue;
                }
                selectedChunks.push_back(std::move(allChunks[item.idx]));
                picked.insert(item.idx);
                selectedCount += 1;
                selectedChars += chunkSize;
                totalSelectedChars += chunkSize;
            }

            if (selectedCount == 0) {
                const auto fallbackIdx = chunkIndexes.front();
                totalSelectedChars += allChunks[fallbackIdx].content.size();
                selectedChunks.push_back(std::move(allChunks[fallbackIdx]));
                picked.insert(fallbackIdx);
                selectedCount = 1;
            }

            droppedChunks += (chunkIndexes.size() - selectedCount);
        }

        if (!selectedChunks.empty()) {
            allChunks = std::move(selectedChunks);
            totalChunkChars = totalSelectedChars;
            if (timingEnabled || poolDebug) {
                spdlog::info("[EmbeddingService] chunk_selection mode={} selected_chunks={} "
                             "dropped_chunks={} "
                             "max_chunks_per_doc={} max_chars_per_doc={} selected_chars={}",
                             selectionCfg.mode ==
                                     ConfigResolver::EmbeddingSelectionPolicy::Mode::Budgeted
                                 ? "budgeted"
                                 : "adaptive",
                             allChunks.size(), droppedChunks, selectionCfg.maxChunksPerDoc,
                             selectionCfg.maxCharsPerDoc, totalChunkChars);
            }
        }
    }

    logPhase("chunk", tChunk,
             fmt::format("docs={} chunks={} avg_chunks_per_doc={:.2f} doc_chars={} "
                         "avg_doc_chars={:.0f} chunk_chars={} avg_chunk_chars={:.0f}",
                         docsToEmbed.size(), allChunks.size(),
                         docsToEmbed.empty() ? 0.0
                                             : (static_cast<double>(allChunks.size()) /
                                                static_cast<double>(docsToEmbed.size())),
                         totalDocChars,
                         docsToEmbed.empty() ? 0.0
                                             : (static_cast<double>(totalDocChars) /
                                                static_cast<double>(docsToEmbed.size())),
                         totalChunkChars,
                         allChunks.empty() ? 0.0
                                           : (static_cast<double>(totalChunkChars) /
                                              static_cast<double>(allChunks.size()))));

    auto markAllFailed = [&]() {
        failed_.fetch_add(docsToEmbed.size());
        std::vector<std::string> failedHashes;
        failedHashes.reserve(docsToEmbed.size());
        for (const auto& doc : docsToEmbed) {
            failedHashes.push_back(doc.hash);
        }
        (void)meta_->batchUpdateDocumentRepairStatuses(failedHashes,
                                                       metadata::RepairStatus::Failed);
    };

    struct DocEmbeddingAccumulator {
        std::vector<float> sumEmbedding;
        std::vector<std::string> sourceChunkIds;
    };
    std::vector<DocEmbeddingAccumulator> docAccumulators(docsToEmbed.size());

    // ============================================================
    // Phase 3: Batch embedding call with sub-batching to avoid timeouts
    // ============================================================
    // Model inference can be slow for large batches. Sub-batch to keep response times reasonable.
    std::size_t kMaxBatchSize = TuneAdvisor::resolvedEmbedDocCap();
    if (kMaxBatchSize == 0 || kMaxBatchSize > 64) {
        kMaxBatchSize = 64;
    }
    if (timingEnabled || poolDebug) {
        spdlog::info("[EmbeddingService] job={} infer_cfg max_sub_batch={} job_hashes={} "
                     "docs_to_embed={} chunks={}",
                     jobTag, kMaxBatchSize, job.hashes.size(), docsToEmbed.size(),
                     allChunks.size());
    }

    uint64_t inferWarnMs = 15000;
    if (const char* s = std::getenv("YAMS_EMBED_SUBBATCH_WARN_MS")) {
        try {
            inferWarnMs = static_cast<uint64_t>(std::stoull(s));
        } catch (...) {
        }
    }

    auto toLower = [](std::string value) {
        std::transform(value.begin(), value.end(), value.begin(),
                       [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        return value;
    };
    auto isLikelyOomError = [&](const std::string& message) {
        const std::string msg = toLower(message);
        return msg.find("out of memory") != std::string::npos ||
               msg.find("resource exhausted") != std::string::npos ||
               msg.find("cuda") != std::string::npos || msg.find("cudnn") != std::string::npos ||
               msg.find("hip") != std::string::npos || msg.find("migraphx") != std::string::npos;
    };

    const auto& gpuInfo = yams::daemon::resource::detectGpu();
    const bool coremlUnified =
        gpuInfo.detected && gpuInfo.provider == "coreml" && gpuInfo.unifiedMemory;
    const std::size_t defaultAdaptiveStartCap = coremlUnified ? 8u : 16u;
    std::size_t adaptiveBatchCap = std::min<std::size_t>(kMaxBatchSize, defaultAdaptiveStartCap);
    const char* safeCapEnv = std::getenv("YAMS_EMBED_GPU_SAFE_BATCH_CAP");
    if (safeCapEnv) {
        try {
            const std::size_t parsed = static_cast<std::size_t>(std::stoull(safeCapEnv));
            if (parsed == 0) {
                adaptiveBatchCap = kMaxBatchSize;
            } else {
                adaptiveBatchCap = std::min<std::size_t>(kMaxBatchSize, parsed);
            }
        } catch (...) {
        }
    }
    std::size_t adaptiveMaxCap = kMaxBatchSize;
    if (!safeCapEnv) {
        // Conservative default ceiling for GPU-heavy paths to avoid long-tail stalls and
        // provider acquire timeouts under contention. CoreML/unified memory is stricter
        // because aggressive growth can exhaust shared system memory.
        adaptiveMaxCap = std::min<std::size_t>(kMaxBatchSize, coremlUnified ? 16u : 32u);
    }
    if (const char* env = std::getenv("YAMS_EMBED_ADAPTIVE_MAX_CAP")) {
        try {
            const std::size_t parsed = static_cast<std::size_t>(std::stoull(env));
            if (parsed > 0) {
                adaptiveMaxCap = std::min<std::size_t>(kMaxBatchSize, parsed);
            }
        } catch (...) {
        }
    }
    adaptiveBatchCap = std::max<std::size_t>(1u, adaptiveBatchCap);
    adaptiveBatchCap = std::min<std::size_t>(adaptiveBatchCap, adaptiveMaxCap);
    std::size_t successfulAdaptiveBatches = 0;
    std::size_t adaptiveGrowthSuccessTarget = coremlUnified ? 6u : 4u;
    if (const char* env = std::getenv("YAMS_EMBED_ADAPTIVE_GROW_SUCCESS")) {
        try {
            const std::size_t parsed = static_cast<std::size_t>(std::stoull(env));
            adaptiveGrowthSuccessTarget = std::clamp<std::size_t>(parsed, 1u, 16u);
        } catch (...) {
        }
    }
    if (timingEnabled || poolDebug) {
        spdlog::info("[EmbeddingService] job={} adaptive_sub_batch_start={} default_start={} "
                     "max_sub_batch={} adaptive_max_cap={} grow_after_successes={} "
                     "gpu_provider='{}' memory_model='{}'",
                     jobTag, adaptiveBatchCap, defaultAdaptiveStartCap, kMaxBatchSize,
                     adaptiveMaxCap, adaptiveGrowthSuccessTarget,
                     gpuInfo.provider.empty() ? "unknown" : gpuInfo.provider,
                     gpuInfo.unifiedMemory ? "unified" : "dedicated");
    }

    // ============================================================
    // Phase 4: Build VectorRecords and insert incrementally to cap peak memory
    // ============================================================
    const auto tBuild = std::chrono::steady_clock::now();
    const auto tInsert = std::chrono::steady_clock::now();
    const std::size_t insertChunkSize =
        std::clamp<std::size_t>(std::max<std::size_t>(64u, kMaxBatchSize * 2u), 64u, 512u);
    std::vector<yams::vector::VectorRecord> insertBuffer;
    insertBuffer.reserve(insertChunkSize);

    std::size_t insertedChunkRecords = 0;
    std::size_t insertedDocRecords = 0;
    auto flushInsertBuffer = [&](std::size_t& insertedCounter) -> bool {
        if (insertBuffer.empty()) {
            return true;
        }
        if (!vdb->insertVectorsBatch(insertBuffer)) {
            logPhase(
                "vdb_insert", tInsert,
                fmt::format("chunk_records={} doc_records={} pending={} chunk_size={} result=fail "
                            "err='{}'",
                            insertedChunkRecords, insertedDocRecords, insertBuffer.size(),
                            insertChunkSize, vdb->getLastError()));
            spdlog::error("EmbeddingService: vector insert failed (pending={}): {}",
                          insertBuffer.size(), vdb->getLastError());
            return false;
        }
        insertedCounter += insertBuffer.size();
        insertBuffer.clear();
        return true;
    };

    std::vector<std::size_t> subChunkIndices;
    subChunkIndices.reserve(std::max<std::size_t>(1u, kMaxBatchSize));
    std::vector<std::string> subBatch;
    subBatch.reserve(std::max<std::size_t>(1u, kMaxBatchSize));

    std::size_t start = 0;
    while (start < allChunks.size()) {
        if (stop_.load(std::memory_order_acquire)) {
            spdlog::info(
                "EmbeddingService: aborting job={} before infer sub-batch start={} (chunks={} "
                "docs={}) due to shutdown",
                jobTag, start, allChunks.size(), docsToEmbed.size());
            if (start == 0) {
                markAllFailed();
            }
            return;
        }
        const std::size_t take = std::min(adaptiveBatchCap, allChunks.size() - start);
        size_t end = start + take;
        subChunkIndices.clear();
        subBatch.clear();
        subChunkIndices.reserve(take);
        subBatch.reserve(take);
        for (size_t i = start; i < end; ++i) {
            subChunkIndices.push_back(i);
            subBatch.push_back(std::move(allChunks[i].content));
        }

        auto restoreMovedChunkContent = [&]() {
            const std::size_t restoreCount = std::min(subChunkIndices.size(), subBatch.size());
            for (std::size_t local = 0; local < restoreCount; ++local) {
                allChunks[subChunkIndices[local]].content = std::move(subBatch[local]);
            }
        };

        spdlog::debug("EmbeddingService: generating embeddings for batch {}-{} of {}", start, end,
                      allChunks.size());

        const auto tInfer = std::chrono::steady_clock::now();
        const uint64_t inferToken = inferTokenCounter_.fetch_add(1, std::memory_order_relaxed) + 1;
        inferSubBatchStarted_.fetch_add(1, std::memory_order_relaxed);
        {
            std::lock_guard<std::mutex> lock(inferTrackerMutex_);
            activeInferSubBatches_[inferToken] = tInfer;
        }

        uint64_t inferDurMs = 0;
        auto inferFinalize = [&](const char* outcome) {
            const uint64_t durMs =
                static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                          std::chrono::steady_clock::now() - tInfer)
                                          .count());
            inferDurMs = durMs;

            inferSubBatchLastDurationMs_.store(durMs, std::memory_order_relaxed);
            uint64_t prevMax = inferSubBatchMaxDurationMs_.load(std::memory_order_relaxed);
            while (durMs > prevMax && !inferSubBatchMaxDurationMs_.compare_exchange_weak(
                                          prevMax, durMs, std::memory_order_relaxed)) {
            }

            inferSubBatchCompleted_.fetch_add(1, std::memory_order_relaxed);
            {
                std::lock_guard<std::mutex> lock(inferTrackerMutex_);
                activeInferSubBatches_.erase(inferToken);
            }

            if (durMs >= inferWarnMs) {
                inferSubBatchWarnCount_.fetch_add(1, std::memory_order_relaxed);
                spdlog::warn(
                    "[EmbeddingService] job={} slow infer {} dur_ms={} sub_batch=[{}, {}) size={} "
                    "total_texts={} model='{}'",
                    jobTag, outcome, durMs, start, end, subBatch.size(), allChunks.size(),
                    modelName);
            }
        };

        struct InferFinalizeGuard {
            std::function<void()> fn;
            ~InferFinalizeGuard() {
                if (fn) {
                    fn();
                }
            }
        } inferGuard{[&]() { inferFinalize("exit"); }};

        if (timingEnabled || poolDebug) {
            spdlog::info("[EmbeddingService] job={} infer_start sub_batch=[{}, {}) size={} "
                         "total_texts={} model='{}'",
                         jobTag, start, end, subBatch.size(), allChunks.size(), modelName);
        }
        logPoolState("before_infer");
        auto embedResult = provider->generateBatchEmbeddingsFor(modelName, subBatch);
        logPoolState("after_infer");
        inferFinalize("done");
        inferGuard.fn = nullptr;
        logPhase("infer", tInfer,
                 fmt::format("sub_batch=[{}, {}) size={} total_texts={} model='{}'", start, end,
                             subBatch.size(), allChunks.size(), modelName));
        if (!embedResult) {
            if (take > 1 && isLikelyOomError(embedResult.error().message)) {
                restoreMovedChunkContent();
                const std::size_t prevCap = adaptiveBatchCap;
                adaptiveBatchCap =
                    std::max<std::size_t>(1u, std::min(adaptiveBatchCap / 2, take / 2));
                successfulAdaptiveBatches = 0;
                spdlog::warn(
                    "EmbeddingService: OOM-like failure for model '{}' at sub-batch [{}-{}) "
                    "size={} (cap {} -> {}), retrying smaller batch: {}",
                    modelName, start, end, take, prevCap, adaptiveBatchCap,
                    embedResult.error().message);
                continue;
            }
            if (take > 1) {
                restoreMovedChunkContent();
                const std::size_t prevCap = adaptiveBatchCap;
                adaptiveBatchCap =
                    std::max<std::size_t>(1u, std::min(adaptiveBatchCap / 2, take / 2));
                successfulAdaptiveBatches = 0;
                spdlog::warn(
                    "EmbeddingService: transient batch failure for model '{}' at sub-batch [{}-{}) "
                    "size={} (cap {} -> {}), retrying smaller batch: {}",
                    modelName, start, end, take, prevCap, adaptiveBatchCap,
                    embedResult.error().message);
                continue;
            }
            spdlog::error("EmbeddingService: batch embedding failed at {}-{}: {}", start, end,
                          embedResult.error().message);
            markAllFailed();
            return;
        }

        const uint64_t adaptiveBackoffMs = std::max<uint64_t>(30000, inferWarnMs * 2);
        if (take > 1 && adaptiveBatchCap > 1 && inferDurMs >= adaptiveBackoffMs) {
            const std::size_t prevCap = adaptiveBatchCap;
            adaptiveBatchCap = std::max<std::size_t>(1u, std::min(adaptiveBatchCap / 2, take / 2));
            successfulAdaptiveBatches = 0;
            if (adaptiveBatchCap != prevCap) {
                spdlog::info(
                    "EmbeddingService: reducing adaptive sub-batch cap {} -> {} after slow infer "
                    "({}ms >= {}ms)",
                    prevCap, adaptiveBatchCap, inferDurMs, adaptiveBackoffMs);
            }
        }

        if (adaptiveBatchCap < adaptiveMaxCap) {
            successfulAdaptiveBatches++;
            if (successfulAdaptiveBatches >= adaptiveGrowthSuccessTarget) {
                const std::size_t prevCap = adaptiveBatchCap;
                adaptiveBatchCap = std::min<std::size_t>(adaptiveMaxCap, adaptiveBatchCap * 2);
                successfulAdaptiveBatches = 0;
                if (adaptiveBatchCap != prevCap) {
                    spdlog::info("EmbeddingService: increasing adaptive sub-batch cap {} -> {}",
                                 prevCap, adaptiveBatchCap);
                }
            }
        }

        auto& batchEmbeddings = embedResult.value();
        if (batchEmbeddings.size() != subBatch.size()) {
            spdlog::error("EmbeddingService: embedding count mismatch in sub-batch ({} vs {})",
                          batchEmbeddings.size(), subBatch.size());
            markAllFailed();
            return;
        }

        for (size_t local = 0; local < batchEmbeddings.size(); ++local) {
            const size_t chunkIdx = start + local;
            auto& chunk = allChunks[chunkIdx];
            auto& emb = batchEmbeddings[local];
            const auto& doc = docsToEmbed[chunk.docIdx];

            auto& acc = docAccumulators[chunk.docIdx];
            if (acc.sumEmbedding.empty()) {
                acc.sumEmbedding.assign(emb.size(), 0.0f);
            }
            const size_t dim = std::min(acc.sumEmbedding.size(), emb.size());
            for (size_t j = 0; j < dim; ++j) {
                acc.sumEmbedding[j] += emb[j];
            }
            acc.sourceChunkIds.push_back(chunk.chunkId);

            yams::vector::VectorRecord rec;
            rec.document_hash = doc.hash;
            rec.chunk_id = chunk.chunkId;
            rec.embedding = std::move(emb);
            rec.content = std::move(subBatch[local]);
            rec.start_offset = chunk.startOffset;
            rec.end_offset = chunk.endOffset;
            rec.level = yams::vector::EmbeddingLevel::CHUNK;
            rec.metadata["name"] = doc.fileName;
            rec.metadata["mime_type"] = doc.mimeType;
            rec.metadata["path"] = doc.filePath;
            insertBuffer.push_back(std::move(rec));

            if (insertBuffer.size() >= insertChunkSize &&
                !flushInsertBuffer(insertedChunkRecords)) {
                markAllFailed();
                return;
            }
        }

        start = end;
    }

    if (!flushInsertBuffer(insertedChunkRecords)) {
        markAllFailed();
        return;
    }

    for (size_t docIdx = 0; docIdx < docsToEmbed.size(); ++docIdx) {
        auto& acc = docAccumulators[docIdx];
        if (acc.sourceChunkIds.empty() || acc.sumEmbedding.empty()) {
            continue;
        }

        float norm = 0.0f;
        for (float v : acc.sumEmbedding) {
            norm += v * v;
        }
        if (norm > 0.0f) {
            norm = std::sqrt(norm);
            for (float& v : acc.sumEmbedding) {
                v /= norm;
            }
        }

        const auto& doc = docsToEmbed[docIdx];
        yams::vector::VectorRecord docRec;
        docRec.document_hash = doc.hash;
        docRec.chunk_id = yams::vector::utils::generateChunkId(doc.hash, 999999);
        docRec.embedding = std::move(acc.sumEmbedding);
        docRec.content = std::move(docPreviews[docIdx]);
        docRec.level = yams::vector::EmbeddingLevel::DOCUMENT;
        docRec.source_chunk_ids = std::move(acc.sourceChunkIds);
        docRec.metadata["name"] = doc.fileName;
        docRec.metadata["mime_type"] = doc.mimeType;
        docRec.metadata["path"] = doc.filePath;
        insertBuffer.push_back(std::move(docRec));

        if (insertBuffer.size() >= insertChunkSize && !flushInsertBuffer(insertedDocRecords)) {
            markAllFailed();
            return;
        }
    }

    if (!flushInsertBuffer(insertedDocRecords)) {
        markAllFailed();
        return;
    }

    logPhase("build_records", tBuild,
             fmt::format("docs={} chunks={} chunk_records={} doc_records={}", docsToEmbed.size(),
                         allChunks.size(), insertedChunkRecords, insertedDocRecords));

    logPhase("vdb_insert", tInsert,
             fmt::format("chunk_records={} doc_records={} total_records={} chunk_size={} result=ok",
                         insertedChunkRecords, insertedDocRecords,
                         insertedChunkRecords + insertedDocRecords, insertChunkSize));

    // Update metadata and repair status for all succeeded documents (single transaction each)
    const auto tMeta = std::chrono::steady_clock::now();
    std::vector<std::string> successHashes;
    successHashes.reserve(docsToEmbed.size());
    for (const auto& doc : docsToEmbed) {
        successHashes.push_back(doc.hash);
    }
    (void)meta_->batchUpdateDocumentEmbeddingStatusByHashes(successHashes, true, modelName);
    (void)meta_->batchUpdateDocumentRepairStatuses(successHashes,
                                                   metadata::RepairStatus::Completed);

    logPhase("metadata_update", tMeta,
             fmt::format("docs={} model='{}'", successHashes.size(), modelName));
    logPoolState("job_end");

    processed_.fetch_add(docsToEmbed.size());

    spdlog::debug("EmbeddingService: batch complete (succeeded={}, skipped={}, failed={})",
                  docsToEmbed.size(), skipped, failedGather);
}

} // namespace daemon
} // namespace yams
