#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <algorithm>
#include <cctype>
#include <future>
#include <string_view>
#include <thread>
#include <unordered_set>
#include <boost/asio.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <yams/api/content_store.h>
#include <yams/common/utf8_utils.h>
#include <yams/daemon/async_batcher.h>
#include <yams/daemon/components/GraphComponent.h>
#include <yams/daemon/components/ConfigResolver.h>
#include <yams/daemon/components/InternalEventBus.h>
#include <yams/daemon/components/KGWriteQueue.h>
#include <yams/daemon/components/PostIngestQueue.h>
#include <yams/daemon/components/ResourceGovernor.h>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/daemon/components/TuningSnapshot.h>
#include <yams/daemon/components/WorkCoordinator.h>
#include <yams/daemon/pressure_limited_poller.h>
#include <yams/daemon/resource/external_entity_provider_adapter.h>
#include <yams/extraction/extraction_util.h>
#include <yams/extraction/text_extractor.h>
#include <yams/ingest/ingest_helpers.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/vector/document_chunker.h>
#include <yams/vector/embedding_generator.h>
#include <yams/vector/vector_database.h>

#include <yams/daemon/components/embed_preparer.h>

using yams::extraction::util::extractDocumentText;

namespace yams::daemon {
namespace {
constexpr size_t kMaxTitleLen = 120;
constexpr size_t kMaxGlinerChars = 2000;
constexpr float kMinTitleConfidence = 0.55f;

// Check if GLiNER title extraction is disabled via environment variable
// Set YAMS_DISABLE_GLINER_TITLES=1 for faster ingestion at the cost of title quality
inline bool isGlinerTitleExtractionDisabled() {
    static const bool disabled = []() {
        const char* env = std::getenv("YAMS_DISABLE_GLINER_TITLES");
        return env && std::string(env) == "1";
    }();
    return disabled;
}

std::string trimCopy(std::string_view input) {
    size_t start = 0;
    size_t end = input.size();
    while (start < end && std::isspace(static_cast<unsigned char>(input[start]))) {
        ++start;
    }
    while (end > start && std::isspace(static_cast<unsigned char>(input[end - 1]))) {
        --end;
    }
    return std::string(input.substr(start, end - start));
}

std::string collapseWhitespace(std::string s) {
    std::string out;
    out.reserve(s.size());
    bool inSpace = false;
    for (unsigned char c : s) {
        if (std::isspace(c)) {
            if (!inSpace) {
                out.push_back(' ');
                inSpace = true;
            }
        } else {
            out.push_back(static_cast<char>(c));
            inSpace = false;
        }
    }
    return out;
}

std::string normalizeTitleCandidate(std::string s) {
    s = trimCopy(s);
    if (s.empty()) {
        return s;
    }
    s = collapseWhitespace(std::move(s));
    if (s.size() > kMaxTitleLen) {
        s.resize(kMaxTitleLen);
    }
    return s;
}

std::string stripCommentPrefix(std::string_view line) {
    std::string s = trimCopy(line);
    if (s.rfind("//", 0) == 0) {
        return trimCopy(std::string_view(s).substr(2));
    }
    if (s.rfind("#", 0) == 0) {
        return trimCopy(std::string_view(s).substr(1));
    }
    if (s.rfind("--", 0) == 0) {
        return trimCopy(std::string_view(s).substr(2));
    }
    if (s.rfind("/*", 0) == 0) {
        s = trimCopy(std::string_view(s).substr(2));
    }
    if (s.rfind("*", 0) == 0) {
        return trimCopy(std::string_view(s).substr(1));
    }
    if (s.rfind("*/", 0) == 0) {
        return trimCopy(std::string_view(s).substr(2));
    }
    return s;
}

std::string extractHtmlTitle(std::string_view text) {
    const size_t maxScan = std::min(text.size(), static_cast<size_t>(4096));
    std::string lower;
    lower.reserve(maxScan);
    for (size_t i = 0; i < maxScan; ++i) {
        lower.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(text[i]))));
    }
    const std::string_view lowerView(lower);
    const auto openPos = lowerView.find("<title");
    if (openPos == std::string_view::npos) {
        return {};
    }
    const auto gtPos = lowerView.find('>', openPos);
    if (gtPos == std::string_view::npos) {
        return {};
    }
    const auto closePos = lowerView.find("</title>", gtPos);
    if (closePos == std::string_view::npos) {
        return {};
    }
    const auto start = gtPos + 1;
    const auto len = closePos - start;
    return normalizeTitleCandidate(std::string(text.substr(start, len)));
}

std::string extractMarkdownHeading(std::string_view text) {
    size_t pos = 0;
    size_t lines = 0;
    const size_t maxLines = 200;
    while (pos < text.size() && lines < maxLines) {
        size_t end = text.find('\n', pos);
        if (end == std::string_view::npos) {
            end = text.size();
        }
        auto line = trimCopy(text.substr(pos, end - pos));
        if (!line.empty()) {
            if (line.rfind("#", 0) == 0) {
                size_t i = 0;
                while (i < line.size() && line[i] == '#') {
                    ++i;
                }
                auto heading = trimCopy(std::string_view(line).substr(i));
                return normalizeTitleCandidate(std::move(heading));
            }
        }
        pos = end + 1;
        ++lines;
    }
    return {};
}

std::string extractCodeSignature(std::string_view text) {
    size_t pos = 0;
    size_t lines = 0;
    const size_t maxLines = 200;
    while (pos < text.size() && lines < maxLines) {
        size_t end = text.find('\n', pos);
        if (end == std::string_view::npos) {
            end = text.size();
        }
        auto rawLine = text.substr(pos, end - pos);
        auto line = stripCommentPrefix(rawLine);
        if (!line.empty()) {
            static constexpr std::string_view kPrefixes[] = {
                "class ",    "struct ", "interface ", "enum ",    "def ",
                "function ", "fn ",     "module ",    "package ", "namespace "};
            for (const auto& prefix : kPrefixes) {
                if (line.rfind(prefix, 0) == 0) {
                    return normalizeTitleCandidate(std::move(line));
                }
            }
        }
        pos = end + 1;
        ++lines;
    }
    return {};
}

std::string extractFirstMeaningfulLine(std::string_view text) {
    size_t pos = 0;
    size_t lines = 0;
    const size_t maxLines = 200;
    while (pos < text.size() && lines < maxLines) {
        size_t end = text.find('\n', pos);
        if (end == std::string_view::npos) {
            end = text.size();
        }
        auto rawLine = text.substr(pos, end - pos);
        auto line = stripCommentPrefix(rawLine);
        if (!line.empty()) {
            return normalizeTitleCandidate(std::move(line));
        }
        pos = end + 1;
        ++lines;
    }
    return {};
}
} // namespace

// Dynamic concurrency limits from TuneAdvisor
std::size_t PostIngestQueue::maxExtractionConcurrent() {
    return std::max<std::size_t>(4u,
                                 static_cast<std::size_t>(TuneAdvisor::postExtractionConcurrent()));
}

std::size_t PostIngestQueue::maxKgConcurrent() {
    return static_cast<std::size_t>(TuneAdvisor::postKgConcurrent());
}

std::size_t PostIngestQueue::maxSymbolConcurrent() {
    return static_cast<std::size_t>(TuneAdvisor::postSymbolConcurrent());
}

std::size_t PostIngestQueue::maxEntityConcurrent() {
    return static_cast<std::size_t>(TuneAdvisor::postEntityConcurrent());
}

std::size_t PostIngestQueue::maxTitleConcurrent() {
    // Title extraction shares ONNX resources with embeddings, so limit concurrency
    // to allow both to run in parallel without starving each other
    return static_cast<std::size_t>(TuneAdvisor::postTitleConcurrent());
}

PostIngestQueue::PostIngestQueue(
    std::shared_ptr<api::IContentStore> store, std::shared_ptr<metadata::MetadataRepository> meta,
    std::vector<std::shared_ptr<extraction::IContentExtractor>> extractors,
    std::shared_ptr<metadata::KnowledgeGraphStore> kg,
    std::shared_ptr<GraphComponent> graphComponent, WorkCoordinator* coordinator,
    WorkCoordinator* entityCoordinator, std::size_t capacity)
    : store_(std::move(store)), meta_(std::move(meta)), extractors_(std::move(extractors)),
      kg_(std::move(kg)), graphComponent_(std::move(graphComponent)), coordinator_(coordinator),
      entityCoordinator_(entityCoordinator), capacity_(capacity ? capacity : 1000) {
    refreshStageAvailability();
    spdlog::info("[PostIngestQueue] Created (parallel processing via WorkCoordinator)");
}

PostIngestQueue::~PostIngestQueue() {
    stop();
    // Wait for all in-flight operations to complete before destroying members.
    // This prevents data races where workers access members during destruction.
    constexpr int maxWaitMs = 1000;
    for (int i = 0; i < maxWaitMs && totalInFlight() > 0; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

void PostIngestQueue::start() {
    spdlog::info("[PostIngestQueue] start() called, stop_={}", stop_.load());
    if (!stop_.load()) {
        refreshStageAvailability();
        logStageAvailabilitySnapshot();
        initializeGradientLimiters();
        spdlog::info("[PostIngestQueue] Spawning channelPoller coroutine...");
        boost::asio::co_spawn(coordinator_->getExecutor(), channelPoller(), boost::asio::detached);
        spdlog::info("[PostIngestQueue] Spawning kgPoller coroutine...");
        boost::asio::co_spawn(coordinator_->getExecutor(), kgPoller(), boost::asio::detached);
        spdlog::info("[PostIngestQueue] Spawning symbolPoller coroutine...");
        boost::asio::co_spawn(coordinator_->getExecutor(), symbolPoller(), boost::asio::detached);
        spdlog::info("[PostIngestQueue] Spawning entityPoller coroutine...");
        auto entityExec =
            entityCoordinator_ ? entityCoordinator_->getExecutor() : coordinator_->getExecutor();
        boost::asio::co_spawn(entityExec, entityPoller(), boost::asio::detached);
        spdlog::info("[PostIngestQueue] Spawning titlePoller coroutine...");
        boost::asio::co_spawn(coordinator_->getExecutor(), titlePoller(), boost::asio::detached);

        constexpr int maxWaitMs = 100;
        for (int i = 0; i < maxWaitMs; ++i) {
            bool allStarted = true;
            for (std::size_t s = 0; s < kStageCount; ++s) {
                if (!stageStarted_[s].load()) {
                    allStarted = false;
                    break;
                }
            }
            if (allStarted)
                break;
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }

        spdlog::info("[PostIngestQueue] Pollers started (extraction={}, kg={}, symbol={}, "
                     "entity={}, title={})",
                     stageStarted_[0].load(), stageStarted_[1].load(), stageStarted_[2].load(),
                     stageStarted_[3].load(), stageStarted_[4].load());
    } else {
        spdlog::warn("[PostIngestQueue] start() skipped because stop_=true");
    }
}

void PostIngestQueue::stop() {
    stop_.store(true);
    TuneAdvisor::setPostIngestStageActive(TuneAdvisor::PostIngestStage::Extraction, false);
    TuneAdvisor::setPostIngestStageActive(TuneAdvisor::PostIngestStage::KnowledgeGraph, false);
    TuneAdvisor::setPostIngestStageActive(TuneAdvisor::PostIngestStage::Symbol, false);
    TuneAdvisor::setPostIngestStageActive(TuneAdvisor::PostIngestStage::Entity, false);
    TuneAdvisor::setPostIngestStageActive(TuneAdvisor::PostIngestStage::Title, false);
    spdlog::info("[PostIngestQueue] Stop requested");
}

// ============================================================================
// Pause/Resume Support (for ResourceGovernor pressure response)
// ============================================================================

namespace {
constexpr TuneAdvisor::PostIngestStage kTuneAdvisorStages[] = {
    TuneAdvisor::PostIngestStage::Extraction, TuneAdvisor::PostIngestStage::KnowledgeGraph,
    TuneAdvisor::PostIngestStage::Symbol,     TuneAdvisor::PostIngestStage::Entity,
    TuneAdvisor::PostIngestStage::Title,
};
} // namespace

void PostIngestQueue::pauseStage(Stage stage) {
    const auto idx = static_cast<std::size_t>(stage);
    stagePaused_[idx].store(true, std::memory_order_release);
    TuneAdvisor::setPostIngestStageActive(kTuneAdvisorStages[idx], false);
    spdlog::info("[PostIngestQueue] Paused {} stage", kStageNames[idx]);
}

void PostIngestQueue::resumeStage(Stage stage) {
    const auto idx = static_cast<std::size_t>(stage);
    stagePaused_[idx].store(false, std::memory_order_release);
    TuneAdvisor::setPostIngestStageActive(kTuneAdvisorStages[idx], true);
    spdlog::info("[PostIngestQueue] Resumed {} stage", kStageNames[idx]);
}

bool PostIngestQueue::isStagePaused(Stage stage) const {
    return stagePaused_[static_cast<std::size_t>(stage)].load(std::memory_order_acquire);
}

void PostIngestQueue::pauseAll() {
    for (std::size_t i = 0; i < kStageCount; ++i) {
        stagePaused_[i].store(true, std::memory_order_release);
        TuneAdvisor::setPostIngestStageActive(kTuneAdvisorStages[i], false);
    }
    spdlog::warn("[PostIngestQueue] All stages paused (emergency mode)");
}

void PostIngestQueue::resumeAll() {
    for (std::size_t i = 0; i < kStageCount; ++i) {
        stagePaused_[i].store(false, std::memory_order_release);
    }
    refreshStageAvailability();
    spdlog::info("[PostIngestQueue] All stages resumed (normal operation)");
}

void PostIngestQueue::setSymbolExtensionMap(std::unordered_map<std::string, std::string> extMap) {
    {
        std::lock_guard<std::mutex> lock(extMapMutex_);
        symbolExtensionMap_ = std::move(extMap);
    }
    refreshStageAvailability();
}

void PostIngestQueue::setEntityProviders(
    std::vector<std::shared_ptr<ExternalEntityProviderAdapter>> providers) {
    {
        std::lock_guard<std::mutex> lock(entityMutex_);
        entityProviders_ = std::move(providers);
    }
    refreshStageAvailability();
}

void PostIngestQueue::setTitleExtractor(search::EntityExtractionFunc extractor) {
    titleExtractor_ = std::move(extractor);
    refreshStageAvailability();
}

void PostIngestQueue::refreshStageAvailability() {
    const bool extractionActive = !stagePaused_[0].load(std::memory_order_acquire);
    TuneAdvisor::setPostIngestStageActive(TuneAdvisor::PostIngestStage::Extraction,
                                          extractionActive);

    const bool kgActive =
        graphComponent_ != nullptr && !stagePaused_[1].load(std::memory_order_acquire);
    TuneAdvisor::setPostIngestStageActive(TuneAdvisor::PostIngestStage::KnowledgeGraph, kgActive);

    bool symbolCapable = false;
    {
        std::lock_guard<std::mutex> lock(extMapMutex_);
        symbolCapable = !symbolExtensionMap_.empty();
    }
    const bool symbolActive = symbolCapable && !stagePaused_[2].load(std::memory_order_acquire);
    TuneAdvisor::setPostIngestStageActive(TuneAdvisor::PostIngestStage::Symbol, symbolActive);

    bool entityCapable = false;
    {
        std::lock_guard<std::mutex> lock(entityMutex_);
        entityCapable = !entityProviders_.empty();
    }
    const bool entityActive = entityCapable && !stagePaused_[3].load(std::memory_order_acquire);
    TuneAdvisor::setPostIngestStageActive(TuneAdvisor::PostIngestStage::Entity, entityActive);

    const bool titleActive =
        (titleExtractor_ != nullptr) && !stagePaused_[4].load(std::memory_order_acquire);
    TuneAdvisor::setPostIngestStageActive(TuneAdvisor::PostIngestStage::Title, titleActive);
}

void PostIngestQueue::logStageAvailabilitySnapshot() const {
    bool symbolCapable = false;
    {
        std::lock_guard<std::mutex> lock(extMapMutex_);
        symbolCapable = !symbolExtensionMap_.empty();
    }
    bool entityCapable = false;
    {
        std::lock_guard<std::mutex> lock(entityMutex_);
        entityCapable = !entityProviders_.empty();
    }

    spdlog::info(
        "[PostIngestQueue] Stage snapshot: active={{extraction={}, kg={}, symbol={}, entity={}, "
        "title={}}} paused={{extraction={}, kg={}, symbol={}, entity={}, title={}}} limits={{"
        "extraction={}, kg={}, symbol={}, entity={}, title={}}}",
        !stagePaused_[0].load(std::memory_order_acquire),
        graphComponent_ != nullptr && !stagePaused_[1].load(std::memory_order_acquire),
        symbolCapable && !stagePaused_[2].load(std::memory_order_acquire),
        entityCapable && !stagePaused_[3].load(std::memory_order_acquire),
        titleExtractor_ != nullptr && !stagePaused_[4].load(std::memory_order_acquire),
        stagePaused_[0].load(std::memory_order_acquire),
        stagePaused_[1].load(std::memory_order_acquire),
        stagePaused_[2].load(std::memory_order_acquire),
        stagePaused_[3].load(std::memory_order_acquire),
        stagePaused_[4].load(std::memory_order_acquire), maxExtractionConcurrent(),
        maxKgConcurrent(), maxSymbolConcurrent(), maxEntityConcurrent(), maxTitleConcurrent());
}

std::size_t PostIngestQueue::resolveChannelCapacity() const {
    std::size_t cap = capacity_;
    if (cap == 0) {
        cap = static_cast<std::size_t>(TuneAdvisor::postIngestQueueMax());
    }
    if (cap == 0) {
        cap = 1;
    }
    return cap;
}

std::size_t PostIngestQueue::boundedStageChannelCapacity(std::size_t defaultCap) const {
    return std::max<std::size_t>(1u, defaultCap);
}

double PostIngestQueue::kgChannelFillRatio(std::size_t* depthOut, std::size_t* capacityOut) const {
    // Observability-only: do not create the channel as a side effect.
    auto ch = InternalEventBus::instance().get_channel<InternalEventBus::KgJob>("kg_jobs");
    if (!ch) {
        if (depthOut)
            *depthOut = 0;
        if (capacityOut)
            *capacityOut = 0;
        return 0.0;
    }

    const std::size_t cap = std::max<std::size_t>(1u, ch->capacity());
    const std::size_t depth = ch->size_approx();
    if (depthOut)
        *depthOut = depth;
    if (capacityOut)
        *capacityOut = cap;
    return static_cast<double>(depth) / static_cast<double>(cap);
}

std::size_t PostIngestQueue::adaptiveExtractionBatchSize(std::size_t baseBatchSize) const {
    baseBatchSize = std::max<std::size_t>(1u, baseBatchSize);
    // Keep extraction commits meaningfully batched even when stage concurrency is clamped.
    // Concurrency governs how many batches run at once; this floor governs docs per batch.
    constexpr std::size_t kExtractionBatchFloor = 4u;
    const std::size_t floorBatch = kExtractionBatchFloor;

    // When KG is saturated, reduce extraction batching to avoid overwhelming the KG stage.
    // We intentionally keep this simple and monotonic.
    std::size_t depth = 0;
    std::size_t cap = 0;
    const double fill = kgChannelFillRatio(&depth, &cap);
    (void)depth;

    if (cap == 0) {
        return std::max(baseBatchSize, floorBatch);
    }

    if (fill > 0.85) {
        return std::max<std::size_t>(floorBatch, baseBatchSize / 4u);
    }
    if (fill > 0.70) {
        return std::max<std::size_t>(floorBatch, baseBatchSize / 2u);
    }
    return std::max(baseBatchSize, floorBatch);
}

std::size_t PostIngestQueue::adaptiveStageBatchSize(std::size_t queueDepth,
                                                    std::size_t baseBatchSize,
                                                    std::size_t batchCap) const {
    const std::size_t tuned = std::max<std::size_t>(1u, std::min(baseBatchSize, batchCap));

    // Keep batches small at low depth for latency, but force larger fan-out under pressure.
    if (queueDepth >= 1024) {
        return std::max<std::size_t>(tuned, std::min<std::size_t>(batchCap, 32u));
    }
    if (queueDepth >= 512) {
        return std::max<std::size_t>(tuned, std::min<std::size_t>(batchCap, 16u));
    }
    if (queueDepth >= 128) {
        return std::max<std::size_t>(tuned, std::min<std::size_t>(batchCap, 8u));
    }
    return tuned;
}

bool PostIngestQueue::isKgChannelBackpressured() const {
    return kgChannelFillRatio() >= PostIngestQueue::kKgBackpressureThreshold;
}

PostIngestQueue::BackpressureStatus PostIngestQueue::getBackpressureStatus() const {
    BackpressureStatus st;
    st.threshold = PostIngestQueue::kKgBackpressureThreshold;
    st.kgFillRatio = kgChannelFillRatio(&st.kgDepth, &st.kgCapacity);
    return st;
}

void PostIngestQueue::checkDrainAndSignal() {
    // Check if queue is now drained (all stages idle)
    if (totalInFlight() == 0) {
        // Only signal if we were previously active (had work)
        bool expected = true;
        if (wasActive_.compare_exchange_strong(expected, false, std::memory_order_acq_rel)) {
            // Queue just became drained - signal corpus stats stale
            if (meta_) {
                meta_->signalCorpusStatsStale();
            }

            // Invoke drain callback (for search engine rebuild trigger)
            DrainCallback cb;
            {
                std::lock_guard<std::mutex> lock(drainCallbackMutex_);
                cb = drainCallback_;
            }
            if (cb) {
                cb();
            }

            spdlog::debug(
                "[PostIngestQueue] Queue drained, signaled corpus stats and drain callback");
        }
    }
}

boost::asio::awaitable<void> PostIngestQueue::channelPoller() {
    spdlog::info("[PostIngestQueue] channelPoller coroutine STARTED");
    const std::size_t channelCapacity = resolveChannelCapacity();
    auto channel =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::PostIngestTask>(
            "post_ingest", channelCapacity);
    spdlog::info("[PostIngestQueue] channelPoller got channel (cap={})", channelCapacity);

    std::shared_ptr<SpscQueue<InternalEventBus::PostIngestTask>> rpcChannel;
    const std::size_t rpcCapacity = static_cast<std::size_t>(TuneAdvisor::postIngestRpcQueueMax());
    if (rpcCapacity > 0) {
        rpcChannel =
            InternalEventBus::instance().get_or_create_channel<InternalEventBus::PostIngestTask>(
                "post_ingest_rpc", rpcCapacity);
        spdlog::info("[PostIngestQueue] channelPoller got RPC channel (cap={})", rpcCapacity);
    }

    PressureLimitedPollerConfig<InternalEventBus::PostIngestTask> cfg;
    cfg.stageName = "extraction";
    cfg.stopFlag = &stop_;
    cfg.startedFlag = &stageStarted_[0];
    cfg.pauseFlag = &stagePaused_[0];
    cfg.wasActiveFlag = &wasActive_;
    cfg.inFlightCounter = &stageInFlight_[0];
    cfg.getLimiterFn = [this]() { return limiters_[0].get(); };
    cfg.maxConcurrentFn = &maxExtractionConcurrent;
    cfg.tryAcquireFn = [this](GradientLimiter* l, const std::string& id, const std::string& s) {
        return tryAcquireLimiterSlot(l, id, s);
    };
    cfg.completeJobFn = [this](const std::string& id, bool ok) { completeJob(id, ok); };
    cfg.checkDrainFn = [this]() { checkDrainAndSignal(); };
    cfg.executor = coordinator_->getExecutor();
    cfg.getHashFn = [](const InternalEventBus::PostIngestTask& t) -> std::string { return t.hash; };
    cfg.batchMode = true;
    cfg.batchLimiterPerTask = false;
    cfg.batchSizeFn = [this]() -> std::size_t {
        const std::size_t base = std::max<std::size_t>(1u, TuneAdvisor::postIngestBatchSize());
        return adaptiveExtractionBatchSize(base);
    };
    cfg.highPriorityChannel = rpcChannel;
    cfg.highPriorityMaxPerBatchFn = [this]() -> std::size_t {
        const std::size_t baseBatchSz =
            std::max<std::size_t>(1u, TuneAdvisor::postIngestBatchSize());
        const std::size_t batchSz = adaptiveExtractionBatchSize(baseBatchSz);

        const std::size_t tunedHp =
            std::max<std::size_t>(1u, TuneAdvisor::postIngestRpcMaxPerBatch());

        std::size_t kgDepth = 0;
        std::size_t kgCap = 0;
        const double kgFill = kgChannelFillRatio(&kgDepth, &kgCap);
        const bool pressure = (kgFill > 0.70);

        // Under KG pressure, skew batches toward RPC tasks to keep interactive calls snappy.
        // Normal mode preserves TuneAdvisor steering.
        const std::size_t pressureHp = std::max<std::size_t>(1u, (batchSz * 3u) / 4u);
        const std::size_t hp = pressure ? std::max(tunedHp, pressureHp) : tunedHp;
        return std::min(hp, batchSz);
    };
    cfg.batchProcessFn = [this](std::vector<InternalEventBus::PostIngestTask>&& tasks) {
        processBatch(std::move(tasks));
    };

    co_await pressureLimitedPoll(channel, std::move(cfg));
}

void PostIngestQueue::enqueue(Task t) {
    static constexpr const char* kChannelName = "post_ingest";
    const std::size_t channelCapacity = resolveChannelCapacity();
    auto channel =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::PostIngestTask>(
            kChannelName, channelCapacity);

    InternalEventBus::PostIngestTask task;
    task.hash = std::move(t.hash);
    task.mime = std::move(t.mime);

    constexpr auto kEnqueueTimeout = std::chrono::milliseconds(250);
    uint32_t waits = 0;
    while (!stop_.load(std::memory_order_acquire)) {
        if (channel->push_wait(task, kEnqueueTimeout)) {
            return;
        }
        ++waits;
        if ((waits % 20u) == 1u) {
            spdlog::warn("[PostIngestQueue] enqueue waiting on full channel (hash={}, waits={})",
                         task.hash, waits);
        }
    }
}

void PostIngestQueue::enqueueBatch(std::vector<Task> tasks) {
    if (tasks.empty()) {
        return;
    }

    static constexpr const char* kChannelName = "post_ingest";
    const std::size_t channelCapacity = resolveChannelCapacity();
    auto channel =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::PostIngestTask>(
            kChannelName, channelCapacity);

    std::vector<InternalEventBus::PostIngestTask> busTasks;
    busTasks.reserve(tasks.size());
    for (auto& t : tasks) {
        InternalEventBus::PostIngestTask task;
        task.hash = std::move(t.hash);
        task.mime = std::move(t.mime);
        busTasks.push_back(std::move(task));
    }

    constexpr auto kEnqueueTimeout = std::chrono::milliseconds(250);
    std::size_t next = 0;
    uint32_t waits = 0;
    while (!stop_.load(std::memory_order_acquire) && next < busTasks.size()) {
        const std::size_t pushed = channel->try_push_many(busTasks, next);
        next += pushed;
        if (next >= busTasks.size()) {
            break;
        }
        if (channel->push_wait(busTasks[next], kEnqueueTimeout)) {
            ++next;
            continue;
        }
        ++waits;
        if ((waits % 20u) == 1u) {
            spdlog::warn(
                "[PostIngestQueue] enqueueBatch waiting on full channel (remaining={}, waits={})",
                busTasks.size() - next, waits);
        }
    }
}

bool PostIngestQueue::tryEnqueue(const Task& t) {
    // KG backpressure: if downstream KG pipeline is saturated, reject upstream work.
    // This prevents unbounded buffering and downstream channel drops.
    if (isKgChannelBackpressured()) {
        const auto n = backpressureRejects_.fetch_add(1, std::memory_order_relaxed) + 1;
        if ((n % 256u) == 1u) {
            const auto st = getBackpressureStatus();
            spdlog::warn(
                "[PostIngestQueue] Backpressure active; rejecting enqueue (kg={}/{} {:.1f}%, "
                "threshold={:.2f}, rejects={})",
                st.kgDepth, st.kgCapacity, st.kgFillRatio * 100.0, st.threshold, n);
        }
        return false;
    }

    static constexpr const char* kChannelName = "post_ingest";
    const std::size_t channelCapacity = resolveChannelCapacity();
    auto channel =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::PostIngestTask>(
            kChannelName, channelCapacity);

    InternalEventBus::PostIngestTask task;
    task.hash = t.hash;
    task.mime = t.mime;

    return channel->try_push(task);
}

bool PostIngestQueue::tryEnqueue(Task&& t) {
    if (isKgChannelBackpressured()) {
        const auto n = backpressureRejects_.fetch_add(1, std::memory_order_relaxed) + 1;
        if ((n % 256u) == 1u) {
            const auto st = getBackpressureStatus();
            spdlog::warn(
                "[PostIngestQueue] Backpressure active; rejecting enqueue (kg={}/{} {:.1f}%, "
                "threshold={:.2f}, rejects={})",
                st.kgDepth, st.kgCapacity, st.kgFillRatio * 100.0, st.threshold, n);
        }
        return false;
    }

    static constexpr const char* kChannelName = "post_ingest";
    const std::size_t channelCapacity = resolveChannelCapacity();
    auto channel =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::PostIngestTask>(
            kChannelName, channelCapacity);

    InternalEventBus::PostIngestTask task;
    task.hash = std::move(t.hash);
    task.mime = std::move(t.mime);

    return channel->try_push(std::move(task));
}

std::size_t PostIngestQueue::size() const {
    static constexpr const char* kChannelName = "post_ingest";
    const std::size_t channelCapacity = resolveChannelCapacity();
    auto channel =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::PostIngestTask>(
            kChannelName, channelCapacity);

    const std::size_t rpcCapacity = static_cast<std::size_t>(TuneAdvisor::postIngestRpcQueueMax());
    auto rpcChannel =
        (rpcCapacity > 0)
            ? InternalEventBus::instance().get_or_create_channel<InternalEventBus::PostIngestTask>(
                  "post_ingest_rpc", rpcCapacity)
            : nullptr;

    const std::size_t normalDepth = channel ? channel->size_approx() : 0;
    const std::size_t rpcDepth = rpcChannel ? rpcChannel->size_approx() : 0;
    return normalDepth + rpcDepth;
}

std::size_t PostIngestQueue::kgQueueDepth() const {
    const std::size_t kgChannelCapacity = boundedStageChannelCapacity(16384);
    auto channel = InternalEventBus::instance().get_or_create_channel<InternalEventBus::KgJob>(
        "kg_jobs", kgChannelCapacity);
    return channel ? channel->size_approx() : 0;
}

std::size_t PostIngestQueue::symbolQueueDepth() const {
    const std::size_t symbolChannelCapacity = boundedStageChannelCapacity(16384);
    auto channel =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::SymbolExtractionJob>(
            "symbol_extraction", symbolChannelCapacity);
    return channel ? channel->size_approx() : 0;
}

std::size_t PostIngestQueue::entityQueueDepth() const {
    const std::size_t entityChannelCapacity = boundedStageChannelCapacity(4096);
    auto channel =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::EntityExtractionJob>(
            "entity_extraction", entityChannelCapacity);
    return channel ? channel->size_approx() : 0;
}

std::size_t PostIngestQueue::titleQueueDepth() const {
    const std::size_t titleChannelCapacity = boundedStageChannelCapacity(4096);
    auto channel =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::TitleExtractionJob>(
            "title_extraction", titleChannelCapacity);
    return channel ? channel->size_approx() : 0;
}

void PostIngestQueue::processTask(const std::string& hash, const std::string& mime) {
    try {
        std::optional<metadata::DocumentInfo> info;
        std::vector<std::string> tags;

        if (meta_) {
            auto infoRes = meta_->batchGetDocumentsByHash(std::vector<std::string>{hash});
            if (infoRes) {
                auto& infoMap = infoRes.value();
                auto it = infoMap.find(hash);
                if (it != infoMap.end() && it->second.id >= 0) {
                    info = it->second;

                    auto tagsRes = meta_->batchGetDocumentTags(std::vector<int64_t>{it->second.id});
                    if (tagsRes) {
                        auto& tagsById = tagsRes.value();
                        auto tagsIt = tagsById.find(it->second.id);
                        if (tagsIt != tagsById.end()) {
                            tags = tagsIt->second;
                        }
                    } else {
                        spdlog::warn("[PostIngestQueue] batchGetDocumentTags failed: {}",
                                     tagsRes.error().message);
                    }
                }
            } else {
                spdlog::warn("[PostIngestQueue] batchGetDocumentsByHash failed: {}",
                             infoRes.error().message);
            }
        }

        // If metadata lookup didn't find a document, still skip per-doc tag query.
        static const std::vector<std::string> kEmptyTags;
        processMetadataStage(hash, mime, info, info ? &tags : &kEmptyTags, {}, {});
        processed_++;
        InternalEventBus::instance().incPostConsumed();
    } catch (const std::exception& e) {
        spdlog::error("[PostIngestQueue] Failed to process {}: {}", hash, e.what());
        failed_++;
    }
}

namespace {

inline bool extensionSupportsEntityProviders(
    const std::vector<std::shared_ptr<yams::daemon::ExternalEntityProviderAdapter>>& providers,
    const std::string& extension) {
    for (const auto& provider : providers) {
        if (provider && provider->supports(extension)) {
            return true;
        }
    }
    return false;
}

} // namespace

void PostIngestQueue::processMetadataStage(
    const std::string& hash, const std::string& mime,
    const std::optional<metadata::DocumentInfo>& infoOpt,
    const std::vector<std::string>* tagsOverride,
    const std::unordered_map<std::string, std::string>& symbolExtensionMap,
    const std::vector<std::shared_ptr<ExternalEntityProviderAdapter>>& entityProviders) {
    if (!store_ || !meta_) {
        spdlog::warn("[PostIngestQueue] store or metadata unavailable; dropping task {}", hash);
        return;
    }

    try {
        auto startTime = std::chrono::steady_clock::now();

        int64_t docId = -1;
        std::string fileName;
        std::string filePath;
        std::string mimeType = mime;
        std::string extension;
        metadata::DocumentInfo info;

        if (infoOpt.has_value()) {
            info = infoOpt.value();
        } else {
            auto infoRes = meta_->getDocumentByHash(hash);
            if (infoRes && infoRes.value().has_value()) {
                info = *infoRes.value();
            } else {
                spdlog::warn(
                    "[PostIngestQueue] Metadata not found for hash {}; content may be orphaned",
                    hash);
                return;
            }
        }
        docId = info.id;
        if (!info.fileName.empty())
            fileName = info.fileName;
        if (!info.filePath.empty())
            filePath = info.filePath;
        if (!info.mimeType.empty())
            mimeType = info.mimeType;
        if (!info.fileExtension.empty())
            extension = info.fileExtension;

        auto txt = extractDocumentText(store_, hash, mimeType, extension, extractors_);
        if (!txt || txt->empty()) {
            spdlog::info("[PostIngestQueue] no text extracted for {} (mime={}, ext={})", hash,
                         mimeType, extension);
            if (docId >= 0) {
                auto updateRes = meta_->updateDocumentExtractionStatus(
                    docId, false, metadata::ExtractionStatus::Failed, "No text extracted");
                if (!updateRes) {
                    spdlog::warn("[PostIngestQueue] Failed to mark extraction failed for {}: {}",
                                 hash, updateRes.error().message);
                }
            }
        } else if (docId >= 0) {
            spdlog::info("[PostIngestQueue] Extracted {} bytes for {} (docId={})", txt->size(),
                         hash, docId);
            auto pr = yams::ingest::persist_content_and_index(*meta_, docId, fileName, *txt,
                                                              mimeType, "post_ingest");
            if (!pr) {
                std::string errorMsg = "Persist failed: " + pr.error().message;
                spdlog::warn("[PostIngestQueue] persist/index failed for {}: {}", hash, errorMsg);
                // Track lock errors for adaptive concurrency scaling
                if (pr.error().message.find("database is locked") != std::string::npos) {
                    TuneAdvisor::reportDbLockError();
                }
                // FIX: Update extraction status to Failed to prevent state inconsistency
                auto updateRes = meta_->updateDocumentExtractionStatus(
                    docId, false, metadata::ExtractionStatus::Failed, errorMsg);
                if (!updateRes) {
                    spdlog::error("[PostIngestQueue] Failed to update extraction status for {}: {}",
                                  hash, updateRes.error().message);
                }
            } else {
                auto duration = std::chrono::steady_clock::now() - startTime;
                double ms = std::chrono::duration<double, std::milli>(duration).count();
                spdlog::info("[PostIngestQueue] Metadata stage completed for {} in {:.2f}ms", hash,
                             ms);
            }
        }

        if (docId >= 0) {
            std::vector<std::string> tags;
            if (tagsOverride) {
                tags = *tagsOverride;
            }
            dispatchToKgChannel(hash, docId, filePath.empty() ? fileName : filePath,
                                std::move(tags), nullptr);

            // Dispatch symbol extraction for code files (if plugin supports this extension)
            {
                // Extension map keys don't have leading dots, but DB stores with dots
                std::string extKey = extension;
                if (!extKey.empty() && extKey[0] == '.') {
                    extKey = extKey.substr(1);
                }
                auto it = symbolExtensionMap.find(extKey);
                if (it != symbolExtensionMap.end()) {
                    dispatchToSymbolChannel(hash, docId, filePath.empty() ? fileName : filePath,
                                            it->second, nullptr);
                }
            }

            // Dispatch entity extraction for binary files (if any entity provider supports this
            // extension)
            if (extensionSupportsEntityProviders(entityProviders, extension)) {
                dispatchToEntityChannel(hash, docId, filePath.empty() ? fileName : filePath,
                                        extension, nullptr);
            }
        }
    } catch (const std::exception& e) {
        spdlog::error("[PostIngestQueue] Metadata stage failed for {}: {}", hash, e.what());
    }
}

std::variant<PostIngestQueue::PreparedMetadataEntry, PostIngestQueue::ExtractionFailure>
PostIngestQueue::prepareMetadataEntry(
    const std::string& hash, const std::string& mime, const metadata::DocumentInfo& info,
    const std::vector<std::string>& tags,
    const std::unordered_map<std::string, std::string>& symbolExtensionMap,
    const std::vector<std::shared_ptr<ExternalEntityProviderAdapter>>& entityProviders) {
    PreparedMetadataEntry prepared;
    prepared.documentId = info.id;
    prepared.hash = hash;
    prepared.fileName = info.fileName;
    prepared.filePath = info.filePath;
    prepared.mimeType = mime.empty() ? info.mimeType : mime;
    prepared.extension = info.fileExtension;
    prepared.tags = tags;

    // Respect per-document opt-out (set by StoreDocumentRequest.noEmbeddings).
    // DocumentService persists this as a real tag key "tag:no_embeddings".
    try {
        for (const auto& t : prepared.tags) {
            if (t == "no_embeddings") {
                prepared.shouldDispatchEmbed = false;
                break;
            }
        }
    } catch (...) {
    }

    // Determine dispatch flags that depend only on document metadata (not extracted text).
    prepared.shouldDispatchKg = (info.id >= 0);

    std::string extKey = prepared.extension;
    if (!extKey.empty() && extKey[0] == '.') {
        extKey = extKey.substr(1);
    }
    auto symIt = symbolExtensionMap.find(extKey);
    if (symIt != symbolExtensionMap.end()) {
        prepared.shouldDispatchSymbol = true;
        prepared.symbolLanguage = symIt->second;
    }

    prepared.shouldDispatchEntity =
        extensionSupportsEntityProviders(entityProviders, prepared.extension);

    // Extract document text
    const bool wantContentBytes = prepared.shouldDispatchSymbol || prepared.shouldDispatchEntity;
    if (wantContentBytes) {
        auto extracted = yams::extraction::util::extractDocumentTextAndBytes(
            store_, hash, prepared.mimeType, prepared.extension, extractors_);
        if (!extracted || extracted->text.empty()) {
            spdlog::debug("[PostIngestQueue] no text extracted for {} (mime={}, ext={})", hash,
                          prepared.mimeType, prepared.extension);
            return ExtractionFailure{info.id, hash, "No text extracted"};
        }
        prepared.extractedText = std::move(extracted->text);
        prepared.contentBytes = std::move(extracted->bytes);
    } else {
        auto txt =
            extractDocumentText(store_, hash, prepared.mimeType, prepared.extension, extractors_);
        if (!txt || txt->empty()) {
            spdlog::debug("[PostIngestQueue] no text extracted for {} (mime={}, ext={})", hash,
                          prepared.mimeType, prepared.extension);
            return ExtractionFailure{info.id, hash, "No text extracted"};
        }
        prepared.extractedText = std::move(*txt);
    }

    // Cap extracted text to 16 MiB to avoid SQLite bind limits
    constexpr size_t kMaxTextToPersistInMetadataBytes = size_t{16} * 1024 * 1024;
    if (prepared.extractedText.size() > kMaxTextToPersistInMetadataBytes) {
        spdlog::warn("[PostIngestQueue] Truncating extracted text for {} from {} to {} bytes", hash,
                     prepared.extractedText.size(), kMaxTextToPersistInMetadataBytes);
        prepared.extractedText.resize(kMaxTextToPersistInMetadataBytes);
    }

    // Detect language
    double langConfidence = 0.0;
    prepared.language =
        yams::extraction::LanguageDetector::detectLanguage(prepared.extractedText, &langConfidence);

    prepared.title = deriveTitle(prepared.extractedText, prepared.fileName, prepared.mimeType,
                                 prepared.extension);

    // Title+NL extraction: single GLiNER call for both title and NL entities
    if (titleExtractor_ && !isGlinerTitleExtractionDisabled()) {
        prepared.shouldDispatchTitle = true;
        // Store snippet for GLiNER inference
        prepared.titleTextSnippet = prepared.extractedText.size() > kMaxGlinerChars
                                        ? prepared.extractedText.substr(0, kMaxGlinerChars)
                                        : prepared.extractedText;
    }

    return prepared;
}

std::string PostIngestQueue::deriveTitle(const std::string& text, const std::string& fileName,
                                         const std::string& mimeType,
                                         const std::string& extension) const {
    if (text.empty()) {
        return fileName;
    }

    // === FAST PATH: Try cheap heuristics first (no ML inference) ===

    // HTML: extract <title> tag
    const bool isHtml = extension == ".html" || extension == ".htm" || mimeType == "text/html";
    if (isHtml) {
        auto title = extractHtmlTitle(text);
        if (!title.empty()) {
            return title;
        }
    }

    // Markdown: extract first heading
    const bool isMarkdown =
        extension == ".md" || extension == ".markdown" || mimeType == "text/markdown";
    if (isMarkdown) {
        auto title = extractMarkdownHeading(text);
        if (!title.empty()) {
            return title;
        }
    }

    // Code: extract class/function/module signature
    auto codeTitle = extractCodeSignature(text);
    if (!codeTitle.empty()) {
        return codeTitle;
    }

    // NOTE: GLiNER inference moved to async title extraction pipeline (titlePoller)
    // to avoid blocking the main extraction pipeline and competing with embeddings
    // for ONNX resources. Title jobs are dispatched after successful extraction
    // and processed asynchronously.

    // === FALLBACK: First meaningful line ===
    auto lineTitle = extractFirstMeaningfulLine(text);
    if (!lineTitle.empty()) {
        return lineTitle;
    }

    return fileName;
}

void PostIngestQueue::processKnowledgeGraphBatch(std::vector<InternalEventBus::KgJob>&& jobs) {
    if (jobs.empty()) {
        return;
    }

    // Count all jobs as consumed for metrics
    for (const auto& job : jobs) {
        (void)job;
        InternalEventBus::instance().incKgConsumed();
    }

    if (!graphComponent_) {
        if (!jobs.empty()) {
            spdlog::warn("[PostIngestQueue] KG batch skipped ({} jobs) - no graphComponent",
                         jobs.size());
        }
        return;
    }

    auto startTime = std::chrono::steady_clock::now();
    spdlog::debug("[PostIngestQueue] KG batch starting for {} documents", jobs.size());

    // Build DocumentGraphContext objects for batch processing
    std::vector<GraphComponent::DocumentGraphContext> contexts;
    contexts.reserve(jobs.size());

    for (auto& job : jobs) {
        GraphComponent::DocumentGraphContext ctx{.documentHash = std::move(job.hash),
                                                 .filePath = std::move(job.filePath),
                                                 .snapshotId = std::nullopt,
                                                 .rootTreeHash = std::nullopt,
                                                 .tags = std::move(job.tags),
                                                 .documentDbId = job.documentId,
                                                 .contentBytes = std::move(job.contentBytes),
                                                 .skipEntityExtraction = true};
        contexts.push_back(std::move(ctx));
    }

    // Submit batch to GraphComponent for parallel processing
    auto result = graphComponent_->onDocumentsIngestedBatch(contexts);

    auto duration = std::chrono::steady_clock::now() - startTime;
    double ms = std::chrono::duration<double, std::milli>(duration).count();

    if (!result) {
        spdlog::error("[PostIngestQueue] KG batch failed: {}", result.error().message);
    } else {
        spdlog::debug("[PostIngestQueue] KG batch completed {} docs in {:.2f}ms (avg {:.2f}ms/doc)",
                      jobs.size(), ms, ms / jobs.size());
    }
}

void PostIngestQueue::dispatchToKgChannel(const std::string& hash, int64_t docId,
                                          const std::string& filePath,
                                          std::vector<std::string> tags,
                                          std::shared_ptr<std::vector<std::byte>> contentBytes) {
    // Do not let a disabled/saturated KG stage throttle extraction throughput.
    // KG is an optional downstream enrichment path; metadata extraction/indexing should continue.
    const bool kgStageActive = (graphComponent_ != nullptr) &&
                               !stagePaused_[1].load(std::memory_order_acquire) &&
                               (maxKgConcurrent() > 0);
    if (!kgStageActive) {
        InternalEventBus::instance().incKgDropped();
        return;
    }

    const std::size_t kgChannelCapacity = boundedStageChannelCapacity(16384);
    auto channel = InternalEventBus::instance().get_or_create_channel<InternalEventBus::KgJob>(
        "kg_jobs", kgChannelCapacity);

    InternalEventBus::KgJob job;
    job.hash = hash;
    job.documentId = docId;
    job.filePath = filePath;
    job.tags = std::move(tags);
    job.contentBytes = std::move(contentBytes);

    // Prefer non-blocking fast-path to avoid stalling extraction under load.
    if (channel->try_push(std::move(job))) {
        InternalEventBus::instance().incKgQueued();
        return;
    }

    // If the channel is briefly full during bulk ingest, wait only a short time.
    static constexpr auto kEnqueueTimeout = std::chrono::milliseconds(10);
    if (!channel->push_wait(std::move(job), kEnqueueTimeout)) {
        const auto n = InternalEventBus::instance().kgDropped();
        if (((n + 1u) % 64u) == 1u) {
            spdlog::warn(
                "[PostIngestQueue] KG channel full (depth={}/{}), dropping job for {} (drops={})",
                channel->size_approx(), channel->capacity(), hash.substr(0, 12), n + 1u);
        }
        InternalEventBus::instance().incKgDropped();
    } else {
        InternalEventBus::instance().incKgQueued();
    }
}

boost::asio::awaitable<void> PostIngestQueue::kgPoller() {
    const std::size_t kgChannelCapacity = boundedStageChannelCapacity(16384);
    auto channel = InternalEventBus::instance().get_or_create_channel<InternalEventBus::KgJob>(
        "kg_jobs", kgChannelCapacity);

    PressureLimitedPollerConfig<InternalEventBus::KgJob> cfg;
    cfg.stageName = "KG";
    cfg.stopFlag = &stop_;
    cfg.startedFlag = &stageStarted_[1];
    cfg.pauseFlag = &stagePaused_[1];
    cfg.wasActiveFlag = &wasActive_;
    cfg.inFlightCounter = &stageInFlight_[1];
    cfg.getLimiterFn = [this]() { return limiters_[1].get(); };
    cfg.maxConcurrentFn = &maxKgConcurrent;
    cfg.tryAcquireFn = [this](GradientLimiter* l, const std::string& id, const std::string& s) {
        return tryAcquireLimiterSlot(l, id, s);
    };
    cfg.completeJobFn = [this](const std::string& id, bool ok) { completeJob(id, ok); };
    cfg.checkDrainFn = [this]() { checkDrainAndSignal(); };
    cfg.executor = coordinator_->getExecutor();
    cfg.getHashFn = [](const InternalEventBus::KgJob& j) -> std::string { return j.hash; };

    cfg.batchMode = true;
    cfg.batchSizeFn = [this]() -> std::size_t {
        const std::size_t tuned = std::max<std::size_t>(1u, TuneAdvisor::postIngestBatchSize());
        return adaptiveStageBatchSize(kgQueueDepth(), tuned, 32u);
    };
    cfg.batchProcessFn = [this](std::vector<InternalEventBus::KgJob>&& jobs) {
        processKnowledgeGraphBatch(std::move(jobs));
    };

    co_await pressureLimitedPoll(channel, std::move(cfg));
}

boost::asio::awaitable<void> PostIngestQueue::symbolPoller() {
    const std::size_t symbolChannelCapacity = boundedStageChannelCapacity(16384);
    auto channel =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::SymbolExtractionJob>(
            "symbol_extraction", symbolChannelCapacity);

    PressureLimitedPollerConfig<InternalEventBus::SymbolExtractionJob> cfg;
    cfg.stageName = "symbol";
    cfg.stopFlag = &stop_;
    cfg.startedFlag = &stageStarted_[2];
    cfg.pauseFlag = &stagePaused_[2];
    cfg.wasActiveFlag = &wasActive_;
    cfg.inFlightCounter = &stageInFlight_[2];
    cfg.getLimiterFn = [this]() { return limiters_[2].get(); };
    cfg.maxConcurrentFn = &maxSymbolConcurrent;
    cfg.tryAcquireFn = [this](GradientLimiter* l, const std::string& id, const std::string& s) {
        return tryAcquireLimiterSlot(l, id, s);
    };
    cfg.completeJobFn = [this](const std::string& id, bool ok) { completeJob(id, ok); };
    cfg.checkDrainFn = [this]() { checkDrainAndSignal(); };
    cfg.executor = coordinator_->getExecutor();
    cfg.getHashFn = [](const InternalEventBus::SymbolExtractionJob& j) -> std::string {
        return j.hash;
    };

    cfg.batchMode = true;
    cfg.batchSizeFn = [this]() -> std::size_t {
        const std::size_t tuned = std::max<std::size_t>(1u, TuneAdvisor::postIngestBatchSize());
        return adaptiveStageBatchSize(symbolQueueDepth(), tuned, 16u);
    };
    cfg.batchProcessFn = [this](std::vector<InternalEventBus::SymbolExtractionJob>&& jobs) {
        processSymbolExtractionBatch(std::move(jobs));
    };

    co_await pressureLimitedPoll(channel, std::move(cfg));
}

void PostIngestQueue::processSymbolExtractionBatch(
    std::vector<InternalEventBus::SymbolExtractionJob>&& jobs) {
    // Metrics are per consumed job even if we dedupe processing.
    for (const auto& j : jobs) {
        (void)j;
        InternalEventBus::instance().incSymbolConsumed();
    }

    if (!graphComponent_) {
        // Preserve prior behavior: treat as a no-op rather than retrying.
        if (!jobs.empty()) {
            spdlog::warn("[PostIngestQueue] Symbol extraction batch skipped ({} jobs) - no "
                         "graphComponent",
                         jobs.size());
        }
        return;
    }

    // Dedupe by hash so we load content at most once per doc per batch.
    std::unordered_set<std::string> seen;
    seen.reserve(jobs.size());

    for (auto& j : jobs) {
        const std::string& hash = j.hash;
        if (hash.empty()) {
            continue;
        }
        if (!seen.insert(hash).second) {
            continue;
        }
        try {
            // Skip already-extracted docs before loading content.
            if (GraphComponent::shouldSkipEntityExtraction(kg_, hash)) {
                spdlog::debug("[PostIngestQueue] Skip symbol extraction for {} (already extracted)",
                              hash.substr(0, 12));
                continue;
            }

            std::vector<std::byte> bytes;
            if (j.contentBytes) {
                bytes = *j.contentBytes;
            } else if (store_) {
                auto contentResult = store_->retrieveBytes(hash);
                if (contentResult) {
                    bytes = std::move(contentResult.value());
                } else {
                    spdlog::warn(
                        "[PostIngestQueue] Failed to load content for symbol extraction: {}",
                        hash.substr(0, 12));
                    continue;
                }
            } else {
                spdlog::warn("[PostIngestQueue] No content store for symbol extraction");
                continue;
            }

            GraphComponent::EntityExtractionJob extractJob;
            extractJob.documentHash = hash;
            extractJob.filePath = j.filePath;
            extractJob.language = j.language;
            extractJob.contentUtf8 =
                std::string(reinterpret_cast<const char*>(bytes.data()), bytes.size());

            auto result = graphComponent_->submitEntityExtraction(std::move(extractJob));
            if (!result) {
                spdlog::warn("[PostIngestQueue] Symbol extraction failed for {}: {}", hash,
                             result.error().message);
            }
        } catch (const std::exception& e) {
            spdlog::error("[PostIngestQueue] Symbol extraction failed for {}: {}", hash, e.what());
        }
    }
}

void PostIngestQueue::dispatchToSymbolChannel(
    const std::string& hash, int64_t docId, const std::string& filePath,
    const std::string& language, std::shared_ptr<std::vector<std::byte>> contentBytes) {
    const std::size_t symbolChannelCapacity = boundedStageChannelCapacity(16384);
    auto channel =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::SymbolExtractionJob>(
            "symbol_extraction", symbolChannelCapacity);

    InternalEventBus::SymbolExtractionJob job;
    job.hash = hash;
    job.documentId = docId;
    job.filePath = filePath;
    job.language = language;
    job.contentBytes = std::move(contentBytes);

    if (!channel->try_push(std::move(job))) {
        spdlog::warn("[PostIngestQueue] Symbol channel full, dropping job for {}", hash);
        InternalEventBus::instance().incSymbolDropped();
    } else {
        spdlog::info("[PostIngestQueue] Dispatched symbol extraction job for {} ({}) lang={}",
                     filePath, hash.substr(0, 12), language);
        InternalEventBus::instance().incSymbolQueued();
    }
}

void PostIngestQueue::processSymbolExtractionStage(
    const std::string& hash, [[maybe_unused]] int64_t docId, const std::string& filePath,
    const std::string& language, std::shared_ptr<std::vector<std::byte>> contentBytes) {
    // Legacy single-item handler (kept for now); metrics are owned by the poller layer.
    if (!graphComponent_) {
        spdlog::warn("[PostIngestQueue] Symbol extraction skipped for {} - no graphComponent",
                     hash);
        return;
    }

    spdlog::info("[PostIngestQueue] Symbol extraction starting for {} ({}) lang={}", filePath,
                 hash.substr(0, 12), language);

    try {
        auto startTime = std::chrono::steady_clock::now();

        // Use GraphComponent to submit the extraction job
        GraphComponent::EntityExtractionJob extractJob;
        extractJob.documentHash = hash;
        extractJob.filePath = filePath;
        extractJob.language = language;

        std::vector<std::byte> bytes;
        if (contentBytes) {
            bytes = *contentBytes;
        } else if (store_) {
            auto contentResult = store_->retrieveBytes(hash);
            if (contentResult) {
                bytes = std::move(contentResult.value());
            } else {
                spdlog::warn("[PostIngestQueue] Failed to load content for symbol extraction: {}",
                             hash.substr(0, 12));
                return;
            }
        } else {
            spdlog::warn("[PostIngestQueue] No content store for symbol extraction");
            return;
        }
        extractJob.contentUtf8 =
            std::string(reinterpret_cast<const char*>(bytes.data()), bytes.size());

        auto result = graphComponent_->submitEntityExtraction(std::move(extractJob));
        if (!result) {
            spdlog::warn("[PostIngestQueue] Symbol extraction failed for {}: {}", hash,
                         result.error().message);
        } else {
            auto duration = std::chrono::steady_clock::now() - startTime;
            double ms = std::chrono::duration<double, std::milli>(duration).count();
            spdlog::debug("[PostIngestQueue] Symbol extraction submitted for {} in {:.2f}ms", hash,
                          ms);
        }
    } catch (const std::exception& e) {
        spdlog::error("[PostIngestQueue] Symbol extraction failed for {}: {}", hash, e.what());
    }
}

void PostIngestQueue::dispatchToEntityChannel(
    const std::string& hash, int64_t docId, const std::string& filePath,
    const std::string& extension, std::shared_ptr<std::vector<std::byte>> contentBytes) {
    const std::size_t entityChannelCapacity = boundedStageChannelCapacity(4096);
    auto channel =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::EntityExtractionJob>(
            "entity_extraction", entityChannelCapacity);

    InternalEventBus::EntityExtractionJob job;
    job.hash = hash;
    job.documentId = docId;
    job.filePath = filePath;
    job.extension = extension;
    job.contentBytes = std::move(contentBytes);

    if (!channel->try_push(std::move(job))) {
        spdlog::warn("[PostIngestQueue] Entity channel full, dropping job for {}", hash);
        InternalEventBus::instance().incEntityDropped();
    } else {
        spdlog::info("[PostIngestQueue] Dispatched entity extraction job for {} ({}) ext={}",
                     filePath, hash.substr(0, 12), extension);
        InternalEventBus::instance().incEntityQueued();
    }
}

boost::asio::awaitable<void> PostIngestQueue::entityPoller() {
    const std::size_t entityChannelCapacity = boundedStageChannelCapacity(4096);
    auto channel =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::EntityExtractionJob>(
            "entity_extraction", entityChannelCapacity);

    PressureLimitedPollerConfig<InternalEventBus::EntityExtractionJob> cfg;
    cfg.stageName = "entity";
    cfg.stopFlag = &stop_;
    cfg.startedFlag = &stageStarted_[3];
    cfg.pauseFlag = &stagePaused_[3];
    cfg.wasActiveFlag = &wasActive_;
    cfg.inFlightCounter = &stageInFlight_[3];
    cfg.getLimiterFn = [this]() { return limiters_[3].get(); };
    cfg.maxConcurrentFn = &maxEntityConcurrent;
    cfg.tryAcquireFn = [this](GradientLimiter* l, const std::string& id, const std::string& s) {
        return tryAcquireLimiterSlot(l, id, s);
    };
    cfg.completeJobFn = [this](const std::string& id, bool ok) { completeJob(id, ok); };
    cfg.checkDrainFn = [this]() { checkDrainAndSignal(); };
    cfg.executor =
        entityCoordinator_ ? entityCoordinator_->getExecutor() : coordinator_->getExecutor();
    cfg.getHashFn = [](const InternalEventBus::EntityExtractionJob& j) -> std::string {
        return j.hash;
    };
    cfg.batchMode = true;
    cfg.batchSizeFn = [this]() -> std::size_t {
        const std::size_t tuned = std::max<std::size_t>(1u, TuneAdvisor::postIngestBatchSize());
        return adaptiveStageBatchSize(entityQueueDepth(), tuned, 16u);
    };
    cfg.batchProcessFn = [this](std::vector<InternalEventBus::EntityExtractionJob>&& jobs) {
        processEntityExtractionBatch(std::move(jobs));
    };

    co_await pressureLimitedPoll(channel, std::move(cfg));
}

void PostIngestQueue::processEntityExtractionBatch(
    std::vector<InternalEventBus::EntityExtractionJob>&& jobs) {
    if (jobs.empty()) {
        return;
    }
    // Entity extraction is expensive; fan out within the batch to use available cores.
    std::vector<std::future<void>> futures;
    futures.reserve(jobs.size());
    for (auto& job : jobs) {
        futures.push_back(std::async(std::launch::async, [this, job = std::move(job)]() mutable {
            processEntityExtractionStage(job.hash, job.documentId, job.filePath, job.extension,
                                         std::move(job.contentBytes));
        }));
    }
    for (auto& f : futures) {
        f.get();
    }
}

void PostIngestQueue::processEntityExtractionStage(
    const std::string& hash, int64_t /*docId*/, const std::string& filePath,
    const std::string& extension, std::shared_ptr<std::vector<std::byte>> contentBytes) {
    spdlog::info("[PostIngestQueue] Entity extraction starting for {} ({}) ext={}", filePath,
                 hash.substr(0, 12), extension);

    try {
        auto startTime = std::chrono::steady_clock::now();

        // Find the entity provider that supports this extension
        std::shared_ptr<ExternalEntityProviderAdapter> provider;
        {
            std::lock_guard<std::mutex> lock(entityMutex_);
            for (const auto& p : entityProviders_) {
                if (p && p->supports(extension)) {
                    provider = p;
                    break;
                }
            }
        }

        if (!provider) {
            spdlog::warn("[PostIngestQueue] No entity provider for extension {}", extension);
            return;
        }

        // Load content from store
        std::vector<std::byte> content;
        if (contentBytes) {
            content = *contentBytes;
        } else if (store_) {
            auto contentResult = store_->retrieveBytes(hash);
            if (contentResult) {
                content = std::move(contentResult.value());
            } else {
                spdlog::warn("[PostIngestQueue] Failed to load content for entity extraction: {}",
                             hash.substr(0, 12));
                return;
            }
        } else {
            spdlog::warn("[PostIngestQueue] No content store for entity extraction");
            return;
        }

        if (!kg_) {
            spdlog::warn("[PostIngestQueue] No KG store for entity extraction");
            return;
        }

        // Track cumulative nodeKey -> nodeId mappings across batches
        // This allows edges to reference nodes from previous batches
        std::unordered_map<std::string, std::int64_t> canonicalKeyToId;
        std::unordered_map<std::string, std::int64_t> versionKeyToId;
        size_t totalNodesInserted = 0;
        size_t totalEdgesInserted = 0;
        size_t totalAliasesInserted = 0;
        const std::string snapshotId = hash;

        // NOTE: Entity embeddings (entity_vectors table) are intentionally NOT generated here.
        // The KG nodes/edges/aliases provide precise structural navigation (call graphs,
        // inheritance, containment). Embeddings would add noise for code navigation where
        // exact matches and graph traversal are preferred. The entity_vectors schema exists
        // for future semantic search use cases (e.g., "find similar functions").

        // Use streaming extraction with per-batch KG insertion
        auto result = provider->extractEntitiesStreaming(
            content, filePath,
            [this, &canonicalKeyToId, &versionKeyToId, &totalNodesInserted, &totalEdgesInserted,
             &totalAliasesInserted, &hash, &snapshotId,
             &filePath](ExternalEntityProviderAdapter::EntityResult batch,
                        const ExternalEntityProviderAdapter::ExtractionProgress& progress) -> bool {
                if (batch.nodes.empty()) {
                    return true; // Continue to next batch
                }

                // Wrap all KG operations in a single WriteBatch transaction
                auto batchRes = kg_->beginWriteBatch();
                if (!batchRes) {
                    spdlog::warn("[PostIngestQueue] Failed to begin WriteBatch: {}",
                                 batchRes.error().message);
                    return true; // Continue despite error
                }
                auto& kgBatch = batchRes.value();

                const bool hasSnapshot = !snapshotId.empty();
                std::vector<metadata::KGNode> canonicalNodes;
                std::vector<metadata::KGNode> versionNodes;
                canonicalNodes.reserve(batch.nodes.size());
                versionNodes.reserve(batch.nodes.size());

                for (const auto& node : batch.nodes) {
                    canonicalNodes.push_back(node);

                    if (hasSnapshot) {
                        metadata::KGNode versionNode = node;
                        std::string baseKey = node.nodeKey;
                        versionNode.nodeKey = baseKey + "@snap:" + snapshotId;
                        std::string baseType = node.type.has_value() ? node.type.value() : "entity";
                        versionNode.type = baseType + "_version";

                        nlohmann::json props = nlohmann::json::object();
                        if (node.properties.has_value()) {
                            try {
                                props = nlohmann::json::parse(node.properties.value());
                            } catch (...) {
                                props = nlohmann::json::object();
                            }
                        }
                        props["snapshot_id"] = snapshotId;
                        props["document_hash"] = snapshotId;
                        props["file_path"] = filePath;
                        props["canonical_key"] = baseKey;
                        versionNode.properties = props.dump();
                        versionNodes.push_back(std::move(versionNode));
                    }
                }

                // Insert canonical nodes and get their IDs
                auto canonicalIds = kgBatch->upsertNodes(canonicalNodes);
                if (!canonicalIds) {
                    spdlog::warn("[PostIngestQueue] Failed to insert batch {} nodes: {}",
                                 progress.batchNumber, canonicalIds.error().message);
                    return true; // Continue despite error - partial success
                }

                // Update key maps with this batch's nodes
                for (size_t i = 0; i < canonicalNodes.size() && i < canonicalIds.value().size();
                     ++i) {
                    canonicalKeyToId[canonicalNodes[i].nodeKey] = canonicalIds.value()[i];
                }
                if (hasSnapshot) {
                    // Insert version nodes and get their IDs
                    auto versionIds = kgBatch->upsertNodes(versionNodes);
                    if (!versionIds) {
                        spdlog::warn(
                            "[PostIngestQueue] Failed to insert batch {} version nodes: {}",
                            progress.batchNumber, versionIds.error().message);
                        return true; // Continue despite error - partial success
                    }
                    for (size_t i = 0; i < versionNodes.size() && i < versionIds.value().size();
                         ++i) {
                        versionKeyToId[canonicalNodes[i].nodeKey] = versionIds.value()[i];
                    }
                    totalNodesInserted += versionIds.value().size();

                    // Link canonical to version nodes
                    std::vector<metadata::KGEdge> observedEdges;
                    observedEdges.reserve(versionNodes.size());
                    for (size_t i = 0;
                         i < canonicalNodes.size() && i < canonicalIds.value().size() &&
                         i < versionIds.value().size();
                         ++i) {
                        metadata::KGEdge edge;
                        edge.srcNodeId = canonicalIds.value()[i];
                        edge.dstNodeId = versionIds.value()[i];
                        edge.relation = "observed_as";
                        edge.weight = 1.0f;
                        nlohmann::json props;
                        props["snapshot_id"] = snapshotId;
                        props["document_hash"] = snapshotId;
                        edge.properties = props.dump();
                        observedEdges.push_back(std::move(edge));
                    }
                    if (!observedEdges.empty()) {
                        kgBatch->addEdgesUnique(observedEdges);
                    }
                } else {
                    for (size_t i = 0; i < canonicalNodes.size() && i < canonicalIds.value().size();
                         ++i) {
                        versionKeyToId[canonicalNodes[i].nodeKey] = canonicalIds.value()[i];
                    }
                    totalNodesInserted += canonicalIds.value().size();
                }

                // Resolve and insert edges
                std::vector<metadata::KGEdge> resolvedEdges;
                for (auto& edge : batch.edges) {
                    try {
                        if (!edge.properties)
                            continue;
                        auto props = nlohmann::json::parse(*edge.properties);
                        std::string srcKey = props.value("_src_key", "");
                        std::string dstKey = props.value("_dst_key", "");

                        auto srcIt = versionKeyToId.find(srcKey);
                        auto dstIt = versionKeyToId.find(dstKey);

                        if (srcIt != versionKeyToId.end() && dstIt != versionKeyToId.end()) {
                            edge.srcNodeId = srcIt->second;
                            edge.dstNodeId = dstIt->second;
                            props.erase("_src_key");
                            props.erase("_dst_key");
                            edge.properties = props.dump();
                            resolvedEdges.push_back(std::move(edge));
                        }
                    } catch (...) {
                        // Skip edges we can't parse
                    }
                }

                if (!resolvedEdges.empty()) {
                    kgBatch->addEdgesUnique(resolvedEdges);
                    totalEdgesInserted += resolvedEdges.size();
                }

                // Resolve and insert aliases
                std::vector<metadata::KGAlias> resolvedAliases;
                for (auto& alias : batch.aliases) {
                    if (alias.source && alias.source->starts_with("_node_key:")) {
                        std::string nodeKey = alias.source->substr(10);
                        auto it = canonicalKeyToId.find(nodeKey);
                        if (it != canonicalKeyToId.end()) {
                            alias.nodeId = it->second;
                            alias.source = "ghidra";
                            resolvedAliases.push_back(std::move(alias));
                        }
                    }
                }

                if (!resolvedAliases.empty()) {
                    kgBatch->addAliases(resolvedAliases);
                    totalAliasesInserted += resolvedAliases.size();
                }

                // Commit the WriteBatch transaction
                auto commitRes = kgBatch->commit();
                if (!commitRes) {
                    spdlog::warn("[PostIngestQueue] Failed to commit WriteBatch: {}",
                                 commitRes.error().message);
                }

                const size_t batchNodesInserted =
                    hasSnapshot ? versionNodes.size() : canonicalNodes.size();
                spdlog::info("[PostIngestQueue] Batch {}/{} ingested for {} "
                             "(nodes={}, edges={}, aliases={}, elapsed={:.1f}s)",
                             progress.batchNumber, progress.totalBatchesEstimate,
                             hash.substr(0, 12), batchNodesInserted, resolvedEdges.size(),
                             resolvedAliases.size(), progress.elapsedSeconds);

                return true; // Continue to next batch
            });

        auto duration = std::chrono::steady_clock::now() - startTime;
        double ms = std::chrono::duration<double, std::milli>(duration).count();

        if (result) {
            spdlog::info("[PostIngestQueue] Entity extraction completed for {} in {:.2f}ms "
                         "(batches={}, nodes={}, edges={}, aliases={})",
                         hash.substr(0, 12), ms, result.value().batchNumber, totalNodesInserted,
                         totalEdgesInserted, totalAliasesInserted);
        } else {
            // Partial success - some batches may have been ingested
            if (totalNodesInserted > 0) {
                spdlog::warn("[PostIngestQueue] Entity extraction partial success for {} "
                             "(nodes={}, edges={}, aliases={}, error={})",
                             hash.substr(0, 12), totalNodesInserted, totalEdgesInserted,
                             totalAliasesInserted, result.error().message);
            } else {
                spdlog::warn("[PostIngestQueue] Entity extraction failed for {}: {}",
                             hash.substr(0, 12), result.error().message);
            }
        }

        InternalEventBus::instance().incEntityConsumed();
    } catch (const std::exception& e) {
        spdlog::error("[PostIngestQueue] Entity extraction failed for {}: {}", hash, e.what());
    }
}

void PostIngestQueue::dispatchToTitleChannel(const std::string& hash, int64_t docId,
                                             const std::string& textSnippet,
                                             const std::string& fallbackTitle,
                                             const std::string& filePath,
                                             const std::string& language,
                                             const std::string& mimeType) {
    const std::size_t titleChannelCapacity = boundedStageChannelCapacity(4096);
    auto channel =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::TitleExtractionJob>(
            "title_extraction", titleChannelCapacity);

    InternalEventBus::TitleExtractionJob job;
    job.hash = hash;
    job.documentId = docId;
    job.textSnippet = textSnippet;
    job.fallbackTitle = fallbackTitle;
    job.filePath = filePath;
    job.language = language;
    job.mimeType = mimeType;

    if (!channel->try_push(std::move(job))) {
        spdlog::debug("[PostIngestQueue] Title channel full, skipping async title for {}",
                      hash.substr(0, 12));
        InternalEventBus::instance().incTitleDropped();
    } else {
        spdlog::debug("[PostIngestQueue] Dispatched title+NL extraction job for {}",
                      hash.substr(0, 12));
        InternalEventBus::instance().incTitleQueued();
    }
}

boost::asio::awaitable<void> PostIngestQueue::titlePoller() {
    const std::size_t titleChannelCapacity = boundedStageChannelCapacity(4096);
    auto channel =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::TitleExtractionJob>(
            "title_extraction", titleChannelCapacity);

    PressureLimitedPollerConfig<InternalEventBus::TitleExtractionJob> cfg;
    cfg.stageName = "title";
    cfg.stopFlag = &stop_;
    cfg.startedFlag = &stageStarted_[4];
    cfg.pauseFlag = &stagePaused_[4];
    cfg.wasActiveFlag = &wasActive_;
    cfg.inFlightCounter = &stageInFlight_[4];
    cfg.getLimiterFn = [this]() { return limiters_[4].get(); };
    cfg.maxConcurrentFn = &maxTitleConcurrent;
    cfg.tryAcquireFn = [this](GradientLimiter* l, const std::string& id, const std::string& s) {
        return tryAcquireLimiterSlot(l, id, s);
    };
    cfg.completeJobFn = [this](const std::string& id, bool ok) { completeJob(id, ok); };
    cfg.checkDrainFn = [this]() { checkDrainAndSignal(); };
    cfg.executor = coordinator_->getExecutor();
    cfg.getHashFn = [](const InternalEventBus::TitleExtractionJob& j) -> std::string {
        return j.hash;
    };
    cfg.isCapableFn = [this]() -> bool { return titleExtractor_ != nullptr; };
    cfg.batchMode = true;
    cfg.batchSizeFn = [this]() -> std::size_t {
        const std::size_t tuned = std::max<std::size_t>(1u, TuneAdvisor::postIngestBatchSize());
        return adaptiveStageBatchSize(titleQueueDepth(), tuned, 32u);
    };
    cfg.batchProcessFn = [this](std::vector<InternalEventBus::TitleExtractionJob>&& jobs) {
        processTitleExtractionBatch(std::move(jobs));
    };

    co_await pressureLimitedPoll(channel, std::move(cfg));
}

void PostIngestQueue::processTitleExtractionBatch(
    std::vector<InternalEventBus::TitleExtractionJob>&& jobs) {
    if (jobs.empty()) {
        return;
    }
    std::vector<std::future<void>> futures;
    futures.reserve(jobs.size());
    for (auto& job : jobs) {
        futures.push_back(std::async(std::launch::async, [this, job = std::move(job)]() mutable {
            processTitleExtractionStage(job.hash, job.documentId, job.textSnippet,
                                        job.fallbackTitle, job.filePath, job.language,
                                        job.mimeType);
        }));
    }
    for (auto& f : futures) {
        f.get();
    }
}

void PostIngestQueue::processTitleExtractionStage(const std::string& hash, int64_t docId,
                                                  const std::string& textSnippet,
                                                  const std::string& fallbackTitle,
                                                  const std::string& filePath,
                                                  const std::string& language,
                                                  const std::string& /*mimeType*/) {
    if (!titleExtractor_) {
        spdlog::debug("[PostIngestQueue] Title+NL extraction skipped for {} - no titleExtractor",
                      hash);
        return;
    }

    spdlog::debug("[PostIngestQueue] Title+NL extraction starting for {} (docId={})",
                  hash.substr(0, 12), docId);

    try {
        auto startTime = std::chrono::steady_clock::now();

        // Combined entity types: title-related + NL entities (merged GLiNER call)
        // Title types for document title extraction
        // NL types for knowledge graph population
        static const std::vector<std::string> kCombinedEntityTypes = {
            // Title-related types
            "title", "heading", "function", "class", "method", "module", "file", "symbol",
            // NL entity types (from Glint plugin defaults)
            "person", "organization", "location", "date", "event", "product", "technology",
            "concept"};

        // Title type set for filtering
        static const std::unordered_set<std::string> kTitleTypes = {
            "title", "heading", "function", "class", "method", "module", "file", "symbol"};

        auto result = titleExtractor_(textSnippet, kCombinedEntityTypes);
        if (!result || !result.value().usedGliner || result.value().concepts.empty()) {
            spdlog::debug("[PostIngestQueue] GLiNER returned no concepts for {}",
                          hash.substr(0, 12));
            InternalEventBus::instance().incTitleConsumed();
            return;
        }

        // Separate title entities from NL entities
        const search::QueryConcept* bestTitle = nullptr;
        std::vector<const search::QueryConcept*> nlEntities;

        for (const auto& qc : result.value().concepts) {
            if (qc.confidence < kMinTitleConfidence || qc.text.empty()) {
                continue;
            }

            if (kTitleTypes.count(qc.type) > 0) {
                // Title candidate
                if (!bestTitle || qc.confidence > bestTitle->confidence) {
                    bestTitle = &qc;
                }
            } else {
                // NL entity for KG
                nlEntities.push_back(&qc);
            }
        }

        // Update title if we found a good candidate
        if (bestTitle) {
            auto newTitle = normalizeTitleCandidate(bestTitle->text);
            if (!newTitle.empty() && newTitle != fallbackTitle) {
                if (meta_ && docId >= 0) {
                    auto updateRes =
                        meta_->setMetadata(docId, "title", metadata::MetadataValue(newTitle));
                    if (!updateRes) {
                        spdlog::warn("[PostIngestQueue] Failed to update title for {}: {}",
                                     hash.substr(0, 12), updateRes.error().message);
                    } else {
                        spdlog::debug("[PostIngestQueue] Title updated for {}: \"{}\"",
                                      hash.substr(0, 12), newTitle.substr(0, 50));
                    }
                }
            }
        }

        // Populate KG with NL entities if we have any and KGWriteQueue is available
        if (!nlEntities.empty() && kgWriteQueue_ && kg_) {
            auto batch = std::make_unique<DeferredKGBatch>();
            batch->sourceFile = filePath;

            auto now = std::chrono::system_clock::now().time_since_epoch().count();

            // Get document database ID for doc entities
            std::optional<std::int64_t> documentDbId;
            if (!hash.empty() && docId >= 0) {
                documentDbId = docId;
                batch->documentIdToDelete = documentDbId; // Delete old doc entities
            }

            // Build document context node
            std::string docNodeKey;
            if (!hash.empty()) {
                docNodeKey = "doc:" + hash;

                metadata::KGNode docNode;
                docNode.nodeKey = docNodeKey;
                docNode.label = common::sanitizeUtf8(filePath);
                docNode.type = "document";
                nlohmann::json docProps;
                docProps["hash"] = hash;
                docProps["path"] = common::sanitizeUtf8(filePath);
                docProps["language"] = common::sanitizeUtf8(language);
                docNode.properties = docProps.dump();
                batch->nodes.push_back(std::move(docNode));
            }

            // Build file context node
            std::string fileNodeKey;
            if (!filePath.empty()) {
                fileNodeKey = "file:" + filePath;

                metadata::KGNode fileNode;
                fileNode.nodeKey = fileNodeKey;
                fileNode.label = common::sanitizeUtf8(filePath);
                fileNode.type = "file";
                nlohmann::json fileProps;
                fileProps["path"] = common::sanitizeUtf8(filePath);
                fileProps["language"] = common::sanitizeUtf8(language);
                if (!filePath.empty()) {
                    fileProps["basename"] =
                        common::sanitizeUtf8(std::filesystem::path(filePath).filename().string());
                }
                if (!hash.empty()) {
                    fileProps["current_hash"] = hash;
                }
                fileNode.properties = fileProps.dump();
                batch->nodes.push_back(std::move(fileNode));
            }

            // Build entity nodes and edges
            std::string targetNodeKey = !docNodeKey.empty() ? docNodeKey : fileNodeKey;
            for (const auto* qc : nlEntities) {
                std::string text = common::sanitizeUtf8(qc->text);
                std::string type = common::sanitizeUtf8(qc->type);

                // Normalize text for canonical matching
                std::string normalizedText = text;
                std::transform(normalizedText.begin(), normalizedText.end(), normalizedText.begin(),
                               ::tolower);

                std::string nodeKey = "nl_entity:" + type + ":" + normalizedText;

                metadata::KGNode node;
                node.nodeKey = nodeKey;
                node.label = text;
                node.type = type;

                nlohmann::json props;
                props["entity_text"] = text;
                props["entity_type"] = type;
                props["confidence"] = qc->confidence;
                props["first_seen_file"] = common::sanitizeUtf8(filePath);
                props["last_seen"] = now;
                if (!hash.empty()) {
                    props["first_seen_hash"] = hash;
                }
                node.properties = props.dump();
                batch->nodes.push_back(std::move(node));

                // Add edge from entity to document/file
                if (!targetNodeKey.empty()) {
                    DeferredEdge edge;
                    edge.srcNodeKey = nodeKey;
                    edge.dstNodeKey = targetNodeKey;
                    edge.relation = "mentioned_in";
                    edge.weight = qc->confidence;

                    nlohmann::json edgeProps;
                    edgeProps["confidence"] = qc->confidence;
                    if (!hash.empty()) {
                        edgeProps["snapshot_id"] = hash;
                    }
                    edge.properties = edgeProps.dump();
                    batch->deferredEdges.push_back(std::move(edge));
                }

                // Add doc entity reference
                if (documentDbId.has_value()) {
                    DeferredDocEntity docEnt;
                    docEnt.documentId = documentDbId.value();
                    docEnt.entityText = text;
                    docEnt.nodeKey = nodeKey;
                    docEnt.startOffset = 0; // Not available from QueryConcept
                    docEnt.endOffset = 0;
                    docEnt.confidence = qc->confidence;
                    docEnt.extractor = "gliner_title_nl";
                    batch->deferredDocEntities.push_back(std::move(docEnt));
                }
            }

            // Enqueue the batch (non-blocking)
            try {
                auto future = kgWriteQueue_->enqueue(std::move(batch));
                // Don't wait for completion - fire and forget for async KG population
                // The KGWriteQueue will batch and commit efficiently
                spdlog::debug("[PostIngestQueue] Queued {} NL entities for KG from {}",
                              nlEntities.size(), hash.substr(0, 12));
            } catch (const std::exception& e) {
                spdlog::warn("[PostIngestQueue] Failed to queue NL entities for KG: {}", e.what());
            }
        }

        auto duration = std::chrono::steady_clock::now() - startTime;
        double ms = std::chrono::duration<double, std::milli>(duration).count();
        spdlog::info("[PostIngestQueue] Title+NL extraction for {} in {:.2f}ms (title={}, nl={})",
                     hash.substr(0, 12), ms, bestTitle ? "yes" : "no", nlEntities.size());

        InternalEventBus::instance().incTitleConsumed();
    } catch (const std::exception& e) {
        spdlog::error("[PostIngestQueue] Title+NL extraction failed for {}: {}", hash, e.what());
    }
}

// =========================================================================
// Gradient-Based Adaptive Concurrency Limiter Integration
// =========================================================================

void PostIngestQueue::initializeGradientLimiters() {
    if (!TuneAdvisor::enableGradientLimiters()) {
        spdlog::info("[PostIngestQueue] Gradient limiters disabled");
        return;
    }

    GradientLimiter::Config cfg;
    cfg.smoothingAlpha = TuneAdvisor::gradientSmoothingAlpha();
    cfg.longWindowAlpha = TuneAdvisor::gradientLongAlpha();
    cfg.warmupSamples = TuneAdvisor::gradientWarmupSamples();
    cfg.tolerance = TuneAdvisor::gradientTolerance();
    cfg.initialLimit = TuneAdvisor::gradientInitialLimit();
    cfg.minLimit = TuneAdvisor::gradientMinLimit();

    // Per-stage maxLimit is the smaller of the global gradient max and the stage cap
    const double globalMax = TuneAdvisor::gradientMaxLimit();

    static constexpr const char* kLimiterNames[] = {"extraction", "kg",    "symbol",
                                                    "entity",     "title", "embed"};
    using ConcurrentFn = uint32_t (*)();
    static constexpr ConcurrentFn kConcurrentFns[] = {
        &TuneAdvisor::postKgConcurrent, &TuneAdvisor::postSymbolConcurrent,
        &TuneAdvisor::postEntityConcurrent, &TuneAdvisor::postTitleConcurrent,
        &TuneAdvisor::postEmbedConcurrent};

    for (std::size_t i = 0; i < kLimiterCount; ++i) {
        double stageMax = 0.0;
        if (i == 0) {
            // Keep extraction limiter aligned with the effective extraction worker cap.
            stageMax = static_cast<double>(maxExtractionConcurrent());
        } else {
            stageMax = static_cast<double>(kConcurrentFns[i - 1]());
        }

        // Some stage caps use 0 as "unset" (rather than "disabled"). A maxLimit of 0 would
        // invert the clamp bounds (minLimit > maxLimit) and crash in libstdc++ debug builds.
        if (stageMax <= 0.0) {
            stageMax = globalMax;
        }

        cfg.maxLimit = std::min(globalMax, stageMax);

        // Defensive: guarantee valid clamp bounds and a consistent initial limit.
        if (cfg.maxLimit < cfg.minLimit) {
            cfg.maxLimit = cfg.minLimit;
        }
        if (cfg.initialLimit > cfg.maxLimit) {
            cfg.initialLimit = cfg.maxLimit;
        }

        limiters_[i] = std::make_unique<GradientLimiter>(kLimiterNames[i], cfg);
    }

    spdlog::info("[PostIngestQueue] Gradient limiters initialized");
}

bool PostIngestQueue::tryAcquireLimiterSlot(GradientLimiter* limiter, const std::string& jobId,
                                            const std::string& stage) {
    if (!limiter) {
        return true; // No limiter configured, allow
    }

    if (!limiter->tryAcquire()) {
        return false; // At limit
    }

    // Track the job
    auto now = std::chrono::steady_clock::now();
    {
        std::lock_guard<std::mutex> lock(activeJobsMutex_);
        activeJobs_[jobId] = {now, now, limiter, stage};
    }

    return true;
}

void PostIngestQueue::completeJob(const std::string& jobId, bool success) {
    ActiveJob job;
    {
        std::lock_guard<std::mutex> lock(activeJobsMutex_);
        auto it = activeJobs_.find(jobId);
        if (it == activeJobs_.end()) {
            return; // Job not tracked
        }
        job = it->second;
        activeJobs_.erase(it);
    }

    auto now = std::chrono::steady_clock::now();
    auto latency = now - job.startTime;
    job.limiter->onJobComplete(latency, success);
}

// =========================================================================
// Parallel Processing & Caching (PBI-05a optimizations)
// =========================================================================

void PostIngestQueue::initializeExtractionSemaphore() {
    // Keep semaphore capacity aligned with the effective extraction floor used by pollers.
    // Using the raw tuned value can collapse runtime concurrency to 1 under pressure.
    const uint32_t maxConcurrent = static_cast<uint32_t>(maxExtractionConcurrent());
    if (maxConcurrent > 0) {
        extractionSemaphore_ = std::make_unique<std::counting_semaphore<>>(maxConcurrent);
        spdlog::info("[PostIngestQueue] Extraction semaphore initialized with {} slots",
                     maxConcurrent);
    }
}

std::optional<metadata::DocumentInfo>
PostIngestQueue::getCachedDocumentInfo(const std::string& hash) {
    // Try cache first
    auto cached = metadataCache_.infoCache.get(hash);
    if (cached) {
        return *cached;
    }

    // Cache miss - query DB
    if (!meta_) {
        return std::nullopt;
    }

    auto result = meta_->getDocumentByHash(hash);
    if (result && result.value().has_value()) {
        auto info = *result.value();
        metadataCache_.infoCache.put(hash, info);
        return info;
    }

    return std::nullopt;
}

std::optional<std::vector<std::string>> PostIngestQueue::getCachedDocumentTags(int64_t docId) {
    // Try cache first
    auto cached = metadataCache_.tagsCache.get(docId);
    if (cached) {
        return *cached;
    }

    // Cache miss - query DB
    if (!meta_) {
        return std::nullopt;
    }

    auto result = meta_->getDocumentTags(docId);
    if (result) {
        auto tags = result.value();
        metadataCache_.tagsCache.put(docId, tags);
        return tags;
    }

    return std::nullopt;
}

PostIngestQueue::StageConfigSnapshot PostIngestQueue::snapshotStageConfig() {
    StageConfigSnapshot snap;
    {
        std::lock_guard<std::mutex> lock(extMapMutex_);
        snap.symbolExtensionMap = symbolExtensionMap_;
    }
    {
        std::lock_guard<std::mutex> lock(entityMutex_);
        snap.entityProviders = entityProviders_;
    }
    return snap;
}

void PostIngestQueue::commitBatchResults(std::vector<PreparedMetadataEntry>& successes,
                                         std::vector<ExtractionFailure>& failures) {
    if (!successes.empty() && meta_) {
        std::vector<metadata::BatchContentEntry> entries;
        entries.reserve(successes.size());

        for (const auto& prepared : successes) {
            metadata::BatchContentEntry entry;
            entry.documentId = prepared.documentId;
            entry.title = prepared.title.empty() ? prepared.fileName : prepared.title;
            entry.contentText = prepared.extractedText;
            entry.mimeType = prepared.mimeType;
            entry.extractionMethod = "post_ingest";
            entry.language = prepared.language;
            entries.push_back(std::move(entry));
        }

        auto batchResult = meta_->batchInsertContentAndIndex(entries);
        if (!batchResult) {
            spdlog::error("[PostIngestQueue] Batch DB write failed: {}",
                          batchResult.error().message);
            if (batchResult.error().message.find("database is locked") != std::string::npos) {
                TuneAdvisor::reportDbLockError();
            }
            for (const auto& prepared : successes) {
                failures.push_back(ExtractionFailure{prepared.documentId, prepared.hash,
                                                     batchResult.error().message});
            }
            successes.clear();
        } else {
            spdlog::debug("[PostIngestQueue] Batch DB write succeeded for {} documents",
                          entries.size());
            std::vector<std::tuple<int64_t, std::string, metadata::MetadataValue>> titleUpdates;
            titleUpdates.reserve(successes.size());
            for (const auto& prepared : successes) {
                if (prepared.title.empty()) {
                    continue;
                }
                titleUpdates.emplace_back(prepared.documentId, "title",
                                          metadata::MetadataValue(prepared.title));
            }
            if (!titleUpdates.empty()) {
                auto metaResult = meta_->setMetadataBatch(titleUpdates);
                if (!metaResult) {
                    spdlog::warn("[PostIngestQueue] Batch title metadata write failed: {}",
                                 metaResult.error().message);
                }
            }
        }
    }

    for (const auto& failure : failures) {
        if (failure.documentId >= 0 && meta_) {
            auto updateRes = meta_->updateDocumentExtractionStatus(
                failure.documentId, false, metadata::ExtractionStatus::Failed,
                failure.errorMessage);
            if (!updateRes) {
                spdlog::warn("[PostIngestQueue] Failed to mark extraction failed for {}: {}",
                             failure.hash, updateRes.error().message);
            }
        }
    }
}

void PostIngestQueue::dispatchSuccesses(const std::vector<PreparedMetadataEntry>& successes) {
    std::unordered_map<std::string, std::shared_ptr<std::vector<std::byte>>> contentByHash;

    // Opportunistically enqueue embedding jobs for successfully extracted/indexed documents.
    // This is the primary path for generating vector embeddings during normal ingestion.
    std::shared_ptr<SpscQueue<InternalEventBus::EmbedJob>> embedQ;
    const uint32_t embedCap = TuneAdvisor::embedChannelCapacity();
    if (embedCap > 0) {
        embedQ = InternalEventBus::instance().get_or_create_channel<InternalEventBus::EmbedJob>(
            "embed_jobs", embedCap);
    }
    std::vector<InternalEventBus::EmbedPreparedDoc> embedPreparedBatch;
    std::vector<std::string> embedHashBatch;
    const std::size_t maxEmbedBatch = TuneAdvisor::resolvedEmbedJobDocCap();
    embedPreparedBatch.reserve(std::min<std::size_t>(maxEmbedBatch, successes.size()));
    embedHashBatch.reserve(std::min<std::size_t>(maxEmbedBatch, successes.size()));
    const bool embedStageActive =
        (TuneAdvisor::postIngestStageActiveMask() &
         (1u << static_cast<uint8_t>(TuneAdvisor::PostIngestStage::Embed))) != 0u;

    auto flushEmbedBatch = [&]() {
        if (!embedQ || (embedPreparedBatch.empty() && embedHashBatch.empty())) {
            embedPreparedBatch.clear();
            embedHashBatch.clear();
            return;
        }

        const std::uint64_t preparedDocsCount = embedPreparedBatch.size();
        std::uint64_t preparedChunksCount = 0;
        for (const auto& d : embedPreparedBatch) {
            preparedChunksCount += static_cast<std::uint64_t>(d.chunks.size());
        }
        const std::uint64_t hashOnlyDocsCount = embedHashBatch.size();

        InternalEventBus::EmbedJob job;
        job.preparedDocs = std::move(embedPreparedBatch);
        job.hashes = std::move(embedHashBatch);
        job.batchSize = static_cast<uint32_t>(job.preparedDocs.size() + job.hashes.size());
        job.hashes.reserve(job.hashes.size() + job.preparedDocs.size());
        for (const auto& doc : job.preparedDocs) {
            job.hashes.push_back(doc.hash);
        }
        job.skipExisting = true;

        constexpr auto kEnqueueTimeout = std::chrono::milliseconds(100);
        uint32_t waits = 0;
        while (!stop_.load(std::memory_order_acquire)) {
            if (embedQ->push_wait(job, kEnqueueTimeout)) {
                InternalEventBus::instance().incEmbedQueued(job.batchSize);
                if (preparedDocsCount > 0) {
                    InternalEventBus::instance().incEmbedPreparedDocsQueued(preparedDocsCount);
                    InternalEventBus::instance().incEmbedPreparedChunksQueued(preparedChunksCount);
                }
                if (hashOnlyDocsCount > 0) {
                    InternalEventBus::instance().incEmbedHashOnlyDocsQueued(hashOnlyDocsCount);
                }
                break;
            }
            ++waits;
            if ((waits % 20u) == 1u) {
                spdlog::warn(
                    "[PostIngestQueue] embed enqueue waiting on full channel (batch={}, waits={})",
                    job.batchSize, waits);
            }
        }
        if (stop_.load(std::memory_order_acquire)) {
            InternalEventBus::instance().incEmbedDropped(job.batchSize);
        }
        embedPreparedBatch.clear();
        embedHashBatch.clear();
    };

    const auto selectionCfg = ConfigResolver::resolveEmbeddingSelectionPolicy();
    const auto chunkPolicy = ConfigResolver::resolveEmbeddingChunkingPolicy();
    auto embedChunker =
        yams::vector::createChunker(chunkPolicy.strategy, chunkPolicy.config, nullptr);

    auto makePreparedDoc = [&](const PreparedMetadataEntry& prepared)
        -> std::optional<InternalEventBus::EmbedPreparedDoc> {
        if (!embedChunker) {
            return std::nullopt;
        }
        embed::EmbedSourceDoc src;
        src.hash = prepared.hash;
        src.extractedText = prepared.extractedText;
        src.fileName = prepared.fileName;
        src.filePath = prepared.filePath;
        src.mimeType = prepared.mimeType;
        return embed::prepareEmbedPreparedDoc(src, *embedChunker, selectionCfg);
    };

    auto getOrLoadContent =
        [this, &contentByHash](const std::string& hash) -> std::shared_ptr<std::vector<std::byte>> {
        if (!store_) {
            return nullptr;
        }
        auto it = contentByHash.find(hash);
        if (it != contentByHash.end()) {
            return it->second;
        }
        auto contentResult = store_->retrieveBytes(hash);
        if (!contentResult) {
            return nullptr;
        }
        auto bytes = std::make_shared<std::vector<std::byte>>(std::move(contentResult.value()));
        contentByHash.emplace(hash, bytes);
        return bytes;
    };

    for (const auto& prepared : successes) {
        if (embedQ && embedStageActive && prepared.shouldDispatchEmbed) {
            if (auto preparedDoc = makePreparedDoc(prepared);
                preparedDoc && !preparedDoc->chunks.empty()) {
                embedPreparedBatch.push_back(std::move(*preparedDoc));
            } else {
                // Fallback: queue by hash.
                embedHashBatch.push_back(prepared.hash);
            }

            if ((embedPreparedBatch.size() + embedHashBatch.size()) >= maxEmbedBatch) {
                flushEmbedBatch();
            }
        }

        std::shared_ptr<std::vector<std::byte>> contentBytes = prepared.contentBytes;
        if (!contentBytes && (prepared.shouldDispatchSymbol || prepared.shouldDispatchEntity)) {
            contentBytes = getOrLoadContent(prepared.hash);
        }
        if (prepared.shouldDispatchKg) {
            dispatchToKgChannel(prepared.hash, prepared.documentId, prepared.filePath,
                                std::vector<std::string>(prepared.tags), contentBytes);
        }
        if (prepared.shouldDispatchSymbol) {
            dispatchToSymbolChannel(prepared.hash, prepared.documentId, prepared.filePath,
                                    prepared.symbolLanguage, contentBytes);
        }
        if (prepared.shouldDispatchEntity) {
            dispatchToEntityChannel(prepared.hash, prepared.documentId, prepared.filePath,
                                    prepared.extension, contentBytes);
        }
        if (prepared.shouldDispatchTitle) {
            dispatchToTitleChannel(prepared.hash, prepared.documentId, prepared.titleTextSnippet,
                                   prepared.fileName, prepared.filePath, prepared.language,
                                   prepared.mimeType);
        }
        InternalEventBus::instance().incPostConsumed();
    }

    flushEmbedBatch();
}

void PostIngestQueue::processBatch(std::vector<InternalEventBus::PostIngestTask>&& tasks) {
    if (tasks.empty()) {
        return;
    }

    auto stageCfg = snapshotStageConfig();

    // Get current concurrency limits from TuneAdvisor
    const uint32_t maxWorkers = static_cast<uint32_t>(maxExtractionConcurrent());
    if (maxWorkers <= 1 || tasks.size() < 4) {
        // Sequential: single chunk
        auto result =
            processChunkParallel(tasks, stageCfg.symbolExtensionMap, stageCfg.entityProviders);
        processed_.fetch_add(result.successes.size(), std::memory_order_relaxed);
        failed_.fetch_add(result.failures.size(), std::memory_order_relaxed);
        commitBatchResults(result.successes, result.failures);
        dispatchSuccesses(result.successes);
        return;
    }

    YAMS_ZONE_SCOPED_N("PostIngestQueue::processBatch");

    // Calculate optimal chunk size
    const size_t numChunks = std::min(static_cast<size_t>(maxWorkers), tasks.size());
    const size_t chunkSize = (tasks.size() + numChunks - 1) / numChunks;

    // Launch parallel chunks using std::async
    std::vector<std::future<ChunkResult>> futures;
    futures.reserve(numChunks);

    for (size_t i = 0; i < tasks.size(); i += chunkSize) {
        size_t end = std::min(i + chunkSize, tasks.size());
        std::vector<InternalEventBus::PostIngestTask> chunk(tasks.begin() + i, tasks.begin() + end);

        futures.push_back(
            std::async(std::launch::async, [this, chunk = std::move(chunk), &stageCfg]() {
                return processChunkParallel(chunk, stageCfg.symbolExtensionMap,
                                            stageCfg.entityProviders);
            }));
    }

    // Collect results
    std::vector<PreparedMetadataEntry> allSuccesses;
    std::vector<ExtractionFailure> allFailures;

    for (auto& future : futures) {
        try {
            ChunkResult result = future.get();
            allSuccesses.insert(allSuccesses.end(),
                                std::make_move_iterator(result.successes.begin()),
                                std::make_move_iterator(result.successes.end()));
            allFailures.insert(allFailures.end(), std::make_move_iterator(result.failures.begin()),
                               std::make_move_iterator(result.failures.end()));
        } catch (const std::exception& e) {
            spdlog::error("[PostIngestQueue] Chunk processing failed: {}", e.what());
        }
    }

    processed_.fetch_add(allSuccesses.size(), std::memory_order_relaxed);
    failed_.fetch_add(allFailures.size(), std::memory_order_relaxed);
    commitBatchResults(allSuccesses, allFailures);
    dispatchSuccesses(allSuccesses);
}

PostIngestQueue::ChunkResult PostIngestQueue::processChunkParallel(
    const std::vector<InternalEventBus::PostIngestTask>& tasks,
    const std::unordered_map<std::string, std::string>& symbolExtensionMap,
    const std::vector<std::shared_ptr<ExternalEntityProviderAdapter>>& entityProviders) {
    ChunkResult result;
    result.successes.reserve(tasks.size());
    result.failures.reserve(tasks.size() / 10);

    // Parallel extraction within the chunk
    std::vector<std::future<std::variant<PreparedMetadataEntry, ExtractionFailure>>> futures;
    futures.reserve(tasks.size());

    for (const auto& task : tasks) {
        const auto taskCopy = task;
        futures.push_back(std::async(
            std::launch::async, [this, taskCopy, &symbolExtensionMap, &entityProviders]() {
                // Acquire semaphore slot for memory-safe extraction
                if (extractionSemaphore_) {
                    extractionSemaphore_->acquire();
                }

                // RAII guard to release semaphore
                struct SemaphoreGuard {
                    std::counting_semaphore<>* sem;
                    explicit SemaphoreGuard(std::counting_semaphore<>* s) : sem(s) {}
                    ~SemaphoreGuard() {
                        if (sem) {
                            sem->release();
                        }
                    }
                    SemaphoreGuard(const SemaphoreGuard&) = delete;
                    SemaphoreGuard& operator=(const SemaphoreGuard&) = delete;
                    SemaphoreGuard(SemaphoreGuard&&) = delete;
                    SemaphoreGuard& operator=(SemaphoreGuard&&) = delete;
                };
                SemaphoreGuard guard(extractionSemaphore_.get());

                // Get cached metadata
                auto infoOpt = getCachedDocumentInfo(taskCopy.hash);
                if (!infoOpt) {
                    return std::variant<PreparedMetadataEntry, ExtractionFailure>(
                        ExtractionFailure{-1, taskCopy.hash, "Metadata not found in cache or DB"});
                }

                auto tagsOpt = getCachedDocumentTags(infoOpt->id);
                const std::vector<std::string> emptyTags;
                const std::vector<std::string>& tags = tagsOpt ? *tagsOpt : emptyTags;

                // Extract and prepare
                return prepareMetadataEntry(taskCopy.hash, taskCopy.mime, *infoOpt, tags,
                                            symbolExtensionMap, entityProviders);
            }));
    }

    // Collect results
    for (auto& future : futures) {
        try {
            auto variant = future.get();
            if (std::holds_alternative<PreparedMetadataEntry>(variant)) {
                result.successes.push_back(std::get<PreparedMetadataEntry>(std::move(variant)));
            } else {
                result.failures.push_back(std::get<ExtractionFailure>(std::move(variant)));
            }
        } catch (const std::exception& e) {
            spdlog::error("[PostIngestQueue] Parallel extraction failed: {}", e.what());
        }
    }

    return result;
}

} // namespace yams::daemon
