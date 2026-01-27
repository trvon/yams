#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <algorithm>
#include <cctype>
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
#include <yams/daemon/components/InternalEventBus.h>
#include <yams/daemon/components/KGWriteQueue.h>
#include <yams/daemon/components/PostIngestQueue.h>
#include <yams/daemon/components/ResourceGovernor.h>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/daemon/components/TuningSnapshot.h>
#include <yams/daemon/components/WorkCoordinator.h>
#include <yams/daemon/resource/external_entity_provider_adapter.h>
#include <yams/extraction/extraction_util.h>
#include <yams/extraction/text_extractor.h>
#include <yams/ingest/ingest_helpers.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/vector/document_chunker.h>
#include <yams/vector/embedding_generator.h>
#include <yams/vector/vector_database.h>

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
    return static_cast<std::size_t>(TuneAdvisor::postExtractionConcurrent());
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
    // Workers may still be executing post-processing (e.g., flushEmbedRetriesOnDrain)
    // after decrementing in-flight counters. Acquire mutex to synchronize with them.
    std::lock_guard<std::mutex> lock(embedRetryMutex_);
}

void PostIngestQueue::start() {
    spdlog::info("[PostIngestQueue] start() called, stop_={}", stop_.load());
    if (!stop_.load()) {
        refreshStageAvailability();
        logStageAvailabilitySnapshot();
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
        for (int i = 0;
             i < maxWaitMs && (!started_.load() || !kgStarted_.load() || !symbolStarted_.load() ||
                               !entityStarted_.load() || !titleStarted_.load());
             ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }

        spdlog::info("[PostIngestQueue] Pollers started (extraction={}, kg={}, symbol={}, "
                     "entity={}, title={})",
                     started_.load(), kgStarted_.load(), symbolStarted_.load(),
                     entityStarted_.load(), titleStarted_.load());
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

void PostIngestQueue::pauseStage(Stage stage) {
    switch (stage) {
        case Stage::Extraction:
            extractionPaused_.store(true, std::memory_order_release);
            TuneAdvisor::setPostIngestStageActive(TuneAdvisor::PostIngestStage::Extraction, false);
            spdlog::info("[PostIngestQueue] Paused Extraction stage");
            break;
        case Stage::KnowledgeGraph:
            kgPaused_.store(true, std::memory_order_release);
            TuneAdvisor::setPostIngestStageActive(TuneAdvisor::PostIngestStage::KnowledgeGraph,
                                                  false);
            spdlog::info("[PostIngestQueue] Paused KnowledgeGraph stage");
            break;
        case Stage::Symbol:
            symbolPaused_.store(true, std::memory_order_release);
            TuneAdvisor::setPostIngestStageActive(TuneAdvisor::PostIngestStage::Symbol, false);
            spdlog::info("[PostIngestQueue] Paused Symbol stage");
            break;
        case Stage::Entity:
            entityPaused_.store(true, std::memory_order_release);
            TuneAdvisor::setPostIngestStageActive(TuneAdvisor::PostIngestStage::Entity, false);
            spdlog::info("[PostIngestQueue] Paused Entity stage");
            break;
        case Stage::Title:
            titlePaused_.store(true, std::memory_order_release);
            TuneAdvisor::setPostIngestStageActive(TuneAdvisor::PostIngestStage::Title, false);
            spdlog::info("[PostIngestQueue] Paused Title stage");
            break;
    }
}

void PostIngestQueue::resumeStage(Stage stage) {
    switch (stage) {
        case Stage::Extraction:
            extractionPaused_.store(false, std::memory_order_release);
            TuneAdvisor::setPostIngestStageActive(TuneAdvisor::PostIngestStage::Extraction, true);
            spdlog::info("[PostIngestQueue] Resumed Extraction stage");
            break;
        case Stage::KnowledgeGraph:
            kgPaused_.store(false, std::memory_order_release);
            TuneAdvisor::setPostIngestStageActive(TuneAdvisor::PostIngestStage::KnowledgeGraph,
                                                  true);
            spdlog::info("[PostIngestQueue] Resumed KnowledgeGraph stage");
            break;
        case Stage::Symbol:
            symbolPaused_.store(false, std::memory_order_release);
            TuneAdvisor::setPostIngestStageActive(TuneAdvisor::PostIngestStage::Symbol, true);
            spdlog::info("[PostIngestQueue] Resumed Symbol stage");
            break;
        case Stage::Entity:
            entityPaused_.store(false, std::memory_order_release);
            TuneAdvisor::setPostIngestStageActive(TuneAdvisor::PostIngestStage::Entity, true);
            spdlog::info("[PostIngestQueue] Resumed Entity stage");
            break;
        case Stage::Title:
            titlePaused_.store(false, std::memory_order_release);
            TuneAdvisor::setPostIngestStageActive(TuneAdvisor::PostIngestStage::Title, true);
            spdlog::info("[PostIngestQueue] Resumed Title stage");
            break;
    }
}

bool PostIngestQueue::isStagePaused(Stage stage) const {
    switch (stage) {
        case Stage::Extraction:
            return extractionPaused_.load(std::memory_order_acquire);
        case Stage::KnowledgeGraph:
            return kgPaused_.load(std::memory_order_acquire);
        case Stage::Symbol:
            return symbolPaused_.load(std::memory_order_acquire);
        case Stage::Entity:
            return entityPaused_.load(std::memory_order_acquire);
        case Stage::Title:
            return titlePaused_.load(std::memory_order_acquire);
    }
    return false;
}

void PostIngestQueue::pauseAll() {
    extractionPaused_.store(true, std::memory_order_release);
    kgPaused_.store(true, std::memory_order_release);
    symbolPaused_.store(true, std::memory_order_release);
    entityPaused_.store(true, std::memory_order_release);
    titlePaused_.store(true, std::memory_order_release);
    TuneAdvisor::setPostIngestStageActive(TuneAdvisor::PostIngestStage::Extraction, false);
    TuneAdvisor::setPostIngestStageActive(TuneAdvisor::PostIngestStage::KnowledgeGraph, false);
    TuneAdvisor::setPostIngestStageActive(TuneAdvisor::PostIngestStage::Symbol, false);
    TuneAdvisor::setPostIngestStageActive(TuneAdvisor::PostIngestStage::Entity, false);
    TuneAdvisor::setPostIngestStageActive(TuneAdvisor::PostIngestStage::Title, false);
    spdlog::warn("[PostIngestQueue] All stages paused (emergency mode)");
}

void PostIngestQueue::resumeAll() {
    extractionPaused_.store(false, std::memory_order_release);
    kgPaused_.store(false, std::memory_order_release);
    symbolPaused_.store(false, std::memory_order_release);
    entityPaused_.store(false, std::memory_order_release);
    titlePaused_.store(false, std::memory_order_release);
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
    const bool extractionActive = !extractionPaused_.load(std::memory_order_acquire);
    TuneAdvisor::setPostIngestStageActive(TuneAdvisor::PostIngestStage::Extraction,
                                          extractionActive);

    const bool kgActive = graphComponent_ != nullptr && !kgPaused_.load(std::memory_order_acquire);
    TuneAdvisor::setPostIngestStageActive(TuneAdvisor::PostIngestStage::KnowledgeGraph, kgActive);

    bool symbolCapable = false;
    {
        std::lock_guard<std::mutex> lock(extMapMutex_);
        symbolCapable = !symbolExtensionMap_.empty();
    }
    const bool symbolActive = symbolCapable && !symbolPaused_.load(std::memory_order_acquire);
    TuneAdvisor::setPostIngestStageActive(TuneAdvisor::PostIngestStage::Symbol, symbolActive);

    bool entityCapable = false;
    {
        std::lock_guard<std::mutex> lock(entityMutex_);
        entityCapable = !entityProviders_.empty();
    }
    const bool entityActive = entityCapable && !entityPaused_.load(std::memory_order_acquire);
    TuneAdvisor::setPostIngestStageActive(TuneAdvisor::PostIngestStage::Entity, entityActive);

    const bool titleActive =
        (titleExtractor_ != nullptr) && !titlePaused_.load(std::memory_order_acquire);
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
        !extractionPaused_.load(std::memory_order_acquire),
        graphComponent_ != nullptr && !kgPaused_.load(std::memory_order_acquire),
        symbolCapable && !symbolPaused_.load(std::memory_order_acquire),
        entityCapable && !entityPaused_.load(std::memory_order_acquire),
        titleExtractor_ != nullptr && !titlePaused_.load(std::memory_order_acquire),
        extractionPaused_.load(std::memory_order_acquire),
        kgPaused_.load(std::memory_order_acquire), symbolPaused_.load(std::memory_order_acquire),
        entityPaused_.load(std::memory_order_acquire), titlePaused_.load(std::memory_order_acquire),
        maxExtractionConcurrent(), maxKgConcurrent(), maxSymbolConcurrent(), maxEntityConcurrent(),
        maxTitleConcurrent());
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

            flushEmbedRetriesOnDrain();

            spdlog::debug(
                "[PostIngestQueue] Queue drained, signaled corpus stats and drain callback");
        }
    }
}

void PostIngestQueue::recordEmbedRetry(const std::vector<std::string>& hashes) {
    if (hashes.empty()) {
        return;
    }
    std::lock_guard<std::mutex> lock(embedRetryMutex_);
    for (const auto& h : hashes) {
        embedRetryHashes_.push_back(h);
    }
}

void PostIngestQueue::notifyEmbedFailure(const std::vector<std::string>& hashes) {
    if (hashes.empty()) {
        return;
    }
    EmbedFailureCallback cb;
    {
        std::lock_guard<std::mutex> lock(embedFailureCallbackMutex_);
        cb = embedFailureCallback_;
    }
    if (cb) {
        cb(hashes);
    }
}

bool PostIngestQueue::dispatchEmbedJobWithRetry(const std::vector<std::string>& hashes,
                                                bool recordOnFailure, bool notifyOnFailure) {
    if (hashes.empty()) {
        return true;
    }

    try {
        const std::size_t capacity = TuneAdvisor::embedChannelCapacity();
        auto embedChannel =
            InternalEventBus::instance().get_or_create_channel<InternalEventBus::EmbedJob>(
                "embed_jobs", capacity);

        if (!embedChannel) {
            if (recordOnFailure) {
                recordEmbedRetry(hashes);
            }
            if (notifyOnFailure) {
                notifyEmbedFailure(hashes);
            }
            for (std::size_t i = 0; i < hashes.size(); ++i) {
                InternalEventBus::instance().incEmbedDropped();
            }
            spdlog::warn("[PostIngestQueue] Embed channel unavailable for batch of {} hashes",
                         hashes.size());
            return false;
        }

        InternalEventBus::EmbedJob job;
        job.hashes = hashes;
        job.batchSize = static_cast<uint32_t>(hashes.size());
        job.skipExisting = true;
        job.modelName = "";

        constexpr int maxRetries = 5;
        constexpr auto kEmbedPushTimeout = std::chrono::milliseconds(100);
        auto backoff = std::chrono::milliseconds(50);
        constexpr auto maxBackoff = std::chrono::milliseconds(1000);

        bool queued = false;
        for (int attempt = 0; attempt <= maxRetries; ++attempt) {
            if (embedChannel->push_wait(job, kEmbedPushTimeout)) {
                queued = true;
                break;
            }
            if (attempt < maxRetries) {
                std::this_thread::sleep_for(backoff);
                backoff = std::min(backoff * 2, maxBackoff);
            }
        }

        if (queued) {
            for (std::size_t i = 0; i < hashes.size(); ++i) {
                InternalEventBus::instance().incEmbedQueued();
            }
            spdlog::debug("[PostIngestQueue] Dispatched embedding job for {} hashes",
                          hashes.size());
            return true;
        }

        if (recordOnFailure) {
            recordEmbedRetry(hashes);
        }
        if (notifyOnFailure) {
            notifyEmbedFailure(hashes);
        }
        for (std::size_t i = 0; i < hashes.size(); ++i) {
            InternalEventBus::instance().incEmbedDropped();
        }
        spdlog::warn("[PostIngestQueue] Embed channel full after retries, dropping batch of {} "
                     "hashes",
                     hashes.size());
        return false;
    } catch (const std::exception& e) {
        spdlog::error("[PostIngestQueue] Embedding batch dispatch failed: {}", e.what());
        return false;
    }
}

void PostIngestQueue::flushEmbedRetriesOnDrain() {
    std::vector<std::string> pending;
    {
        std::lock_guard<std::mutex> lock(embedRetryMutex_);
        if (embedRetryHashes_.empty()) {
            return;
        }
        pending.assign(embedRetryHashes_.begin(), embedRetryHashes_.end());
        embedRetryHashes_.clear();
    }

    const std::size_t batchSize = std::max<std::size_t>(1u, TuneAdvisor::postIngestBatchSize());
    std::vector<std::string> batch;
    batch.reserve(batchSize);

    for (const auto& hash : pending) {
        batch.push_back(hash);
        if (batch.size() >= batchSize) {
            if (!dispatchEmbedJobWithRetry(batch, true, true)) {
                // If dispatch fails again, keep the hashes queued for repair.
            }
            batch.clear();
        }
    }
    if (!batch.empty()) {
        (void)dispatchEmbedJobWithRetry(batch, true, true);
    }
}

boost::asio::awaitable<void> PostIngestQueue::channelPoller() {
    spdlog::info("[PostIngestQueue] channelPoller coroutine STARTED");
    const std::size_t channelCapacity = resolveChannelCapacity();
    auto channel =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::PostIngestTask>(
            "post_ingest", channelCapacity);
    spdlog::info("[PostIngestQueue] channelPoller got channel (cap={})", channelCapacity);

    boost::asio::steady_timer timer(co_await boost::asio::this_coro::executor);
    spdlog::info("[PostIngestQueue] channelPoller got timer");

    started_.store(true);

    // Adaptive backoff for CPU efficiency with governance floor
    constexpr auto kMinIdleDelay = std::chrono::milliseconds(5);  // Governance floor
    constexpr auto kMaxIdleDelay = std::chrono::milliseconds(50); // Idle ceiling
    auto idleDelay = kMinIdleDelay;

    while (!stop_.load()) {
        bool didWork = false;
        InternalEventBus::PostIngestTask task;
        const std::size_t batchSize = std::max<std::size_t>(1u, TuneAdvisor::postIngestBatchSize());
        std::vector<InternalEventBus::PostIngestTask> batch;
        batch.reserve(batchSize);
        // Dynamic concurrency limit from TuneAdvisor
        std::size_t maxConcurrent = maxExtractionConcurrent();
        // Graduated pressure response for CPU-aware throttling
        auto pressureLevel = ResourceGovernor::instance().getPressureLevel();
        switch (pressureLevel) {
            case ResourcePressureLevel::Emergency:
                maxConcurrent = 0; // Halt (backstop for pauseAll)
                break;
            case ResourcePressureLevel::Critical:
                maxConcurrent = 1; // Minimal concurrency
                break;
            case ResourcePressureLevel::Warning:
                maxConcurrent = std::max<std::size_t>(1, maxConcurrent / 2);
                break;
            default:
                break;
        }
        if (extractionPaused_.load(std::memory_order_acquire) || maxConcurrent == 0) {
            timer.expires_after(kMinIdleDelay); // Always fast when paused
            co_await timer.async_wait(boost::asio::use_awaitable);
            continue;
        }
        while (inFlight_.load() < maxConcurrent && batch.size() < batchSize &&
               channel->try_pop(task)) {
            didWork = true;
            inFlight_.fetch_add(1);
            batch.push_back(std::move(task));
        }

        if (didWork && !batch.empty()) {
            wasActive_.store(true, std::memory_order_release);
            const std::size_t batchCount = batch.size();
            boost::asio::post(coordinator_->getExecutor(),
                              [this, batch = std::move(batch), batchCount]() mutable {
                                  processBatch(std::move(batch));
                                  inFlight_.fetch_sub(batchCount);
                                  checkDrainAndSignal();
                              });
        }

        if (didWork) {
            idleDelay = kMinIdleDelay; // Reset on work
            continue;
        }

        // Adaptive backoff when idle
        timer.expires_after(idleDelay);
        co_await timer.async_wait(boost::asio::use_awaitable);
        if (idleDelay < kMaxIdleDelay) {
            idleDelay = std::min(idleDelay * 2, kMaxIdleDelay);
        }
    }

    spdlog::info("[PostIngestQueue] Channel poller exited");
}

void PostIngestQueue::processBatch(std::vector<InternalEventBus::PostIngestTask>&& tasks) {
    if (tasks.empty()) {
        return;
    }

    std::unordered_map<std::string, metadata::DocumentInfo> infoMap;
    std::unordered_map<std::string, std::shared_ptr<std::vector<std::byte>>> contentByHash;
    std::unordered_map<int64_t, std::vector<std::string>> tagsByDocId;
    static const std::vector<std::string> kEmptyTags;

    std::unordered_map<std::string, std::string> symbolExtensionMap;
    {
        std::lock_guard<std::mutex> lock(extMapMutex_);
        symbolExtensionMap = symbolExtensionMap_;
    }

    std::vector<std::shared_ptr<ExternalEntityProviderAdapter>> entityProviders;
    {
        std::lock_guard<std::mutex> lock(entityMutex_);
        entityProviders = entityProviders_;
    }

    if (meta_) {
        std::vector<std::string> hashes;
        hashes.reserve(tasks.size());
        std::unordered_set<std::string> seen;
        seen.reserve(tasks.size());
        for (const auto& task : tasks) {
            if (seen.insert(task.hash).second) {
                hashes.push_back(task.hash);
            }
        }

        if (!hashes.empty()) {
            auto infoRes = meta_->batchGetDocumentsByHash(hashes);
            if (infoRes) {
                infoMap = std::move(infoRes).value();

                std::vector<int64_t> docIds;
                docIds.reserve(infoMap.size());
                for (const auto& [_, info] : infoMap) {
                    if (info.id >= 0) {
                        docIds.push_back(info.id);
                    }
                }

                if (!docIds.empty()) {
                    auto tagsRes = meta_->batchGetDocumentTags(docIds);
                    if (tagsRes) {
                        tagsByDocId = std::move(tagsRes).value();
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
    }

    // =========================================================================
    // 4-PHASE BATCHED METADATA PROCESSING
    // Phase 1: Prepare - extract text for all tasks
    // Phase 2: Batch DB write - single transaction for all successes
    // Phase 3: Handle failures - update extraction status
    // Phase 4: Dispatch - send to channels
    // =========================================================================

    std::vector<PreparedMetadataEntry> successes;
    std::vector<ExtractionFailure> failures;
    std::vector<const InternalEventBus::PostIngestTask*> fallbackTasks; // Tasks without info

    successes.reserve(tasks.size());
    failures.reserve(tasks.size() / 10); // Expect ~10% failure rate

    // Phase 1: PREPARATION - extract text for all documents
    for (const auto& task : tasks) {
        try {
            auto it = infoMap.find(task.hash);
            if (it != infoMap.end()) {
                const auto tagsIt = tagsByDocId.find(it->second.id);
                const std::vector<std::string>& tags =
                    (tagsIt != tagsByDocId.end()) ? tagsIt->second : kEmptyTags;

                auto result = prepareMetadataEntry(task.hash, task.mime, it->second, tags,
                                                   symbolExtensionMap, entityProviders);

                if (auto* prepared = std::get_if<PreparedMetadataEntry>(&result)) {
                    successes.push_back(std::move(*prepared));
                } else {
                    failures.push_back(std::get<ExtractionFailure>(result));
                }
            } else {
                // No DocumentInfo in batch lookup - use fallback path
                fallbackTasks.push_back(&task);
            }
        } catch (const std::exception& e) {
            spdlog::error("[PostIngestQueue] Preparation failed for {}: {}", task.hash, e.what());
            failed_++;
        }
    }

    // Phase 2: BATCH DB WRITE - single transaction for all successful extractions
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
            // All successes become failures
            for (const auto& prepared : successes) {
                failures.push_back(ExtractionFailure{prepared.documentId, prepared.hash,
                                                     batchResult.error().message});
            }
            successes.clear();
        } else {
            spdlog::info("[PostIngestQueue] Batch DB write succeeded for {} documents",
                         entries.size());
            for (const auto& prepared : successes) {
                if (!prepared.title.empty()) {
                    (void)meta_->setMetadata(prepared.documentId, "title",
                                             metadata::MetadataValue(prepared.title));
                }
            }
        }
    }

    // Phase 3: HANDLE FAILURES - update extraction status for failed documents
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

    // Phase 4: DISPATCH - send successful documents to channels
    const std::size_t embedBatchThreshold = TuneAdvisor::postIngestBatchSize();
    std::vector<std::string> embeddingHashes;
    embeddingHashes.reserve(successes.size() + fallbackTasks.size());

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
        std::shared_ptr<std::vector<std::byte>> contentBytes;
        if (prepared.shouldDispatchKg || prepared.shouldDispatchSymbol ||
            prepared.shouldDispatchEntity) {
            contentBytes = getOrLoadContent(prepared.hash);
        }
        if (prepared.shouldDispatchKg) {
            dispatchToKgChannel(prepared.hash, prepared.documentId, prepared.fileName,
                                std::vector<std::string>(prepared.tags), contentBytes);
        }
        if (prepared.shouldDispatchSymbol) {
            dispatchToSymbolChannel(prepared.hash, prepared.documentId, prepared.fileName,
                                    prepared.symbolLanguage, contentBytes);
        }
        if (prepared.shouldDispatchEntity) {
            dispatchToEntityChannel(prepared.hash, prepared.documentId, prepared.fileName,
                                    prepared.extension, contentBytes);
        }
        if (prepared.shouldDispatchTitle) {
            dispatchToTitleChannel(prepared.hash, prepared.documentId, prepared.titleTextSnippet,
                                   prepared.fileName, prepared.filePath, prepared.language,
                                   prepared.mimeType);
        }
        embeddingHashes.push_back(prepared.hash);
        if (embeddingHashes.size() >= embedBatchThreshold) {
            processEmbeddingBatch(embeddingHashes);
            embeddingHashes.clear();
        }
        processed_++;
        InternalEventBus::instance().incPostConsumed();
    }

    // Handle fallback tasks (those without DocumentInfo) using legacy path
    for (const auto* taskPtr : fallbackTasks) {
        try {
            processMetadataStage(taskPtr->hash, taskPtr->mime, std::nullopt, &kEmptyTags,
                                 symbolExtensionMap, entityProviders);
            embeddingHashes.push_back(taskPtr->hash);
            if (embeddingHashes.size() >= embedBatchThreshold) {
                processEmbeddingBatch(embeddingHashes);
                embeddingHashes.clear();
            }
            processed_++;
            InternalEventBus::instance().incPostConsumed();
        } catch (const std::exception& e) {
            spdlog::error("[PostIngestQueue] Fallback processing failed for {}: {}", taskPtr->hash,
                          e.what());
            failed_++;
        }
    }

    // Dispatch any remaining embedding hashes
    if (!embeddingHashes.empty()) {
        processEmbeddingBatch(embeddingHashes);
    }
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

    constexpr int maxRetries = 10;
    constexpr auto baseBackoff = std::chrono::milliseconds(50);
    constexpr auto maxBackoff = std::chrono::milliseconds(1000);

    for (int i = 0; i < maxRetries; ++i) {
        if (channel->try_push(task)) {
            return;
        }
        auto delay = std::min(baseBackoff * (1 << i), maxBackoff);
        std::this_thread::sleep_for(delay);
    }

    spdlog::error("[PostIngestQueue] Channel full after {} retries, dropping task for hash: {}",
                  maxRetries, task.hash);
}

bool PostIngestQueue::tryEnqueue(const Task& t) {
    // Check admission control before accepting work
    if (!ResourceGovernor::instance().canAdmitWork()) {
        spdlog::debug("[PostIngestQueue] Rejecting enqueue: admission control blocked");
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
    // Check admission control before accepting work
    if (!ResourceGovernor::instance().canAdmitWork()) {
        spdlog::debug("[PostIngestQueue] Rejecting enqueue: admission control blocked");
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
    return channel ? channel->size_approx() : 0;
}

std::size_t PostIngestQueue::kgQueueDepth() const {
    constexpr std::size_t kgChannelCapacity = 16384;
    auto channel = InternalEventBus::instance().get_or_create_channel<InternalEventBus::KgJob>(
        "kg_jobs", kgChannelCapacity);
    return channel ? channel->size_approx() : 0;
}

std::size_t PostIngestQueue::symbolQueueDepth() const {
    constexpr std::size_t symbolChannelCapacity = 16384;
    auto channel =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::SymbolExtractionJob>(
            "symbol_extraction", symbolChannelCapacity);
    return channel ? channel->size_approx() : 0;
}

std::size_t PostIngestQueue::entityQueueDepth() const {
    constexpr std::size_t entityChannelCapacity = 4096;
    auto channel =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::EntityExtractionJob>(
            "entity_extraction", entityChannelCapacity);
    return channel ? channel->size_approx() : 0;
}

std::size_t PostIngestQueue::titleQueueDepth() const {
    constexpr std::size_t titleChannelCapacity = 4096;
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
        processEmbeddingBatch(std::vector<std::string>{hash});
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
                spdlog::warn("[PostIngestQueue] persist/index failed for {}: {}", hash,
                             pr.error().message);
                // Track lock errors for adaptive concurrency scaling
                if (pr.error().message.find("database is locked") != std::string::npos) {
                    TuneAdvisor::reportDbLockError();
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
            dispatchToKgChannel(hash, docId, fileName, std::move(tags), nullptr);

            // Dispatch symbol extraction for code files (if plugin supports this extension)
            {
                // Extension map keys don't have leading dots, but DB stores with dots
                std::string extKey = extension;
                if (!extKey.empty() && extKey[0] == '.') {
                    extKey = extKey.substr(1);
                }
                auto it = symbolExtensionMap.find(extKey);
                if (it != symbolExtensionMap.end()) {
                    dispatchToSymbolChannel(hash, docId, fileName, it->second, nullptr);
                }
            }

            // Dispatch entity extraction for binary files (if any entity provider supports this
            // extension)
            if (extensionSupportsEntityProviders(entityProviders, extension)) {
                dispatchToEntityChannel(hash, docId, fileName, extension, nullptr);
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

    // Extract document text
    auto txt =
        extractDocumentText(store_, hash, prepared.mimeType, prepared.extension, extractors_);
    if (!txt || txt->empty()) {
        spdlog::debug("[PostIngestQueue] no text extracted for {} (mime={}, ext={})", hash,
                      prepared.mimeType, prepared.extension);
        return ExtractionFailure{info.id, hash, "No text extracted"};
    }

    prepared.extractedText = std::move(*txt);

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

    // Determine dispatch flags
    prepared.shouldDispatchKg = (info.id >= 0);

    // Symbol extraction: check if extension is in the symbol map
    std::string extKey = prepared.extension;
    if (!extKey.empty() && extKey[0] == '.') {
        extKey = extKey.substr(1);
    }
    auto symIt = symbolExtensionMap.find(extKey);
    if (symIt != symbolExtensionMap.end()) {
        prepared.shouldDispatchSymbol = true;
        prepared.symbolLanguage = symIt->second;
    }

    // Entity extraction: check if any provider supports this extension
    prepared.shouldDispatchEntity =
        extensionSupportsEntityProviders(entityProviders, prepared.extension);

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

void PostIngestQueue::processKnowledgeGraphStage(
    const std::string& hash, int64_t docId, const std::string& filePath,
    const std::vector<std::string>& tags, std::shared_ptr<std::vector<std::byte>> contentBytes) {
    if (!graphComponent_) {
        spdlog::warn("[PostIngestQueue] KG stage skipped for {} - no graphComponent", hash);
        return;
    }

    spdlog::info("[PostIngestQueue] KG stage starting for {} ({})", filePath, hash.substr(0, 12));

    try {
        auto startTime = std::chrono::steady_clock::now();

        GraphComponent::DocumentGraphContext ctx{.documentHash = hash,
                                                 .filePath = filePath,
                                                 .tags = tags,
                                                 .documentDbId = docId,
                                                 .contentBytes = std::move(contentBytes),
                                                 .skipEntityExtraction = true};

        auto result = graphComponent_->onDocumentIngested(ctx);
        if (!result) {
            spdlog::warn("[PostIngestQueue] Graph ingestion failed for {}: {}", hash,
                         result.error().message);
        } else {
            auto duration = std::chrono::steady_clock::now() - startTime;
            double ms = std::chrono::duration<double, std::milli>(duration).count();
            spdlog::debug("[PostIngestQueue] KG stage completed for {} in {:.2f}ms", hash, ms);
        }
        InternalEventBus::instance().incKgConsumed();
    } catch (const std::exception& e) {
        spdlog::error("[PostIngestQueue] KG stage failed for {}: {}", hash, e.what());
    }
}

void PostIngestQueue::processEmbeddingBatch(const std::vector<std::string>& hashes) {
    if (hashes.empty()) {
        return;
    }

    (void)dispatchEmbedJobWithRetry(hashes, true, true);
}

void PostIngestQueue::dispatchToKgChannel(const std::string& hash, int64_t docId,
                                          const std::string& filePath,
                                          std::vector<std::string> tags,
                                          std::shared_ptr<std::vector<std::byte>> contentBytes) {
    constexpr std::size_t kgChannelCapacity = 16384;
    auto channel = InternalEventBus::instance().get_or_create_channel<InternalEventBus::KgJob>(
        "kg_jobs", kgChannelCapacity);

    InternalEventBus::KgJob job;
    job.hash = hash;
    job.documentId = docId;
    job.filePath = filePath;
    job.tags = std::move(tags);
    job.contentBytes = std::move(contentBytes);

    if (!channel->try_push(std::move(job))) {
        spdlog::warn("[PostIngestQueue] KG channel full, dropping job for {}", hash);
        InternalEventBus::instance().incKgDropped();
    } else {
        spdlog::info("[PostIngestQueue] Dispatched KG job for {} ({})", filePath,
                     hash.substr(0, 12));
        InternalEventBus::instance().incKgQueued();
    }
}

boost::asio::awaitable<void> PostIngestQueue::kgPoller() {
    constexpr std::size_t kgChannelCapacity = 16384;
    auto channel = InternalEventBus::instance().get_or_create_channel<InternalEventBus::KgJob>(
        "kg_jobs", kgChannelCapacity);

    boost::asio::steady_timer timer(co_await boost::asio::this_coro::executor);

    kgStarted_.store(true);

    // Adaptive backoff for CPU efficiency with governance floor
    constexpr auto kMinIdleDelay = std::chrono::milliseconds(5);  // Governance floor
    constexpr auto kMaxIdleDelay = std::chrono::milliseconds(50); // Idle ceiling
    auto idleDelay = kMinIdleDelay;

    while (!stop_.load()) {
        bool didWork = false;
        InternalEventBus::KgJob job;
        // Dynamic concurrency limit from TuneAdvisor
        std::size_t maxConcurrent = maxKgConcurrent();
        // Graduated pressure response for CPU-aware throttling
        auto pressureLevel = ResourceGovernor::instance().getPressureLevel();
        switch (pressureLevel) {
            case ResourcePressureLevel::Emergency:
                maxConcurrent = 0; // Halt (backstop for pauseAll)
                break;
            case ResourcePressureLevel::Critical:
                maxConcurrent = 1; // Minimal concurrency
                break;
            case ResourcePressureLevel::Warning:
                maxConcurrent = std::max<std::size_t>(1, maxConcurrent / 2);
                break;
            default:
                break;
        }
        if (kgPaused_.load(std::memory_order_acquire) || maxConcurrent == 0) {
            timer.expires_after(kMinIdleDelay); // Always fast when paused
            co_await timer.async_wait(boost::asio::use_awaitable);
            continue;
        }
        while (kgInFlight_.load() < maxConcurrent && channel->try_pop(job)) {
            didWork = true;
            wasActive_.store(true, std::memory_order_release);
            kgInFlight_.fetch_add(1);
            boost::asio::post(coordinator_->getExecutor(),
                              [this, hash = std::move(job.hash), docId = job.documentId,
                               filePath = std::move(job.filePath), tags = std::move(job.tags),
                               contentBytes = std::move(job.contentBytes)]() mutable {
                                  processKnowledgeGraphStage(hash, docId, filePath, tags,
                                                             std::move(contentBytes));
                                  kgInFlight_.fetch_sub(1);
                                  checkDrainAndSignal();
                              });
        }

        if (didWork) {
            idleDelay = kMinIdleDelay; // Reset on work
            continue;
        }

        // Adaptive backoff when idle
        timer.expires_after(idleDelay);
        co_await timer.async_wait(boost::asio::use_awaitable);
        if (idleDelay < kMaxIdleDelay) {
            idleDelay = std::min(idleDelay * 2, kMaxIdleDelay);
        }
    }

    spdlog::info("[PostIngestQueue] KG poller exited");
}

boost::asio::awaitable<void> PostIngestQueue::symbolPoller() {
    constexpr std::size_t symbolChannelCapacity = 16384;
    auto channel =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::SymbolExtractionJob>(
            "symbol_extraction", symbolChannelCapacity);

    boost::asio::steady_timer timer(co_await boost::asio::this_coro::executor);

    symbolStarted_.store(true);
    spdlog::info("[PostIngestQueue] Symbol extraction poller started");

    // Adaptive backoff for CPU efficiency with governance floor
    constexpr auto kMinIdleDelay = std::chrono::milliseconds(5);  // Governance floor
    constexpr auto kMaxIdleDelay = std::chrono::milliseconds(50); // Idle ceiling
    auto idleDelay = kMinIdleDelay;

    while (!stop_.load()) {
        bool didWork = false;
        InternalEventBus::SymbolExtractionJob job;
        // Dynamic concurrency limit from TuneAdvisor
        std::size_t maxConcurrent = maxSymbolConcurrent();
        // Graduated pressure response for CPU-aware throttling
        auto pressureLevel = ResourceGovernor::instance().getPressureLevel();
        switch (pressureLevel) {
            case ResourcePressureLevel::Emergency:
                maxConcurrent = 0; // Halt (backstop for pauseAll)
                break;
            case ResourcePressureLevel::Critical:
                maxConcurrent = 1; // Minimal concurrency
                break;
            case ResourcePressureLevel::Warning:
                maxConcurrent = std::max<std::size_t>(1, maxConcurrent / 2);
                break;
            default:
                break;
        }
        if (symbolPaused_.load(std::memory_order_acquire) || maxConcurrent == 0) {
            timer.expires_after(kMinIdleDelay); // Always fast when paused
            co_await timer.async_wait(boost::asio::use_awaitable);
            continue;
        }
        while (symbolInFlight_.load() < maxConcurrent && channel->try_pop(job)) {
            didWork = true;
            wasActive_.store(true, std::memory_order_release);
            symbolInFlight_.fetch_add(1);
            boost::asio::post(coordinator_->getExecutor(),
                              [this, hash = std::move(job.hash), docId = job.documentId,
                               filePath = std::move(job.filePath),
                               language = std::move(job.language),
                               contentBytes = std::move(job.contentBytes)]() mutable {
                                  processSymbolExtractionStage(hash, docId, filePath, language,
                                                               std::move(contentBytes));
                                  symbolInFlight_.fetch_sub(1);
                                  checkDrainAndSignal();
                              });
        }

        if (didWork) {
            idleDelay = kMinIdleDelay; // Reset on work
            continue;
        }

        // Adaptive backoff when idle
        timer.expires_after(idleDelay);
        co_await timer.async_wait(boost::asio::use_awaitable);
        if (idleDelay < kMaxIdleDelay) {
            idleDelay = std::min(idleDelay * 2, kMaxIdleDelay);
        }
    }

    spdlog::info("[PostIngestQueue] Symbol extraction poller exited");
}

void PostIngestQueue::dispatchToSymbolChannel(
    const std::string& hash, int64_t docId, const std::string& filePath,
    const std::string& language, std::shared_ptr<std::vector<std::byte>> contentBytes) {
    constexpr std::size_t symbolChannelCapacity = 16384;
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
        InternalEventBus::instance().incSymbolConsumed();
    } catch (const std::exception& e) {
        spdlog::error("[PostIngestQueue] Symbol extraction failed for {}: {}", hash, e.what());
    }
}

void PostIngestQueue::dispatchToEntityChannel(
    const std::string& hash, int64_t docId, const std::string& filePath,
    const std::string& extension, std::shared_ptr<std::vector<std::byte>> contentBytes) {
    constexpr std::size_t entityChannelCapacity = 4096;
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
    constexpr std::size_t entityChannelCapacity = 4096;
    auto channel =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::EntityExtractionJob>(
            "entity_extraction", entityChannelCapacity);

    boost::asio::steady_timer timer(co_await boost::asio::this_coro::executor);

    entityStarted_.store(true);
    spdlog::info("[PostIngestQueue] Entity extraction poller started");

    // Adaptive backoff for CPU efficiency with governance floor
    constexpr auto kMinIdleDelay = std::chrono::milliseconds(5);  // Governance floor
    constexpr auto kMaxIdleDelay = std::chrono::milliseconds(50); // Idle ceiling
    auto idleDelay = kMinIdleDelay;

    while (!stop_.load()) {
        bool didWork = false;
        InternalEventBus::EntityExtractionJob job;
        // Dynamic concurrency limit from TuneAdvisor
        std::size_t maxConcurrent = maxEntityConcurrent();
        // Graduated pressure response for CPU-aware throttling
        auto pressureLevel = ResourceGovernor::instance().getPressureLevel();
        switch (pressureLevel) {
            case ResourcePressureLevel::Emergency:
                maxConcurrent = 0; // Halt (backstop for pauseAll)
                break;
            case ResourcePressureLevel::Critical:
                maxConcurrent = 1; // Minimal concurrency
                break;
            case ResourcePressureLevel::Warning:
                maxConcurrent = std::max<std::size_t>(1, maxConcurrent / 2);
                break;
            default:
                break;
        }
        if (entityPaused_.load(std::memory_order_acquire) || maxConcurrent == 0) {
            timer.expires_after(kMinIdleDelay); // Always fast when paused
            co_await timer.async_wait(boost::asio::use_awaitable);
            continue;
        }
        while (entityInFlight_.load() < maxConcurrent && channel->try_pop(job)) {
            didWork = true;
            wasActive_.store(true, std::memory_order_release);
            entityInFlight_.fetch_add(1);
            auto entityExec = entityCoordinator_ ? entityCoordinator_->getExecutor()
                                                 : coordinator_->getExecutor();
            boost::asio::post(entityExec, [this, hash = std::move(job.hash), docId = job.documentId,
                                           filePath = std::move(job.filePath),
                                           extension = std::move(job.extension),
                                           contentBytes = std::move(job.contentBytes)]() mutable {
                processEntityExtractionStage(hash, docId, filePath, extension,
                                             std::move(contentBytes));
                entityInFlight_.fetch_sub(1);
                checkDrainAndSignal();
            });
        }

        if (didWork) {
            idleDelay = kMinIdleDelay; // Reset on work
            continue;
        }

        // Adaptive backoff when idle
        timer.expires_after(idleDelay);
        co_await timer.async_wait(boost::asio::use_awaitable);
        if (idleDelay < kMaxIdleDelay) {
            idleDelay = std::min(idleDelay * 2, kMaxIdleDelay);
        }
    }

    spdlog::info("[PostIngestQueue] Entity extraction poller exited");
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
    constexpr std::size_t titleChannelCapacity = 4096;
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
    constexpr std::size_t titleChannelCapacity = 4096;
    auto channel =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::TitleExtractionJob>(
            "title_extraction", titleChannelCapacity);

    boost::asio::steady_timer timer(co_await boost::asio::this_coro::executor);

    titleStarted_.store(true);
    spdlog::info("[PostIngestQueue] Title extraction poller started");

    // Adaptive backoff for CPU efficiency with governance floor
    constexpr auto kMinIdleDelay = std::chrono::milliseconds(5);  // Governance floor
    constexpr auto kMaxIdleDelay = std::chrono::milliseconds(50); // Idle ceiling
    auto idleDelay = kMinIdleDelay;

    while (!stop_.load()) {
        bool didWork = false;
        InternalEventBus::TitleExtractionJob job;
        // Dynamic concurrency limit
        std::size_t maxConcurrent = maxTitleConcurrent();
        // Graduated pressure response for CPU-aware throttling
        auto pressureLevel = ResourceGovernor::instance().getPressureLevel();
        switch (pressureLevel) {
            case ResourcePressureLevel::Emergency:
                maxConcurrent = 0; // Halt (backstop for pauseAll)
                break;
            case ResourcePressureLevel::Critical:
                maxConcurrent = 1; // Minimal concurrency
                break;
            case ResourcePressureLevel::Warning:
                maxConcurrent = std::max<std::size_t>(1, maxConcurrent / 2);
                break;
            default:
                break;
        }
        if (titlePaused_.load(std::memory_order_acquire) || maxConcurrent == 0) {
            timer.expires_after(kMinIdleDelay); // Always fast when paused
            co_await timer.async_wait(boost::asio::use_awaitable);
            continue;
        }
        while (titleInFlight_.load() < maxConcurrent && channel->try_pop(job)) {
            didWork = true;
            wasActive_.store(true, std::memory_order_release);
            titleInFlight_.fetch_add(1);
            boost::asio::post(
                coordinator_->getExecutor(),
                [this, hash = std::move(job.hash), docId = job.documentId,
                 textSnippet = std::move(job.textSnippet),
                 fallbackTitle = std::move(job.fallbackTitle), filePath = std::move(job.filePath),
                 language = std::move(job.language), mimeType = std::move(job.mimeType)]() {
                    processTitleExtractionStage(hash, docId, textSnippet, fallbackTitle, filePath,
                                                language, mimeType);
                    titleInFlight_.fetch_sub(1);
                    checkDrainAndSignal();
                });
        }

        if (didWork) {
            idleDelay = kMinIdleDelay; // Reset on work
            continue;
        }

        // Adaptive backoff when idle
        timer.expires_after(idleDelay);
        co_await timer.async_wait(boost::asio::use_awaitable);
        if (idleDelay < kMaxIdleDelay) {
            idleDelay = std::min(idleDelay * 2, kMaxIdleDelay);
        }
    }

    spdlog::info("[PostIngestQueue] Title extraction poller exited");
}

void PostIngestQueue::processTitleExtractionStage(const std::string& hash, int64_t docId,
                                                  const std::string& textSnippet,
                                                  const std::string& fallbackTitle,
                                                  const std::string& filePath,
                                                  const std::string& language,
                                                  const std::string& mimeType) {
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

} // namespace yams::daemon
