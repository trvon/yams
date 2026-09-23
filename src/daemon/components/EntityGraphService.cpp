#include <yams/daemon/components/EntityGraphService.h>

#include <spdlog/spdlog.h>
#include <algorithm>
#include <chrono>
#include <boost/asio.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <yams/core/assert.hpp>
#include <yams/daemon/components/InternalEventBus.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/daemon/components/TuningManager.h>
#include <yams/daemon/components/TuningSnapshot.h>
#include <yams/daemon/components/WorkCoordinator.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/profiling.h>

namespace yams::daemon {

EntityGraphService::EntityGraphService(ServiceManager* services, std::size_t /*workers*/)
    : services_(services) {}

EntityGraphService::~EntityGraphService() {
    stop();
}

void EntityGraphService::start() {
    YAMS_ZONE_SCOPED_N("EntityGraph::start");
    if (!services_)
        return;
    auto* coordinator = services_->getWorkCoordinator();
    if (!coordinator)
        return;

    stop_.store(false);
    coordinator->spawnDetached(coordinator->getExecutor(), channelPoller());
    spdlog::debug("EntityGraphService: channel poller started");
}

void EntityGraphService::stop() {
    YAMS_ZONE_SCOPED_N("EntityGraph::stop");
    stop_.store(true);
}

boost::asio::awaitable<void> EntityGraphService::channelPoller() {
    YAMS_ZONE_SCOPED_N("EntityGraph::channelPoller");
    constexpr std::size_t kChannelCapacity = 4096;
    auto channel =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::EntityGraphJob>(
            "entity_graph_jobs", kChannelCapacity);

    boost::asio::steady_timer timer(co_await boost::asio::this_coro::executor);

    constexpr auto kMinIdleDelay = std::chrono::milliseconds(1);
    auto idleDelay = kMinIdleDelay;

    auto maxIdleDelay = []() {
        if (auto snap = TuningSnapshotRegistry::instance().get()) {
            if (snap->daemonIdle) {
                return std::chrono::milliseconds(
                    std::max<uint32_t>(TuneAdvisor::idleTickMs(), snap->workerPollMs));
            }
        }
        return std::chrono::milliseconds(10);
    };

    while (!stop_.load(std::memory_order_relaxed)) {
        bool didWork = false;
        InternalEventBus::EntityGraphJob busJob;

        while (channel->try_pop(busJob)) {
            didWork = true;

            Job job;
            job.documentHash = std::move(busJob.documentHash);
            job.filePath = std::move(busJob.filePath);
            job.contentUtf8 = std::move(busJob.contentUtf8);
            job.language = std::move(busJob.language);
            job.mimeType = std::move(busJob.mimeType);
            job.documentDbId = busJob.documentDbId;
            job.knowledgeGraphToken = std::move(busJob.knowledgeGraphToken);
            job.knowledgeGraphCompletion = std::move(busJob.knowledgeGraphCompletion);

            try {
                bool success = process(job);
                if (!success)
                    failed_.fetch_add(1, std::memory_order_relaxed);
            } catch (const std::exception& e) {
                spdlog::error("EntityGraphService: exception processing {}: {}", job.filePath,
                              e.what());
                failed_.fetch_add(1, std::memory_order_relaxed);
            } catch (...) {
                spdlog::error("EntityGraphService: unknown exception processing {}", job.filePath);
                failed_.fetch_add(1, std::memory_order_relaxed);
            }
            processed_.fetch_add(1, std::memory_order_relaxed);
            InternalEventBus::instance().incEntityGraphConsumed();
        }

        if (didWork) {
            idleDelay = kMinIdleDelay;
            continue;
        }

        timer.expires_after(idleDelay);
        co_await timer.async_wait(boost::asio::use_awaitable);
        const auto maxIdle = maxIdleDelay();
        if (idleDelay < maxIdle) {
            idleDelay = std::min(idleDelay * 2, maxIdle);
        }
    }

    spdlog::debug("EntityGraphService: channel poller exited");
}

Result<void> EntityGraphService::submitExtraction(Job job) {
    YAMS_ZONE_SCOPED_N("EntityGraph::submitExtraction");
    if (stop_.load(std::memory_order_relaxed)) {
        return Error{ErrorCode::InvalidState, "service_stopped"};
    }
    if (!services_) {
        return Error{ErrorCode::InternalError, "no_services"};
    }

    accepted_.fetch_add(1, std::memory_order_relaxed);

    // Route through InternalEventBus for centralized backpressure and observability
    auto& bus = InternalEventBus::instance();
    auto filePath = job.filePath; // capture before move
    InternalEventBus::EntityGraphJob busJob;
    busJob.documentHash = std::move(job.documentHash);
    busJob.filePath = std::move(job.filePath);
    busJob.contentUtf8 = std::move(job.contentUtf8);
    busJob.language = std::move(job.language);
    busJob.mimeType = std::move(job.mimeType);
    busJob.documentDbId = job.documentDbId;
    busJob.knowledgeGraphToken = std::move(job.knowledgeGraphToken);
    busJob.knowledgeGraphCompletion = std::move(job.knowledgeGraphCompletion);

    constexpr std::size_t kChannelCapacity = 4096;
    auto channel = bus.get_or_create_channel<InternalEventBus::EntityGraphJob>("entity_graph_jobs",
                                                                               kChannelCapacity);

    if (channel->try_push(std::move(busJob))) {
        bus.incEntityGraphQueued();
        TuningManager::notifyWakeup();
    } else {
        bus.incEntityGraphDropped();
        spdlog::debug("EntityGraphService: channel full, dropping job for {}", filePath);
        accepted_.fetch_sub(1, std::memory_order_relaxed);
        return Error{ErrorCode::ResourceExhausted, "entity_graph_jobs channel full"};
    }

    return Result<void>();
}

EntityGraphService::Stats EntityGraphService::getStats() const {
    YAMS_ZONE_SCOPED_N("EntityGraph::getStats");
    YAMS_DCHECK(processed_.load(std::memory_order_relaxed) +
                        failed_.load(std::memory_order_relaxed) <=
                    accepted_.load(std::memory_order_relaxed) + 100,
                "processed+failed exceeds accepted");
    return {accepted_.load(std::memory_order_relaxed), processed_.load(std::memory_order_relaxed),
            failed_.load(std::memory_order_relaxed)};
}

bool EntityGraphService::process(Job& job) {
    YAMS_ZONE_SCOPED_N("EntityGraph::process");
    if (!services_)
        return false;
    if (!services_->getKgStore()) {
        spdlog::debug("EntityGraphService: no KG store available");
        return job.knowledgeGraphToken.empty();
    }

    // Code-symbol extraction was removed in v0.20 and NL entities are written by the
    // PostIngestQueue title+NL stage, so the graph stage only records its own completion.
    if (job.knowledgeGraphToken.empty())
        return true;
    if (job.knowledgeGraphCompletion &&
        !job.knowledgeGraphCompletion->markCommitted(KnowledgeGraphCompletionStage::Graph)) {
        return true;
    }
    auto repo = services_->getMetadataRepo();
    if (!repo)
        return false;
    return repo->completeKnowledgeGraphEnrichment(job.documentDbId, job.knowledgeGraphToken)
        .has_value();
}

} // namespace yams::daemon
