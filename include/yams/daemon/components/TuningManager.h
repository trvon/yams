#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <string>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/strand.hpp>

namespace yams::daemon {

class ServiceManager;
class WorkCoordinator;
struct StateComponent;

// Centralized tuning controller owned by ServiceManager.
// Periodically reads metrics and TuneAdvisor policies and coordinates
// resource allocation across daemon subsystems (IPC CPU/IO pools, writer budgets, etc.).
class TuningManager {
public:
    enum class PostIngestScaleTestMode : uint8_t {
        Normal = 0,
        ForceBusy,
        ForceIdle,
    };

    TuningManager(ServiceManager* sm, StateComponent* state, WorkCoordinator* coordinator);
    ~TuningManager();

    void start();
    void stop();

    // Optional hook: invoked to adjust RepairCoordinator tokens/batch
    void setRepairControlHook(std::function<void(uint32_t tokens, uint32_t batch)> cb) {
        setRepair_ = std::move(cb);
    }

    void setWriterBudgetHook(std::function<void(std::size_t)> cb) {
        setWriterBudget_ = std::move(cb);
    }

    // Optional hook: invoked to resize SocketServer connection slots (PBI-085)
    void setConnectionSlotsHook(std::function<void(std::size_t)> cb) {
        setConnectionSlots_ = std::move(cb);
    }

    static void testing_rebalanceTargetsByQueue(std::array<uint32_t, 6>& targets,
                                                const std::array<uint32_t, 6>& floors,
                                                const std::array<std::size_t, 6>& queueDepths,
                                                const std::array<bool, 6>& active);
    static uint32_t testing_computeEmbedScaleBias(std::size_t embedQueued,
                                                  uint64_t embedDroppedDelta,
                                                  std::size_t postQueued,
                                                  std::size_t embedInFlight);
    static int32_t testing_computeContentionBudgetAdjustment(
        std::size_t waitingRequests, std::uint64_t waitMicrosDelta, std::size_t timeoutDelta,
        std::size_t failedDelta, std::size_t processedDelta, std::uint32_t healthyTicks);
    static void testing_setPostIngestScaleTestMode(PostIngestScaleTestMode mode);
    static PostIngestScaleTestMode testing_postIngestScaleTestMode();
    static bool testing_shouldAllowZeroPostIngestTargets(bool daemonIdle, bool postIngestBusy);

private:
    boost::asio::awaitable<void> tuningLoop();
    void tick_once();
    static void rebalanceTargetsByQueue(std::array<uint32_t, 6>& targets,
                                        const std::array<uint32_t, 6>& floors,
                                        const std::array<std::size_t, 6>& queueDepths,
                                        const std::array<bool, 6>& active);
    static uint32_t computeEmbedScaleBias(std::size_t embedQueued, uint64_t embedDroppedDelta,
                                          std::size_t postQueued, std::size_t embedInFlight);
    static int32_t
    computeContentionBudgetAdjustment(std::size_t waitingRequests, std::uint64_t waitMicrosDelta,
                                      std::size_t timeoutDelta, std::size_t failedDelta,
                                      std::size_t processedDelta, std::uint32_t healthyTicks);
    static bool shouldAllowZeroPostIngestTargets(bool daemonIdle, bool postIngestBusy);

    ServiceManager* sm_;
    StateComponent* state_;
    WorkCoordinator* coordinator_;
    boost::asio::strand<boost::asio::io_context::executor_type> strand_;
    std::atomic<bool> running_{false};
    std::future<void> tuningFuture_{};

    std::function<void(uint32_t, uint32_t)> setRepair_{};
    std::chrono::steady_clock::time_point repairBusySince_{};
    std::chrono::steady_clock::time_point repairReadySince_{};
    std::chrono::steady_clock::time_point repairRateWindowStart_{};
    uint64_t repairBatchesAtWindowStart_{0};

    std::function<void(std::size_t)> setWriterBudget_{};

    // Connection slot resizing hook (PBI-085)
    std::function<void(std::size_t)> setConnectionSlots_{};

    // ONNX concurrency registry configuration tracking
    std::atomic<bool> onnxRegistryConfigured_{false};
    void configureOnnxConcurrencyRegistry();

    // Issue 3 fix: hysteresis counters as members instead of static locals
    // so they reset across start/stop cycles and pressure transitions.

    // ONNX lane tracking (configureOnnxConcurrencyRegistry)
    uint32_t lastOnnxMax_{0};
    uint32_t lastOnnxGliner_{0};
    uint32_t lastOnnxEmbed_{0};
    uint32_t lastOnnxReranker_{0};

    // Pool resizing hysteresis (tick_once pool section)
    uint32_t ipcHighTicks_{0};
    uint32_t ipcLowTicks_{0};
    uint32_t ioHighTicks_{0};
    uint32_t ioLowTicks_{0};

    // Connection slot resizing hysteresis (PBI-085)
    uint32_t slotHighTicks_{0};
    uint32_t slotLowTicks_{0};

    // Issue 6 fix: track previous pressure level for de-escalation detection
    uint8_t previousPressureLevel_{0};

    // Track cumulative bus drop counter to derive per-tick delta.
    uint64_t previousEmbedDropped_{0};

    // Track previous counters for contention-aware post-ingest budget tuning.
    std::size_t previousPostProcessed_{0};
    std::uint64_t previousWriteWaitMicros_{0};
    std::uint64_t previousReadWaitMicros_{0};
    std::size_t previousWriteTimeoutCount_{0};
    std::size_t previousReadTimeoutCount_{0};
    std::size_t previousWriteFailedAcquisitions_{0};
    std::size_t previousReadFailedAcquisitions_{0};
    std::uint32_t contentionHealthyTicks_{0};
};

} // namespace yams::daemon
