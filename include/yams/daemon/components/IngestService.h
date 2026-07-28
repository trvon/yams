#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/strand.hpp>

namespace yams::daemon {

class ServiceManager;
class WorkCoordinator;

class IngestService {
public:
    IngestService(ServiceManager* sm, WorkCoordinator* coordinator);
    ~IngestService();

    void start();
    void stop();

#ifdef YAMS_TESTING
    static std::uint32_t testing_resolveStoreBatchLimit(std::uint32_t configuredBatchSize,
                                                        double cpuUsagePercent, bool canAdmitWork,
                                                        bool correctnessMode, bool backlogHigh) {
        return resolveStoreBatchLimit(configuredBatchSize, cpuUsagePercent, canAdmitWork,
                                      correctnessMode, backlogHigh);
    }
#endif

private:
    static std::uint32_t resolveStoreBatchLimit(std::uint32_t configuredBatchSize,
                                                double cpuUsagePercent, bool canAdmitWork,
                                                bool correctnessMode, bool backlogHigh);
    boost::asio::awaitable<void> channelPoller();
    void notifyLifecycle();

    ServiceManager* sm_;
    WorkCoordinator* coordinator_;
    boost::asio::strand<boost::asio::io_context::executor_type> strand_;
    std::atomic<bool> stop_{false};
    std::atomic<bool> startGuard_{false};
    std::atomic<bool> running_{false};
    std::mutex lifecycleMutex_;
    std::condition_variable lifecycleCv_;
};

} // namespace yams::daemon
