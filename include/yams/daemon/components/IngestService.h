#pragma once

#include <atomic>
#include <yams/app/services/services.hpp>
#include <yams/compat/thread_stop_compat.h>

namespace yams::daemon {

class ServiceManager;

class IngestService {
public:
    IngestService(ServiceManager* sm, size_t threads = 1);
    ~IngestService();

    void start();
    void stop();

private:
    void workerLoop(yams::compat::stop_token token);

    ServiceManager* sm_;
    std::vector<yams::compat::jthread> threads_;
    std::atomic<bool> stop_{false};
};

} // namespace yams::daemon
