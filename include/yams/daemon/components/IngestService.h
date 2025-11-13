#pragma once

#include <atomic>
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

private:
    boost::asio::awaitable<void> channelPoller();

    ServiceManager* sm_;
    WorkCoordinator* coordinator_;
    boost::asio::strand<boost::asio::io_context::executor_type> strand_;
    std::atomic<bool> stop_{false};
};

} // namespace yams::daemon
