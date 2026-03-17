#pragma once

#include <atomic>
#include <memory>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/strand.hpp>
#include <taskflow/taskflow.hpp>

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
    std::atomic<bool> running_{false};
    std::unique_ptr<tf::Executor> tfExecutor_;
};

} // namespace yams::daemon
