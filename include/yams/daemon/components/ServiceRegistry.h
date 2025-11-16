#pragma once

#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <typeindex>
#include <unordered_map>
#include <vector>
#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/awaitable.hpp>

namespace yams {
namespace daemon {

enum class HealthState { Starting, Ready, Degraded, Failed, Stopped };

struct Health {
    HealthState state{HealthState::Stopped};
    std::string reason;
};

struct StartPolicy {
    bool autoStart{true};
    bool restartOnFailure{true};
    std::chrono::milliseconds backoffMin{std::chrono::milliseconds(250)};
    std::chrono::milliseconds backoffMax{std::chrono::milliseconds(5000)};
};

// Unconstrained template APIs for broad compatibility; conformance checked at use sites.

class ServiceRegistry {
public:
    ServiceRegistry() = default;
    ~ServiceRegistry() = default;

    // Register a service with optional dependencies and start policy
    template <typename Svc>
    void register_service(std::shared_ptr<Svc> svc, std::vector<std::string> deps = {},
                          StartPolicy policy = {}) {
        std::lock_guard<std::mutex> lk(mtx_);
        Entry e;
        e.ptr = std::move(svc);
        e.deps = std::move(deps);
        e.policy = policy;
        e.name = typeid(Svc).name(); // Use type name as service name
        services_[std::type_index(typeid(Svc))] = std::move(e);
    }

    // Retrieve a service by type
    template <typename Svc> std::shared_ptr<Svc> get() const {
        std::lock_guard<std::mutex> lk(mtx_);
        auto it = services_.find(std::type_index(typeid(Svc)));
        if (it == services_.end())
            return {};
        return std::static_pointer_cast<Svc>(it->second.ptr);
    }

    // Coroutine: start all registered services respecting dependencies and bounded parallelism
    boost::asio::awaitable<void>
    co_start_all(boost::asio::any_io_executor ex,
                 std::size_t max_parallel = std::thread::hardware_concurrency());

    // Request routing stubs (future implementation)
    template <typename Request, typename Handler>
    void register_handler(std::string_view /*serviceName*/, Handler&& /*h*/) {
        // TODO: PBI-010-09
    }

    template <typename Request> auto route(const Request& /*req*/) -> boost::asio::awaitable<void> {
        // TODO: PBI-010-10
        co_return;
    }

private:
    struct Entry {
        std::shared_ptr<void> ptr;
        std::vector<std::string> deps;
        StartPolicy policy;
        std::string name;
    };

    mutable std::mutex mtx_;
    std::unordered_map<std::type_index, Entry> services_;
};

} // namespace daemon
} // namespace yams
