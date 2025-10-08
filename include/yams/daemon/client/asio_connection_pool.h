#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include <boost/asio/awaitable.hpp>

#include <yams/daemon/client/asio_connection.h>
#include <yams/daemon/client/transport_options.h>

namespace yams::daemon {

// Lightweight pool that reuses a single AsioConnection per socket path when pooling is enabled.
class AsioConnectionPool : public std::enable_shared_from_this<AsioConnectionPool> {
public:
    static std::shared_ptr<AsioConnectionPool> get_or_create(const TransportOptions& opts);

    boost::asio::awaitable<std::shared_ptr<AsioConnection>> acquire();

    AsioConnectionPool(const TransportOptions& opts, bool shared);

private:
    boost::asio::awaitable<std::shared_ptr<AsioConnection>> create_connection();

    TransportOptions opts_;
    bool shared_{true};
    std::mutex mutex_;
    std::shared_ptr<AsioConnection> cachedStrong_;
    std::weak_ptr<AsioConnection> cached_;
};

} // namespace yams::daemon
