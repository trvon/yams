#include <yams/daemon/components/ServiceRegistry.h>

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/this_coro.hpp>

#include <spdlog/spdlog.h>

#include <yams/compat/thread_stop_compat.h>
#include <yams/core/types.h>

// NOTE: sr::result template removed - was causing compilation errors
// PBI-010 implementation will properly define service result types

namespace yams {
namespace daemon {

using yams::Error;
using yams::ErrorCode;
using yams::Result;

boost::asio::awaitable<void> ServiceRegistry::co_start_all(boost::asio::any_io_executor /*ex*/,
                                                           std::size_t /*max_parallel*/) {
    // Minimal stub: startup logic (dependency toposort and bounded parallelism) will be
    // implemented in PBI-010 tasks T-010-02/T-010-03. For now, this is a no-op.
    co_return;
}

} // namespace daemon
} // namespace yams
