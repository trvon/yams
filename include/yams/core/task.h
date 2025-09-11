#pragma once

#include <boost/asio/awaitable.hpp>

namespace yams {

// Canonical coroutine type alias for the codebase: use Boost.Asio awaitable
template <typename T> using Task = boost::asio::awaitable<T>;

} // namespace yams

// Provide alias into yams::daemon so existing unqualified uses resolve
namespace yams::daemon {
template <typename T = void> using Task = ::yams::Task<T>;
}
