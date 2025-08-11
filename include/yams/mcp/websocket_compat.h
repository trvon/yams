#pragma once

// WebSocketPP compatibility with newer Boost.Asio (1.70.0+)
// This header provides compatibility shims for WebSocketPP with modern Boost versions

#include <boost/version.hpp>
#include <boost/asio.hpp>
#include <memory>
#include <chrono>

// Only apply compatibility fixes for Boost 1.70.0 and later
#if BOOST_VERSION >= 107000

// Include this before WebSocketPP headers to apply fixes
#ifndef WEBSOCKETPP_BOOST_ASIO_COMPAT_H
#define WEBSOCKETPP_BOOST_ASIO_COMPAT_H

namespace boost {
namespace asio {

// 1. io_service was renamed to io_context
#ifndef BOOST_ASIO_HAS_IO_SERVICE
using io_service = io_context;
#endif

// 2. Fix strand_ptr definition for WebSocketPP
// WebSocketPP expects strand_ptr to be a shared_ptr to strand
template<typename Executor>
using strand_ptr = std::shared_ptr<strand<Executor>>;

// For backward compatibility, define the old strand_ptr as int
// This is what WebSocketPP transport code expects to see
namespace transport {
namespace asio {
using strand_ptr = int;  // WebSocketPP uses this as a flag/type indicator
} // namespace asio
} // namespace transport

// 3. Timer compatibility - expires_from_now() was deprecated
// Create a wrapper function that WebSocketPP can use
template<typename Timer>
auto expires_from_now(Timer& timer) -> typename Timer::duration {
    auto now = Timer::clock_type::now();
    auto expiry = timer.expiry();
    return std::chrono::duration_cast<typename Timer::duration>(expiry - now);
}

// 4. is_neg function for WebSocketPP timer handling
template<typename Duration>
bool is_neg(const Duration& d) {
    return d.count() < 0;
}

} // namespace asio
} // namespace boost

// 5. Preprocessor macros to patch WebSocketPP transport code
// These need to be defined before including WebSocketPP headers

// Fix the io_service_ptr type definition
#define WEBSOCKETPP_BOOST_ASIO_IO_SERVICE_PTR_TYPE boost::asio::io_context*

// Fix strand operations - make them no-ops since we're not using strands
#define WEBSOCKETPP_TRANSPORT_ASIO_STRAND_WRAP(handler) handler
#define WEBSOCKETPP_TRANSPORT_ASIO_STRAND_POST(io_service, handler) io_service->post(handler)

// Mark that compatibility layer is active
#define WEBSOCKETPP_BOOST_ASIO_COMPAT_ACTIVE

#endif // WEBSOCKETPP_BOOST_ASIO_COMPAT_H
#endif // BOOST_VERSION >= 107000