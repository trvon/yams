// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors
#pragma once

#include <boost/asio/error.hpp>
#include <boost/system/error_code.hpp>

#include <string>

namespace yams::daemon::detail {

// Errors that mean the daemon went away (stopped or restarted), not a fault in this process.
// Linux usually reports EPIPE or ECONNRESET; macOS often reports ENOTCONN or ECONNABORTED.
inline bool isExpectedDisconnectError(const boost::system::error_code& ec) {
    if (ec == boost::asio::error::broken_pipe || ec == boost::asio::error::connection_reset ||
        ec == boost::asio::error::connection_aborted || ec == boost::asio::error::not_connected ||
        ec == boost::asio::error::shut_down || ec == boost::asio::error::eof) {
        return true;
    }
    const auto msg = ec.message();
    return msg.find("Broken pipe") != std::string::npos ||
           msg.find("Connection reset") != std::string::npos ||
           msg.find("End of file") != std::string::npos || msg.find("EPIPE") != std::string::npos ||
           msg.find("ECONNRESET") != std::string::npos;
}

// Error text for a failed write. The OS text alone ("Socket is not connected") does not say
// that the connection is the thing that failed, so lead with that and keep the OS detail.
inline std::string describeWriteError(const boost::system::error_code& ec) {
    return std::string(isExpectedDisconnectError(ec) ? "Connection closed by the daemon: "
                                                     : "Connection write failed: ") +
           ec.message();
}

// Detail for a failed read. The read loop hands this to every request waiting on the
// connection, so a daemon that stopped must say so; the OS text stays because callers match it
// ("End of file").
inline std::string describeReadError(const boost::system::error_code& ec) {
    return isExpectedDisconnectError(ec) ? "Connection closed by the daemon: " + ec.message()
                                         : ec.message();
}

} // namespace yams::daemon::detail
