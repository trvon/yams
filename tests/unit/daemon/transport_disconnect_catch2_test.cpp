// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

#include <catch2/catch_test_macros.hpp>

#include <boost/asio/error.hpp>
#include <boost/system/error_code.hpp>

#include "../../../src/daemon/client/transport_disconnect.h"

using yams::daemon::detail::describeReadError;
using yams::daemon::detail::describeWriteError;
using yams::daemon::detail::isExpectedDisconnectError;

TEST_CASE("A daemon that goes away mid-write is an expected disconnect",
          "[daemon][client][transport]") {
    // Linux reports EPIPE or ECONNRESET; macOS often reports ENOTCONN or ECONNABORTED.
    for (auto ec : {boost::asio::error::broken_pipe, boost::asio::error::connection_reset,
                    boost::asio::error::not_connected, boost::asio::error::connection_aborted,
                    boost::asio::error::shut_down}) {
        INFO(boost::system::error_code(ec).message());
        CHECK(isExpectedDisconnectError(ec));
    }
    CHECK(isExpectedDisconnectError(boost::asio::error::eof));
    CHECK_FALSE(isExpectedDisconnectError(boost::asio::error::access_denied));
    CHECK_FALSE(isExpectedDisconnectError(boost::asio::error::no_buffer_space));
}

TEST_CASE("A failed write says the connection closed and keeps the OS detail",
          "[daemon][client][transport]") {
    const boost::system::error_code notConnected = boost::asio::error::not_connected;
    const auto closed = describeWriteError(notConnected);
    CHECK(closed.find("Connection closed") != std::string::npos);
    CHECK(closed.find(notConnected.message()) != std::string::npos);

    const boost::system::error_code denied = boost::asio::error::access_denied;
    const auto failed = describeWriteError(denied);
    CHECK(failed.find("Connection write failed") != std::string::npos);
    CHECK(failed.find(denied.message()) != std::string::npos);
}

TEST_CASE("A failed read says the daemon closed the connection and keeps the OS detail",
          "[daemon][client][transport]") {
    // macOS reports a daemon that stops mid-request as EOF or ENOTCONN on the read side; the
    // read loop passes this text to every waiting request.
    // eof is a misc_errors value, the others basic_errors, so name the element type.
    for (const boost::system::error_code code :
         {boost::system::error_code(boost::asio::error::eof),
          boost::system::error_code(boost::asio::error::not_connected),
          boost::system::error_code(boost::asio::error::connection_reset)}) {
        const auto text = describeReadError(code);
        INFO(text);
        CHECK(text.find("Connection closed by the daemon") != std::string::npos);
        CHECK(text.find(code.message()) != std::string::npos);
    }

    const boost::system::error_code denied = boost::asio::error::access_denied;
    CHECK(describeReadError(denied) == denied.message());
}
