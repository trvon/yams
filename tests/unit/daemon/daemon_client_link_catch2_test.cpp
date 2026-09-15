// Copyright (c) 2026 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <catch2/catch_test_macros.hpp>
#include <yams/daemon/client/global_io_context.h>

// Deliberately links only the public daemon-client dependency, not the broad daemon/test
// dependency bundle: extraction of implementation bodies must preserve this consumer seam.
TEST_CASE("daemon client exports its standalone dependencies", "[unit][daemon][client-link]") {
    CHECK_FALSE(yams::daemon::GlobalIOContext::instance().get_io_context().stopped());
}
