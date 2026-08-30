// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors
#pragma once

// Internal parser seams shared by the direct-P2P fuzz harnesses and focused tests.

// pi-lens-ignore: fatal error
#include <yams/core/types.h>

#include <cstddef>
#include <span>

namespace yams::daemon::p2p {

struct DeltaExchangeOptions;

namespace detail {

Result<void> validateHandshakeControlFrame(std::span<const std::byte> frame);
Result<void> validateDeltaControlFrame(std::span<const std::byte> frame,
                                       const DeltaExchangeOptions& options);

} // namespace detail
} // namespace yams::daemon::p2p
