// Copyright (c) 2026 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <span>

#include <spdlog/spdlog.h>

#include "startup_fuzz_support.h"

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size) {
    // A standalone fuzz process owns its logger and the process-global tinyfsm.
    spdlog::set_level(spdlog::level::off);
    const auto bytes = std::span<const std::uint8_t>{data, size};
    const auto replay = yams::fuzz::replayStartup(bytes);
    if (!replay.ok()) {
        std::fprintf(stderr, "startup replay failed at byte %zu: %s\n", replay.offset,
                     replay.error);
        std::abort();
    }
    if (!yams::fuzz::semanticScoresBounded(bytes)) {
        std::fprintf(stderr, "startup semantic-score invariant failed\n");
        std::abort();
    }
    return 0;
}
