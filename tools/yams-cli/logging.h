#pragma once

#include <iostream>
#include <memory>
#include <spdlog/sinks/ostream_sink.h>
#include <spdlog/spdlog.h>

namespace yams::cli {

// Install before constructing the CLI: command callbacks and embedded startup
// can emit diagnostics before parsed logging flags have been applied.
// The stream must outlive the logger; production uses the process-owned stderr.
inline void initializeCliLogging(std::ostream& diagnostics = std::cerr) {
    auto sink = std::make_shared<spdlog::sinks::ostream_sink_mt>(diagnostics, true);
    auto logger = std::make_shared<spdlog::logger>("yams-cli", std::move(sink));
    logger->set_level(spdlog::level::warn);
    logger->set_pattern("[%H:%M:%S] [%l] %v");
    spdlog::set_default_logger(std::move(logger));
}

} // namespace yams::cli
