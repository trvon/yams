#pragma once

// Host-side helper: forward OnnxConcurrencyRegistry diagnostics into this
// binary's spdlog default logger. Header-only on purpose so the spdlog calls
// compile into the host, never into libyams_onnx_resource.

#include <spdlog/spdlog.h>
#include <yams/daemon/resource/OnnxConcurrencyRegistry.h>

namespace yams::daemon {

inline void forwardOnnxRegistryLogToSpdlog(OnnxRegistryLogLevel level,
                                           const char* message) noexcept {
    auto spdLevel = spdlog::level::debug;
    switch (level) {
        case OnnxRegistryLogLevel::Debug:
            spdLevel = spdlog::level::debug;
            break;
        case OnnxRegistryLogLevel::Info:
            spdLevel = spdlog::level::info;
            break;
        case OnnxRegistryLogLevel::Warn:
            spdLevel = spdlog::level::warn;
            break;
    }
    try {
        spdlog::default_logger_raw()->log(spdLevel, message);
    } catch (...) {
    }
}

inline void installOnnxRegistrySpdlogSink() noexcept {
    OnnxConcurrencyRegistry::setLogSink(&forwardOnnxRegistryLogToSpdlog);
}

} // namespace yams::daemon
