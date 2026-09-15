#pragma once

// Shared client transport types. Extracted from daemon_client.h so that
// modules which only need transport configuration (e.g. sandbox_detection)
// do not have to depend on the full DaemonClient declaration (Brick audit
// lead #3: daemon_client <-> sandbox_detection type-level cycle).
#include <chrono>
#include <cstddef>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>

#include <boost/asio/any_io_executor.hpp>

namespace yams::daemon {

class IClientTransport;

enum class ClientTransportMode {
    Auto,
    Socket,
    InProcess,
};

struct ClientConfig {
    std::filesystem::path socketPath;
    std::filesystem::path proxySocketPath;
    std::filesystem::path pidFile;
    std::filesystem::path dataDir;
    std::filesystem::path daemonBinary;
    std::filesystem::path configPath;
    std::string logLevel;
    std::chrono::milliseconds connectTimeout{1000};
    std::chrono::milliseconds headerTimeout{30000};
    std::chrono::milliseconds bodyTimeout{60000};
    std::chrono::milliseconds requestTimeout{5000};
    size_t maxRetries = 3;
    std::chrono::milliseconds retryBaseDelay{75};
    bool autoStart = true;
    bool enableCircuitBreaker = true;
    bool enableChunkedResponses = true;
    size_t maxChunkSize = static_cast<size_t>(512) * static_cast<size_t>(1024);
    size_t maxInflight = 128;
    bool progressiveOutput = true;
    bool singleUseConnections = false;
    bool disableStreamingForLargeQueries = false;
    bool acceptCompressed = false;
    ClientTransportMode transportMode = ClientTransportMode::Auto;
    std::optional<boost::asio::any_io_executor> executor;
    std::shared_ptr<IClientTransport> transport;
};

} // namespace yams::daemon
