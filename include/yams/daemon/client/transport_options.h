#pragma once

#include <chrono>
#include <filesystem>

namespace yams::daemon {

struct TransportOptions {
    std::filesystem::path socketPath;
    std::chrono::milliseconds headerTimeout{30000};
    std::chrono::milliseconds bodyTimeout{60000};
    std::chrono::milliseconds requestTimeout{5000};
    std::size_t maxInflight{128};
    bool poolEnabled{true};
};

} // namespace yams::daemon
