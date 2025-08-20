#pragma once

#include <yams/core/types.h>

#include <cstdint>
#include <mutex>
#include <unordered_map>
#include <vector>

namespace yams::daemon {

class RetrievalSessionManager {
public:
    struct Session {
        std::vector<std::byte> data;
        uint64_t totalSize = 0;
        uint64_t maxBytes = 0; // 0 = unlimited
        uint32_t chunkSize = 256 * 1024;
        std::chrono::steady_clock::time_point createdAt;
    };

    RetrievalSessionManager() = default;

    uint64_t create(std::vector<std::byte>&& bytes, uint32_t chunkSize, uint64_t maxBytes);
    Result<std::string> chunk(uint64_t transferId, uint64_t offset, uint32_t length,
                              uint64_t& bytesRemaining);
    void end(uint64_t transferId);
    void cleanupExpired(std::chrono::seconds ttl = std::chrono::seconds(60));

    std::optional<Session> get(uint64_t transferId);

private:
    std::mutex mu_;
    std::unordered_map<uint64_t, Session> sessions_;
    uint64_t nextId_ = 1;
};

} // namespace yams::daemon
