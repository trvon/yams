#include <yams/daemon/ipc/retrieval_session.h>

namespace yams::daemon {

uint64_t RetrievalSessionManager::create(std::vector<std::byte>&& bytes, uint32_t chunkSize,
                                         uint64_t maxBytes) {
    std::scoped_lock lk(mu_);
    uint64_t id = nextId_++;
    Session s;
    s.totalSize = bytes.size();
    s.maxBytes = maxBytes;
    s.chunkSize = chunkSize;
    s.createdAt = std::chrono::steady_clock::now();
    s.data = std::move(bytes);
    sessions_.emplace(id, std::move(s));
    return id;
}

Result<std::string> RetrievalSessionManager::chunk(uint64_t transferId, uint64_t offset,
                                                   uint32_t length, uint64_t& bytesRemaining) {
    std::scoped_lock lk(mu_);
    auto it = sessions_.find(transferId);
    if (it == sessions_.end()) {
        return Error{ErrorCode::NotFound, "Invalid transferId"};
    }
    const auto& s = it->second;
    if (offset > s.totalSize) {
        return Error{ErrorCode::InvalidArgument, "Offset beyond total size"};
    }
    uint64_t cap = s.maxBytes > 0 ? std::min<uint64_t>(s.maxBytes, s.totalSize) : s.totalSize;
    if (offset >= cap) {
        bytesRemaining = 0;
        return std::string{}; // empty chunk
    }
    uint64_t maxLen = cap - offset;
    uint64_t len = std::min<uint64_t>(length, maxLen);
    std::string out;
    out.resize(static_cast<size_t>(len));
    std::memcpy(out.data(), reinterpret_cast<const char*>(s.data.data()) + offset,
                static_cast<size_t>(len));
    bytesRemaining = (cap - (offset + len));
    return out;
}

void RetrievalSessionManager::end(uint64_t transferId) {
    std::scoped_lock lk(mu_);
    sessions_.erase(transferId);
}

void RetrievalSessionManager::cleanupExpired(std::chrono::seconds ttl) {
    std::scoped_lock lk(mu_);
    auto now = std::chrono::steady_clock::now();
    for (auto it = sessions_.begin(); it != sessions_.end();) {
        if (now - it->second.createdAt > ttl) {
            it = sessions_.erase(it);
        } else {
            ++it;
        }
    }
}

std::optional<RetrievalSessionManager::Session> RetrievalSessionManager::get(uint64_t transferId) {
    std::scoped_lock lk(mu_);
    auto it = sessions_.find(transferId);
    if (it == sessions_.end())
        return std::nullopt;
    return it->second;
}

} // namespace yams::daemon
