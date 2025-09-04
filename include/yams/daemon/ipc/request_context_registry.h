#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <unordered_map>

#include <yams/daemon/ipc/request_context.h>

namespace yams {
namespace daemon {

class RequestContextRegistry {
public:
    static inline RequestContextRegistry& instance() noexcept {
        static RequestContextRegistry g;
        return g;
    }

    void register_context(uint64_t request_id, std::weak_ptr<RequestContext> ctx) {
        std::lock_guard<std::mutex> lk(m_);
        map_[request_id] = std::move(ctx);
    }
    void deregister_context(uint64_t request_id) {
        std::lock_guard<std::mutex> lk(m_);
        map_.erase(request_id);
    }
    bool cancel(uint64_t request_id) {
        std::lock_guard<std::mutex> lk(m_);
        auto it = map_.find(request_id);
        if (it == map_.end())
            return false;
        if (auto sp = it->second.lock()) {
            sp->canceled.store(true, std::memory_order_relaxed);
            return true;
        }
        return false;
    }

private:
    std::mutex m_;
    std::unordered_map<uint64_t, std::weak_ptr<RequestContext>> map_;
};

} // namespace daemon
} // namespace yams
