/*
 * yams/src/downloader/resume_store.cpp
 *
 * In-memory ResumeStore implementation (per-run).
 *
 * - Implements yams::downloader::IResumeStore
 * - Stores ETag/Last-Modified and completed range map in an in-memory table
 * - Thread-safe with shared_mutex
 * - Intended as a simple MVP to enable resume logic; persistence is stubbed as TODO
 *
 * Persistence (future work):
 * - Optionally persist state under [storage.staging_dir]/downloader/resume.db (JSON or SQLite)
 * - Merge-on-load and prune on finalize
 */

#include <yams/downloader/downloader.hpp>

#include <spdlog/spdlog.h>

#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace yams::downloader {

class InMemoryResumeStore final : public IResumeStore {
public:
    InMemoryResumeStore() = default;
    ~InMemoryResumeStore() override = default;

    Expected<std::optional<State>> load(std::string_view url) override {
        if (url.empty()) {
            return Error{ErrorCode::InvalidArgument, "ResumeStore.load: empty url"};
        }

        std::shared_lock lk(mutex_);
        auto it = table_.find(std::string(url));
        if (it == table_.end()) {
            return std::optional<State>{std::nullopt};
        }
        return std::optional<State>{it->second};
    }

    Expected<void> save(std::string_view url, const State& state) override {
        if (url.empty()) {
            return Error{ErrorCode::InvalidArgument, "ResumeStore.save: empty url"};
        }

        {
            std::unique_lock lk(mutex_);
            table_[std::string(url)] = state;
        }

        spdlog::debug(
            "ResumeStore: saved state for url='{}' (totalBytes={}, etag={}, lastModified={})", url,
            state.totalBytes, state.etag.value_or(""), state.lastModified.value_or(""));
        return Expected<void>{};
    }

    void remove(std::string_view url) noexcept override {
        if (url.empty()) {
            return;
        }
        std::unique_lock lk(mutex_);
        (void)table_.erase(std::string(url));
    }

private:
    std::unordered_map<std::string, State> table_;
    mutable std::shared_mutex mutex_;
};

// Optional factory for convenience
std::unique_ptr<IResumeStore> makeInMemoryResumeStore() {
    return std::make_unique<InMemoryResumeStore>();
}

} // namespace yams::downloader