// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <yams/app/services/services.hpp>
#include <yams/app/services/session_service.hpp>
#include <yams/common/gitignore.h>

namespace yams::daemon {

/// Keeps the directories pinned by watch-enabled sessions indexed.
///
/// It polls: each due target is walked, files whose size or modification time changed are
/// submitted for indexing, and files that have been gone for a grace period are removed
/// from the index. What it has seen is kept per target and persisted, so a daemon restart
/// does not resubmit a tree it already indexed.
///
/// Not thread-safe; the daemon drives it from one coroutine.
class SessionWatcher {
public:
    using WallClock = std::chrono::system_clock;

    /// One directory a session wants kept indexed.
    struct Target {
        std::string session;
        std::string sessionUuid; ///< distinguishes a session from an earlier one of that name
        std::filesystem::path directory;
        std::uint32_t intervalMs{0};
    };

    struct Options {
        /// Where snapshots are persisted (one file per target). Empty keeps them in memory.
        std::filesystem::path stateDir;
        /// How long a file must stay gone before its documents are removed. A checkout or
        /// rebase makes files vanish and reappear, and a short visit to another branch
        /// should not cost a re-index of everything that branch lacks.
        std::chrono::seconds removalGrace{std::chrono::minutes(10)};
        /// A tree with more files than this is not watched. Watching is for a project's
        /// sources; a temporary or data directory with hundreds of thousands of files would
        /// otherwise be fed into the index whole.
        std::size_t maxFilesPerTarget{50'000};
        /// While a target sees no change its interval doubles, this many times at most.
        std::uint32_t maxBackoffDoublings{3};
        /// Polled for every directory entry; a true answer abandons the pass.
        std::function<bool()> stopRequested;
        /// Polled before each target and every 256 entries; false abandons the pass.
        std::function<bool()> admit;
    };

    enum class ScanOutcome : std::uint8_t {
        Clean,      ///< walked; everything that changed was indexed or removed
        Incomplete, ///< walk failed or was abandoned, or some path is held back for retry
    };

    SessionWatcher() = default;
    explicit SessionWatcher(Options options) : options_(std::move(options)) {}

    void setOptions(Options options) { options_ = std::move(options); }
    [[nodiscard]] const Options& options() const noexcept { return options_; }
    void setRemovalGrace(std::chrono::seconds grace) { options_.removalGrace = grace; }

    /// Every directory pinned by a session with watching enabled, whichever is current.
    [[nodiscard]] static std::vector<Target>
    targets(const app::services::ISessionService& sessions);

    /// Poll delay for a base interval: idle when nothing is watched, never below 100 ms.
    [[nodiscard]] static std::chrono::milliseconds delay(bool watchEnabled,
                                                         std::uint32_t intervalMs);

    [[nodiscard]] static app::services::AddDirectoryRequest
    makeRequest(std::string_view session, const std::filesystem::path& directory,
                std::vector<std::string> changed);

    /// Walk one target now, regardless of whether it is due.
    ScanOutcome scan(app::services::IIndexingService& indexing,
                     app::services::IDocumentService* documents, const Target& target,
                     WallClock::time_point now);

    /// Scan every target that is due and return how long until the next one is.
    std::chrono::milliseconds runPass(const app::services::ISessionService& sessions,
                                      app::services::IIndexingService& indexing,
                                      app::services::IDocumentService* documents,
                                      WallClock::time_point now);

    /// Interval the target currently runs at, including back-off. Zero for an unknown target.
    [[nodiscard]] std::chrono::milliseconds currentInterval(const Target& target) const;

private:
    struct FileState {
        std::uint64_t mtime{0};
        std::uint64_t size{0};
        /// Seconds since the epoch when the file was first seen missing; 0 while present.
        std::int64_t missingSince{0};

        bool operator==(const FileState&) const = default;
        [[nodiscard]] bool sameContentAs(const FileState& other) const noexcept {
            return mtime == other.mtime && size == other.size;
        }
    };

    struct CachedGitignore {
        std::uint64_t stamp{0};
        std::vector<yams::common::GitignoreRule> rules;
    };

    struct TargetState {
        bool loaded{false};
        bool dirty{false};                                // snapshot differs from disk
        std::unordered_map<std::string, FileState> files; // by path relative to the target
        std::unordered_map<std::string, CachedGitignore> gitignores; // by relative directory
        std::unordered_set<std::string> failingPaths;                // warned once, retried quietly
        WallClock::time_point nextDue;
        std::uint32_t idleStreak{0};
        bool warnedNotDirectory{false};
        bool warnedTooLarge{false};
    };

    struct WalkResult;

    [[nodiscard]] static std::string keyOf(const Target& target);
    [[nodiscard]] std::filesystem::path snapshotPath(const Target& target) const;
    void loadSnapshot(const Target& target, TargetState& state) const;
    void saveSnapshot(const Target& target, TargetState& state) const;
    [[nodiscard]] bool walk(const Target& target, TargetState& state, WalkResult& result) const;
    [[nodiscard]] bool stopRequested() const {
        return options_.stopRequested && options_.stopRequested();
    }
    [[nodiscard]] bool admitted() const { return !options_.admit || options_.admit(); }

    Options options_;
    std::unordered_map<std::string, TargetState> states_;
};

} // namespace yams::daemon
