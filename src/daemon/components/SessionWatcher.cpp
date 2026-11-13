// SPDX-License-Identifier: GPL-3.0-or-later
#include <yams/daemon/components/SessionWatcher.h>

#include <algorithm>
#include <charconv>
#include <fstream>
#include <system_error>

#include <spdlog/spdlog.h>

#include <yams/common/fs_utils.h>
#include <yams/common/log_rate_limiter.h>
#include <yams/core/types.h>
#include <yams/core/uuid.h>

namespace yams::daemon {

namespace {

namespace fs = std::filesystem;

constexpr auto kWarnInterval = std::chrono::minutes(10);
constexpr std::string_view kSnapshotMagic = "yams-watch-snapshot 1";
constexpr auto kNotDirectoryRetry = std::chrono::seconds(60);

std::int64_t epochSeconds(SessionWatcher::WallClock::time_point when) {
    return std::chrono::duration_cast<std::chrono::seconds>(when.time_since_epoch()).count();
}

template <typename T> bool parseNumber(std::string_view text, T& out) {
    const auto* first = text.data();
    const auto* last = first + text.size();
    const auto [end, error] = std::from_chars(first, last, out);
    return error == std::errc{} && end == last;
}

// Splits off the next space-delimited field.
std::string_view nextField(std::string_view& line) {
    const auto space = line.find(' ');
    const auto field = line.substr(0, space);
    line = space == std::string_view::npos ? std::string_view{} : line.substr(space + 1);
    return field;
}

} // namespace

struct SessionWatcher::WalkResult {
    std::unordered_map<std::string, FileState> current; // by relative path
    std::error_code error;
    bool abandoned{false};
};

std::vector<SessionWatcher::Target>
SessionWatcher::targets(const app::services::ISessionService& sessions) {
    // Watching belongs to each session, not to whichever one is current: `yams watch` in a
    // second project must not stop the first, and closing the current session must not stop
    // them all.
    std::vector<Target> targets;
    for (const auto& name : sessions.listSessions()) {
        if (!sessions.watchEnabled(name)) {
            continue;
        }
        const auto intervalMs = sessions.watchIntervalMs(name);
        for (const auto& directory : sessions.getPinnedPatterns(name)) {
            targets.push_back({name, directory, intervalMs});
        }
    }
    return targets;
}

std::chrono::milliseconds SessionWatcher::delay(bool watchEnabled, std::uint32_t intervalMs) {
    constexpr auto kIdleDelay = std::chrono::milliseconds(2000);
    constexpr std::uint32_t kMinimumIntervalMs = 100;
    if (!watchEnabled) {
        return kIdleDelay;
    }
    return std::chrono::milliseconds(std::max(kMinimumIntervalMs, intervalMs));
}

app::services::AddDirectoryRequest SessionWatcher::makeRequest(std::string_view session,
                                                               const fs::path& directory,
                                                               std::vector<std::string> changed) {
    app::services::AddDirectoryRequest request;
    request.directoryPath = directory.string();
    request.includePatterns = std::move(changed);
    request.recursive = true;
    request.sessionId = std::string(session);
    // The walk already applied every .gitignore in the tree, negations included. The
    // indexer reads only the root file and would drop paths a nested rule re-included.
    request.noGitignore = true;
    return request;
}

std::string SessionWatcher::keyOf(const Target& target) {
    return target.session + '\n' + target.directory.generic_string();
}

fs::path SessionWatcher::snapshotPath(const Target& target) const {
    if (options_.stateDir.empty()) {
        return {};
    }
    return options_.stateDir / (yams::core::shortHash(keyOf(target)) + ".snapshot");
}

void SessionWatcher::loadSnapshot(const Target& target, TargetState& state) const {
    const auto path = snapshotPath(target);
    if (path.empty()) {
        return;
    }
    std::ifstream input(path);
    if (!input) {
        return;
    }
    std::string line;
    // The file name is a short hash, so the header says which target the snapshot is for.
    if (!std::getline(input, line) || line != kSnapshotMagic || !std::getline(input, line) ||
        line != "session " + target.session || !std::getline(input, line) ||
        line != "directory " + target.directory.generic_string()) {
        return;
    }
    std::unordered_map<std::string, FileState> files;
    while (std::getline(input, line)) {
        std::string_view rest{line};
        FileState file;
        if (!parseNumber(nextField(rest), file.missingSince) ||
            !parseNumber(nextField(rest), file.mtime) || !parseNumber(nextField(rest), file.size) ||
            rest.empty()) {
            return; // damaged: start over rather than trust part of it
        }
        files.emplace(std::string(rest), file);
    }
    state.files = std::move(files);
}

void SessionWatcher::saveSnapshot(const Target& target, TargetState& state) const {
    const auto path = snapshotPath(target);
    if (path.empty()) {
        state.dirty = false;
        return;
    }
    yams::common::ensureDirectories(path.parent_path());
    auto temporary = path;
    temporary += ".tmp";
    {
        std::ofstream output(temporary, std::ios::trunc);
        if (!output) {
            return; // still dirty: retried on the next pass
        }
        output << kSnapshotMagic << '\n'
               << "session " << target.session << '\n'
               << "directory " << target.directory.generic_string() << '\n';
        for (const auto& [relative, file] : state.files) {
            // One record per line; a name with a line break is left out and is simply
            // submitted again after a restart.
            if (relative.find('\n') != std::string::npos) {
                continue;
            }
            output << file.missingSince << ' ' << file.mtime << ' ' << file.size << ' ' << relative
                   << '\n';
        }
        if (!output.good()) {
            return;
        }
    }
    std::error_code error;
    fs::rename(temporary, path, error);
    if (!error) {
        state.dirty = false;
    }
}

bool SessionWatcher::walk(const Target& target, TargetState& state, WalkResult& result) const {
    yams::common::GitignoreStack ignore;
    std::unordered_set<std::string> gitignoreDirectories;
    std::vector<std::string_view> segments;
    std::size_t visited = 0;

    const std::function<bool(const fs::path&, const std::string&, std::size_t)> visit =
        [&](const fs::path& absolute, const std::string& relativeDirectory,
            std::size_t depth) -> bool {
        std::vector<fs::directory_entry> entries;
        std::error_code error;
        for (fs::directory_iterator it(absolute, error), end; !error && it != end;
             it.increment(error)) {
            entries.push_back(*it);
        }
        if (error) {
            result.error = error;
            return false;
        }

        // A .gitignore applies to its own directory, so it is read before its siblings.
        bool pushed = false;
        for (const auto& entry : entries) {
            if (entry.path().filename() != ".gitignore") {
                continue;
            }
            std::error_code statError;
            const auto mtime = entry.last_write_time(statError);
            const auto size = statError ? std::uintmax_t{0} : entry.file_size(statError);
            const auto stamp = statError
                                   ? std::uint64_t{0}
                                   : (static_cast<std::uint64_t>(mtime.time_since_epoch().count()) ^
                                      (static_cast<std::uint64_t>(size) << 1)) |
                                         1U;
            auto& cached = state.gitignores[relativeDirectory];
            if (cached.stamp != stamp || stamp == 0) {
                std::ifstream input(entry.path());
                cached.rules = input ? yams::common::parseGitignoreRules(input)
                                     : std::vector<yams::common::GitignoreRule>{};
                cached.stamp = stamp;
            }
            gitignoreDirectories.insert(relativeDirectory);
            if (!cached.rules.empty()) {
                ignore.push(depth, &cached.rules);
                pushed = true;
            }
            break;
        }

        for (const auto& entry : entries) {
            // Shutdown waits a bounded time for the watcher, and pressure can rise during a
            // long walk. Either way the pass is dropped with the snapshot untouched.
            if (stopRequested() || ((++visited & 0xFFU) == 0 && !admitted())) {
                result.abandoned = true;
                return false;
            }
            const auto name = entry.path().filename().string();
            const auto relative = relativeDirectory.empty() ? name : relativeDirectory + '/' + name;
            yams::common::detail::split_path_segments(relative, segments);

            std::error_code statError;
            if (entry.is_directory(statError) && !entry.is_symlink(statError)) {
                if (name == ".git" || ignore.ignored(segments, true)) {
                    continue;
                }
                if (!visit(entry.path(), relative, depth + 1)) {
                    return false;
                }
                continue;
            }
            if (!entry.is_regular_file(statError)) {
                continue; // sockets, devices, dangling links
            }
            if (ignore.ignored(segments, false)) {
                continue;
            }
            const auto size = entry.file_size(statError);
            if (statError) {
                result.error = statError;
                return false;
            }
            const auto mtime = entry.last_write_time(statError);
            if (statError) {
                result.error = statError;
                return false;
            }
            result.current[relative] =
                FileState{static_cast<std::uint64_t>(mtime.time_since_epoch().count()),
                          static_cast<std::uint64_t>(size), 0};
        }
        if (pushed) {
            ignore.pop();
        }
        return true;
    };

    if (!visit(target.directory, std::string{}, 0)) {
        return false;
    }
    std::erase_if(state.gitignores,
                  [&](const auto& cached) { return !gitignoreDirectories.contains(cached.first); });
    return true;
}

SessionWatcher::ScanOutcome SessionWatcher::scan(app::services::IIndexingService& indexing,
                                                 app::services::IDocumentService* documents,
                                                 const Target& target, WallClock::time_point now) {
    auto& state = states_[keyOf(target)];
    const auto baseInterval = delay(true, target.intervalMs);
    const auto finish = [&](ScanOutcome outcome, bool active) {
        // Back off while nothing changes; anything changing or failing returns to the base.
        state.idleStreak = active || outcome == ScanOutcome::Incomplete
                               ? 0
                               : std::min(state.idleStreak + 1, options_.maxBackoffDoublings);
        state.nextDue = now + baseInterval * (std::uint64_t{1} << state.idleStreak);
        return outcome;
    };

    std::error_code error;
    if (target.directory.empty() || !fs::is_directory(target.directory, error)) {
        // Session selectors may also be globs for retrieval scoping; only a directory can
        // be watched. Say so once rather than skipping it silently.
        if (!state.warnedNotDirectory) {
            state.warnedNotDirectory = true;
            spdlog::warn("[SessionWatcher] session '{}' selector '{}' is not a directory; it is "
                         "not watched",
                         target.session, target.directory.string());
        }
        state.nextDue = now + kNotDirectoryRetry;
        return ScanOutcome::Incomplete;
    }
    state.warnedNotDirectory = false;
    if (!state.loaded) {
        loadSnapshot(target, state);
        state.loaded = true;
    }

    WalkResult walked;
    if (!walk(target, state, walked)) {
        if (walked.error) {
            // Retried every pass until the directory is readable again.
            static yams::common::LogRateLimiter scanFailureLog{kWarnInterval};
            if (auto held = scanFailureLog.admit()) {
                spdlog::warn("[SessionWatcher] scan failed for '{}': {}{}",
                             target.directory.string(), walked.error.message(),
                             yams::common::LogRateLimiter::suppressedSuffix(*held));
            }
        }
        return finish(ScanOutcome::Incomplete, false);
    }

    const auto absoluteOf = [&](const std::string& relative) {
        return (target.directory / relative).string();
    };
    bool allSucceeded = true;
    const auto noteFailure = [&](const std::string& relative, std::string_view what,
                                 std::string_view detail) {
        allSucceeded = false;
        // A directory full of failing files reports each once, but not thousands of lines.
        static yams::common::LogRateLimiter firstFailureLog{kWarnInterval};
        if (state.failingPaths.insert(relative).second) {
            if (auto held = firstFailureLog.admit()) {
                spdlog::warn("[SessionWatcher] {} failed for '{}': {} (retrying quietly){}", what,
                             absoluteOf(relative), detail,
                             yams::common::LogRateLimiter::suppressedSuffix(*held));
                return;
            }
        }
        spdlog::debug("[SessionWatcher] {} failing for '{}': {}", what, absoluteOf(relative),
                      detail);
    };

    auto next = std::move(walked.current);
    std::vector<std::string> changed;
    for (const auto& [relative, file] : next) {
        const auto previous = state.files.find(relative);
        // A file back with the content it had (a checkout away and back) is not re-indexed.
        if (previous == state.files.end() || !previous->second.sameContentAs(file)) {
            changed.push_back(relative);
        }
    }
    // Each path that fails is held back on its own, so one bad path is retried alone and
    // never blocks the snapshot from advancing for everything else.
    const auto holdBack = [&](const std::string& relative) {
        const auto previous = state.files.find(relative);
        if (previous != state.files.end()) {
            next[relative] = previous->second; // old fingerprint: still "changed" next pass
            next[relative].missingSince = 0;
        } else {
            next.erase(relative);
        }
    };

    bool active = false;
    if (!changed.empty()) {
        active = true;
        auto indexed =
            indexing.addDirectory(makeRequest(target.session, target.directory, changed));
        if (!indexed) {
            for (const auto& relative : changed) {
                holdBack(relative);
            }
            allSucceeded = false;
            static yams::common::LogRateLimiter indexFailureLog{kWarnInterval};
            if (auto held = indexFailureLog.admit()) {
                spdlog::warn("[SessionWatcher] indexing failed for '{}': {}{}",
                             target.directory.string(), indexed.error().message,
                             yams::common::LogRateLimiter::suppressedSuffix(*held));
            }
        } else {
            std::unordered_set<std::string> failed;
            if (indexed.value().filesFailed != 0) {
                for (const auto& result : indexed.value().results) {
                    if (!result.success) {
                        failed.insert(result.path);
                    }
                }
            }
            for (const auto& relative : changed) {
                // Without per-file results, a reported failure holds back every changed file.
                const bool held = indexed.value().filesFailed != 0 &&
                                  (failed.empty() || failed.contains(absoluteOf(relative)));
                if (held) {
                    holdBack(relative);
                    noteFailure(relative, "indexing", "indexing reported a failure");
                } else {
                    state.failingPaths.erase(relative);
                }
            }
        }
    }

    const auto nowSeconds = epochSeconds(now);
    for (const auto& [relative, previous] : state.files) {
        if (next.contains(relative)) {
            continue;
        }
        std::error_code existsError;
        if (fs::exists(target.directory / relative, existsError)) {
            continue; // still there but ignored now: leave its documents alone
        }
        auto missing = previous;
        if (missing.missingSince == 0) {
            missing.missingSince = nowSeconds;
        }
        if (nowSeconds - missing.missingSince < options_.removalGrace.count()) {
            next[relative] = missing; // may be a checkout or rebase in progress
            continue;
        }
        if (documents == nullptr) {
            next[relative] = missing;
            noteFailure(relative, "removal", "document service unavailable");
            continue;
        }
        active = true;
        app::services::DeleteByNameRequest request;
        request.name = absoluteOf(relative);
        // The file is gone, so every document stored at this path is stale. Without force,
        // a path indexed more than once ("Multiple documents match") could never be removed.
        request.force = true;
        auto deleted = documents->deleteByName(request);
        std::string failure;
        if (!deleted) {
            if (deleted.error().code != ErrorCode::NotFound) {
                failure = deleted.error().message;
            }
        } else if (!deleted.value().errors.empty()) {
            failure = deleted.value().errors.front().error.value_or("unknown error");
        }
        if (!failure.empty()) {
            next[relative] = missing; // keep it tracked so the removal is retried
            noteFailure(relative, "removal", failure);
        } else {
            state.failingPaths.erase(relative);
        }
    }

    if (next != state.files) {
        state.files = std::move(next);
        state.dirty = true;
    }
    if (state.dirty) {
        saveSnapshot(target, state);
    }
    return finish(allSucceeded ? ScanOutcome::Clean : ScanOutcome::Incomplete, active);
}

std::chrono::milliseconds SessionWatcher::runPass(const app::services::ISessionService& sessions,
                                                  app::services::IIndexingService& indexing,
                                                  app::services::IDocumentService* documents,
                                                  WallClock::time_point now) {
    const auto watched = targets(sessions);
    if (watched.empty()) {
        states_.clear();
        return delay(false, 0);
    }
    std::unordered_set<std::string> keys;
    for (const auto& target : watched) {
        keys.insert(keyOf(target));
    }
    std::erase_if(states_, [&](const auto& state) { return !keys.contains(state.first); });

    bool halted = false;
    for (const auto& target : watched) {
        if (now < states_[keyOf(target)].nextDue) {
            continue;
        }
        // Checked again for every target: an earlier walk may have raised the pressure.
        if (halted || stopRequested() || !admitted()) {
            halted = true;
            continue;
        }
        scan(indexing, documents, target, now);
    }

    auto wait = std::chrono::milliseconds::max();
    for (const auto& target : watched) {
        const auto due = states_[keyOf(target)].nextDue;
        // A target that was due but not admitted is tried again after its base interval.
        const auto remaining =
            due > now ? std::chrono::duration_cast<std::chrono::milliseconds>(due - now)
                      : delay(true, target.intervalMs);
        wait = std::min(wait, remaining);
    }
    return std::max(wait, delay(true, 0));
}

std::chrono::milliseconds SessionWatcher::currentInterval(const Target& target) const {
    const auto state = states_.find(keyOf(target));
    if (state == states_.end()) {
        return std::chrono::milliseconds{0};
    }
    return delay(true, target.intervalMs) * (std::uint64_t{1} << state->second.idleStreak);
}

} // namespace yams::daemon
