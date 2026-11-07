#pragma once

#include <cstdint>
#include <filesystem>
#include <functional>
#include <map>
#include <string>
#include <string_view>
#include <vector>

namespace yams::cli {

using RepoPath = std::filesystem::path;

struct CliProcessResult {
    int exitCode{0};
    std::string output;
};

// No shell is used. Output is bounded; failures are exceptions handled by CLI commands.
CliProcessResult runCliProcess(const std::vector<std::string>& arguments,
                               std::size_t maxBytes = std::size_t{32} * 1024 * 1024,
                               const std::function<bool(std::string_view)>& onLine = {});

struct RepoFiles {
    RepoPath root;
    std::string worktree;
    std::string branch;
    std::vector<RepoPath> tracked;
};

RepoFiles readRepoFiles(const std::filesystem::path& directory, bool includeUntracked = false);
std::filesystem::path resolveWorktree(const std::filesystem::path& directory,
                                      std::string_view name);

struct RepoIndexFile {
    std::string hash;
    std::int64_t indexed{0};
    std::int64_t documentId{0};
};
using RepoIndex = std::map<std::filesystem::path, RepoIndexFile>;

struct RepoDiff {
    std::size_t tracked{0};
    std::vector<std::filesystem::path> changed;
    std::vector<std::filesystem::path> missing;
    bool stale() const { return !changed.empty() || !missing.empty(); }
};

// Caller supplies the content-store hasher, keeping comparison independently testable.
RepoDiff compareRepoIndex(const RepoFiles& files, const RepoIndex& index,
                          const std::function<std::string(const std::filesystem::path&)>& hashFile);

} // namespace yams::cli
