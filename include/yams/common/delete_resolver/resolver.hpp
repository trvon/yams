#pragma once

#include <algorithm>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

#include <yams/common/pattern_utils.h>
#include <yams/core/types.h>
#include <yams/metadata/metadata_repository.h>

namespace yams::common::resolve {

// Shared options for name/path resolution
struct Options {
    bool recursive = false;      // For directory selectors
    bool includeHidden = true;   // Include dotfiles when resolving directory/pattern
    bool forceAmbiguous = false; // Allow returning all matches when a name is ambiguous
};

// Unified result type for backends
// - For delete: id = content hash; display = file path or name
// - For add:    id = absolute file/directory path; display = same
struct Target {
    std::string id;
    std::string display;
};

// Backend policy concept
template <class B>
concept ResolverBackend = requires(B b, std::string_view name, std::string_view pattern,
                                   std::string_view directory, Options opt) {
    { b.resolveName(name, opt) } -> std::same_as<Result<std::vector<Target>>>;
    { b.resolvePattern(pattern, opt) } -> std::same_as<Result<std::vector<Target>>>;
    { b.resolveDirectory(directory, opt) } -> std::same_as<Result<std::vector<Target>>>;
};

// Filesystem backend (used by "add" to normalize inputs to absolute paths)
struct FilesystemBackend {
    // Resolve a single name/path into an absolute path or "-" (stdin sentinel)
    inline Result<std::vector<Target>> resolveName(std::string_view name, const Options&) {
        namespace fs = std::filesystem;
        if (name == "-") {
            // stdin sentinel, preserved as-is
            return std::vector<Target>{{"-", "-"}};
        }

        std::error_code ec;
        fs::path abs = fs::absolute(fs::path{name}, ec);
        if (ec) {
            return Error{ErrorCode::InvalidArgument,
                         "Failed to normalize to absolute path: " + std::string{name}};
        }
        if (!fs::exists(abs)) {
            return Error{ErrorCode::FileNotFound, "Path not found: " + abs.string()};
        }

        auto normalize_display = [](const std::string& p) -> std::string {
#ifdef __APPLE__
            // On macOS, /var is a symlink to /private/var; tests expect the /var variant.
            const std::string prefix = "/private/var/";
            if (p.rfind(prefix, 0) == 0) {
                return std::string{"/var/"} + p.substr(prefix.size());
            }
#endif
            return p;
        };

        const auto absStr = abs.string();
        const auto disp = normalize_display(absStr);
        // For files: return the absolute file; for directories: return the absolute directory.
        // Recursion and include/exclude are applied by the caller/daemon.
        return std::vector<Target>{{disp, disp}};
    }

    // Optional glob expansion for add; by default not supported (shell typically expands anyway)
    inline Result<std::vector<Target>> resolvePattern(std::string_view, const Options&) {
        return Error{ErrorCode::InvalidArgument, "Pattern expansion is not supported for add"};
    }

    // Normalize directory path and validate existence. The daemon handles recursion.
    inline Result<std::vector<Target>> resolveDirectory(std::string_view directory,
                                                        const Options& opt) {
        namespace fs = std::filesystem;

        if (!opt.recursive) {
            return Error{ErrorCode::InvalidArgument, "Directory requires recursive=true"};
        }

        std::error_code ec;
        fs::path abs = fs::absolute(fs::path{directory}, ec);
        if (ec) {
            return Error{ErrorCode::InvalidArgument, "Failed to normalize directory path"};
        }
        if (!fs::exists(abs) || !fs::is_directory(abs)) {
            return Error{ErrorCode::InvalidArgument, "Not a directory: " + abs.string()};
        }
        auto normalize_display = [](const std::string& p) -> std::string {
#ifdef __APPLE__
            const std::string prefix = "/private/var/";
            if (p.rfind(prefix, 0) == 0) {
                return std::string{"/var/"} + p.substr(prefix.size());
            }
#endif
            return p;
        };
        const auto absStr = abs.string();
        const auto disp = normalize_display(absStr);
        return std::vector<Target>{{disp, disp}};
    }
};

// Metadata backend (used by "delete" to resolve to content hashes via repository)
struct MetadataBackend {
    std::shared_ptr<metadata::IMetadataRepository> repo;

    inline Result<std::vector<Target>> resolveName(std::string_view name, const Options& opt) {
        if (!repo) {
            return Error{ErrorCode::NotInitialized, "Metadata repository not available"};
        }

        // Prefer suffix-path lookup first (align with existing CLI behavior)
        auto like = std::string("%/") + std::string{name};
        auto docs = repo->findDocumentsByPath(like);
        if (!docs) {
            // Try exact fallback (path may be stored as-is)
            docs = repo->findDocumentsByPath(std::string{name});
            if (!docs) {
                return Error{docs.error().code, "Failed to query documents for name"};
            }
        }

        std::vector<Target> out;
        for (const auto& d : docs.value()) {
            if (d.fileName == name || d.filePath == name) {
                out.push_back(Target{d.sha256Hash, !d.fileName.empty() ? d.fileName : d.filePath});
            }
        }

        if (out.empty()) {
            return Error{ErrorCode::NotFound, "No documents found with name: " + std::string{name}};
        }
        if (out.size() > 1 && !opt.forceAmbiguous) {
            return Error{ErrorCode::InvalidOperation,
                         "Multiple documents match name '" + std::string{name} +
                             "'. Set forceAmbiguous to true to delete all matches."};
        }
        return out;
    }

    inline Result<std::vector<Target>> resolvePattern(std::string_view pattern,
                                                      const Options& opt) {
        (void)opt;
        if (!repo) {
            return Error{ErrorCode::NotInitialized, "Metadata repository not available"};
        }

        // Coarse prefilter with SQL LIKE, then exact wildcard filtering
        auto like = yams::common::glob_to_sql_like(pattern);
        if (!like.empty() && like.front() != '/') {
            like = "%/" + like;
        }
        auto docs = repo->findDocumentsByPath(like);
        if (!docs) {
            return Error{docs.error().code, "Failed to query documents for pattern"};
        }

        std::vector<Target> out;
        for (const auto& d : docs.value()) {
            if (yams::common::wildcard_match(d.fileName, pattern) ||
                yams::common::wildcard_match(d.filePath, pattern)) {
                out.push_back(Target{d.sha256Hash, !d.fileName.empty() ? d.fileName : d.filePath});
            }
        }

        if (out.empty()) {
            return Error{ErrorCode::NotFound,
                         "No documents matched pattern: " + std::string{pattern}};
        }
        return out;
    }

    inline Result<std::vector<Target>> resolveDirectory(std::string_view directory,
                                                        const Options& opt) {
        if (!repo) {
            return Error{ErrorCode::NotInitialized, "Metadata repository not available"};
        }
        if (!opt.recursive) {
            return Error{ErrorCode::InvalidArgument,
                         "Directory deletion requires recursive=true for safety"};
        }

        // Build LIKE pattern for directory contents
        std::string dir = std::string{directory};
        std::string prefix = dir;
        if (!dir.empty() && dir.back() != '/') {
            dir.push_back('/');
            prefix.push_back('/');
        }
        auto like = dir + "%";

        auto docs = repo->findDocumentsByPath(like);
        if (!docs) {
            return Error{docs.error().code, "Failed to query directory: " + std::string{directory}};
        }

        std::vector<Target> out;
        for (const auto& d : docs.value()) {
            // Ensure true prefix match (avoid LIKE false positives)
            if (d.filePath.rfind(prefix, 0) == 0 || d.filePath == directory) {
                if (!opt.includeHidden && !d.fileName.empty() && d.fileName.front() == '.') {
                    continue;
                }
                // Directory listings should present concise names
                out.push_back(Target{d.sha256Hash, !d.fileName.empty() ? d.fileName : d.filePath});
            }
        }

        if (out.empty()) {
            return Error{ErrorCode::NotFound,
                         "No documents found in directory: " + std::string{directory}};
        }
        return out;
    }
};

// Generic resolver adapter using a backend policy
template <ResolverBackend B> class Resolver {
public:
    explicit Resolver(B backend, Options opt = {}) : backend_(std::move(backend)), opt_(opt) {}

    // Resolve a list of names
    inline Result<std::vector<Target>> names(const std::vector<std::string>& ns) {
        std::vector<Target> all;
        for (const auto& n : ns) {
            auto r = backend_.resolveName(n, opt_);
            if (!r) {
                return r.error();
            }
            all.insert(all.end(), r.value().begin(), r.value().end());
        }
        dedup(all);
        return all;
    }

    // Resolve a single glob pattern
    inline Result<std::vector<Target>> pattern(std::string_view p) {
        auto r = backend_.resolvePattern(p, opt_);
        if (!r) {
            return r.error();
        }
        auto v = std::move(r.value());
        dedup(v);
        return v;
    }

    // Resolve a directory (recursive required by options)
    inline Result<std::vector<Target>> directory(std::string_view d) {
        auto r = backend_.resolveDirectory(d, opt_);
        if (!r) {
            return r.error();
        }
        auto v = std::move(r.value());
        dedup(v);
        return v;
    }

private:
    static inline void dedup(std::vector<Target>& v) {
        std::unordered_set<std::string> seen;
        std::vector<Target> out;
        out.reserve(v.size());
        for (auto& t : v) {
            if (seen.insert(t.id).second) {
                out.push_back(std::move(t));
            }
        }
        v.swap(out);
    }

    B backend_;
    Options opt_;
};

} // namespace yams::common::resolve