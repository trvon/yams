#include "ort_runtime_resolution.h"

#include <set>
#include <system_error>

namespace yams::onnx_util {
namespace fs = std::filesystem;

const char* toString(OrtCandidateSource source) noexcept {
    switch (source) {
        case OrtCandidateSource::Override:
            return "override";
        case OrtCandidateSource::Config:
            return "configured";
        case OrtCandidateSource::System:
            return "system";
        case OrtCandidateSource::Bundled:
            return "bundled";
        case OrtCandidateSource::Legacy:
            return "legacy";
    }
    return "unknown";
}

bool looksLikeOrtLibrary(const fs::path& path) {
    const std::string name = path.filename().string();
#ifdef _WIN32
    return name == "onnxruntime.dll";
#elif defined(__APPLE__)
    return name.rfind("libonnxruntime", 0) == 0 && path.extension() == ".dylib" &&
           name.find("providers") == std::string::npos;
#else
    return name == "libonnxruntime.so" || name.rfind("libonnxruntime.so.", 0) == 0;
#endif
}

std::vector<std::string> defaultOrtSonames() {
#ifdef _WIN32
    return {"onnxruntime.dll"};
#elif defined(__APPLE__)
    return {"libonnxruntime.1.dylib", "libonnxruntime.dylib"};
#else
    return {"libonnxruntime.so.1", "libonnxruntime.so"};
#endif
}

std::vector<fs::path> defaultOrtSystemDirs() {
#ifdef _WIN32
    return {};
#elif defined(__APPLE__)
    return {"/opt/homebrew/opt/onnxruntime/lib", "/usr/local/opt/onnxruntime/lib",
            "/opt/homebrew/lib", "/usr/local/lib"};
#else
    // Distro packages do not always provide the libonnxruntime.so.1 name the loader
    // search uses (Debian ships libonnxruntime.so.1.23), so also scan the usual
    // library directories for versioned names.
    return {"/usr/local/lib",
#if defined(__x86_64__)
            "/usr/lib/x86_64-linux-gnu",
#elif defined(__aarch64__)
            "/usr/lib/aarch64-linux-gnu",
#endif
            "/usr/lib64",
            "/usr/lib",
            "/opt/onnxruntime/lib",
            "/opt/onnxruntime/lib64"};
#endif
}

namespace {

class CandidateList {
public:
    explicit CandidateList(const OrtSearchInputs& in) : in_(in) {}

    void add(const fs::path& path, OrtCandidateSource source) {
        if (path.empty()) {
            return;
        }
        if (seen_.insert(path.lexically_normal().string()).second) {
            out_.push_back({path, source});
        }
    }

    // Exact names first (stable, preferred spellings), then any other runtime
    // library found in the directory (versioned names such as .so.1.23.0).
    void addDirectory(const fs::path& dir, OrtCandidateSource source) {
        if (dir.empty()) {
            return;
        }
        for (const auto& name : in_.sonames) {
            add(dir / name, source);
        }
        if (in_.listDir) {
            for (const auto& entry : in_.listDir(dir)) {
                if (looksLikeOrtLibrary(entry)) {
                    add(entry, source);
                }
            }
        }
    }

    // A configured/override value may name a file or a directory.
    void addFileOrDirectory(const std::string& value, OrtCandidateSource source) {
        if (value.empty()) {
            return;
        }
        const fs::path p(value);
        if (looksLikeOrtLibrary(p)) {
            add(p, source);
        } else {
            addDirectory(p, source);
        }
    }

    std::vector<OrtCandidate> take() { return std::move(out_); }

private:
    const OrtSearchInputs& in_;
    std::set<std::string> seen_;
    std::vector<OrtCandidate> out_;
};

} // namespace

std::vector<OrtCandidate> planOrtCandidates(const OrtSearchInputs& in) {
    CandidateList list(in);

    if (!in.overrideLibrary.empty() || !in.overrideDirectory.empty()) {
        if (!in.overrideLibrary.empty()) {
            list.add(fs::path(in.overrideLibrary), OrtCandidateSource::Override);
        }
        list.addFileOrDirectory(in.overrideDirectory, OrtCandidateSource::Override);
        return list.take();
    }

    list.addFileOrDirectory(in.configuredLibrary, OrtCandidateSource::Config);

    // System: bare sonames go through the platform loader search (ld.so cache,
    // LD_LIBRARY_PATH, DYLD paths, the exe directory / PATH on Windows).
    for (const auto& name : in.sonames) {
        list.add(fs::path(name), OrtCandidateSource::System);
    }
    for (const auto& dir : in.systemDirs) {
        list.addDirectory(dir, OrtCandidateSource::System);
    }

    if (!in.moduleDir.empty()) {
        // Plugins live in <libdir>/yams/plugins; the private copy is a sibling dir.
        list.addDirectory(in.moduleDir.parent_path() / "onnxruntime", OrtCandidateSource::Bundled);
        list.addDirectory(in.moduleDir / "onnxruntime", OrtCandidateSource::Bundled);

        list.addDirectory(in.moduleDir, OrtCandidateSource::Legacy);
        list.addDirectory(in.moduleDir.parent_path(), OrtCandidateSource::Legacy);
        list.addDirectory(in.moduleDir.parent_path().parent_path(), OrtCandidateSource::Legacy);
    }
    return list.take();
}

OrtCandidateSource classifyLoadedRuntime(const OrtSearchInputs& in, const OrtCandidate& candidate,
                                         const fs::path& resolvedPath) {
    if (candidate.source != OrtCandidateSource::System || in.moduleDir.empty() ||
        resolvedPath.empty()) {
        return candidate.source;
    }
    const fs::path loadedDir = resolvedPath.parent_path().lexically_normal();
    for (const fs::path& bundled :
         {in.moduleDir.parent_path() / "onnxruntime", in.moduleDir / "onnxruntime"}) {
        if (loadedDir == bundled.lexically_normal()) {
            return OrtCandidateSource::Bundled;
        }
    }
    return candidate.source;
}

OrtSelection selectOrtCandidate(const std::vector<OrtCandidate>& candidates,
                                const std::function<OrtProbeResult(const OrtCandidate&)>& probe) {
    OrtSelection sel;
    for (std::size_t i = 0; i < candidates.size(); ++i) {
        OrtProbeResult r = probe(candidates[i]);
        if (r.status == OrtProbeStatus::Accepted) {
            sel.chosenIndex = static_cast<int>(i);
            sel.chosen = std::move(r);
            return sel;
        }
        if (r.status == OrtProbeStatus::LoadFailed) {
            // Absent candidates are the common case; keep only real load errors.
            if (r.detail.empty()) {
                continue;
            }
        }
        if (r.status == OrtProbeStatus::IncompatibleApi) {
            sel.sawIncompatible = true;
        }
        sel.skipped.push_back(std::string(toString(candidates[i].source)) + " " +
                              candidates[i].path.string() + ": " + r.detail);
    }
    return sel;
}

} // namespace yams::onnx_util
