#include "ort_runtime_loader.h"

#include "ort_cxx_api_wrapper.h"
#include "ort_runtime_resolution.h"

#include <spdlog/spdlog.h>
#include <yams/compat/dlfcn.h>
#include <yams/daemon/resource/OnnxConcurrencyRegistry.h>

#include <algorithm>
#include <array>
#include <cctype>
#include <cstdlib>
#include <filesystem>
#include <sstream>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

#ifdef _WIN32
#include <windows.h>
#endif

namespace yams::onnx_util {
namespace fs = std::filesystem;

namespace {

constexpr int kMinOrtMajor = 1;
constexpr int kMinOrtMinor = 23;
constexpr int kMinOrtPatch = 0;

using OrtGetApiBaseFn = const OrtApiBase*(ORT_API_CALL*)(void);
using yams::daemon::OnnxRegistryLogLevel;

bool versionAtLeast(const std::string& version, int minMajor, int minMinor, int minPatch) {
    std::array<int, 3> parsed{0, 0, 0};
    std::size_t index = 0;
    std::size_t i = 0;

    while (i < version.size() && index < parsed.size()) {
        while (i < version.size() && !std::isdigit(static_cast<unsigned char>(version[i]))) {
            ++i;
        }
        if (i >= version.size()) {
            break;
        }

        int value = 0;
        while (i < version.size() && std::isdigit(static_cast<unsigned char>(version[i]))) {
            value = (value * 10) + (version[i] - '0');
            ++i;
        }
        parsed[index++] = value;
    }

    if (parsed[0] != minMajor) {
        return parsed[0] > minMajor;
    }
    if (parsed[1] != minMinor) {
        return parsed[1] > minMinor;
    }
    return parsed[2] >= minPatch;
}

std::string minVersionString() {
    return std::to_string(kMinOrtMajor) + "." + std::to_string(kMinOrtMinor) + "." +
           std::to_string(kMinOrtPatch);
}

fs::path currentModuleDir() {
#ifdef _WIN32
    HMODULE module = nullptr;
    if (!GetModuleHandleExA(GET_MODULE_HANDLE_EX_FLAG_FROM_ADDRESS |
                                GET_MODULE_HANDLE_EX_FLAG_UNCHANGED_REFCOUNT,
                            reinterpret_cast<LPCSTR>(&currentModuleDir), &module) ||
        module == nullptr) {
        return {};
    }
    std::array<char, 4096> buffer{};
    const DWORD len = GetModuleFileNameA(module, buffer.data(), static_cast<DWORD>(buffer.size()));
    if (len == 0 || len >= buffer.size()) {
        return {};
    }
    return fs::path(std::string(buffer.data(), len)).parent_path();
#else
    Dl_info info{};
    if (dladdr(reinterpret_cast<void*>(&currentModuleDir), &info) == 0 ||
        info.dli_fname == nullptr) {
        return {};
    }
    return fs::path(info.dli_fname).parent_path();
#endif
}

std::string resolveLoadedPath([[maybe_unused]] void* handle, OrtGetApiBaseFn getApiBase,
                              const fs::path& fallback) {
#ifdef _WIN32
    if (handle != nullptr) {
        std::array<char, 4096> buffer{};
        const DWORD len = GetModuleFileNameA(static_cast<HMODULE>(handle), buffer.data(),
                                             static_cast<DWORD>(buffer.size()));
        if (len > 0 && len < buffer.size()) {
            return std::string(buffer.data(), len);
        }
    }
#else
    Dl_info info{};
    if (getApiBase != nullptr && dladdr(reinterpret_cast<void*>(getApiBase), &info) != 0 &&
        info.dli_fname != nullptr) {
        return info.dli_fname;
    }
#endif
    return fallback.string();
}

std::vector<fs::path> listDirectory(const fs::path& dir) {
    std::vector<fs::path> out;
    std::error_code ec;
    if (!fs::is_directory(dir, ec)) {
        return out;
    }
    for (const auto& entry : fs::directory_iterator(dir, ec)) {
        if (ec) {
            break;
        }
        out.push_back(entry.path());
    }
    std::sort(out.begin(), out.end());
    return out;
}

OrtSearchInputs searchInputs(const std::string& configured) {
    OrtSearchInputs in;
    // Existing explicit override for debugging and pinning (kept for compatibility).
    if (const char* lib = std::getenv("YAMS_ONNX_RUNTIME_LIB")) {
        in.overrideLibrary = lib;
    }
    if (const char* dir = std::getenv("YAMS_ONNX_RUNTIME_DIR")) {
        in.overrideDirectory = dir;
    }
    in.configuredLibrary = configured;
    in.moduleDir = currentModuleDir();
    in.sonames = defaultOrtSonames();
    in.systemDirs = defaultOrtSystemDirs();
#ifdef _WIN32
    if (const char* path = std::getenv("PATH")) {
        std::stringstream stream(path);
        std::string part;
        while (std::getline(stream, part, ';')) {
            if (!part.empty()) {
                in.systemDirs.emplace_back(part);
            }
        }
    }
#endif
    in.listDir = listDirectory;
    return in;
}

// Runtime-selection messages go to the plugin's logger and to the host log
// (through the libyams_onnx_resource sink the daemon installs).
void logBoth(OnnxRegistryLogLevel level, const std::string& msg) {
    switch (level) {
        case OnnxRegistryLogLevel::Warn:
            spdlog::warn("{}", msg);
            break;
        case OnnxRegistryLogLevel::Info:
            spdlog::info("{}", msg);
            break;
        case OnnxRegistryLogLevel::Debug:
            spdlog::debug("{}", msg);
            break;
    }
    yams::daemon::OnnxConcurrencyRegistry::emitHostLog(level, msg.c_str());
}

const char* chosenReason(OrtCandidateSource source, bool sawIncompatible) {
    switch (source) {
        case OrtCandidateSource::Override:
            return "pinned by YAMS_ONNX_RUNTIME_LIB/YAMS_ONNX_RUNTIME_DIR";
        case OrtCandidateSource::Config:
            return "configured runtime_library";
        case OrtCandidateSource::System:
            return "compatible system copy preferred";
        case OrtCandidateSource::Bundled:
            return sawIncompatible ? "no compatible system copy" : "no system copy found";
        case OrtCandidateSource::Legacy:
            return "found next to the plugin";
    }
    return "";
}

} // namespace

OrtRuntimeLoader& OrtRuntimeLoader::instance() {
    static OrtRuntimeLoader loader;
    return loader;
}

void OrtRuntimeLoader::setConfiguredLibrary(std::string pathOrDir) {
    std::lock_guard<std::mutex> lock(mutex_);
    configuredLibrary_ = std::move(pathOrDir);
}

const OrtRuntimeInfo& OrtRuntimeLoader::ensureLoaded() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (attempted_) {
        return info_;
    }

    attempted_ = true;
    info_.attempted = true;

    const OrtSearchInputs inputs = searchInputs(configuredLibrary_);
    const auto candidates = planOrtCandidates(inputs);

    void* chosenHandle = nullptr;
    OrtGetApiBaseFn chosenGetApiBase = nullptr;
    const OrtApiBase* chosenApiBase = nullptr;
    const OrtApi* chosenApi = nullptr;

    auto probe = [&](const OrtCandidate& cand) -> OrtProbeResult {
        OrtProbeResult r;
        const std::string candStr = cand.path.string();
        const bool explicitPath = cand.path.has_parent_path();
        std::error_code ec;
        // Explicit paths that do not exist are absent; bare sonames go to the loader.
        if (explicitPath && !fs::exists(cand.path, ec)) {
            return r;
        }
        void* handle = dlopen(candStr.c_str(), RTLD_NOW | RTLD_LOCAL);
        if (handle == nullptr) {
            const char* err = dlerror();
            if (explicitPath) {
                r.detail = err ? err : "dlopen failed";
            }
            return r;
        }
        auto* symbol = dlsym(handle, "OrtGetApiBase");
        if (symbol == nullptr) {
            r.status = OrtProbeStatus::MissingApiBase;
            r.detail = "does not export OrtGetApiBase";
            dlclose(handle);
            return r;
        }
        auto getApiBase = reinterpret_cast<OrtGetApiBaseFn>(symbol);
        const OrtApiBase* apiBase = getApiBase();
        if (apiBase == nullptr || apiBase->GetApi == nullptr ||
            apiBase->GetVersionString == nullptr) {
            r.status = OrtProbeStatus::MissingApiBase;
            r.detail = "returned an invalid OrtApiBase";
            dlclose(handle);
            return r;
        }
        const char* versionCstr = apiBase->GetVersionString();
        r.version = versionCstr ? versionCstr : "unknown";
        const OrtApi* api = apiBase->GetApi(ORT_API_VERSION);
        if (api == nullptr) {
            r.status = OrtProbeStatus::IncompatibleApi;
            r.detail = "ONNX Runtime " + r.version + " does not provide API version " +
                       std::to_string(ORT_API_VERSION);
            dlclose(handle);
            return r;
        }
        if (!versionAtLeast(r.version, kMinOrtMajor, kMinOrtMinor, kMinOrtPatch)) {
            r.status = OrtProbeStatus::IncompatibleApi;
            r.detail = "ONNX Runtime " + r.version + " is older than the minimum supported " +
                       minVersionString();
            dlclose(handle);
            return r;
        }
        r.status = OrtProbeStatus::Accepted;
        chosenHandle = handle;
        chosenGetApiBase = getApiBase;
        chosenApiBase = apiBase;
        chosenApi = api;
        return r;
    };

    const OrtSelection sel = selectOrtCandidate(candidates, probe);
    info_.skipped = sel.skipped;
    for (const auto& skipped : sel.skipped) {
        logBoth(OnnxRegistryLogLevel::Info, "[ONNX] Skipped runtime candidate " + skipped);
    }

    if (sel.chosenIndex >= 0) {
        const OrtCandidate& cand = candidates[static_cast<std::size_t>(sel.chosenIndex)];
        Ort::InitApi(chosenApi);
        handle_ = chosenHandle;
        apiBase_ = chosenApiBase;
        api_ = chosenApi;

        info_.available = true;
        info_.reason = "loaded";
        info_.version = sel.chosen.version;
        info_.libraryPath = resolveLoadedPath(chosenHandle, chosenGetApiBase, cand.path);
        const OrtCandidateSource loadedSource =
            classifyLoadedRuntime(inputs, cand, fs::path(info_.libraryPath));
        info_.source = toString(loadedSource);
        if (api_->GetBuildInfoString != nullptr) {
            if (const char* buildInfo = api_->GetBuildInfoString()) {
                info_.buildInfo = buildInfo;
            }
        }

        logBoth(OnnxRegistryLogLevel::Info,
                "[ONNX] Using " + info_.source + " ONNX Runtime " + info_.version + " from " +
                    info_.libraryPath + " (" +
                    (loadedSource != cand.source
                         ? std::string("already loaded in this process")
                         : std::string(chosenReason(cand.source, sel.sawIncompatible))) +
                    ")");
        return info_;
    }

    info_.available = false;
    if (sel.sawIncompatible) {
        info_.reason = "unsupported_runtime_version";
    } else if (!sel.skipped.empty()) {
        info_.reason = "runtime_load_failed";
    } else {
        info_.reason = "runtime_not_found";
    }
    // Consumers prefix this with "ONNX Runtime unavailable: ".
    info_.errorMessage = sel.skipped.empty()
                             ? std::string("no ONNX Runtime library found (system or bundled)")
                             : "no compatible ONNX Runtime library (" + sel.skipped.back() + ")";
    logBoth(OnnxRegistryLogLevel::Warn, "[ONNX] ONNX Runtime unavailable: " + info_.errorMessage);
    return info_;
}

bool OrtRuntimeLoader::isAvailable() {
    return ensureLoaded().available;
}

std::vector<std::string> OrtRuntimeLoader::availableProviders() {
    const OrtRuntimeInfo& info = ensureLoaded();
    if (!info.available) {
        return {};
    }

    std::lock_guard<std::mutex> lock(mutex_);

    if (api_ == nullptr || api_->GetAvailableProviders == nullptr ||
        api_->ReleaseAvailableProviders == nullptr) {
        return {};
    }

    char** providers = nullptr;
    int len = 0;
    OrtStatus* status = api_->GetAvailableProviders(&providers, &len);
    if (status != nullptr) {
        if (api_->ReleaseStatus != nullptr) {
            api_->ReleaseStatus(status);
        }
        return {};
    }

    std::vector<std::string> out;
    out.reserve(static_cast<std::size_t>(std::max(len, 0)));
    for (int i = 0; i < len; ++i) {
        if (providers != nullptr && providers[i] != nullptr) {
            out.emplace_back(providers[i]);
        }
    }

    status = api_->ReleaseAvailableProviders(providers, len);
    if (status != nullptr && api_->ReleaseStatus != nullptr) {
        api_->ReleaseStatus(status);
    }

    return out;
}

#ifdef YAMS_TESTING
void OrtRuntimeLoader::resetForTesting() {
    std::lock_guard<std::mutex> lock(mutex_);
    Ort::InitApi(static_cast<const OrtApi*>(nullptr));
    if (handle_ != nullptr) {
        dlclose(handle_);
        handle_ = nullptr;
    }
    apiBase_ = nullptr;
    api_ = nullptr;
    attempted_ = false;
    info_ = {};
}

extern "C" void yams_onnx_test_reset_runtime_loader() {
    OrtRuntimeLoader::instance().resetForTesting();
}
#endif

} // namespace yams::onnx_util
