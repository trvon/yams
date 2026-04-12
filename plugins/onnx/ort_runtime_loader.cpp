#include "ort_runtime_loader.h"

#include "ort_cxx_api_wrapper.h"

#include <spdlog/spdlog.h>
#include <yams/compat/dlfcn.h>

#include <algorithm>
#include <array>
#include <cctype>
#include <cstdlib>
#include <filesystem>
#include <set>
#include <sstream>
#include <string>
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

std::vector<std::string> platformLibraryNames() {
#ifdef _WIN32
    return {"onnxruntime.dll"};
#elif __APPLE__
    return {"libonnxruntime.dylib", "libonnxruntime.1.dylib"};
#else
    return {"libonnxruntime.so", "libonnxruntime.so.1"};
#endif
}

bool looksLikeRuntimeLibrary(const fs::path& path) {
    const std::string fileName = path.filename().string();
#ifdef _WIN32
    return fileName == "onnxruntime.dll";
#elif __APPLE__
    return fileName.rfind("libonnxruntime", 0) == 0 && path.extension() == ".dylib";
#else
    if (fileName == "libonnxruntime.so") {
        return true;
    }
    return fileName.rfind("libonnxruntime.so.", 0) == 0;
#endif
}

void appendCandidate(std::vector<fs::path>& out, const fs::path& candidate) {
    if (candidate.empty()) {
        return;
    }
    out.push_back(candidate);
}

void appendDirectoryCandidates(std::vector<fs::path>& out, const fs::path& dir) {
    if (dir.empty()) {
        return;
    }

    for (const auto& name : platformLibraryNames()) {
        appendCandidate(out, dir / name);
    }

    std::error_code ec;
    if (!fs::exists(dir, ec) || !fs::is_directory(dir, ec)) {
        return;
    }

    for (const auto& entry : fs::directory_iterator(dir, ec)) {
        if (ec || !entry.is_regular_file(ec)) {
            continue;
        }
        if (looksLikeRuntimeLibrary(entry.path())) {
            appendCandidate(out, entry.path());
        }
    }
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

std::vector<fs::path> collectCandidates() {
    std::vector<fs::path> candidates;

    const char* runtimeLib = std::getenv("YAMS_ONNX_RUNTIME_LIB");
    const char* runtimeDir = std::getenv("YAMS_ONNX_RUNTIME_DIR");
    if ((runtimeLib && *runtimeLib) || (runtimeDir && *runtimeDir)) {
        if (runtimeLib && *runtimeLib) {
            appendCandidate(candidates, fs::path(runtimeLib));
        }
        if (runtimeDir && *runtimeDir) {
            appendDirectoryCandidates(candidates, fs::path(runtimeDir));
        }
    } else {
        const fs::path moduleDir = currentModuleDir();
        if (!moduleDir.empty()) {
            appendDirectoryCandidates(candidates, moduleDir);
            appendDirectoryCandidates(candidates, moduleDir.parent_path());
            appendDirectoryCandidates(candidates, moduleDir / "..");
            appendDirectoryCandidates(candidates, moduleDir / "../..");
        }

#ifdef __APPLE__
        appendDirectoryCandidates(candidates, "/opt/homebrew/opt/onnxruntime/lib");
        appendDirectoryCandidates(candidates, "/usr/local/opt/onnxruntime/lib");
        appendDirectoryCandidates(candidates, "/opt/homebrew/lib");
        appendDirectoryCandidates(candidates, "/usr/local/lib");
#elif defined(_WIN32)
        if (const char* path = std::getenv("PATH")) {
            std::stringstream stream(path);
            std::string part;
            while (std::getline(stream, part, ';')) {
                if (!part.empty()) {
                    appendDirectoryCandidates(candidates, fs::path(part));
                }
            }
        }
#else
        appendDirectoryCandidates(candidates, "/usr/local/lib");
        appendDirectoryCandidates(candidates, "/usr/lib");
        appendDirectoryCandidates(candidates, "/opt/onnxruntime/lib");
        appendDirectoryCandidates(candidates, "/opt/onnxruntime/lib64");
#endif

        for (const auto& name : platformLibraryNames()) {
            appendCandidate(candidates, fs::path(name));
        }
    }

    std::vector<fs::path> deduped;
    std::set<std::string> seen;
    for (const auto& candidate : candidates) {
        const std::string key = candidate.string();
        if (key.empty() || !seen.insert(key).second) {
            continue;
        }
        deduped.push_back(candidate);
    }
    return deduped;
}

} // namespace

OrtRuntimeLoader& OrtRuntimeLoader::instance() {
    static OrtRuntimeLoader loader;
    return loader;
}

const OrtRuntimeInfo& OrtRuntimeLoader::ensureLoaded() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (attempted_) {
        return info_;
    }

    attempted_ = true;
    info_.attempted = true;

    bool sawUnsupported = false;
    std::string lastUnsupportedMessage;
    std::string lastLoadMessage;

    for (const auto& candidate : collectCandidates()) {
        const std::string candidateStr = candidate.string();
        void* handle = dlopen(candidateStr.c_str(), RTLD_NOW | RTLD_LOCAL);
        if (handle == nullptr) {
            const char* err = dlerror();
            lastLoadMessage = candidateStr + ": " + (err ? err : "dlopen failed");
            continue;
        }

        auto* symbol = dlsym(handle, "OrtGetApiBase");
        if (symbol == nullptr) {
            const char* err = dlerror();
            lastLoadMessage = candidateStr + ": missing OrtGetApiBase";
            if (err && *err) {
                lastLoadMessage += " (";
                lastLoadMessage += err;
                lastLoadMessage += ")";
            }
            dlclose(handle);
            continue;
        }

        auto getApiBase = reinterpret_cast<OrtGetApiBaseFn>(symbol);
        const OrtApiBase* apiBase = getApiBase();
        if (apiBase == nullptr || apiBase->GetApi == nullptr ||
            apiBase->GetVersionString == nullptr) {
            lastLoadMessage = candidateStr + ": invalid OrtApiBase";
            dlclose(handle);
            continue;
        }

        const char* versionCstr = apiBase->GetVersionString();
        const std::string version = versionCstr ? versionCstr : "unknown";
        const OrtApi* api = apiBase->GetApi(ORT_API_VERSION);
        if (api == nullptr || !versionAtLeast(version, kMinOrtMajor, kMinOrtMinor, kMinOrtPatch)) {
            sawUnsupported = true;
            lastUnsupportedMessage = candidateStr + " reports ONNX Runtime " + version +
                                     "; minimum supported version is " + minVersionString();
            dlclose(handle);
            continue;
        }

        Ort::InitApi(api);
        handle_ = handle;
        apiBase_ = apiBase;
        api_ = api;

        info_.available = true;
        info_.reason = "loaded";
        info_.version = version;
        info_.libraryPath = resolveLoadedPath(handle, getApiBase, candidate);
        if (api_->GetBuildInfoString != nullptr) {
            const char* buildInfo = api_->GetBuildInfoString();
            if (buildInfo != nullptr) {
                info_.buildInfo = buildInfo;
            }
        }

        spdlog::info("[ONNX] Loaded runtime {} from {}", info_.version,
                     info_.libraryPath.empty() ? candidateStr : info_.libraryPath);
        return info_;
    }

    info_.available = false;
    if (sawUnsupported) {
        info_.reason = "unsupported_runtime_version";
        info_.errorMessage = std::move(lastUnsupportedMessage);
    } else if (!lastLoadMessage.empty()) {
        info_.reason = "runtime_load_failed";
        info_.errorMessage = std::move(lastLoadMessage);
    } else {
        info_.reason = "runtime_not_found";
        info_.errorMessage = "No compatible ONNX Runtime library found";
    }

    spdlog::warn("[ONNX] {}", info_.errorMessage);
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
