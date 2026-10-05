#pragma once

#include <mutex>
#include <string>
#include <vector>

struct OrtApi;
struct OrtApiBase;

namespace yams::onnx_util {

struct OrtRuntimeInfo {
    bool attempted{false};
    bool available{false};
    std::string reason;
    std::string libraryPath;
    std::string version;
    std::string buildInfo;
    std::string errorMessage;
    // Which copy was chosen: override, configured, system, bundled or legacy.
    std::string source;
    // Candidates that were found but rejected, with the reason.
    std::vector<std::string> skipped;
};

class OrtRuntimeLoader {
public:
    static OrtRuntimeLoader& instance();

    // Plugin config "runtime_library" ([plugins.onnx] / [plugins.glint]): a library
    // file or a directory. Must be set before the first ensureLoaded() to matter.
    void setConfiguredLibrary(std::string pathOrDir);

    const OrtRuntimeInfo& ensureLoaded();
    bool isAvailable();
    std::vector<std::string> availableProviders();

#ifdef YAMS_TESTING
    void resetForTesting();
#endif

private:
    OrtRuntimeLoader() = default;

    std::mutex mutex_;
    std::string configuredLibrary_;
    bool attempted_{false};
    void* handle_{nullptr};
    const OrtApiBase* apiBase_{nullptr};
    const OrtApi* api_{nullptr};
    OrtRuntimeInfo info_;
};

#ifdef YAMS_TESTING
extern "C" void yams_onnx_test_reset_runtime_loader();
#endif

} // namespace yams::onnx_util
