#pragma once

// ONNX Runtime resolution policy, shared by the ONNX and Glint plugins.
//
// The plugins never link ONNX Runtime; they dlopen() it. Candidates are tried
// in this order and the first compatible one wins:
//   1. explicit override: YAMS_ONNX_RUNTIME_LIB / YAMS_ONNX_RUNTIME_DIR (existing
//      debugging override), then the plugin config key "runtime_library"
//      ([plugins.onnx] / [plugins.glint] in config.toml)
//   2. a system ONNX Runtime: the standard loader search by soname
//      (libonnxruntime.so.1 / libonnxruntime.1.dylib / onnxruntime.dll), then the
//      usual install prefixes (/opt/onnxruntime, Homebrew)
//   3. the private copy YAMS ships next to its plugins: <libdir>/yams/onnxruntime/
//   4. legacy locations next to the plugin (build trees, older layouts)
// A candidate is compatible when it exports OrtGetApiBase and
// GetApi(ORT_API_VERSION) is non-null for the API version the plugins are
// compiled against (and it meets the minimum supported version). Incompatible
// candidates are skipped, not fatal. This file is pure policy so it can be
// tested without a real runtime.

#include <filesystem>
#include <functional>
#include <string>
#include <vector>

namespace yams::onnx_util {

enum class OrtCandidateSource { Override, Config, System, Bundled, Legacy };

const char* toString(OrtCandidateSource source) noexcept;

struct OrtCandidate {
    std::filesystem::path path;
    OrtCandidateSource source{OrtCandidateSource::System};
};

struct OrtSearchInputs {
    std::string overrideLibrary;     // YAMS_ONNX_RUNTIME_LIB
    std::string overrideDirectory;   // YAMS_ONNX_RUNTIME_DIR
    std::string configuredLibrary;   // plugin config "runtime_library" (file or directory)
    std::filesystem::path moduleDir; // directory of the plugin doing the lookup
    // Library file names probed by the loader search and in directories.
    std::vector<std::string> sonames;
    // System directories searched after the soname lookup.
    std::vector<std::filesystem::path> systemDirs;
    // Lists directory entries; injectable so tests need no filesystem.
    std::function<std::vector<std::filesystem::path>(const std::filesystem::path&)> listDir;
};

// Ordered, de-duplicated candidate list. When an override is set, only the
// override candidates are returned (it is an explicit pin).
std::vector<OrtCandidate> planOrtCandidates(const OrtSearchInputs& inputs);

enum class OrtProbeStatus { Accepted, LoadFailed, MissingApiBase, IncompatibleApi };

struct OrtProbeResult {
    OrtProbeStatus status{OrtProbeStatus::LoadFailed};
    std::string version;
    std::string detail;
};

struct OrtSelection {
    int chosenIndex{-1};
    OrtProbeResult chosen;
    std::vector<std::string> skipped; // "<source> <path>: <why>" per rejected candidate
    bool sawIncompatible{false};
};

// Probe candidates in order; stop at the first accepted one.
OrtSelection selectOrtCandidate(const std::vector<OrtCandidate>& candidates,
                                const std::function<OrtProbeResult(const OrtCandidate&)>& probe);

// A bare-soname (system) lookup returns an already-loaded library with that soname,
// e.g. the bundled copy another plugin loaded first. Report what was actually
// loaded: Bundled when the resolved file lives in a bundled directory, otherwise
// the candidate's own source.
OrtCandidateSource classifyLoadedRuntime(const OrtSearchInputs& inputs,
                                         const OrtCandidate& candidate,
                                         const std::filesystem::path& resolvedPath);

// Default library names and system directories for the current platform.
std::vector<std::string> defaultOrtSonames();
std::vector<std::filesystem::path> defaultOrtSystemDirs();

// True for file names that look like an ONNX Runtime core library.
bool looksLikeOrtLibrary(const std::filesystem::path& path);

} // namespace yams::onnx_util
