// ONNX Runtime resolution policy shared by the ONNX and Glint plugins:
// explicit override > configured path > system copy > bundled private copy >
// legacy locations, skipping candidates whose API version is incompatible.
// Library names follow the host platform (ELF .so names on Linux, .dylib on macOS).

#include <catch2/catch_test_macros.hpp>

#include "plugins/ort_runtime/ort_runtime_resolution.h"

#include <algorithm>
#include <string>
#include <vector>

using namespace yams::onnx_util;
namespace fs = std::filesystem;

namespace {

#ifdef __APPLE__
const std::string kSoname = "libonnxruntime.1.dylib";
const std::string kDevName = "libonnxruntime.dylib";
const std::string kVersioned = "libonnxruntime.1.23.0.dylib";
const std::string kNewer = "libonnxruntime.1.24.1.dylib";
const std::string kProviders = "libonnxruntime_providers_shared.dylib";
#else
const std::string kSoname = "libonnxruntime.so.1";
const std::string kDevName = "libonnxruntime.so";
const std::string kVersioned = "libonnxruntime.so.1.23.0";
const std::string kNewer = "libonnxruntime.so.1.24.1";
const std::string kProviders = "libonnxruntime_providers_shared.so";
#endif

const fs::path kPluginDir = "/usr/lib/yams/plugins";
const fs::path kBundledDir = "/usr/lib/yams/onnxruntime";
const fs::path kSystemDir = "/opt/onnxruntime/lib";

OrtSearchInputs platformInputs() {
    OrtSearchInputs in;
    in.moduleDir = kPluginDir;
    in.sonames = {kSoname, kDevName};
    in.systemDirs = {kSystemDir};
    in.listDir = [](const fs::path& dir) -> std::vector<fs::path> {
        if (dir == kBundledDir) {
            return {dir / kVersioned, dir / kProviders, dir / "README"};
        }
        return {};
    };
    return in;
}

int indexOf(const std::vector<OrtCandidate>& c, const fs::path& p) {
    for (std::size_t i = 0; i < c.size(); ++i) {
        if (c[i].path == p) {
            return static_cast<int>(i);
        }
    }
    return -1;
}

} // namespace

TEST_CASE("ORT candidates: configured, then system, then bundled, then legacy",
          "[plugins][onnx][ort][catch2]") {
    auto in = platformInputs();
    in.configuredLibrary = (fs::path("/srv/ort") / kNewer).string();
    const auto c = planOrtCandidates(in);

    REQUIRE(!c.empty());
    CHECK(c.front().source == OrtCandidateSource::Config);
    CHECK(c.front().path == fs::path("/srv/ort") / kNewer);

    const int sysSoname = indexOf(c, fs::path(kSoname));
    const int sysDir = indexOf(c, kSystemDir / kSoname);
    const int bundled = indexOf(c, kBundledDir / kSoname);
    const int bundledVersioned = indexOf(c, kBundledDir / kVersioned);
    const int legacy = indexOf(c, kPluginDir / kSoname);
    REQUIRE(sysSoname > 0);
    REQUIRE(sysDir > sysSoname);
    REQUIRE(bundled > sysDir);
    REQUIRE(bundledVersioned > bundled);
    REQUIRE(legacy > bundledVersioned);
    CHECK(c[sysSoname].source == OrtCandidateSource::System);
    CHECK(c[bundled].source == OrtCandidateSource::Bundled);
    CHECK(c[legacy].source == OrtCandidateSource::Legacy);

    // Provider plugins and unrelated files are never probed as the core runtime.
    CHECK(std::none_of(c.begin(), c.end(), [](const OrtCandidate& x) {
        const auto name = x.path.filename().string();
        return name.find("providers_shared") != std::string::npos || name == "README";
    }));
}

TEST_CASE("ORT candidates: an override pins the runtime", "[plugins][onnx][ort][catch2]") {
    auto in = platformInputs();
    in.configuredLibrary = "/srv/ort";
    in.overrideLibrary = (fs::path("/tmp/ort") / kSoname).string();
    const auto c = planOrtCandidates(in);
    REQUIRE(c.size() == 1);
    CHECK(c[0].source == OrtCandidateSource::Override);
    CHECK(c[0].path == fs::path("/tmp/ort") / kSoname);
}

TEST_CASE("ORT candidates: a configured directory expands to library names",
          "[plugins][onnx][ort][catch2]") {
    auto in = platformInputs();
    in.configuredLibrary = "/srv/ort/lib";
    const auto c = planOrtCandidates(in);
    REQUIRE(c.size() >= 2);
    CHECK(c[0].path == fs::path("/srv/ort/lib") / kSoname);
    CHECK(c[0].source == OrtCandidateSource::Config);
    CHECK(c[1].path == fs::path("/srv/ort/lib") / kDevName);
}

TEST_CASE("ORT selection prefers a compatible system copy over the bundled one",
          "[plugins][onnx][ort][catch2]") {
    const auto c = planOrtCandidates(platformInputs());
    std::vector<std::string> probed;
    const auto sel = selectOrtCandidate(c, [&](const OrtCandidate& cand) {
        probed.push_back(cand.path.string());
        OrtProbeResult r;
        if (cand.path == fs::path(kSoname) || cand.source == OrtCandidateSource::Bundled) {
            r.status = OrtProbeStatus::Accepted;
            r.version = "1.23.0";
        }
        return r;
    });
    REQUIRE(sel.chosenIndex >= 0);
    CHECK(c[sel.chosenIndex].source == OrtCandidateSource::System);
    CHECK(probed.size() == 1); // stops at the first compatible candidate
}

TEST_CASE("ORT selection skips an incompatible system copy and falls back to the bundled one",
          "[plugins][onnx][ort][catch2]") {
    const auto c = planOrtCandidates(platformInputs());
    const auto sel = selectOrtCandidate(c, [](const OrtCandidate& cand) {
        OrtProbeResult r;
        if (cand.path == fs::path(kSoname)) {
            r.status = OrtProbeStatus::IncompatibleApi;
            r.version = "1.16.3";
            r.detail = "ONNX Runtime 1.16.3 does not provide API version 23";
        } else if (cand.source == OrtCandidateSource::Bundled) {
            r.status = OrtProbeStatus::Accepted;
            r.version = "1.23.0";
        }
        return r;
    });
    REQUIRE(sel.chosenIndex >= 0);
    CHECK(c[sel.chosenIndex].source == OrtCandidateSource::Bundled);
    CHECK(sel.sawIncompatible);
    REQUIRE(sel.skipped.size() == 1);
    CHECK(sel.skipped[0].find("system " + kSoname) == 0);
    CHECK(sel.skipped[0].find("API version 23") != std::string::npos);
}

TEST_CASE("ORT selection reports no runtime without treating absent files as errors",
          "[plugins][onnx][ort][catch2]") {
    const auto c = planOrtCandidates(platformInputs());
    const auto sel = selectOrtCandidate(c, [](const OrtCandidate&) { return OrtProbeResult{}; });
    CHECK(sel.chosenIndex == -1);
    CHECK(sel.skipped.empty());
    CHECK_FALSE(sel.sawIncompatible);
}

TEST_CASE("ORT soname lookup that returns the bundled copy is reported as bundled",
          "[plugins][onnx][ort][catch2]") {
    // A second plugin's soname lookup returns the copy the first plugin already
    // loaded from <libdir>/yams/onnxruntime.
    const auto in = platformInputs();
    const OrtCandidate soname{fs::path(kSoname), OrtCandidateSource::System};
    CHECK(classifyLoadedRuntime(in, soname, kBundledDir / kSoname) == OrtCandidateSource::Bundled);
    CHECK(classifyLoadedRuntime(in, soname, kSystemDir / kVersioned) == OrtCandidateSource::System);
    const OrtCandidate configured{fs::path("/srv/ort") / kSoname, OrtCandidateSource::Config};
    CHECK(classifyLoadedRuntime(in, configured, fs::path("/srv/ort") / kSoname) ==
          OrtCandidateSource::Config);
}
