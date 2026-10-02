// ONNX Runtime resolution policy shared by the ONNX and Glint plugins:
// explicit override > configured path > system copy > bundled private copy >
// legacy locations, skipping candidates whose API version is incompatible.

#include <catch2/catch_test_macros.hpp>

#include "plugins/ort_runtime/ort_runtime_resolution.h"

#include <algorithm>
#include <map>
#include <string>
#include <vector>

using namespace yams::onnx_util;
namespace fs = std::filesystem;

namespace {

OrtSearchInputs linuxInputs() {
    OrtSearchInputs in;
    in.moduleDir = "/usr/lib/yams/plugins";
    in.sonames = {"libonnxruntime.so.1", "libonnxruntime.so"};
    in.systemDirs = {"/opt/onnxruntime/lib"};
    in.listDir = [](const fs::path& dir) -> std::vector<fs::path> {
        if (dir == fs::path("/usr/lib/yams/onnxruntime")) {
            return {dir / "libonnxruntime.so.1.23.0", dir / "libonnxruntime_providers_shared.so",
                    dir / "README"};
        }
        return {};
    };
    return in;
}

std::vector<std::string> paths(const std::vector<OrtCandidate>& c) {
    std::vector<std::string> out;
    for (const auto& x : c) {
        out.push_back(x.path.string());
    }
    return out;
}

int indexOf(const std::vector<OrtCandidate>& c, const std::string& p) {
    for (std::size_t i = 0; i < c.size(); ++i) {
        if (c[i].path.string() == p) {
            return static_cast<int>(i);
        }
    }
    return -1;
}

} // namespace

TEST_CASE("ORT candidates: configured, then system, then bundled, then legacy",
          "[plugins][onnx][ort][catch2]") {
    auto in = linuxInputs();
    in.configuredLibrary = "/srv/ort/libonnxruntime.so.1.24.1";
    const auto c = planOrtCandidates(in);

    REQUIRE(!c.empty());
    CHECK(c.front().source == OrtCandidateSource::Config);
    CHECK(c.front().path == fs::path("/srv/ort/libonnxruntime.so.1.24.1"));

    const int sysSoname = indexOf(c, "libonnxruntime.so.1");
    const int sysDir = indexOf(c, "/opt/onnxruntime/lib/libonnxruntime.so.1");
    const int bundled = indexOf(c, "/usr/lib/yams/onnxruntime/libonnxruntime.so.1");
    const int bundledVersioned = indexOf(c, "/usr/lib/yams/onnxruntime/libonnxruntime.so.1.23.0");
    const int legacy = indexOf(c, "/usr/lib/yams/plugins/libonnxruntime.so.1");
    REQUIRE(sysSoname > 0);
    REQUIRE(sysDir > sysSoname);
    REQUIRE(bundled > sysDir);
    REQUIRE(bundledVersioned > bundled);
    REQUIRE(legacy > bundledVersioned);
    CHECK(c[sysSoname].source == OrtCandidateSource::System);
    CHECK(c[bundled].source == OrtCandidateSource::Bundled);
    CHECK(c[legacy].source == OrtCandidateSource::Legacy);

    // Provider plugins and unrelated files are never probed as the core runtime.
    const auto all = paths(c);
    CHECK(std::none_of(all.begin(), all.end(), [](const std::string& p) {
        return p.find("providers_shared") != std::string::npos ||
               p.find("README") != std::string::npos;
    }));
}

TEST_CASE("ORT candidates: an override pins the runtime", "[plugins][onnx][ort][catch2]") {
    auto in = linuxInputs();
    in.configuredLibrary = "/srv/ort";
    in.overrideLibrary = "/tmp/ort/libonnxruntime.so.1";
    const auto c = planOrtCandidates(in);
    REQUIRE(c.size() == 1);
    CHECK(c[0].source == OrtCandidateSource::Override);
    CHECK(c[0].path == fs::path("/tmp/ort/libonnxruntime.so.1"));
}

TEST_CASE("ORT candidates: a configured directory expands to library names",
          "[plugins][onnx][ort][catch2]") {
    auto in = linuxInputs();
    in.configuredLibrary = "/srv/ort/lib";
    const auto c = planOrtCandidates(in);
    REQUIRE(c.size() >= 2);
    CHECK(c[0].path == fs::path("/srv/ort/lib/libonnxruntime.so.1"));
    CHECK(c[0].source == OrtCandidateSource::Config);
    CHECK(c[1].path == fs::path("/srv/ort/lib/libonnxruntime.so"));
}

TEST_CASE("ORT selection prefers a compatible system copy over the bundled one",
          "[plugins][onnx][ort][catch2]") {
    const auto c = planOrtCandidates(linuxInputs());
    std::vector<std::string> probed;
    const auto sel = selectOrtCandidate(c, [&](const OrtCandidate& cand) {
        probed.push_back(cand.path.string());
        OrtProbeResult r;
        if (cand.path == fs::path("libonnxruntime.so.1") ||
            cand.source == OrtCandidateSource::Bundled) {
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
    const auto c = planOrtCandidates(linuxInputs());
    const auto sel = selectOrtCandidate(c, [](const OrtCandidate& cand) {
        OrtProbeResult r;
        if (cand.path == fs::path("libonnxruntime.so.1")) {
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
    CHECK(sel.skipped[0].find("system libonnxruntime.so.1") == 0);
    CHECK(sel.skipped[0].find("API version 23") != std::string::npos);
}

TEST_CASE("ORT selection reports no runtime without treating absent files as errors",
          "[plugins][onnx][ort][catch2]") {
    const auto c = planOrtCandidates(linuxInputs());
    const auto sel = selectOrtCandidate(c, [](const OrtCandidate&) { return OrtProbeResult{}; });
    CHECK(sel.chosenIndex == -1);
    CHECK(sel.skipped.empty());
    CHECK_FALSE(sel.sawIncompatible);
}

TEST_CASE("ORT soname lookup that returns the bundled copy is reported as bundled",
          "[plugins][onnx][ort][catch2]") {
    // A second plugin's dlopen("libonnxruntime.so.1") returns the copy the first
    // plugin already loaded from <libdir>/yams/onnxruntime.
    const auto in = linuxInputs();
    const OrtCandidate soname{"libonnxruntime.so.1", OrtCandidateSource::System};
    CHECK(classifyLoadedRuntime(in, soname, "/usr/lib/yams/onnxruntime/libonnxruntime.so.1") ==
          OrtCandidateSource::Bundled);
    CHECK(classifyLoadedRuntime(in, soname, "/usr/lib/x86_64-linux-gnu/libonnxruntime.so.1.23") ==
          OrtCandidateSource::System);
    const OrtCandidate configured{"/srv/ort/libonnxruntime.so.1", OrtCandidateSource::Config};
    CHECK(classifyLoadedRuntime(in, configured, "/srv/ort/libonnxruntime.so.1") ==
          OrtCandidateSource::Config);
}
