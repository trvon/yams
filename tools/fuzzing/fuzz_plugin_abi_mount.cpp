#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>

#include <yams/daemon/resource/abi_plugin_loader.h>

namespace fs = std::filesystem;

namespace {

constexpr size_t kMaxConfigBytes = 2048;

fs::path resolvePluginPath() {
    if (const char* fromEnv = std::getenv("YAMS_FUZZ_ABI_PLUGIN_PATH")) {
        if (*fromEnv) {
            return fs::path(fromEnv);
        }
    }

    const fs::path p1 = "/src/build/fuzzing/tools/fuzzing/libfuzz_abi_test_plugin.so";
    const fs::path p2 = "/src/build/fuzzing/tools/fuzzing/fuzz_abi_test_plugin.so";
    if (fs::exists(p1))
        return p1;
    return p2;
}

std::string configFromBytes(const uint8_t* data, size_t size) {
    if (!data || size == 0) {
        return "{}";
    }
    const size_t n = std::min(size, kMaxConfigBytes);
    std::string out(reinterpret_cast<const char*>(data), n);
    for (char& c : out) {
        unsigned char uc = static_cast<unsigned char>(c);
        if (uc < 0x20 || uc == '"' || uc == '\\') {
            c = '_';
        }
    }
    return std::string("{\"fuzz\":\"") + out + "\"}";
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (!data || size == 0) {
        return 0;
    }

    const fs::path pluginPath = resolvePluginPath();
    if (!fs::exists(pluginPath)) {
        return 0;
    }

    const fs::path trustRoot = pluginPath.parent_path();
    if (trustRoot.empty() || !fs::exists(trustRoot)) {
        return 0;
    }

    const fs::path trustFile = fs::temp_directory_path() / "yams_fuzz_plugin_abi.trust";

    yams::daemon::AbiPluginLoader loader;
    loader.setTrustFile(trustFile);
    (void)loader.trustAdd(trustRoot);

    std::string configJson = configFromBytes(data, size);

    auto loaded = loader.load(pluginPath, configJson);
    if (loaded) {
        const auto& name = loaded.value().name;

        (void)loader.health(name);

        // Drive interface probing with fixed known + fuzz-shaped IDs/versions.
        (void)loader.getInterface(name, "fuzz_iface_v1", 1);
        (void)loader.getInterface(name, "content_extractor_v1", 1);

        const uint32_t v = static_cast<uint32_t>(data[0]);
        std::string dynId = "iface_";
        dynId += static_cast<char>('a' + (data[0] % 26));
        (void)loader.getInterface(name, dynId, v);

        (void)loader.unload(name);
    }

    std::error_code ec;
    fs::remove(trustFile, ec);
    return 0;
}
