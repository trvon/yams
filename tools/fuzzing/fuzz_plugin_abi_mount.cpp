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
    if (!data || size == 0)
        return 0;

    // Persistent mode: avoid expensive per-iteration filesystem I/O.
    // This fuzzer only ever loads controlled, in-tree plugins.
    static const bool envInit = [] {
        setenv("YAMS_PLUGIN_TRUST_ALL", "1", 1);
        return true;
    }();
    (void)envInit;

    static const fs::path pluginPath = resolvePluginPath();
    if (!fs::exists(pluginPath))
        return 0;

    yams::daemon::AbiPluginLoader loader;

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

    return 0;
}
