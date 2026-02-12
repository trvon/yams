#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

#include <yams/daemon/resource/abi_plugin_loader.h>

namespace fs = std::filesystem;

namespace {

constexpr size_t kMaxConfigBytes = 2048;

std::vector<fs::path> pluginCandidates() {
    std::vector<fs::path> out;
    const fs::path base = "/src/build/fuzzing/tools/fuzzing";

    const fs::path test1 = base / "libfuzz_abi_test_plugin.so";
    const fs::path test2 = base / "libfuzz_abi_negotiation_plugin.so";

    if (fs::exists(test1))
        out.push_back(test1);
    if (fs::exists(test2))
        out.push_back(test2);
    return out;
}

std::string configFromBytes(const uint8_t* data, size_t size) {
    if (!data || size == 0)
        return "{}";
    const size_t n = std::min(size, kMaxConfigBytes);
    std::string payload(reinterpret_cast<const char*>(data), n);
    for (char& c : payload) {
        unsigned char uc = static_cast<unsigned char>(c);
        if (uc < 0x20 || uc == '"' || uc == '\\') {
            c = '_';
        }
    }
    return std::string("{\"neg\":\"") + payload + "\"}";
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (!data || size == 0)
        return 0;

    auto plugins = pluginCandidates();
    if (plugins.empty()) {
        return 0;
    }

    const fs::path pluginRoot = plugins.front().parent_path();
    const fs::path trustFile = fs::temp_directory_path() / "yams_fuzz_plugin_abi_negotiation.trust";

    yams::daemon::AbiPluginLoader loader;
    loader.setTrustFile(trustFile);
    (void)loader.trustAdd(pluginRoot);

    loader.setNamePolicy((data[0] & 1) ? yams::daemon::AbiPluginLoader::NamePolicy::Spec
                                       : yams::daemon::AbiPluginLoader::NamePolicy::Relaxed);

    (void)loader.scanDirectory(pluginRoot);

    std::string configJson = configFromBytes(data, size);

    size_t start = static_cast<size_t>(data[0]) % plugins.size();
    for (size_t i = 0; i < plugins.size(); ++i) {
        const fs::path& p = plugins[(start + i) % plugins.size()];
        auto loaded = loader.load(p, configJson);
        if (!loaded) {
            continue;
        }

        const auto& info = loaded.value();
        (void)loader.health(info.name);

        for (const auto& iface : info.interfaces) {
            uint32_t v = 1u + static_cast<uint32_t>(data[(i + iface.size()) % size] % 3u);
            (void)loader.getInterface(info.name, iface, v);
        }

        std::string dynId = "iface_";
        dynId += static_cast<char>('a' + (data[i % size] % 26));
        (void)loader.getInterface(info.name, dynId, static_cast<uint32_t>(data[(i + 1) % size]));

        (void)loader.unload(info.name);
    }

    std::error_code ec;
    fs::remove(trustFile, ec);
    return 0;
}
