#include <cstdlib>
#include <fcntl.h>
#include <fstream>
#include <map>
#include <poll.h>
#include <regex>
#include <set>
#include <signal.h>
#include <sstream>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/resource/abi_plugin_loader.h>

namespace yams::daemon {

struct AbiPluginHost::Impl {
    AbiPluginLoader loader;
    ServiceManager* sm{nullptr};
    static PluginDescriptor map(const AbiPluginLoader::ScanResult& sr) {
        PluginDescriptor d;
        d.name = sr.name;
        d.version = sr.version;
        d.abiVersion = sr.abiVersion;
        d.path = sr.path;
        d.manifestJson = sr.manifestJson;
        d.interfaces = sr.interfaces;
        return d;
    }
};

AbiPluginHost::AbiPluginHost(ServiceManager* sm, const std::filesystem::path& trustFile)
    : pImpl(std::make_unique<Impl>()) {
    pImpl->sm = sm;
    if (!trustFile.empty())
        pImpl->loader.setTrustFile(trustFile);
    // Configure name policy from daemon config, if available
    try {
        std::string policy;
        if (sm)
            policy = sm->getConfig().pluginNamePolicy;
        if (const char* env = std::getenv("YAMS_PLUGIN_NAME_POLICY"))
            policy = env;
        for (auto& c : policy)
            c = static_cast<char>(std::tolower(c));
        if (policy == "spec")
            pImpl->loader.setNamePolicy(AbiPluginLoader::NamePolicy::Spec);
        else
            pImpl->loader.setNamePolicy(AbiPluginLoader::NamePolicy::Relaxed);
    } catch (...) {
    }
}

AbiPluginHost::~AbiPluginHost() = default;

void AbiPluginHost::setTrustFile(const std::filesystem::path& trustFile) {
    pImpl->loader.setTrustFile(trustFile);
}

Result<PluginDescriptor> AbiPluginHost::scanTarget(const std::filesystem::path& file) {
    auto r = pImpl->loader.scanTarget(file);
    if (!r)
        return r.error();
    return Impl::map(r.value());
}

Result<std::vector<PluginDescriptor>>
AbiPluginHost::scanDirectory(const std::filesystem::path& dir) {
    auto r = pImpl->loader.scanDirectory(dir);
    if (!r)
        return r.error();
    std::vector<PluginDescriptor> out;
    out.reserve(r.value().size());
    for (auto& sr : r.value())
        out.push_back(Impl::map(sr));
    return out;
}

Result<PluginDescriptor> AbiPluginHost::load(const std::filesystem::path& file,
                                             const std::string& configJson) {
    auto r = pImpl->loader.load(file, configJson);
    if (!r)
        return r.error();
    return Impl::map(r.value());
}

Result<void> AbiPluginHost::unload(const std::string& name) {
    return pImpl->loader.unload(name);
}

std::vector<PluginDescriptor> AbiPluginHost::listLoaded() const {
    std::vector<PluginDescriptor> out;
    for (auto& sr : pImpl->loader.loaded())
        out.push_back(Impl::map(sr));
    return out;
}

std::vector<std::filesystem::path> AbiPluginHost::trustList() const {
    return pImpl->loader.trustList();
}

Result<void> AbiPluginHost::trustAdd(const std::filesystem::path& p) {
    return pImpl->loader.trustAdd(p);
}

Result<void> AbiPluginHost::trustRemove(const std::filesystem::path& p) {
    return pImpl->loader.trustRemove(p);
}

Result<std::string> AbiPluginHost::health(const std::string& name) {
    return pImpl->loader.health(name);
}

Result<void*> AbiPluginHost::getInterface(const std::string& name, const std::string& ifaceId,
                                          uint32_t version) {
    return pImpl->loader.getInterface(name, ifaceId, version);
}

std::vector<std::pair<std::filesystem::path, std::string>> AbiPluginHost::getLastScanSkips() const {
    std::vector<std::pair<std::filesystem::path, std::string>> out;
    try {
        for (const auto& s : pImpl->loader.getLastSkips()) {
            out.emplace_back(s.path, s.reason);
        }
    } catch (...) {
    }
    return out;
}

} // namespace yams::daemon
