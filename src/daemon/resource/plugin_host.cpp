#include <yams/daemon/resource/plugin_host.h>

#include <yams/daemon/resource/abi_plugin_loader.h>

#include <mutex>

namespace yams::daemon {

struct AbiPluginHost::Impl {
    mutable std::mutex mutex;
    AbiPluginLoader loader;
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

AbiPluginHost::AbiPluginHost(const std::filesystem::path& trustFile)
    : pImpl(std::make_unique<Impl>()) {
    if (!trustFile.empty())
        pImpl->loader.setTrustFile(trustFile);
}

AbiPluginHost::~AbiPluginHost() = default;

void AbiPluginHost::setTrustFile(const std::filesystem::path& trustFile) {
    std::lock_guard lock(pImpl->mutex);
    pImpl->loader.setTrustFile(trustFile);
}

void AbiPluginHost::setNamePolicy(AbiPluginHost::NamePolicy policy) {
    std::lock_guard lock(pImpl->mutex);
    pImpl->loader.setNamePolicy(policy == AbiPluginHost::NamePolicy::Spec
                                    ? AbiPluginLoader::NamePolicy::Spec
                                    : AbiPluginLoader::NamePolicy::Relaxed);
}

Result<PluginDescriptor> AbiPluginHost::scanTarget(const std::filesystem::path& file) {
    std::lock_guard lock(pImpl->mutex);
    auto r = pImpl->loader.scanTarget(file);
    if (!r)
        return r.error();
    return Impl::map(r.value());
}

Result<std::vector<PluginDescriptor>>
AbiPluginHost::scanDirectory(const std::filesystem::path& dir) {
    std::lock_guard lock(pImpl->mutex);
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
    std::lock_guard lock(pImpl->mutex);
    std::error_code ec;
    auto requested = std::filesystem::weakly_canonical(file, ec);
    if (ec) {
        requested = file.lexically_normal();
        ec.clear();
    }
    for (const auto& loaded : pImpl->loader.loaded()) {
        auto loadedPath = std::filesystem::weakly_canonical(loaded.path, ec);
        if (ec) {
            loadedPath = loaded.path.lexically_normal();
            ec.clear();
        }
        if (loadedPath == requested) {
            return Impl::map(loaded);
        }
    }
    auto r = pImpl->loader.load(file, configJson);
    if (!r)
        return r.error();
    return Impl::map(r.value());
}

Result<void> AbiPluginHost::unload(const std::string& name) {
    std::lock_guard lock(pImpl->mutex);
    return pImpl->loader.unload(name);
}

std::vector<PluginDescriptor> AbiPluginHost::listLoaded() const {
    std::lock_guard lock(pImpl->mutex);
    std::vector<PluginDescriptor> out;
    for (auto& sr : pImpl->loader.loaded())
        out.push_back(Impl::map(sr));
    return out;
}

std::vector<std::filesystem::path> AbiPluginHost::trustList() const {
    std::lock_guard lock(pImpl->mutex);
    return pImpl->loader.trustList();
}

Result<void> AbiPluginHost::trustAdd(const std::filesystem::path& p) {
    std::lock_guard lock(pImpl->mutex);
    return pImpl->loader.trustAdd(p);
}

Result<void> AbiPluginHost::trustRemove(const std::filesystem::path& p) {
    std::lock_guard lock(pImpl->mutex);
    return pImpl->loader.trustRemove(p);
}

Result<std::string> AbiPluginHost::health(const std::string& name) {
    std::lock_guard lock(pImpl->mutex);
    return pImpl->loader.health(name);
}

Result<void*> AbiPluginHost::getInterface(const std::string& name,    // plugin name
                                          const std::string& ifaceId, // interface ID
                                          uint32_t version) {
    std::lock_guard lock(pImpl->mutex);
    return pImpl->loader.getInterface(name, ifaceId, version);
}

Result<std::shared_ptr<void>> AbiPluginHost::acquireKeepAlive(const std::string& name) const {
    std::lock_guard lock(pImpl->mutex);
    return pImpl->loader.acquireKeepAlive(name);
}

Result<AbiPluginHost::InterfaceLease> AbiPluginHost::acquireInterface(const std::string& name,
                                                                      const std::string& ifaceId,
                                                                      uint32_t version) {
    std::lock_guard lock(pImpl->mutex);
    auto interface = pImpl->loader.getInterface(name, ifaceId, version);
    if (!interface) {
        return interface.error();
    }
    auto keepAlive = pImpl->loader.acquireKeepAlive(name);
    if (!keepAlive) {
        return keepAlive.error();
    }
    return InterfaceLease{interface.value(), keepAlive.value()};
}

std::vector<std::pair<std::filesystem::path, std::string>> AbiPluginHost::getLastScanSkips() const {
    std::lock_guard lock(pImpl->mutex);
    std::vector<std::pair<std::filesystem::path, std::string>> out;
    for (const auto& skip : pImpl->loader.getLastSkips()) {
        out.emplace_back(skip.path, skip.reason);
    }
    return out;
}

} // namespace yams::daemon
