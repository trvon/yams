#pragma once

#include <filesystem>
#include <memory>
#include <string>
#include <vector>
#include <yams/core/types.h>

namespace yams::daemon {

class ServiceManager;

struct PluginDescriptor {
    std::string name;
    std::string version;
    uint32_t abiVersion{0};
    std::filesystem::path path;
    std::string manifestJson;
    std::vector<std::string> interfaces;
};

class IPluginHost {
public:
    virtual ~IPluginHost() = default;

    virtual Result<PluginDescriptor> scanTarget(const std::filesystem::path& file) = 0;
    virtual Result<std::vector<PluginDescriptor>>
    scanDirectory(const std::filesystem::path& dir) = 0;
    virtual Result<PluginDescriptor> load(const std::filesystem::path& file,
                                          const std::string& configJson) = 0;
    virtual Result<void> unload(const std::string& name) = 0;
    virtual std::vector<PluginDescriptor> listLoaded() const = 0;

    virtual std::vector<std::filesystem::path> trustList() const = 0;
    virtual Result<void> trustAdd(const std::filesystem::path& p) = 0;
    virtual Result<void> trustRemove(const std::filesystem::path& p) = 0;

    virtual Result<std::string> health(const std::string& name) = 0;
};

class AbiPluginHost : public IPluginHost {
public:
    explicit AbiPluginHost(ServiceManager* sm, const std::filesystem::path& trustFile = {});
    ~AbiPluginHost();
    void setTrustFile(const std::filesystem::path& trustFile);

    Result<PluginDescriptor> scanTarget(const std::filesystem::path& file) override;
    Result<std::vector<PluginDescriptor>> scanDirectory(const std::filesystem::path& dir) override;
    Result<PluginDescriptor> load(const std::filesystem::path& file,
                                  const std::string& configJson) override;
    Result<void> unload(const std::string& name) override;
    std::vector<PluginDescriptor> listLoaded() const override;
    std::vector<std::filesystem::path> trustList() const override;
    Result<void> trustAdd(const std::filesystem::path& p) override;
    Result<void> trustRemove(const std::filesystem::path& p) override;
    Result<std::string> health(const std::string& name) override;

    // Retrieve a raw interface pointer from a loaded plugin
    Result<void*> getInterface(const std::string& name, const std::string& ifaceId,
                               uint32_t version);

private:
    struct Impl;
    std::unique_ptr<Impl> pImpl;
};

class WasmPluginHost : public IPluginHost {
public:
    explicit WasmPluginHost(const std::filesystem::path& trustFile = {});
    ~WasmPluginHost();

    Result<PluginDescriptor> scanTarget(const std::filesystem::path& file) override;
    Result<std::vector<PluginDescriptor>> scanDirectory(const std::filesystem::path& dir) override;
    Result<PluginDescriptor> load(const std::filesystem::path& file,
                                  const std::string& configJson) override;
    Result<void> unload(const std::string& name) override;
    std::vector<PluginDescriptor> listLoaded() const override;
    std::vector<std::filesystem::path> trustList() const override;
    Result<void> trustAdd(const std::filesystem::path& p) override;
    Result<void> trustRemove(const std::filesystem::path& p) override;
    Result<std::string> health(const std::string& name) override;

    void setTrustFile(const std::filesystem::path& trustFile);

private:
    struct Impl;
    std::unique_ptr<Impl> pImpl_;
};

class ExternalPluginHost : public IPluginHost {
public:
    void setTrustFile(const std::filesystem::path& trustFile);

public:
    Result<PluginDescriptor> scanTarget(const std::filesystem::path& file) override;
    Result<std::vector<PluginDescriptor>> scanDirectory(const std::filesystem::path& dir) override;
    Result<PluginDescriptor> load(const std::filesystem::path& file,
                                  const std::string& configJson) override;
    Result<void> unload(const std::string& name) override;
    std::vector<PluginDescriptor> listLoaded() const override;
    std::vector<std::filesystem::path> trustList() const override;
    Result<void> trustAdd(const std::filesystem::path& p) override;
    Result<void> trustRemove(const std::filesystem::path& p) override;
    Result<std::string> health(const std::string& name) override;
};

} // namespace yams::daemon
