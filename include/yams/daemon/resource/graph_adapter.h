#pragma once

#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace yams::daemon {

// Minimal Graph Adapter interface (expand as the spec matures)
class IGraphAdapter {
public:
    virtual ~IGraphAdapter() = default;
    virtual std::string getAdapterName() const = 0;
    virtual std::string getVersion() const = 0;
    // Optional capability probe; default true
    virtual bool isAvailable() const { return true; }
};

using GraphAdapterFactory = std::function<std::unique_ptr<IGraphAdapter>()>;

// Registry (implemented in the daemon; currently backed by the plugin loader)
void registerGraphAdapter(const std::string& name, GraphAdapterFactory factory);
std::vector<std::string> getRegisteredGraphAdapters();
std::unique_ptr<IGraphAdapter> createGraphAdapter(const std::string& preferred = "");

} // namespace yams::daemon
