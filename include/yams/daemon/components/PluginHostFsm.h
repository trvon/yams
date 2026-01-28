#pragma once

#include <algorithm>
#include <mutex>
#include <string>
#include <vector>

namespace yams::daemon {

enum class PluginHostState { NotInitialized, ScanningDirectories, LoadingPlugins, Ready, Failed };

struct PluginHostSnapshot {
    PluginHostState state{PluginHostState::NotInitialized};
    std::string lastError;
    std::size_t loadedCount{0};
    std::vector<std::string> loadedPlugins;
};

struct PluginScanStartedEvent {
    std::size_t directoryCount{0};
};
struct PluginLoadedEvent {
    std::string name;
};
struct AllPluginsLoadedEvent {
    std::size_t count{0};
};
struct PluginLoadFailedEvent {
    std::string error;
};

class PluginHostFsm {
public:
    PluginHostSnapshot snapshot() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return snap_;
    }

    void dispatch(const PluginScanStartedEvent&) {
        std::lock_guard<std::mutex> lock(mutex_);
        transitionTo(PluginHostState::ScanningDirectories);
    }
    void dispatch(const PluginLoadedEvent& ev) {
        std::lock_guard<std::mutex> lock(mutex_);
        transitionTo(PluginHostState::LoadingPlugins);
        ++snap_.loadedCount;
        if (std::find(snap_.loadedPlugins.begin(), snap_.loadedPlugins.end(), ev.name) ==
            snap_.loadedPlugins.end()) {
            snap_.loadedPlugins.push_back(ev.name);
        }
    }
    void dispatch(const AllPluginsLoadedEvent& ev) {
        std::lock_guard<std::mutex> lock(mutex_);
        snap_.loadedCount = ev.count;
        transitionTo(PluginHostState::Ready);
    }
    void dispatch(const PluginLoadFailedEvent& ev) {
        std::lock_guard<std::mutex> lock(mutex_);
        snap_.lastError = ev.error;
        transitionTo(PluginHostState::Failed);
    }

private:
    void transitionTo(PluginHostState next) { snap_.state = next; }

    mutable std::mutex mutex_;
    PluginHostSnapshot snap_{};
};

} // namespace yams::daemon
