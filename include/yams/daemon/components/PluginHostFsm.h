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
struct PluginUnloadedEvent {
    std::string name;
};
struct AllPluginsLoadedEvent {
    std::size_t count{0};
};
struct PluginLoadRejectedEvent {
    std::string error;
};
struct PluginLoadFailedEvent {
    std::string error;
};

// Per-instance state. This was a tinyfsm machine, whose state is static, so every PluginManager in
// a process shared one plugin-host state and each one's construction reset the others.
// Transitions are unchanged: every event applies from every state.
class PluginHostFsm {
public:
    PluginHostFsm() = default;

    PluginHostSnapshot snapshot() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return snap_;
    }

    template <typename E> void dispatch(const E& ev) {
        std::lock_guard<std::mutex> lock(mutex_);
        apply(ev);
    }

private:
    void apply(const PluginScanStartedEvent&) {
        snap_.state = PluginHostState::ScanningDirectories;
    }
    void apply(const PluginLoadedEvent& ev) {
        if (std::find(snap_.loadedPlugins.begin(), snap_.loadedPlugins.end(), ev.name) ==
            snap_.loadedPlugins.end()) {
            snap_.loadedPlugins.push_back(ev.name);
        }
        snap_.loadedCount = snap_.loadedPlugins.size();
        snap_.state = PluginHostState::LoadingPlugins;
    }
    void apply(const PluginUnloadedEvent& ev) {
        std::erase(snap_.loadedPlugins, ev.name);
        snap_.loadedCount = snap_.loadedPlugins.size();
        snap_.state = PluginHostState::Ready;
    }
    void apply(const AllPluginsLoadedEvent& ev) {
        snap_.loadedCount = ev.count;
        snap_.state = PluginHostState::Ready;
    }
    void apply(const PluginLoadRejectedEvent& ev) {
        snap_.lastError = ev.error;
        snap_.state = PluginHostState::Ready;
    }
    void apply(const PluginLoadFailedEvent& ev) {
        snap_.lastError = ev.error;
        snap_.state = PluginHostState::Failed;
    }

    mutable std::mutex mutex_;
    PluginHostSnapshot snap_{};
};

} // namespace yams::daemon
