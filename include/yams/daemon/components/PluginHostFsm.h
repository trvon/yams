#pragma once

#include <algorithm>
#include <string>
#include <vector>

namespace yams::daemon {

enum class PluginHostState {
    NotInitialized,
    ScanningDirectories,
    VerifyingTrust,
    LoadingPlugins,
    Ready,
    Failed
};

struct PluginHostSnapshot {
    PluginHostState state{PluginHostState::NotInitialized};
    std::string lastError;
    std::size_t loadedCount{0};
    std::vector<std::string> loadedPlugins;
};

struct PluginScanStartedEvent {
    std::size_t directoryCount{0};
};
struct PluginTrustVerifiedEvent { /* path */
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
    PluginHostSnapshot snapshot() const { return snap_; }

    void dispatch(const PluginScanStartedEvent&) {
        transitionTo(PluginHostState::ScanningDirectories);
    }
    void dispatch(const PluginTrustVerifiedEvent&) {
        // Trust verified - transition to VerifyingTrust state
        // This allows trust to be configured before scanning begins
        if (snap_.state == PluginHostState::NotInitialized) {
            transitionTo(PluginHostState::VerifyingTrust);
        }
        // Otherwise, stay in current state (don't regress)
    }
    void dispatch(const PluginLoadedEvent& ev) {
        transitionTo(PluginHostState::LoadingPlugins);
        ++snap_.loadedCount;
        if (std::find(snap_.loadedPlugins.begin(), snap_.loadedPlugins.end(), ev.name) ==
            snap_.loadedPlugins.end()) {
            snap_.loadedPlugins.push_back(ev.name);
        }
    }
    void dispatch(const AllPluginsLoadedEvent& ev) {
        snap_.loadedCount = ev.count;
        transitionTo(PluginHostState::Ready);
    }
    void dispatch(const PluginLoadFailedEvent& ev) {
        snap_.lastError = ev.error;
        transitionTo(PluginHostState::Failed);
    }

private:
    void transitionTo(PluginHostState next) { snap_.state = next; }

    PluginHostSnapshot snap_{};
};

} // namespace yams::daemon
