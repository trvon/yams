#pragma once

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
        transitionTo(PluginHostState::VerifyingTrust);
    }
    void dispatch(const PluginLoadedEvent&) {
        transitionTo(PluginHostState::LoadingPlugins);
        ++snap_.loadedCount;
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
