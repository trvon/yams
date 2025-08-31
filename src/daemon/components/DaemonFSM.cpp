#include <yams/daemon/components/DaemonFSM.h>

#include <algorithm>

namespace yams::daemon {

DaemonFSM::DaemonFSM() {
    ready_[Subsystem::IpcServer] = false;
    ready_[Subsystem::ContentStore] = false;
    ready_[Subsystem::Database] = false;
    ready_[Subsystem::MetadataRepo] = false;
    ready_[Subsystem::SearchEngine] = false;
    ready_[Subsystem::ModelProvider] = false;
    ready_[Subsystem::VectorIndex] = false;
    ready_[Subsystem::Plugins] = false;

    progress_[Subsystem::SearchEngine] = 0;
    progress_[Subsystem::VectorIndex] = 0;
    progress_[Subsystem::ModelProvider] = 0;
}

void DaemonFSM::on(Event e) {
    std::scoped_lock lk(mu_);
    auto s = current_.load();
    switch (e) {
        case Event::IpcListening:
            if (s == State::Starting) {
                current_ = State::IPCListening;
            }
            ready_[Subsystem::IpcServer] = true;
            break;
        case Event::ContentStoreReady:
            ready_[Subsystem::ContentStore] = true;
            break;
        case Event::DatabaseReady:
            ready_[Subsystem::Database] = true;
            break;
        case Event::MetadataRepoReady:
            ready_[Subsystem::MetadataRepo] = true;
            break;
        case Event::SearchEngineReady:
            ready_[Subsystem::SearchEngine] = true;
            break;
        case Event::ModelProviderReady:
            ready_[Subsystem::ModelProvider] = true;
            break;
        case Event::VectorIndexReady:
            ready_[Subsystem::VectorIndex] = true;
            break;
        case Event::PluginsReady:
            ready_[Subsystem::Plugins] = true;
            break;
        case Event::ShutdownRequested:
            current_ = State::ShuttingDown;
            break;
        case Event::Stopped:
            current_ = State::Stopped;
            break;
    }
    recompute();
    cv_.notify_all();
}

bool DaemonFSM::isAtLeast(State s) const {
    return static_cast<int>(current_.load()) >= static_cast<int>(s);
}

bool DaemonFSM::waitFor(State s, std::chrono::milliseconds timeout) {
    std::unique_lock lk(mu_);
    return cv_.wait_for(lk, timeout, [&] { return isAtLeast(s); });
}

void DaemonFSM::observed(Subsystem s, bool ready) {
    std::scoped_lock lk(mu_);
    ready_[s] = ready;
    recompute();
    cv_.notify_all();
}

void DaemonFSM::progress(Subsystem s, int pct) {
    std::scoped_lock lk(mu_);
    progress_[s] = std::clamp(pct, 0, 100);
    cv_.notify_all();
}

const char* DaemonFSM::keyOf(Subsystem s) {
    switch (s) {
        case Subsystem::IpcServer:
            return "ipc_server";
        case Subsystem::ContentStore:
            return "content_store";
        case Subsystem::Database:
            return "database";
        case Subsystem::MetadataRepo:
            return "metadata_repo";
        case Subsystem::SearchEngine:
            return "search_engine";
        case Subsystem::ModelProvider:
            return "model_provider";
        case Subsystem::VectorIndex:
            return "vector_index";
        case Subsystem::Plugins:
            return "plugins";
    }
    return "unknown";
}

std::string DaemonFSM::overallStatus() const {
    switch (current_.load()) {
        case State::Starting:
            return "Starting";
        case State::IPCListening:
            return "Initializing";
        case State::CoreReady:
            return "Initializing";
        case State::QueryReady:
            return "Initializing";
        case State::Ready:
            return "Ready";
        case State::Degraded:
            return "Degraded";
        case State::ShuttingDown:
            return "ShuttingDown";
        case State::Stopped:
            return "Stopped";
    }
    return "Unknown";
}

std::map<std::string, bool> DaemonFSM::readinessMap() const {
    std::map<std::string, bool> m;
    for (auto& [k, v] : ready_)
        m[keyOf(k)] = v;
    return m;
}

std::map<std::string, int> DaemonFSM::progressMap() const {
    std::map<std::string, int> m;
    for (auto& [k, v] : progress_)
        m[keyOf(k)] = v;
    return m;
}

void DaemonFSM::recompute() {
    // Determine state based on readiness
    const bool ipc = ready_[Subsystem::IpcServer];
    const bool cs = ready_[Subsystem::ContentStore];
    const bool db = ready_[Subsystem::Database];
    const bool mr = ready_[Subsystem::MetadataRepo];
    const bool se = ready_[Subsystem::SearchEngine];
    (void)ready_[Subsystem::ModelProvider];
    const bool vi = ready_[Subsystem::VectorIndex];
    const bool pl = ready_[Subsystem::Plugins];

    auto prev = current_.load();
    if (prev == State::ShuttingDown || prev == State::Stopped)
        return;

    if (!ipc) {
        current_ = State::Starting;
    } else if (!(cs && db && mr)) {
        current_ = State::IPCListening;
    } else if (!(se && vi)) {
        current_ = State::CoreReady;
    } else if (!(pl /*optional*/)) {
        current_ = State::QueryReady;
    } else {
        current_ = State::Ready;
    }
}

} // namespace yams::daemon
