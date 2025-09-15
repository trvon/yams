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
#include <yams/daemon/resource/plugin_host.h>
#include <yams/daemon/resource/wasm_runtime.h>

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

// WASM host: lightweight implementation (manifest via sidecar .manifest.json)
struct WasmPluginHost::Impl {
    struct Loaded {
        PluginDescriptor desc;
        std::string configJson;
        std::unique_ptr<WasmRuntime> runtime;
    };
    std::map<std::string, Loaded> loaded; // by name
    std::filesystem::path trustFile;
    std::set<std::filesystem::path> trusted;

    static std::vector<std::string> parseInterfaces(const std::string& manifestJson) {
        std::vector<std::string> out;
        try {
            std::regex ifaceArray("\\\"interfaces\\\"\\s*:\\s*\\[(.*?)\\]");
            std::smatch m;
            if (std::regex_search(manifestJson, m, ifaceArray)) {
                std::string inner = m[1].str();
                std::regex strItem("\\\"([^\\\"]+)\\\"");
                for (std::sregex_iterator it(inner.begin(), inner.end(), strItem), end; it != end;
                     ++it) {
                    out.push_back((*it)[1].str());
                }
            }
        } catch (...) {
        }
        return out;
    }

    void loadTrust() {
        trusted.clear();
        if (trustFile.empty())
            return;
        std::ifstream in(trustFile);
        if (!in)
            return;
        std::string line;
        while (std::getline(in, line)) {
            if (line.empty())
                continue;
            std::error_code ec;
            auto p = std::filesystem::weakly_canonical(std::filesystem::path(line), ec);
            if (ec)
                p = std::filesystem::path(line);
            trusted.insert(p);
        }
    }
    void saveTrust() const {
        if (trustFile.empty())
            return;
        std::filesystem::create_directories(trustFile.parent_path());
        std::ofstream out(trustFile);
        for (const auto& p : trusted)
            out << p.string() << "\n";
    }
    bool isTrusted(const std::filesystem::path& p) const {
        if (trusted.empty())
            return false;
        auto canon = std::filesystem::weakly_canonical(p);
        for (const auto& t : trusted) {
            if (canon.string().rfind(t.string(), 0) == 0)
                return true;
        }
        return false;
    }
};

WasmPluginHost::WasmPluginHost(const std::filesystem::path& trustFile)
    : pImpl_(std::make_unique<Impl>()) {
    if (!trustFile.empty())
        setTrustFile(trustFile);
}
WasmPluginHost::~WasmPluginHost() = default;
void WasmPluginHost::setTrustFile(const std::filesystem::path& trustFile) {
    pImpl_->trustFile = trustFile;
    pImpl_->loadTrust();
}

static std::string readFileToString(const std::filesystem::path& file) {
    std::ifstream in(file, std::ios::binary);
    if (!in)
        return {};
    std::ostringstream ss;
    ss << in.rdbuf();
    return ss.str();
}

Result<PluginDescriptor> WasmPluginHost::scanTarget(const std::filesystem::path& file) {
    using namespace std::string_literals;
    if (!std::filesystem::exists(file)) {
        return Error{ErrorCode::FileNotFound, "WASM plugin not found: " + file.string()};
    }
    // Expect sidecar manifest: <plugin>.manifest.json
    auto manifestPath = file;
    manifestPath += ".manifest.json";
    PluginDescriptor d;
    d.path = file;
    d.name = file.stem().string();
    d.version = "1.0.0"; // optional, may be overridden by manifest
    d.abiVersion = 0;    // N/A for WASM; keep 0
    if (std::filesystem::exists(manifestPath)) {
        d.manifestJson = readFileToString(manifestPath);
        if (!d.manifestJson.empty()) {
            d.interfaces = Impl::parseInterfaces(d.manifestJson);
            // try to extract version/name (best-effort)
            try {
                std::regex ver("\\\"version\\\"\\s*:\\s*\\\"([^\\\"]+)\\\"");
                std::smatch m;
                if (std::regex_search(d.manifestJson, m, ver))
                    d.version = m[1].str();
                std::regex nm("\\\"name\\\"\\s*:\\s*\\\"([^\\\"]+)\\\"");
                if (std::regex_search(d.manifestJson, m, nm))
                    d.name = m[1].str();
            } catch (...) {
            }
        }
    }
    return d;
}

Result<std::vector<PluginDescriptor>>
WasmPluginHost::scanDirectory(const std::filesystem::path& dir) {
    if (!std::filesystem::exists(dir) || !std::filesystem::is_directory(dir)) {
        return Error{ErrorCode::InvalidPath, "Not a directory: " + dir.string()};
    }
    std::vector<PluginDescriptor> out;
    for (const auto& e : std::filesystem::directory_iterator(dir)) {
        if (!e.is_regular_file())
            continue;
        auto p = e.path();
        if (p.extension() == ".wasm") {
            if (auto r = scanTarget(p))
                out.push_back(r.value());
        }
    }
    return out;
}

Result<PluginDescriptor> WasmPluginHost::load(const std::filesystem::path& file,
                                              const std::string& configJson) {
    namespace fs = std::filesystem;
    std::error_code ec;
    fs::path canon = fs::weakly_canonical(file, ec);
    if (ec)
        canon = file;

    // Refuse world-writable plugin path (and immediate parent)
    auto perms = fs::status(canon, ec).permissions();
    if (!ec && ((perms & fs::perms::others_write) != fs::perms::none)) {
        return Error{ErrorCode::Unauthorized,
                     "World-writable plugin path refused: " + canon.string()};
    }
    auto parent = canon.parent_path();
    if (!parent.empty()) {
        auto pperms = fs::status(parent, ec).permissions();
        if (!ec && ((pperms & fs::perms::others_write) != fs::perms::none)) {
            return Error{ErrorCode::Unauthorized,
                         "World-writable parent directory refused: " + parent.string()};
        }
    }

    if (!pImpl_->isTrusted(canon)) {
        return Error{ErrorCode::Unauthorized, "WASM plugin path not trusted: " + canon.string()};
    }
    auto r = scanTarget(canon);
    if (!r)
        return r.error();
    Impl::Loaded ld;
    ld.desc = r.value();
    ld.configJson = configJson;
    // Initialize WASM runtime (stubs when wasmtime-cpp unavailable)
    ld.runtime = std::make_unique<WasmRuntime>();
    {
        WasmRuntime::Limits lims;
        if (const char* e = std::getenv("YAMS_WASM_CALL_TIMEOUT_MS")) {
            try {
                lims.call_timeout_ms = std::max(0, std::stoi(e));
            } catch (...) {
            }
        }
        if (const char* e = std::getenv("YAMS_WASM_MEMORY_MAX_MB")) {
            try {
                int mb = std::max(16, std::stoi(e));
                lims.memory_max_bytes = static_cast<uint64_t>(mb) * 1024ull * 1024ull;
            } catch (...) {
            }
        }
        if (const char* e = std::getenv("YAMS_WASM_MAX_INSTANCES")) {
            try {
                lims.max_instances = std::max(1, std::stoi(e));
            } catch (...) {
            }
        }
        // Enforce max_instances limit across loaded WASM plugins
        if (static_cast<int>(pImpl_->loaded.size()) >= lims.max_instances) {
            return Error{ErrorCode::ResourceExhausted, "Maximum WASM plugin instances reached"};
        }
        ld.runtime->setLimits(lims);
    }
    auto init = ld.runtime->load(canon, configJson);
    if (!init.ok) {
        return Error{ErrorCode::InvalidState, init.error};
    }
    pImpl_->loaded[ld.desc.name] = std::move(ld);
    return pImpl_->loaded[ld.desc.name].desc;
}

Result<void> WasmPluginHost::unload(const std::string& name) {
    pImpl_->loaded.erase(name);
    return Result<void>();
}

std::vector<PluginDescriptor> WasmPluginHost::listLoaded() const {
    std::vector<PluginDescriptor> v;
    for (const auto& [k, ld] : pImpl_->loaded)
        v.push_back(ld.desc);
    return v;
}

std::vector<std::filesystem::path> WasmPluginHost::trustList() const {
    return std::vector<std::filesystem::path>(pImpl_->trusted.begin(), pImpl_->trusted.end());
}

Result<void> WasmPluginHost::trustAdd(const std::filesystem::path& p) {
    namespace fs = std::filesystem;
    std::error_code ec;
    fs::path canon = fs::weakly_canonical(p, ec);
    if (ec)
        canon = p;

    // Refuse world-writable paths (and immediate parent)
    auto perms = fs::status(canon, ec).permissions();
    if (!ec && ((perms & fs::perms::others_write) != fs::perms::none)) {
        return Error{ErrorCode::InvalidArgument, "Refusing world-writable path: " + canon.string()};
    }
    auto parent = canon.parent_path();
    if (!parent.empty()) {
        auto pperms = fs::status(parent, ec).permissions();
        if (!ec && ((pperms & fs::perms::others_write) != fs::perms::none)) {
            return Error{ErrorCode::InvalidArgument,
                         "Refusing world-writable parent: " + parent.string()};
        }
    }

    pImpl_->trusted.insert(canon);
    pImpl_->saveTrust();
    return Result<void>();
}

Result<void> WasmPluginHost::trustRemove(const std::filesystem::path& p) {
    namespace fs = std::filesystem;
    std::error_code ec;
    fs::path canon = fs::weakly_canonical(p, ec);
    if (ec)
        canon = p;
    pImpl_->trusted.erase(canon);
    pImpl_->saveTrust();
    return Result<void>();
}

Result<std::string> WasmPluginHost::health(const std::string& name) {
    if (pImpl_->loaded.find(name) == pImpl_->loaded.end()) {
        return Error{ErrorCode::NotFound, "Not loaded"};
    }
    return std::string("{\"status\":\"ok\"}");
}

// External host: metadata scanning (yams-plugin.json)
namespace {
struct ExtState {
    std::filesystem::path trustFile;
    std::set<std::filesystem::path> trusted;

    // Loaded descriptors (by name)
    std::map<std::string, PluginDescriptor> loaded;

    // External supervision: process and pipe state per plugin
    std::map<std::string, pid_t> pids;
    std::map<std::string, int> stdin_fds;  // daemon writes → plugin stdin
    std::map<std::string, int> stdout_fds; // daemon reads ← plugin stdout

    // Process metadata for restart/backoff
    std::map<std::string, std::string> cmds;           // launch command
    std::map<std::string, std::filesystem::path> cwds; // working dir
    std::map<std::string, int> attempts;               // restart attempts (exp backoff)
    std::map<std::string, int> crashes;                // crash count
    std::map<std::string, std::chrono::steady_clock::time_point> lastExit; // last exit time
};
static ExtState& extState() {
    static ExtState s;
    return s;
}

// Compute exponential backoff in milliseconds with cap
static int extBackoffMs(int attempt, int minMs = 1000, int maxMs = 60000) {
    long long ms = static_cast<long long>(minMs) << attempt;
    if (ms > maxMs)
        ms = maxMs;
    if (ms < minMs)
        ms = minMs;
    return static_cast<int>(ms);
}

// Spawn a plugin process and perform handshake (manifest -> init)
// Returns true on success and initializes pids/stdin/stdout maps.
static bool extSpawnAndHandshake(const std::string& name, const std::string& cmd,
                                 const std::filesystem::path& cwd, std::string& err) {
    int inpipe[2] = {-1, -1};
    int outpipe[2] = {-1, -1};
    if (pipe(inpipe) != 0 || pipe(outpipe) != 0) {
        if (inpipe[0] != -1) {
            ::close(inpipe[0]);
            ::close(inpipe[1]);
        }
        if (outpipe[0] != -1) {
            ::close(outpipe[0]);
            ::close(outpipe[1]);
        }
        err = "Failed to create pipes";
        return false;
    }

    pid_t pid = fork();
    if (pid < 0) {
        ::close(inpipe[0]);
        ::close(inpipe[1]);
        ::close(outpipe[0]);
        ::close(outpipe[1]);
        err = "fork() failed";
        return false;
    }

    if (pid == 0) {
        // Child
        ::dup2(inpipe[0], STDIN_FILENO);
        ::dup2(outpipe[1], STDOUT_FILENO);
        ::close(inpipe[0]);
        ::close(inpipe[1]);
        ::close(outpipe[0]);
        ::close(outpipe[1]);
        ::chdir(cwd.c_str());
        execl("/bin/sh", "sh", "-c", cmd.c_str(), (char*)nullptr);
        _exit(127);
    }

    // Parent
    ::close(inpipe[0]);
    ::close(outpipe[1]);

    int flags = fcntl(outpipe[0], F_GETFL, 0);
    if (flags >= 0)
        (void)fcntl(outpipe[0], F_SETFL, flags | O_NONBLOCK);

    // Manifest handshake
    {
        std::string hs =
            "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"handshake.manifest\",\"params\":{}}\n";
        if (::write(inpipe[1], hs.data(), hs.size()) < 0) {
            ::kill(pid, SIGKILL);
            ::close(inpipe[1]);
            ::close(outpipe[0]);
            (void)waitpid(pid, nullptr, 0);
            err = "Failed to write handshake to plugin";
            return false;
        }
        struct pollfd pfd{outpipe[0], POLLIN, 0};
        if (::poll(&pfd, 1, 3000) <= 0) {
            ::kill(pid, SIGKILL);
            ::close(inpipe[1]);
            ::close(outpipe[0]);
            (void)waitpid(pid, nullptr, 0);
            err = "Plugin manifest handshake timeout";
            return false;
        }
        char buf[1024];
        ssize_t rd = ::read(outpipe[0], buf, sizeof(buf));
        std::string resp;
        if (rd > 0)
            resp.assign(buf, buf + rd);
        if (resp.find("\"result\"") == std::string::npos &&
            resp.find("\"interfaces\"") == std::string::npos) {
            ::kill(pid, SIGKILL);
            ::close(inpipe[1]);
            ::close(outpipe[0]);
            (void)waitpid(pid, nullptr, 0);
            err = "Plugin manifest handshake invalid";
            return false;
        }
    }

    // Init
    {
        std::string init =
            "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"plugin.init\",\"params\":{}}\n";
        if (::write(inpipe[1], init.data(), init.size()) < 0) {
            ::kill(pid, SIGKILL);
            ::close(inpipe[1]);
            ::close(outpipe[0]);
            (void)waitpid(pid, nullptr, 0);
            err = "Failed to write init to plugin";
            return false;
        }
        struct pollfd pfd{outpipe[0], POLLIN, 0};
        if (::poll(&pfd, 1, 3000) <= 0) {
            ::kill(pid, SIGKILL);
            ::close(inpipe[1]);
            ::close(outpipe[0]);
            (void)waitpid(pid, nullptr, 0);
            err = "Plugin init handshake timeout";
            return false;
        }
        char buf[1024];
        ssize_t rd = ::read(outpipe[0], buf, sizeof(buf));
        std::string resp;
        if (rd > 0)
            resp.assign(buf, buf + rd);
        if (resp.find("\"result\"") == std::string::npos &&
            resp.find("\"ok\"") == std::string::npos) {
            ::kill(pid, SIGKILL);
            ::close(inpipe[1]);
            ::close(outpipe[0]);
            (void)waitpid(pid, nullptr, 0);
            err = "Plugin init handshake invalid";
            return false;
        }
    }

    // Record state
    extState().pids[name] = pid;
    extState().stdin_fds[name] = inpipe[1];
    extState().stdout_fds[name] = outpipe[0];
    extState().attempts[name] = 0; // reset on successful start
    return true;
}

// Supervise once: if process exited and backoff elapsed, restart
static void extSuperviseOnce(const std::string& name) {
    auto& st = extState();
    auto itPid = st.pids.find(name);
    // If not tracked or no process, nothing to do
    if (itPid == st.pids.end())
        return;

    int status = 0;
    pid_t w = ::waitpid(itPid->second, &status, WNOHANG);
    if (w == 0)
        return; // still running

    // Process exited
    st.lastExit[name] = std::chrono::steady_clock::now();
    st.crashes[name] += 1;

    // Close fds and clear pid
    auto itIn = st.stdin_fds.find(name);
    auto itOut = st.stdout_fds.find(name);
    if (itIn != st.stdin_fds.end()) {
        ::close(itIn->second);
        st.stdin_fds.erase(itIn);
    }
    if (itOut != st.stdout_fds.end()) {
        ::close(itOut->second);
        st.stdout_fds.erase(itOut);
    }
    st.pids.erase(name);

    // Check if we have command/cwd to restart
    auto itCmd = st.cmds.find(name);
    auto itCwd = st.cwds.find(name);
    if (itCmd == st.cmds.end() || itCwd == st.cwds.end())
        return;

    int attempt = st.attempts[name];
    int delayMs = extBackoffMs(attempt);
    auto now = std::chrono::steady_clock::now();
    auto last = st.lastExit[name];
    if (last.time_since_epoch().count() == 0 ||
        std::chrono::duration_cast<std::chrono::milliseconds>(now - last).count() < delayMs) {
        return; // backoff window not elapsed yet
    }

    // Try restart
    std::string err;
    if (extSpawnAndHandshake(name, itCmd->second, itCwd->second, err)) {
        st.attempts[name] = 0; // reset on success
    } else {
        st.attempts[name] = attempt + 1;
    }
}

static bool extIsTrusted(const std::filesystem::path& p) {
    if (extState().trusted.empty())
        return false;
    auto canon = std::filesystem::weakly_canonical(p);
    for (const auto& t : extState().trusted) {
        if (canon.string().rfind(t.string(), 0) == 0)
            return true;
    }
    return false;
}
static void extLoadTrust() {
    auto& s = extState();
    s.trusted.clear();
    if (s.trustFile.empty())
        return;
    std::ifstream in(s.trustFile);
    if (!in)
        return;
    std::string line;
    while (std::getline(in, line)) {
        if (line.empty())
            continue;
        std::error_code ec;
        auto p = std::filesystem::weakly_canonical(std::filesystem::path(line), ec);
        if (ec)
            p = std::filesystem::path(line);
        s.trusted.insert(p);
    }
}
static void extSaveTrust() {
    auto& s = extState();
    if (s.trustFile.empty())
        return;
    std::filesystem::create_directories(s.trustFile.parent_path());
    std::ofstream out(s.trustFile);
    for (const auto& p : s.trusted)
        out << p.string() << "\n";
}
static std::string slurp(const std::filesystem::path& file) {
    std::ifstream in(file);
    if (!in)
        return {};
    std::ostringstream ss;
    ss << in.rdbuf();
    return ss.str();
}
} // namespace

Result<PluginDescriptor> ExternalPluginHost::scanTarget(const std::filesystem::path& file) {
    namespace fs = std::filesystem;
    if (!fs::exists(file))
        return Error{ErrorCode::FileNotFound, file.string()};
    fs::path meta = file;
    if (fs::is_directory(file))
        meta /= "yams-plugin.json";
    if (!fs::exists(meta))
        return Error{ErrorCode::InvalidArgument, "No yams-plugin.json in target"};
    PluginDescriptor d;
    d.path = file;
    d.abiVersion = 0;
    d.manifestJson = slurp(meta);
    // Best-effort parse for name/version/interfaces
    try {
        std::regex nameRe("\\\"name\\\"\\s*:\\s*\\\"([^\\\"]+)\\\"");
        std::regex verRe("\\\"version\\\"\\s*:\\s*\\\"([^\\\"]+)\\\"");
        std::regex ifaceArray("\\\"interfaces\\\"\\s*:\\s*\\[(.*?)\\]");
        std::smatch m;
        if (std::regex_search(d.manifestJson, m, nameRe))
            d.name = m[1].str();
        else
            d.name = file.filename().string();
        if (std::regex_search(d.manifestJson, m, verRe))
            d.version = m[1].str();
        else
            d.version = "0.0.0";
        if (std::regex_search(d.manifestJson, m, ifaceArray)) {
            std::string inner = m[1].str();
            std::regex strItem("\"([^\"]+)\"");
            for (std::sregex_iterator it(inner.begin(), inner.end(), strItem), end; it != end; ++it)
                d.interfaces.push_back((*it)[1].str());
        }
    } catch (...) {
    }
    return d;
}

Result<std::vector<PluginDescriptor>>
ExternalPluginHost::scanDirectory(const std::filesystem::path& dir) {
    namespace fs = std::filesystem;
    if (!fs::exists(dir) || !fs::is_directory(dir))
        return Error{ErrorCode::InvalidPath, dir.string()};
    std::vector<PluginDescriptor> out;
    for (const auto& e : fs::directory_iterator(dir)) {
        if (e.is_directory()) {
            auto meta = e.path() / "yams-plugin.json";
            if (fs::exists(meta)) {
                auto r = scanTarget(e.path());
                if (r)
                    out.push_back(r.value());
            }
        }
    }
    return out;
}

Result<PluginDescriptor> ExternalPluginHost::load(const std::filesystem::path& file,
                                                  const std::string& /*configJson*/) {
    namespace fs = std::filesystem;
    std::error_code ec;
    fs::path canon = fs::weakly_canonical(file, ec);
    if (ec)
        canon = file;

    // Refuse world-writable plugin directory path (and parent)
    auto perms = fs::status(canon, ec).permissions();
    if (!ec && ((perms & fs::perms::others_write) != fs::perms::none)) {
        return Error{ErrorCode::Unauthorized, "World-writable plugin path refused"};
    }
    auto parent = canon.parent_path();
    if (!parent.empty()) {
        auto pperms = fs::status(parent, ec).permissions();
        if (!ec && ((pperms & fs::perms::others_write) != fs::perms::none)) {
            return Error{ErrorCode::Unauthorized, "World-writable parent directory refused"};
        }
    }

    if (!extIsTrusted(canon))
        return Error{ErrorCode::Unauthorized, "External plugin path not trusted"};
    auto r = scanTarget(canon);
    if (!r)
        return r.error();
    auto d = r.value();

    // Parse command from yams-plugin.json (look for "cmd": "...")
    std::string cmd;
    try {
        std::regex cmdRe("\\\"cmd\\\"\\s*:\\s*\\\"([^\\\"]+)\\\"");
        std::smatch m;
        if (std::regex_search(d.manifestJson, m, cmdRe))
            cmd = m[1].str();
    } catch (...) {
    }
    if (cmd.empty()) {
        return Error{ErrorCode::InvalidArgument, "External plugin manifest missing 'cmd' field"};
    }

    // Alpha gate: require YAMS_EXTERNAL_HOST_ALPHA=1 to spawn; otherwise metadata-only
    const char* alpha = std::getenv("YAMS_EXTERNAL_HOST_ALPHA");
    if (!alpha || std::string(alpha) != "1") {
        extState().loaded[d.name] = d;
        return d;
    }
    try {
        // Create pipes for stdin/stdout
        int inpipe[2] = {-1, -1};
        int outpipe[2] = {-1, -1};
        if (pipe(inpipe) != 0 || pipe(outpipe) != 0) {
            if (inpipe[0] != -1) {
                ::close(inpipe[0]);
                ::close(inpipe[1]);
            }
            if (outpipe[0] != -1) {
                ::close(outpipe[0]);
                ::close(outpipe[1]);
            }
            return Error{ErrorCode::InternalError, "Failed to create pipes"};
        }

        pid_t pid = fork();
        if (pid < 0) {
            ::close(inpipe[0]);
            ::close(inpipe[1]);
            ::close(outpipe[0]);
            ::close(outpipe[1]);
            return Error{ErrorCode::InternalError, "fork() failed"};
        }

        if (pid == 0) {
            // Child: connect pipes and exec shell command
            ::dup2(inpipe[0], STDIN_FILENO);
            ::dup2(outpipe[1], STDOUT_FILENO);
            // Close inherited fds
            ::close(inpipe[0]);
            ::close(inpipe[1]);
            ::close(outpipe[0]);
            ::close(outpipe[1]);
            // Execute using /bin/sh -c "cmd" in plugin directory
            ::chdir(canon.c_str());
            execl("/bin/sh", "sh", "-c", cmd.c_str(), (char*)nullptr);
            _exit(127);
        }

        // Parent: close unused ends
        ::close(inpipe[0]);
        ::close(outpipe[1]);

        // Set non-blocking read on stdout
        int flags = fcntl(outpipe[0], F_GETFL, 0);
        if (flags >= 0)
            (void)fcntl(outpipe[0], F_SETFL, flags | O_NONBLOCK);

        // Handshake: request manifest, then init
        std::string hs =
            "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"handshake.manifest\",\"params\":{}}\n";
        ssize_t wr = ::write(inpipe[1], hs.data(), hs.size());
        if (wr < 0) {
            ::kill(pid, SIGKILL);
            ::close(inpipe[1]);
            ::close(outpipe[0]);
            (void)waitpid(pid, nullptr, 0);
            return Error{ErrorCode::InternalError, "Failed to write handshake to plugin"};
        }

        // Wait for manifest response with a short timeout
        struct pollfd pfd{outpipe[0], POLLIN, 0};
        int pr = ::poll(&pfd, 1, 3000);
        if (pr <= 0) {
            ::kill(pid, SIGKILL);
            ::close(inpipe[1]);
            ::close(outpipe[0]);
            (void)waitpid(pid, nullptr, 0);
            return Error{ErrorCode::Timeout, "Plugin manifest handshake timeout"};
        }

        // Read available bytes (best-effort)
        char buf[1024];
        ssize_t rd = ::read(outpipe[0], buf, sizeof(buf));
        std::string resp;
        if (rd > 0)
            resp.assign(buf, buf + rd);

        // Minimal sanity: expect a JSON-RPC result-like payload
        if (resp.find("\"result\"") == std::string::npos &&
            resp.find("\"interfaces\"") == std::string::npos) {
            ::kill(pid, SIGKILL);
            ::close(inpipe[1]);
            ::close(outpipe[0]);
            (void)waitpid(pid, nullptr, 0);
            return Error{ErrorCode::InvalidState, "Plugin manifest handshake invalid"};
        }

        // Send plugin.init
        std::string init =
            "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"plugin.init\",\"params\":{}}\n";
        wr = ::write(inpipe[1], init.data(), init.size());
        if (wr < 0) {
            ::kill(pid, SIGKILL);
            ::close(inpipe[1]);
            ::close(outpipe[0]);
            (void)waitpid(pid, nullptr, 0);
            return Error{ErrorCode::InternalError, "Failed to write init to plugin"};
        }

        // Wait for init response
        pfd.fd = outpipe[0];
        pfd.events = POLLIN;
        pfd.revents = 0;
        pr = ::poll(&pfd, 1, 3000);
        if (pr <= 0) {
            ::kill(pid, SIGKILL);
            ::close(inpipe[1]);
            ::close(outpipe[0]);
            (void)waitpid(pid, nullptr, 0);
            return Error{ErrorCode::Timeout, "Plugin init handshake timeout"};
        }

        // Read init response
        rd = ::read(outpipe[0], buf, sizeof(buf));
        resp.clear();
        if (rd > 0)
            resp.assign(buf, buf + rd);

        // Validate init response
        if (resp.find("\"result\"") == std::string::npos &&
            resp.find("\"ok\"") == std::string::npos) {
            ::kill(pid, SIGKILL);
            ::close(inpipe[1]);
            ::close(outpipe[0]);
            (void)waitpid(pid, nullptr, 0);
            return Error{ErrorCode::InvalidState, "Plugin init handshake invalid"};
        }

        // Store supervision state
        // Record loaded descriptor and launch metadata for supervision
        extState().loaded[d.name] = d;
        extState().cmds[d.name] = cmd;
        extState().cwds[d.name] = canon;
        extState().attempts[d.name] = 0;
        extState().crashes[d.name] = 0;
        extState().pids[d.name] = pid;
        extState().stdin_fds[d.name] = inpipe[1];
        extState().stdout_fds[d.name] = outpipe[0];

        return d;
    } catch (const std::exception& e) {
        return Error{ErrorCode::InternalError, e.what()};
    } catch (...) {
        return Error{ErrorCode::Unknown, "External plugin load failed"};
    }
}

Result<void> ExternalPluginHost::unload(const std::string& name) {
    auto& st = extState();
    // Send shutdown if process exists
    auto itPid = st.pids.find(name);
    if (itPid != st.pids.end()) {
        int inFd = -1, outFd = -1;
        auto itIn = st.stdin_fds.find(name);
        auto itOut = st.stdout_fds.find(name);
        if (itIn != st.stdin_fds.end())
            inFd = itIn->second;
        if (itOut != st.stdout_fds.end())
            outFd = itOut->second;

        // Best-effort shutdown JSON-RPC
        if (inFd >= 0) {
            std::string shutdownMsg =
                "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"yams.shutdown\",\"params\":{}}\n";
            (void)::write(inFd, shutdownMsg.data(), shutdownMsg.size());
            ::close(inFd);
            st.stdin_fds.erase(itIn);
        }
        // Wait for process to exit (up to ~2s)
        int status = 0;
        for (int i = 0; i < 20; ++i) {
            pid_t w = ::waitpid(itPid->second, &status, WNOHANG);
            if (w == itPid->second)
                break;
            ::usleep(100000);
        }
        // Force kill if still running
        (void)::kill(itPid->second, SIGTERM);
        (void)::kill(itPid->second, SIGKILL);
        (void)::waitpid(itPid->second, nullptr, 0);

        if (outFd >= 0) {
            ::close(outFd);
            st.stdout_fds.erase(name);
        }
        st.pids.erase(itPid);
    }

    st.loaded.erase(name);
    return Result<void>();
}

std::vector<PluginDescriptor> ExternalPluginHost::listLoaded() const {
    std::vector<PluginDescriptor> v;
    for (auto& [k, d] : extState().loaded)
        v.push_back(d);
    return v;
}

std::vector<std::filesystem::path> ExternalPluginHost::trustList() const {
    return std::vector<std::filesystem::path>(extState().trusted.begin(), extState().trusted.end());
}

Result<void> ExternalPluginHost::trustAdd(const std::filesystem::path& p) {
    namespace fs = std::filesystem;
    std::error_code ec;
    fs::path canon = fs::weakly_canonical(p, ec);
    if (ec)
        canon = p;

    // Refuse world-writable paths (and immediate parent)
    auto perms = fs::status(canon, ec).permissions();
    if (!ec && ((perms & fs::perms::others_write) != fs::perms::none)) {
        return Error{ErrorCode::InvalidArgument, "Refusing world-writable path: " + canon.string()};
    }
    auto parent = canon.parent_path();
    if (!parent.empty()) {
        auto pperms = fs::status(parent, ec).permissions();
        if (!ec && ((pperms & fs::perms::others_write) != fs::perms::none)) {
            return Error{ErrorCode::InvalidArgument,
                         "Refusing world-writable parent: " + parent.string()};
        }
    }

    extState().trusted.insert(canon);
    extSaveTrust();
    return Result<void>();
}
Result<void> ExternalPluginHost::trustRemove(const std::filesystem::path& p) {
    namespace fs = std::filesystem;
    std::error_code ec;
    fs::path canon = fs::weakly_canonical(p, ec);
    if (ec)
        canon = p;
    extState().trusted.erase(canon);
    extSaveTrust();
    return Result<void>();
}

Result<std::string> ExternalPluginHost::health(const std::string& name) {
    auto& st = extState();
    if (!st.loaded.count(name))
        return Error{ErrorCode::NotFound, "Not loaded"};

    // Supervise once to detect exit and possibly restart (backoff)
    extSuperviseOnce(name);

    auto itPid = st.pids.find(name);
    auto itIn = st.stdin_fds.find(name);
    auto itOut = st.stdout_fds.find(name);
    if (itPid == st.pids.end() || itIn == st.stdin_fds.end() || itOut == st.stdout_fds.end()) {
        return Error{ErrorCode::NotFound, "Not running"};
    }

    // Request plugin.health over JSON-RPC
    std::string req = "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"plugin.health\",\"params\":{}}\n";
    ssize_t wr = ::write(itIn->second, req.data(), req.size());
    if (wr < 0) {
        return Error{ErrorCode::InternalError, "Failed to write health request"};
    }

    struct pollfd pfd{itOut->second, POLLIN, 0};
    int pr = ::poll(&pfd, 1, 1000);
    if (pr <= 0) {
        return Error{ErrorCode::Timeout, "Health request timeout"};
    }

    char buf[512];
    ssize_t rd = ::read(itOut->second, buf, sizeof(buf));
    if (rd <= 0) {
        return Error{ErrorCode::InvalidState, "No health response"};
    }
    std::string resp(buf, buf + rd);
    if (resp.find("\"error\"") != std::string::npos) {
        return Error{ErrorCode::InvalidState, "Health error"};
    }
    return resp;
}

void ExternalPluginHost::setTrustFile(const std::filesystem::path& trustFile) {
    extState().trustFile = trustFile;
    extLoadTrust();
}

} // namespace yams::daemon
