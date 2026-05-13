#include <yams/cli/doctor/checks/plugin_check.h>

#include <yams/cli/doctor/plugin_trust.h>
#include <yams/cli/daemon_helpers.h>
#include <yams/cli/result_helpers.h>
#include <yams/cli/yams_cli.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/plugins/model_provider_v1.h>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <stdexcept>
#include <string>

#ifdef _WIN32
#include <windows.h>
#define RTLD_LAZY 0
#define RTLD_LOCAL 0

static void* dlopen_impl(const char* filename, int /*flags*/) {
    return LoadLibraryA(filename);
}

static void* dlsym_impl(void* handle, const char* symbol) {
    return reinterpret_cast<void*>(GetProcAddress(static_cast<HMODULE>(handle), symbol));
}

static int dlclose_impl(void* handle) {
    return FreeLibrary(static_cast<HMODULE>(handle)) ? 0 : -1;
}

static const char* dlerror_impl() {
    static char buf[256];
    FormatMessageA(FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS, nullptr,
                   GetLastError(), MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT), buf, sizeof(buf),
                   nullptr);
    return buf;
}

#define dlopen dlopen_impl
#define dlsym dlsym_impl
#define dlclose dlclose_impl
#define dlerror dlerror_impl
#else
#include <dlfcn.h>
#endif

namespace yams::cli::doctor {

void PluginCheck::execute(std::ostream& os, YamsCLI* cli, const Config& cfg) {
    namespace fs = std::filesystem;

    os << "Plugin Doctor: " << cfg.arg << "\n";
    fs::path target(cfg.arg);
    if (!fs::exists(target)) {
        auto rp = yams::cli::doctor::PluginTrust::resolveByName(cfg.arg);
        if (rp)
            target = *rp;
    }
    if (!fs::exists(target)) {
        os << "  [FAIL] Not found as path or name in default dirs\n";
        return;
    }

    auto trusted = yams::cli::doctor::PluginTrust::readTrusted();
    bool isTrusted = yams::cli::doctor::PluginTrust::isTrustedPath(target, trusted);

    void* handle = dlopen(target.string().c_str(), RTLD_LAZY | RTLD_LOCAL);
    if (!handle) {
        const char* dlErr = dlerror();
        os << "  [FAIL] dlopen: " << (dlErr ? dlErr : "unknown") << "\n";
        return;
    }
    auto close = [&]() { dlclose(handle); };
    auto get_abi = reinterpret_cast<int (*)()>(dlsym(handle, "yams_plugin_get_abi_version"));
    auto get_name = reinterpret_cast<const char* (*)()>(dlsym(handle, "yams_plugin_get_name"));
    auto get_ver = reinterpret_cast<const char* (*)()>(dlsym(handle, "yams_plugin_get_version"));
    auto get_manifest =
        reinterpret_cast<const char* (*)()>(dlsym(handle, "yams_plugin_get_manifest_json"));
    bool have_core = (get_abi && get_name && get_ver);
    if (!have_core) {
        os << "  [FAIL] Missing required ABI symbols (get_abi/get_name/get_version)\n";
        close();
        return;
    }
    int abi = get_abi();
    std::string pname = get_name() ? get_name() : "";
    std::string pver = get_ver() ? get_ver() : "";
    std::string manifest = (get_manifest && get_manifest()) ? get_manifest() : std::string();
    os << "  Name: " << pname << "  Version: " << pver << "  ABI: " << abi << "\n";
    os << "  Trusted: " << (isTrusted ? "yes" : "no") << "\n";

    std::string iface = cfg.ifaceId.empty() ? std::string("model_provider_v1") : cfg.ifaceId;
    uint32_t ivers = (cfg.ifaceVersion == 0 ? 1u : cfg.ifaceVersion);
    using GetIfaceFn = int (*)(const char*, uint32_t, void**);
    dlerror();
    auto get_iface = reinterpret_cast<GetIfaceFn>(dlsym(handle, "yams_plugin_get_interface"));
    const char* dlerr = dlerror();
    if (dlerr || !get_iface) {
        os << "  Interface: [SKIP] get_interface symbol not found\n";
    } else {
        void* out = nullptr;
        int rc = get_iface(iface.c_str(), ivers, &out);
        os << "  Interface: " << iface << " v" << ivers << " -> "
           << ((rc == 0 && out) ? "AVAILABLE" : "UNAVAILABLE") << "\n";
        if (rc == 0 && out && iface == std::string(YAMS_IFACE_MODEL_PROVIDER_V1) &&
            ivers == YAMS_IFACE_MODEL_PROVIDER_V1_VERSION) {
            auto* prov = reinterpret_cast<yams_model_provider_v1*>(out);
            bool has_batch = prov->generate_embedding_batch != nullptr;
            os << "  Batch API: " << (has_batch ? "PRESENT" : "MISSING") << "\n";
            if (has_batch) {
                const char* model_id = nullptr;
                std::string chosen;
                if (const char* pref = std::getenv("YAMS_PREFERRED_MODEL"))
                    chosen = pref;
                if (chosen.empty()) {
                    const char* candidates[] = {"nomic-embed-text-v1.5", "all-mpnet-base-v2",
                                                nullptr};
                    for (int i = 0; candidates[i]; ++i) {
                        chosen = candidates[i];
                        break;
                    }
                }
                model_id = chosen.c_str();
                bool loaded = false;
                if (prov->is_model_loaded) {
                    bool out_loaded = false;
                    if (prov->is_model_loaded(prov->self, model_id, &out_loaded) == YAMS_OK)
                        loaded = out_loaded;
                }
                if (!loaded && prov->load_model) {
                    (void)prov->load_model(prov->self, model_id, nullptr, nullptr);
                }
                const char* texts_c[2] = {"hello", "world"};
                size_t lens[2] = {5, 5};
                float* vecs = nullptr;
                size_t out_b = 0, out_d = 0;
                int brc = prov->generate_embedding_batch(
                    prov->self, model_id, reinterpret_cast<const uint8_t* const*>(texts_c), lens, 2,
                    &vecs, &out_b, &out_d);
                if (brc == YAMS_OK && out_b == 2 && out_d > 0 && vecs != nullptr) {
                    os << "  Batch probe: OK (batch=" << out_b << ", dim=" << out_d << ")\n";
                    if (prov->free_embedding_batch)
                        prov->free_embedding_batch(prov->self, vecs, out_b, out_d);
                } else {
                    os << "  Batch probe: FAIL (status=" << brc << ")\n";
                    if (vecs && prov->free_embedding_batch)
                        prov->free_embedding_batch(prov->self, vecs, out_b, out_d);
                }
            }
        }
    }
    close();

    if (!cfg.noDaemonProbe) {
        try {
            using namespace yams::daemon;
            yams::daemon::ClientConfig daemonCfg;
            if (cli)
                if (cli->hasExplicitDataDir()) {
                    daemonCfg.dataDir = cli->getDataPath();
                }
            daemonCfg.requestTimeout = std::chrono::milliseconds(4000);
            auto leaseRes = yams::cli::acquire_cli_daemon_client_shared(daemonCfg);
            if (!leaseRes) {
                throw std::runtime_error(leaseRes.error().message);
            }
            auto leaseHandle = std::move(leaseRes.value());
            auto& client = **leaseHandle;
            PluginLoadRequest req;
            req.pathOrName = target.string();
            req.dryRun = true;
            auto r = yams::cli::run_result<yams::daemon::PluginLoadResponse>(
                client.call(req), std::chrono::seconds(4));
            if (!r) {
                os << "  Daemon: DRY-RUN LOAD -> FAIL: " << r.error().message << "\n";
            } else {
                const auto& lr = r.value();
                os << "  Daemon: DRY-RUN LOAD -> OK (" << lr.record.name << ")\n";
            }
        } catch (const std::exception& e) {
            os << "  Daemon: DRY-RUN LOAD -> ERROR: " << e.what() << "\n";
        }
    }
}

} // namespace yams::cli::doctor
