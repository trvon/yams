#include <atomic>
#include <chrono>
#include <cstring>
#include <random>
#include <sstream>
#include <thread>
#include <vector>
#include <yams/daemon/resource/wasm_runtime.h>

#if defined(YAMS_HAVE_WASMTIME)
#if __has_include(<wasmtime.hh>)
#include <wasmtime.hh>
#define YAMS_WASMTIME_AVAILABLE 1
#else
#define YAMS_WASMTIME_AVAILABLE 0
#endif
#else
#define YAMS_WASMTIME_AVAILABLE 0
#endif

namespace yams::daemon {

struct WasmRuntime::Impl {
    WasmRuntime::Limits limits;
#if YAMS_WASMTIME_AVAILABLE
    std::unique_ptr<wasmtime::Engine> engine;
    std::unique_ptr<wasmtime::Store<>> store;
    std::thread epoch_thread;
    std::atomic<bool> epoch_stop{false};
    std::atomic<uint64_t> epoch_counter{0};
    bool epoch_enabled{false};
    std::optional<wasmtime::Module> module;
    std::optional<wasmtime::Linker> linker;
    std::optional<wasmtime::Instance> instance;
    std::optional<wasmtime::Memory> memory;

    static std::optional<wasmtime::Func> getFunc(wasmtime::Store<>& st,
                                                 const wasmtime::Instance& inst, const char* name) {
        auto exp = inst.get_export(st, name);
        if (!exp)
            return std::nullopt;
        if (auto f = exp->func())
            return f;
        return std::nullopt;
    }

    static std::optional<wasmtime::Memory> getMemory(wasmtime::Store<>& st,
                                                     const wasmtime::Instance& inst) {
        auto exp = inst.get_export(st, "memory");
        if (!exp)
            return std::nullopt;
        if (auto m = exp->memory())
            return m;
        return std::nullopt;
    }

    // Call guest malloc-like allocator if present (exported as "alloc")
    static std::optional<uint32_t> guest_alloc(wasmtime::Store<>& st,
                                               const wasmtime::Instance& inst, size_t size) {
        auto f = getFunc(st, inst, "alloc");
        if (!f)
            return std::nullopt;
        wasmtime::Val args[1] = {wasmtime::Val::i32(static_cast<int32_t>(size))};
        wasmtime::Val results[1];
        auto res = f->call(st, args, results);
        if (!res)
            return std::nullopt;
        return static_cast<uint32_t>(results[0].i32());
    }
#endif
};

WasmRuntime::WasmRuntime() : pImpl_(std::make_unique<Impl>()) {
#if YAMS_WASMTIME_AVAILABLE
    try {
        wasmtime::Config cfg;
        cfg.epoch_interruption(true);
        pImpl_->engine = std::make_unique<wasmtime::Engine>(cfg);
        pImpl_->store = std::make_unique<wasmtime::Store<>>(*pImpl_->engine);
        pImpl_->epoch_enabled = true;
        pImpl_->epoch_stop.store(false, std::memory_order_relaxed);
        pImpl_->epoch_thread = std::thread([this]() {
            using namespace std::chrono_literals;
            while (!pImpl_->epoch_stop.load(std::memory_order_relaxed)) {
                try {
                    pImpl_->engine->increment_epoch();
                    pImpl_->epoch_counter.fetch_add(1, std::memory_order_relaxed);
                } catch (...) {
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        });
    } catch (...) {
        // Fallback to default engine/store without epoch interruption
        try {
            pImpl_->engine = std::make_unique<wasmtime::Engine>();
            pImpl_->store = std::make_unique<wasmtime::Store<>>(*pImpl_->engine);
        } catch (...) {
        }
    }
#endif
}
WasmRuntime::~WasmRuntime() {
#if YAMS_WASMTIME_AVAILABLE
    try {
        pImpl_->epoch_stop.store(true, std::memory_order_relaxed);
        if (pImpl_->epoch_thread.joinable())
            pImpl_->epoch_thread.join();
    } catch (...) {
    }
#endif
}

void WasmRuntime::setLimits(const Limits& limits) {
    pImpl_->limits = limits;
}
WasmRuntime::Limits WasmRuntime::getLimits() const {
    return pImpl_->limits;
}

WasmRuntime::Result WasmRuntime::load(const std::filesystem::path& wasmFile,
                                      const std::string& /*configJson*/) {
    WasmRuntime::Result r;
    if (!std::filesystem::exists(wasmFile)) {
        r.ok = false;
        r.error = "WASM file not found: " + wasmFile.string();
        return r;
    }
#if YAMS_WASMTIME_AVAILABLE
    try {
        pImpl_->module.emplace(*pImpl_->engine, wasmFile.string());
        pImpl_->linker.emplace(*pImpl_->engine);
        // Do not add WASI by default (sandboxed). Wire basic hostcalls under "yams".
#if YAMS_WASMTIME_AVAILABLE
        try {
            auto host_now = wasmtime::Func::wrap(*pImpl_->store, []() -> int64_t {
                using namespace std::chrono;
                return duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
            });
            auto host_log =
                wasmtime::Func::wrap(*pImpl_->store, [this](int32_t lvl_ptr, int32_t lvl_len,
                                                            int32_t msg_ptr, int32_t msg_len) {
                    if (!pImpl_->memory)
                        return;
                    auto mem = *pImpl_->memory;
                    auto& st = *pImpl_->store;
                    uint8_t* base = static_cast<uint8_t*>(mem.data(st));
                    size_t mem_size = mem.data_size(st);
                    auto safe = [&](int32_t off, int32_t len) -> std::string {
                        if (off < 0 || len < 0)
                            return {};
                        if (static_cast<size_t>(off) + static_cast<size_t>(len) > mem_size)
                            return {};
                        return std::string(reinterpret_cast<const char*>(base + off),
                                           static_cast<size_t>(len));
                    };
                    std::string lvl = safe(lvl_ptr, lvl_len);
                    std::string msg = safe(msg_ptr, msg_len);
                    (void)lvl;
                    if (!msg.empty()) {
                        fprintf(stdout, "[wasm] %s\n", msg.c_str());
                        fflush(stdout);
                    }
                });
            auto host_rand =
                wasmtime::Func::wrap(*pImpl_->store, [this](int32_t ptr, int32_t len) -> int32_t {
                    if (!pImpl_->memory)
                        return -1;
                    auto mem = *pImpl_->memory;
                    auto& st = *pImpl_->store;
                    uint8_t* base = static_cast<uint8_t*>(mem.data(st));
                    size_t mem_size = mem.data_size(st);
                    if (ptr < 0 || len < 0)
                        return -1;
                    if (static_cast<size_t>(ptr) + static_cast<size_t>(len) > mem_size)
                        return -1;
                    std::random_device rd;
                    for (int32_t i = 0; i < len; ++i)
                        base[ptr + i] = static_cast<uint8_t>(rd());
                    return 0;
                });
            pImpl_->linker->define("yams", "host_now_ms", host_now);
            pImpl_->linker->define("yams", "host_log", host_log);
            pImpl_->linker->define("yams", "host_random_fill", host_rand);
        } catch (...) {
            // ignore hostcall wiring errors
        }
#endif
        pImpl_->instance.emplace(pImpl_->linker->instantiate(*pImpl_->store, *pImpl_->module));
        auto mem = Impl::getMemory(*pImpl_->store, *pImpl_->instance);
        if (!mem) {
            r.ok = false;
            r.error = "WASM module does not export memory";
            return r;
        }
        pImpl_->memory = mem;
        // Best-effort memory limit check on initial memory
        if (pImpl_->limits.memory_max_bytes > 0) {
            size_t init_size = pImpl_->memory->data_size(*pImpl_->store);
            if (init_size > pImpl_->limits.memory_max_bytes) {
                r.ok = false;
                r.error = "WASM initial memory exceeds limit";
                return r;
            }
        }
        r.ok = true;
        return r;
    } catch (const std::exception& e) {
        r.ok = false;
        r.error = e.what();
        return r;
    }
#else
    r.ok = false;
    r.error = "wasmtime-cpp not available at build time";
#endif
    return r;
}

WasmRuntime::Result WasmRuntime::callHeadObject(const std::string& key,
                                                const std::string& optsJson) {
    return callJsonExport("object_storage_v1_head", key, optsJson);
}

WasmRuntime::Result WasmRuntime::callJsonExport(const std::string& exportName, const std::string& a,
                                                const std::string& b) {
    WasmRuntime::Result r;
#if YAMS_WASMTIME_AVAILABLE
    try {
        if (!pImpl_->instance || !pImpl_->memory) {
            r.ok = false;
            r.error = "WASM instance not loaded";
            return r;
        }
        auto& store = *pImpl_->store;
        auto inst = *pImpl_->instance;
        auto mem = *pImpl_->memory;

        // Best-effort memory limit check before call/allocations
        if (pImpl_->limits.memory_max_bytes > 0) {
            size_t cur_size = mem.data_size(store);
            if (cur_size > pImpl_->limits.memory_max_bytes) {
                r.ok = false;
                r.error = "WASM memory limit exceeded";
                return r;
            }
        }

        auto aPtrOpt = Impl::guest_alloc(store, inst, a.size());
        auto bPtrOpt = Impl::guest_alloc(store, inst, b.size());
        if (!aPtrOpt || !bPtrOpt) {
            r.ok = false;
            r.error = "Guest allocator 'alloc' not found or failed";
            return r;
        }
        uint32_t aPtr = *aPtrOpt;
        uint32_t bPtr = *bPtrOpt;

        uint8_t* base = static_cast<uint8_t*>(mem.data(store));
        size_t mem_size = mem.data_size(store);
        if (aPtr + a.size() > mem_size || bPtr + b.size() > mem_size) {
            r.ok = false;
            r.error = "Guest memory overflow";
            return r;
        }
        // Enforce that we don't operate when guest memory already exceeds limit
        if (pImpl_->limits.memory_max_bytes > 0 && mem_size > pImpl_->limits.memory_max_bytes) {
            r.ok = false;
            r.error = "WASM memory limit exceeded";
            return r;
        }

        std::memcpy(base + aPtr, a.data(), a.size());
        std::memcpy(base + bPtr, b.data(), b.size());

        auto funcOpt = Impl::getFunc(store, inst, exportName.c_str());
        if (!funcOpt) {
            r.ok = false;
            r.error = std::string("Export '") + exportName + "' not found";
            return r;
        }
        auto func = *funcOpt;
        wasmtime::Val args[4] = {wasmtime::Val::i32(static_cast<int32_t>(aPtr)),
                                 wasmtime::Val::i32(static_cast<int32_t>(a.size())),
                                 wasmtime::Val::i32(static_cast<int32_t>(bPtr)),
                                 wasmtime::Val::i32(static_cast<int32_t>(b.size()))};
        wasmtime::Val results[1];
        auto start = std::chrono::steady_clock::now();
        uint64_t _epoch_ticks = 0;
#if YAMS_WASMTIME_AVAILABLE
        try {
            if (pImpl_->epoch_enabled && pImpl_->limits.call_timeout_ms > 0) {
                _epoch_ticks = static_cast<uint64_t>(std::max(1, pImpl_->limits.call_timeout_ms));
                pImpl_->store->set_epoch_deadline(
                    pImpl_->epoch_counter.load(std::memory_order_relaxed) + _epoch_ticks);
            }
        } catch (...) {
        }
#endif
#if YAMS_WASMTIME_AVAILABLE
        /* epoch deadline set relative above */
#endif
        auto cres = func.call(store, args, results);
#if YAMS_WASMTIME_AVAILABLE
        /* epoch deadline cleared below if needed */
#endif
#if YAMS_WASMTIME_AVAILABLE
        try {
            if (_epoch_ticks)
                pImpl_->store->set_epoch_deadline(0);
        } catch (...) {
        }
#endif
        auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                              std::chrono::steady_clock::now() - start)
                              .count();
        if (!cres) {
            r.ok = false;
            r.error = "WASM call failed";
            return r;
        }
        if (pImpl_->limits.call_timeout_ms > 0 && elapsed_ms > pImpl_->limits.call_timeout_ms) {
            r.ok = false;
            r.error = "WASM call timeout";
            return r;
        }

        // Best-effort memory limit check after call/possible growth
        if (pImpl_->limits.memory_max_bytes > 0) {
            size_t after_size = mem.data_size(store);
            if (after_size > pImpl_->limits.memory_max_bytes) {
                r.ok = false;
                r.error = "WASM memory limit exceeded after call";
                return r;
            }
        }

        uint64_t packed = static_cast<uint64_t>(results[0].i64());
        uint32_t outPtr = static_cast<uint32_t>(packed >> 32);
        uint32_t outLen = static_cast<uint32_t>(packed & 0xffffffffu);
        if (outPtr + outLen > mem.data_size(store)) {
            r.ok = false;
            r.error = "Guest returned invalid buffer range";
            return r;
        }
        r.json.assign(reinterpret_cast<const char*>(base + outPtr), outLen);
        r.ok = true;
        return r;
    } catch (const std::exception& e) {
        r.ok = false;
        r.error = e.what();
        return r;
    }
#else
    r.ok = false;
    r.error = "wasmtime-cpp not available at build time";
#endif
    return r;
}

} // namespace yams::daemon
