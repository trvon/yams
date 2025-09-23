#pragma once

#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace yams::daemon {

// Lightweight wrapper around wasmtime-cpp (when available) to load a WASM module
// and call simple exported functions. When wasmtime is not available at build time,
// this class compiles to stubs that return errors.
class WasmRuntime {
public:
    struct Result {
        bool ok{false};
        std::string error;
        std::string json; // for head-object JSON payloads
    };

    struct Limits {
        uint64_t memory_max_bytes = 128ULL * 1024ULL * 1024ULL;
        int max_instances = 10;
        int call_timeout_ms = 5000;
    };

    WasmRuntime();
    ~WasmRuntime();

    // Configure runtime limits (memory, concurrency, timeouts)
    void setLimits(const Limits& limits);
    Limits getLimits() const;

    // Load a WASM module from file (WASI sandboxed, no ambient FS/net by default)
    Result load(const std::filesystem::path& wasmFile, const std::string& configJson);

    // Call an exported function that returns a JSON blob located in guest memory.
    // Convention: export name `object_storage_v1_head`, signature:
    //   (i32 key_ptr, i32 key_len, i32 opts_ptr, i32 opts_len) -> i64 (ptr<<32 | len)
    // The host will copy the [ptr,len] bytes from guest memory into Result::json.
    Result callHeadObject(const std::string& key, const std::string& optsJson);

    // Generalized two-argument JSON-returning export: calls `exportName(a_ptr,a_len,b_ptr,b_len)`
    // returning i64 (ptr<<32 | len) into guest memory, which is then copied to Result::json.
    Result callJsonExport(const std::string& exportName, const std::string& a,
                          const std::string& b);

    // GraphAdapter v1 convenience wrappers (JSON-bridged exports)
    Result callGraphAdapter(const std::string& method, const std::string& a, const std::string& b);
    Result callGraphAdapterListGraphs(const std::string& filterJson);
    Result callGraphAdapterExportGraph(const std::string& graphId, const std::string& optionsJson);

private:
    struct Impl;
    std::unique_ptr<Impl> pImpl_;
};

} // namespace yams::daemon
