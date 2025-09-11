# DR WASM Plugin Example (Prototype)

This folder contains a minimal example of a WASM-based DR plugin that the YAMS WASM host can load.

Goals:
- Provide a tiny module that exports required functions so the host can scan and load it.
- Demonstrate a simple readiness call returning JSON.

Exports required by the current host prototype:
- `memory`: linear memory (default export) for passing buffers.
- `alloc(size: i32) -> i32`: guest allocator to request a region to copy inputs.
- `dr_provider_v1_is_replication_ready(key_ptr: i32, key_len: i32, opts_ptr: i32, opts_len: i32) -> i64`:
  - Returns a packed pointer+length: `(ptr << 32) | len`, where `ptr` points into guest memory and `len` is number of bytes.
  - The host copies that JSON payload out of guest memory.

Manifest sidecar (required): place a JSON file next to the module with suffix `.manifest.json`.

Example `plugin.wasm.manifest.json`:
```
{
  "name": "dr_wasm",
  "version": "0.1.0",
  "interfaces": ["dr_provider_v1"]
}
```

Build (Python + WAT, requires `wat2wasm` in PATH):
```
python3 build_wasm.py
```
This produces `plugin.wasm` and `plugin.wasm.manifest.json`.

Test harness (ctest):
```
export TEST_WASM_PLUGIN_FILE=$(pwd)/plugin.wasm
ctest -R wasm_plugin_harness_test --output-on-failure
```

Notes:
- This example is intentionally minimal and returns a static readiness result. Extend it to parse inputs and compute real status.
- The YAMS WASM host currently disables ambient WASI by default; add hostcalls as needed in future iterations.

