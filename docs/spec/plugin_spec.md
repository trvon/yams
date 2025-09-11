## Host Services API (v1)

While the primary plugin model involves the host calling into plugins that implement specific interfaces, some plugins may require the ability to call back into the YAMS host to leverage its core application services. The Host Services API provides a secure and versioned mechanism for this.

- **Host Context**: During initialization, the host passes a `yams_plugin_host_context_v1` struct to the plugin via the `yams_plugin_init` function. This struct contains function pointers to the services that the host exposes.
- **Versioning**: The Host Services API is versioned independently of the main plugin ABI. The version is specified in the `yams_plugin_host_context_v1` struct.
- **C-ABI**: The service interfaces are exposed as C-style v-tables of function pointers. The data structures for requests and responses are also converted to C-style structs. The full definition is in `include/yams/plugins/host_services_v1.h`.
- **Memory Management**: For any function that returns data in a response struct, the host is responsible for allocating the memory for that struct and its contents. The plugin is responsible for calling the corresponding `free_*_response` function (provided in the same service v-table) to release the memory.

Example usage in a plugin:

```c
#include "yams/plugins/abi.h"
#include "yams/plugins/host_services_v1.h"

static const yams_document_service_v1* g_doc_service = NULL;

int yams_plugin_init(const char* config_json, const void* host_context) {
    if (host_context == NULL) {
        return YAMS_PLUGIN_ERR_INVALID;
    }
    const yams_plugin_host_context_v1* ctx = (const yams_plugin_host_context_v1*)host_context;
    if (ctx->version != YAMS_PLUGIN_HOST_SERVICES_API_VERSION) {
        return YAMS_PLUGIN_ERR_INCOMPATIBLE;
    }
    g_doc_service = ctx->document_service;
    return YAMS_PLUGIN_OK;
}

void my_plugin_function() {
    if (g_doc_service) {
        yams_list_documents_request_t req = { .limit = 10 };
        yams_list_documents_response_t* res = NULL;
        if (g_doc_service->list(g_doc_service->handle, &req, &res) == 0 && res) {
            // ... use the response ...
            g_doc_service->free_list_documents_response(res);
        }
    }
}
``` -> { passed: bool, failures: [path] }
  - pointer_gate(manifest_id) -> { allow_advance: bool, reason?: string }

- sql_db_v1 (optional)
  - connect(cfg), exec(sql, params), query(sql, params) -> rows, begin/commit/rollback

- vector_store_v1 (optional)
  - upsert(vectors), query(emb, topk), delete(ids), stats()

- model_provider_v1 (v1)
  - Identity: id="model_provider_v1", version=1
  - Header: include/yams/plugins/model_provider_v1.h
  - Vtable fields:
    - set_progress_callback(self, cb, user)
    - load_model(self, model_id, model_path?, options_json?)
    - unload_model(self, model_id)
    - is_model_loaded(self, model_id, out_loaded)
    - get_loaded_models(self, out_ids, out_count) / free_model_list(self, ids, count)
    - generate_embedding(self, model_id, input_bytes, len, out_vec, out_dim) / free_embedding(self, vec, dim)
    - generate_embedding_batch(self, model_id, inputs[], lens[], batch, out_vecs, out_batch, out_dim) / free_embedding_batch(self, vecs, batch, dim)
  - Memory: plugin allocates all returned buffers; host frees with free_* methods from the same vtable (never free()).
  - Progress: callback may be invoked from background threads; phases: PROBE, DOWNLOAD, LOAD, WARMUP, READY; bytes current/total optional.
  - Status codes: int ABI — YAMS_OK, YAMS_ERR_INVALID_ARG, YAMS_ERR_NOT_FOUND, YAMS_ERR_IO, YAMS_ERR_INTERNAL, YAMS_ERR_UNSUPPORTED.

## Transport Bindings

- C‑ABI (Native)
  - Symbols (see include/yams/plugins/abi.h):
    - yams_plugin_get_abi_version() -> int
    - yams_plugin_get_name() -> const char*
    - yams_plugin_get_version() -> const char*
    - yams_plugin_get_manifest_json() -> const char*
    - yams_plugin_get_interface(const char* iface_id, uint32_t ver, void** out_iface)
    - yams_plugin_init(const char* config_json, const void* host_context) -> int
    - yams_plugin_shutdown()
    - yams_plugin_get_health_json(char** out_json) (optional)
  - Interface functions are exposed via typed vtables returned by get_interface().

- WASM Host (WASI) (Sandbox)
  - Build to WASI target. Interfaces are specified via WIT files; host provides imports:
    - hostcalls for IO mediation (e.g., object_storage.put, .get)
    - time, logging, random sources (limited)
  - Minimal WIT sketch (object_storage_v1):
```
package yams:plugins/object-storage-v1@1.0.0

interface object-storage-v1 {
  put-object: func(path: string, bytes: list<u8>, sha256hex: string) -> result<string, u32>
  get-object: func(path: string) -> result<list<u8>, u32>
  head-object: func(path: string) -> result<record { size: u64, etag: string }, u32>
  // ... multipart, list, delete, object-lock, etc.
}
```
  - Errors map to u32 ErrorCode. Host enforces no direct FS/net; all cloud ops via hostcalls.

- External Process (JSON‑RPC)
  - Framing: newline‑delimited JSON over stdio or local socket.
  - Methods: `plugin.init`, `plugin.shutdown`, `iface.call` with `{id, version, method, params}`.
  - Health: `plugin.health` -> JSON status.

  JSON‑RPC envelope (example):
  ```json
  {"jsonrpc":"2.0","id":"42","method":"plugin.init","params":{"config":"{...}"}}
  {"jsonrpc":"2.0","id":"42","result":{"ok":true}}
  {"jsonrpc":"2.0","id":"43","method":"iface.call","params":{"id":"object_storage_v1","version":1,"method":"put_object","params":{"path":"a","sha256hex":"...","bytes":"<base64>"}}}
  {"jsonrpc":"2.0","id":"43","result":{"etag":"..."}}
  ```

## Lifecycle
- Scan: host dlsym/WASM introspection/JSON‑RPC handshake to read manifest (no init side‑effects).
- Load: trust‑check -> init(config_json) -> Interface registry -> health (optional).
- Calls: host marshals requests to interface binding (C‑ABI direct calls; WASM/External via hostcalls/JSON‑RPC).
- Unload: shutdown(); reclaim handles/processes; evict from registry.

## Error Model
- Map plugin errors to YAMS ErrorCode; preserve message.
- Host wraps transport failures with appropriate codes (Unauthorized, Timeout, Internal, NotImplemented).
 - Canonical ErrorCode mapping:
   - 0 Success, 1 FileNotFound, 2 PermissionDenied, 3 CorruptedData, 4 StorageFull,
     5 InvalidArgument, 6 NetworkError, 7 Unauthorized, 8 Timeout, 9 NotImplemented,
     10 InternalError, 11 NotFound, 12 RateLimited, 13 ValidationError (expandable)

## Security & Trust
- Trust profiles: default‑deny; trusted paths in config (and CLI trust add/list/remove).
- WASM default sandbox: no ambient net/files; capabilities declared in manifest; host allows only declared ops.
- External process: supervised, resource caps, no shell expansion; commandline controlled by host.

## Observability
- Health JSON: `{ status: ok|degraded|error, details?: {...} }` when available.
- Host metrics: `plugins_loaded`, `plugins_failed`, `plugins_trusted`, per‑plugin call counts/latency.

## Compatibility & Versioning
- Interface versions increase monotonically; host may support multiple versions in parallel.
- Plugins must declare exact `{id, version}` tuples in manifest.
- ABI version applies to C‑ABI. WASM/External use interface versioning and host protocol version.

## Capabilities Registry (initial)
- `net:aws` — Outbound AWS network calls permitted via host (External/WASM)
- `object_lock` — Plugin can orchestrate object lock retention
- `rtc_metrics` — Plugin can read CRR/RTC replication metrics and expose lag
- `fs:temp` — Temporary filesystem access (External)


---

# Examples & Guidance

## S3 Plugin (External, Python)
- Implements `object_storage_v1` + `dr_provider_v1` using boto3.
- Runner: Python module with a small JSON‑RPC server over stdio.
- Manifest declares `net:aws`, `object_lock`, `rtc_metrics`.

## R2 Plugin (External, Python)
- Same interfaces, but readiness is presence‑only; manifest omits `rtc_metrics`.

## Packaging & Install
- Publish artifacts to a plugin repo; CLI `yams plugin install` can fetch into a trusted dir.
- Provide sample configs and minimal integration tests.
