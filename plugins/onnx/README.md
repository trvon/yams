# YAMS ONNX Plugin (Host‑Backed Model Provider)

This plugin implements a host‑backed ONNX model provider via the YAMS C‑ABI. When loaded, the daemon retrieves the `model_provider_v1` interface table from the plugin and adopts it as the active `IModelProvider`.

Interface
- Advertises: `{ id: "model_provider_v1", version: 2 }` (v1.2 pre‑stable)
- Base API: load/unload models; single and batch embeddings.
- Extensions implemented:
  - `get_embedding_dim`
  - `get_runtime_info_json` → returns `{ backend: "onnxruntime", pipeline: "raw_ort", model, dim, intra_threads, inter_threads }`
  - `free_string`
  - `set_threading` (applies per-model threading overrides; reloaded lazily on next inference)

Status
- The daemon uses the v1.2 helpers to populate embedding runtime details in status when this provider is adopted.

Future
- When ONNX Runtime GenAI is wired in YAMS, a GenAI pipeline will be preferred when present and may report `pipeline: genai`.

The plugin is designed to be robust in production and ergonomic in local workflows. It supports model discovery from Hugging Face Hub and local paths, parses model metadata (dimension, max sequence length, pooling/normalization), and communicates those capabilities back to YAMS for correct allocation, validation, and search behavior.

## Feature Summary
- Host‑loaded C‑ABI provider (`model_provider_v1`).
- Hugging Face model resolution: repo IDs, full URLs, or `hf://` scheme.
- Local model loading: directory with `model.onnx` (or compatible) and optional tokenizer/config.
- Automatic parameter discovery:
  - Embedding dimension (from ONNX output or metadata).
  - Max sequence length (from model/config; sensible fallback if absent).
  - Pooling strategy (CLS/mean/max) and vector normalization preferences.
  - Model version and hash (for staleness/compatibility checks).
- Caching and offline use with HF cache directory.
- Batch embedding and single‑pass normalization.

## Supported Model Sources

You can identify a model by any of the following in YAMS (e.g., in `[embeddings].preferred_model`):

- Hugging Face repo ID: `sentence-transformers/all-MiniLM-L6-v2`
- HF URL: `https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2`
- Explicit scheme: `hf://sentence-transformers/all-MiniLM-L6-v2`
- Local path: `/path/to/model_dir` (expects an ONNX model and optional tokenizer/config)

The plugin resolves Hugging Face models by:
- Checking the local HF cache (see `YAMS_ONNX_HF_CACHE` below).
- Downloading artifacts when permitted (configurable; see Security & Offline).
- Selecting a best‑known ONNX file (e.g., `model.onnx`, `onnx/model.onnx`).
- Reading `config.json` / `sentence_bert_config.json` where present to derive pooling/sequence length.

## Configuration

There are two layers of configuration: YAMS config (what users normally edit) and plugin‑specific options (advanced tuning), passed as JSON when the daemon asks the plugin to load a model.

### YAMS `config.toml`

```
[embeddings]
preferred_model = "sentence-transformers/all-MiniLM-L6-v2"   # or hf://... or local path
model_path       = "~/.yams/models"                           # optional hint for local/managed models
keep_model_hot   = true
preload_on_startup = true
```

### Environment variables (plugin)

- `YAMS_ONNX_HF_TOKEN` — Optional Hugging Face token for gated models.
- `YAMS_ONNX_HF_CACHE` — HF cache directory override (default HF cache rules apply).
- `YAMS_ONNX_OFFLINE` — If set to `1/true`, avoid network; only use cached/local artifacts.
- `YAMS_ONNX_LOG_LEVEL` — `trace|debug|info|warn|error` (plugin’s internal logging).

### Plugin options JSON (advanced)

When YAMS calls `load_model(model_id, model_path, options_json)`, the plugin can receive an options JSON like:

```
{
  "hf": { "allow_download": true, "revision": "main", "token": "${ENV:YAMS_ONNX_HF_TOKEN}" },
  "runtime": { "intra_threads": 4, "inter_threads": 1, "execution_providers": ["CPUExecutionProvider"] },
  "pooling": "auto",               // "auto"|"cls"|"mean"|"max"
  "normalize": true,               // L2 normalize output vectors
  "max_seq_len_override": 512,     // optional override when config is missing
  "embedding_dim_override": 384    // optional override when graph lacks shape metadata
}
```

If `options_json` is omitted, the plugin uses sensible defaults and infers properties from the model/config.

## Parameter Discovery (What gets parsed)

The plugin extracts and reports model properties via `get_loaded_models()` and `get_model_info`:

- Embedding dimension: Prefer model metadata (e.g., `pooler_output`, ONNX graph output dims). Fallback to Sentence‑Transformers config or the ONNX output tensor shape.
- Max sequence length: Prefer `max_seq_length` or tokenizer config. Fallback to 512 if missing and warn.
- Pooling strategy: Parse Sentence‑Transformers config (e.g., `pooling_mode_cls_token`, `pooling_mode_mean_tokens`). If absent, default to mean pooling.
- Normalization: Honor `normalize_embeddings` or plugin option `normalize` (default true for ST family).
- Model version/capabilities: Read `config.json` fields like `architectures`, tokenizer type, vocab size.

These values inform YAMS sizing (vector dim), chunking/tokenization limits, and search normalization.

## Usage Examples

CLI (daemon running):

```
# Trust + load (one‑time)
yams plugin trust add /usr/local/lib/yams/plugins
yams plugin load /usr/local/lib/yams/plugins/libyams_onnx_plugin.so

# Load model from HF repo ID (with default revision)
yams embeddings load --model sentence-transformers/all-MiniLM-L6-v2

# Load from explicit HF URL
yams embeddings load --model https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2

# Load local path
yams embeddings load --model /opt/models/all-MiniLM-L6-v2

# Generate embeddings
yams embeddings gen --input "hello world"
```

Configuration‑driven (recommended):

```
[embeddings]
preferred_model = "hf://sentence-transformers/all-MiniLM-L6-v2"
keep_model_hot = true
preload_on_startup = true
```

## Caching & Offline

- The plugin respects the HF cache (and `YAMS_ONNX_HF_CACHE`) to avoid re‑downloading.
- Set `YAMS_ONNX_OFFLINE=1` to disallow network fetches; if artifacts are missing, `load_model` fails fast with a clear error.
- You can mirror models to a local directory and reference that path directly to ensure fully offline operation.

## Interop with YAMS

When adopted by the daemon:
- The plugin’s embedding dimension is used to size/persist vectors in `vectors.db`.
- The max sequence length influences request chunking and truncation safeguards.
- Pooling/normalization decisions are applied consistently for batch and single embedding calls.
- `get_loaded_models` informs `model status` API and readiness probes.

## Troubleshooting

- “Model not found”: Verify the repo ID/URL is correct and accessible; set `YAMS_ONNX_HF_TOKEN` if needed.
- “No ONNX file detected”: Some repos ship only PyTorch weights. Use an ONNX‑converted variant, or export.
- “Dimension mismatch”: If a pre‑existing vector DB has a different dimension, either re‑embed or supply `embedding_dim_override` to maintain compatibility (only if you know the true dim).
- “Timeout/slow load”: Disable downloads (`YAMS_ONNX_OFFLINE=1`) and pre‑place model files, or enable `preload_on_startup`.

## Security & Trust

Only plugins from trusted directories are allowed. The trust list lives at `~/.config/yams/plugins_trust.txt`. For additional defense, production deployments should lock down plugin directories and pre‑stage models in a read‑only location.

## GPU Acceleration

The ONNX plugin supports GPU acceleration via platform-specific execution providers. To enable GPU support, build with the appropriate Conan option:

### macOS (CoreML + Apple Neural Engine)
```bash
# CoreML is included in standard macOS ONNX Runtime builds
conan install . -o onnxruntime/*:with_gpu=coreml
meson setup builddir
meson compile -C builddir
```
CoreML automatically uses Apple Silicon's Neural Engine and GPU for compatible operations.

### Linux (CUDA / NVIDIA GPUs)
```bash
# Download CUDA-enabled ONNX Runtime binary
conan install . -o onnxruntime/*:with_gpu=cuda
meson setup builddir
meson compile -C builddir
```
Requires CUDA toolkit and cuDNN installed on the system.

### Windows (DirectML / Any DX12 GPU)
```powershell
# Download DirectML-enabled ONNX Runtime binary
conan install . -o onnxruntime/*:with_gpu=directml
meson setup builddir
meson compile -C builddir
```
DirectML works with any DirectX 12 capable GPU (NVIDIA, AMD, Intel).

### Enabling GPU at Runtime

Set `enable_gpu: true` in the plugin options JSON or embedding config:

```json
{
  "runtime": {
    "execution_providers": ["CUDAExecutionProvider", "CPUExecutionProvider"]
  }
}
```

Or via YAMS config:
```toml
[embeddings]
enable_gpu = true
```

The plugin automatically falls back to CPU if GPU initialization fails.

## Build

From the repo root:

```
cmake -S . -B build/yams-release -DCMAKE_BUILD_TYPE=Release
cmake --build build/yams-release -j
```

Or build the plugin standalone:

```
cmake -S plugins/onnx -B build/onnx-plugin -DCMAKE_BUILD_TYPE=Release
cmake --build build/onnx-plugin -j
```

Install (from root build):

```
sudo cmake --install build/yams-release
# installs to ${CMAKE_INSTALL_LIBDIR}/yams/plugins, often /usr/local/lib/yams/plugins
```

## Trust + Load (CLI)

The daemon only loads plugins from trusted directories. Trust a directory then load the plugin:

```
yams plugin trust add /usr/local/lib/yams/plugins
yams plugin scan
yams plugin load /usr/local/lib/yams/plugins/libyams_onnx_plugin.so
```

After load, the daemon prefers this host‑backed provider. Check status or run embedding features as usual.

## Test Harness (ctest)

Two ways to run the harness:

1) In‑repo build:
```
ctest -R onnx_plugin_harness_test --output-on-failure
```

2) External/installed plugin:
```
export TEST_ONNX_PLUGIN_FILE=/path/to/libyams_onnx_plugin.so
ctest -R onnx_plugin_harness_test --output-on-failure
```

---

By documenting HF URL support, parameter discovery, and configuration knobs here, we set the ground truth for refactoring the implementation to match this behavior. If any gaps exist in the current code, treat this README as the spec for the next hardening pass.
