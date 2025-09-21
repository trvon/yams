docker run --rm -it ghcr.io/trvon/yams:latest --version
docker run --rm -it -v $HOME/yams-data:/var/lib/yams ghcr.io/trvon/yams:latest yams init --non-interactive
# Build System (Developer Reference)

Compact overview of how to build, test, and extend YAMS. Start with the **Quick Loop**, then dive deeper.

## 1. Stack
| Layer | Tool |
|-------|------|
| Build | CMake ≥ 3.25 |
| Dependencies | Conan 2.x |
| Generator | Ninja (preferred) |
| Compilers | Clang ≥14 / GCC ≥11 |
| Optional | ccache, lld, clang-tidy |

## 2. Directory Layout & Presets
Out-of-source builds under `build/` per configuration. Generated presets:
- Debug: `build/yams-debug` (inner: `build/yams-debug/build`)
- Release: `build/yams-release` (inner: `build/yams-release/build`)

Use the preset (never build inside source). Artifacts + CTest run from inner dir but presets abstract that.

## 3. One-Time: Profile
```bash
conan profile detect --force
```

## 4. Quick Loop (Conan + Meson)
```bash
# Debug dependencies
conan install . -of build/debug -s build_type=Debug -b missing

# Initial configure
meson setup build/debug \
  --prefix /usr/local \
  --native-file build/debug/build-debug/conan/conan_meson_native.ini

# Reconfigure after option changes (idempotent)
meson setup build/debug --reconfigure \
  --prefix /usr/local \
  --native-file build/debug/build-debug/conan/conan_meson_native.ini

meson compile -C build/debug
meson test -C build/debug
```

Release variant: swap preset/output folder + `-s build_type=Release`.

## 5. ONNX / GenAI Paths
Options (Conan scope):
- `enable_onnx` (default True) — toggles embedding / GenAI integration.
- `use_conan_onnx` (default False) — when True pull packaged ORT (currently 1.18.x); when False allow internal newer ORT path.

CMake variables:
- `YAMS_ENABLE_ONNX` — mirrors `enable_onnx`.
- `YAMS_BUILD_INTERNAL_ONNXRUNTIME` — triggers provider to fetch/build ORT (version via `YAMS_INTERNAL_ORT_VERSION`).

Internal build example (new GenAI headers):
```bash
conan install . -of build/yams-debug -s build_type=Debug -b missing \
  -o yams/*:enable_onnx=True -o yams/*:use_conan_onnx=False
cmake --preset yams-debug -DYAMS_BUILD_INTERNAL_ONNXRUNTIME=ON
cmake --build --preset yams-debug -j
```
If a stale `onnxruntime::onnxruntime` target persists, remove the build directory to force provider logic.

Lazy discovery behavior (root CMake): it now performs a quiet `find_package(onnxruntime)`; missing packages no longer hard-fail. Actual target creation or internal build happens inside `src/vector` and `plugins/onnx` via `OnnxRuntimeProvider.cmake`. Warnings guide you to enable `use_conan_onnx` or `YAMS_BUILD_INTERNAL_ONNXRUNTIME` as needed.

Plain CMake (no Conan) supplying external ORT:
```bash
cmake -S . -B build -G Ninja -DYAMS_ENABLE_ONNX=ON -DCMAKE_PREFIX_PATH=/path/to/onnxruntime
cmake --build build -j
```

## 6. Configure & Build (Presets)
```bash
cmake --preset yams-debug   # or yams-release
cmake --build --preset yams-debug -j
```
Install:
```bash
sudo cmake --install build/yams-release
```

## 7. Tests
```bash
ctest --preset yams-debug --output-on-failure
ctest --preset yams-release --output-on-failure
```
Targeted plugin/unit tests:
```bash
cmake --build --preset yams-debug --target s3_signer_tests object_storage_adapter_tests
ctest --preset yams-debug -R S3SignerUnitTests --output-on-failure
```
S3 smoke (opt-in): add `-DYAMS_TEST_S3_PLUGIN_INTEGRATION=ON` then build & run `s3_plugin_smoke_test`.

Integration tests (S3 / ONNX) off by default; enable only while working on those components.

## 8. Key Build Options
| CMake Var | Purpose |
|-----------|---------|
| YAMS_BUILD_CLI | Build command-line interface |
| YAMS_BUILD_MCP_SERVER | Build MCP server |
| YAMS_BUILD_TESTS / YAMS_BUILD_BENCHMARKS | Self-explanatory |
| YAMS_ENABLE_ONNX | Enable ONNX vector/GenAI paths |
| YAMS_BUILD_INTERNAL_ONNXRUNTIME | Fetch/build ORT when Conan package insufficient |
| YAMS_INTERNAL_ORT_VERSION | Override internal ORT version (cache var) |
| YAMS_ENABLE_PDF | PDF extraction (FetchContent PDFium) |
| YAMS_HAVE_WASMTIME | Enable WASM host codepaths |
| YAMS_ENABLE_SANITIZERS | Debug ASan/UBSan toggle |

Disable clang-tidy (single configure):
```bash
cmake --preset yams-debug -D CMAKE_CXX_CLANG_TIDY= -D CMAKE_C_CLANG_TIDY=
```

## 9. Meson Notes

Meson is now the primary build system. Conan generates `build/<cfg>/build-<cfg>/conan/conan_meson_native.ini` which is passed via `--native-file`. Use `--prefix /usr/local` so `meson install` places artifacts in a standard path and leverage `--reconfigure` for subsequent configuration changes (it is safe to run repeatedly).
````markdown
docker run --rm -it ghcr.io/trvon/yams:latest --version
docker run --rm -it -v $HOME/yams-data:/var/lib/yams ghcr.io/trvon/yams:latest yams init --non-interactive
# Build System (Developer Reference)

Compact overview of how to build, test, and extend YAMS. Start with the **Quick Loop**, then dive deeper.

## 1. Stack
| Layer | Tool |
|-------|------|
| Build | CMake ≥ 3.25 |
| Dependencies | Conan 2.x |
| Generator | Ninja (preferred) |
| Compilers | Clang ≥14 / GCC ≥11 |
| Optional | ccache, lld, clang-tidy |

## 2. Directory Layout & Presets
Out-of-source builds under `build/` per configuration. Generated presets:
- Debug: `build/yams-debug` (inner: `build/yams-debug/build`)
- Release: `build/yams-release` (inner: `build/yams-release/build`)

Use the preset (never build inside source). Artifacts + CTest run from inner dir but presets abstract that.

## 3. One-Time: Profile
```bash
conan profile detect --force
```

## 4. Quick Loop (Conan)
```bash
# Debug
conan install . -of build/yams-debug -s build_type=Debug -b missing \
  -o yams/*:enable_onnx=True
cmake --preset yams-debug
cmake --build --preset yams-debug -j
ctest --preset yams-debug --output-on-failure
```

Release variant: swap preset/output folder + `-s build_type=Release`.

## 5. ONNX / GenAI Paths
Conan option:
- `enable_onnx` (default True) — enables ONNX vector + GenAI adapter code paths.

CMake variable:
- `YAMS_ENABLE_ONNX` — mirrors `enable_onnx`.

Internal ONNX Runtime source builds have been removed. YAMS now relies on an
existing `onnxruntime::onnxruntime` target (Conan or system). If ONNX is enabled
but the runtime target is absent you get a warning and stub GenAI code is used.

### Automatic GenAI Header Fetch
If ONNX is enabled and the detected ONNX Runtime does not ship GenAI C++ headers,
the build auto-downloads the `onnxruntime-genai` archive (v0.9.1) and exposes its
headers via `onnxruntime::genai_headers` (interface). No new options or env vars.

Flow:
1. Root CMake does a quiet `find_package(onnxruntime)`.
2. Provider scans for `onnxruntime/genai/embedding.h`.
3. If missing, it fetches and extracts the GenAI extension headers (one-time cache).
4. Detection module try-compiles against a candidate header. On success it defines
   `YAMS_GENAI_RUNTIME_PRESENT` consumed by vector library & ONNX plugin.

Provider log snippets:
| Log | Meaning |
|-----|---------|
| `[OnnxRuntimeProvider] onnxruntime distribution already includes GenAI headers` | Packaged/runtime already ships extension headers. |
| `[OnnxRuntimeProvider] onnxruntime-genai headers provided (v0.9.1)` | Auto-fetch succeeded; headers now available. |
| `[OnnxRuntimeProvider] onnxruntime-genai headers still unavailable after fetch attempt` | Fetch/extract failed; continue with stubs. |
| `[OnnxRuntimeProvider] No ONNX Runtime target found (no internal build fallback enabled)` | Runtime absent; ONNX/GenAI features stubbed. |

Detection messages:
| Log | Meaning |
|-----|---------|
| `[GenAI] ONNX GenAI headers usable (probe succeeded)` | Headers compile; adaptive path active. |
| `[GenAI] No candidate ONNX GenAI headers found in exported include dirs` | No headers; stub implementation. |

Plain CMake (no Conan) supplying external ORT:
```bash
cmake -S . -B build -G Ninja -DYAMS_ENABLE_ONNX=ON -DCMAKE_PREFIX_PATH=/path/to/onnxruntime
cmake --build build -j
```

## 6. Configure & Build (Presets)
```bash
cmake --preset yams-debug   # or yams-release
cmake --build --preset yams-debug -j
```
Install:
```bash
sudo cmake --install build/yams-release
```

## 7. Tests
```bash
ctest --preset yams-debug --output-on-failure
ctest --preset yams-release --output-on-failure
```
Targeted plugin/unit tests:
```bash
cmake --build --preset yams-debug --target s3_signer_tests object_storage_adapter_tests
ctest --preset yams-debug -R S3SignerUnitTests --output-on-failure
```
S3 smoke (opt-in): add `-DYAMS_TEST_S3_PLUGIN_INTEGRATION=ON` then build & run `s3_plugin_smoke_test`.

Integration tests (S3 / ONNX) off by default; enable only while working on those components.

## 8. Key Build Options
| CMake Var | Purpose |
|-----------|---------|
| YAMS_BUILD_CLI | Build command-line interface |
| YAMS_BUILD_MCP_SERVER | Build MCP server |
| YAMS_BUILD_TESTS / YAMS_BUILD_BENCHMARKS | Self-explanatory |
| YAMS_ENABLE_ONNX | Enable ONNX vector/GenAI paths |
| YAMS_ENABLE_PDF | PDF extraction (FetchContent PDFium) |
| YAMS_HAVE_WASMTIME | Enable WASM host codepaths |
| YAMS_ENABLE_SANITIZERS | Debug ASan/UBSan toggle |

Disable clang-tidy (single configure):
```bash
cmake --preset yams-debug -D CMAKE_CXX_CLANG_TIDY= -D CMAKE_C_CLANG_TIDY=
```

## 9. Dependency Management Notes
Edit `conanfile.py` then re-run `conan install` for each configuration. Prefer pinned versions. Use `--build=missing` to compile absent binaries. Boost is overridden to a consistent version. ORT availability determined by `enable_onnx` and presence of a packaged/runtime target; GenAI headers auto-fetched if missing.

## 10. Common Scenarios
| Scenario | Commands |
|---------|----------|
| Fresh debug loop | Conan install (Debug) → preset build → ctest |
| (Removed) Switch ORT to internal | No longer supported; provide newer ORT externally |
| System-only (no Conan) | Provide all libs, configure manually with flags |
| Release packaging | Conan install (Release) → build → `cpack`/package target |

## 11. CI Flow
```bash
conan profile detect --force
conan install . -of build/yams-release -s build_type=Release --build=missing
cmake --preset yams-release
cmake --build --preset yams-release -j$(nproc)
ctest --preset yams-release --output-on-failure -j$(nproc)
```
Cache `~/.conan` + optionally `build/`.

## 12. Troubleshooting Quick Table
| Symptom | Fix |
|---------|-----|
| Preset missing | Re-run Conan install (regenerates presets) |
| Runtime missing | Ensure `enable_onnx=True` in Conan options or supply system ORT path |
| ONNX disabled | Check option + provider messages |
| Link errors after dep change | Delete inner `CMakeCache.txt` or rebuild from empty folder |
| Tests undiscovered | Use `ctest --preset ...`; ensure `YAMS_BUILD_TESTS=ON` |
| Slow incremental builds | Ensure Unity build active (Debug preset) or enable ccache |

## 13. Conventions
Out-of-source only. Keep Release for perf/benchmarks; iterate in Debug. Prefer presets over ad hoc flags. Avoid committing generated build artifacts.

## 14. Toolchain Modules
`ToolchainDetect.cmake` picks lld/ThinLTO when available. `Sanitizers.cmake` enables ASan/UBSan (and optionally TSAN) in Debug. Provider module: `OnnxRuntimeProvider.cmake` selects existing runtime and auto-fetches GenAI headers if absent.

---
Revision notes:
- Removed internal ORT build path; added automatic GenAI header fetch (no new options).
- Consolidated ONNX & GenAI guidance into section 5.
- Introduced quick scenario table + troubleshooting table.

### Diagnostics: Provider Log Lines
During configure you may see:

| Log Snippet | Meaning | Action |
|-------------|---------|--------|
| `[OnnxRuntimeProvider] Reusing existing onnxruntime::onnxruntime target` | Target from Conan/system reused. | None. |
| `[OnnxRuntimeProvider] onnxruntime distribution already includes GenAI headers` | Runtime ships GenAI headers. | None. |
| `[OnnxRuntimeProvider] onnxruntime-genai headers provided (v0.9.1)` | Extension headers fetched. | None. |
| `[OnnxRuntimeProvider] onnxruntime-genai headers still unavailable after fetch attempt` | Fetch failed; stub mode. | Check network or provide newer ORT. |
| `[OnnxRuntimeProvider] No ONNX Runtime target found (no internal build fallback enabled)` | Runtime missing; stubs used. | Provide ORT or disable ONNX. |

GenAI detection messages:
| Message | Interpretation |
|---------|----------------|
| `[GenAI] ONNX GenAI headers usable (probe succeeded)` | Adaptive GenAI path active. |
| `[GenAI] No candidate ONNX GenAI headers found in exported include dirs` | GenAI headers absent; stubs compiled. |

````
