# Developer Build System

Concise overview of YAMS build and dependency tooling for contributors.

## Stack

- Build system: CMake (3.25+)
- Dependency manager: Conan 2.x
- Generators: Ninja (recommended), Unix Makefiles supported
- Compilers: Clang ≥14 or GCC ≥11 (macOS/Linux)
- Optional: ccache for faster rebuilds

## Build directories and presets

Conan generates CMakePresets and an out-of-source build tree per configuration:

- Release:
  - Conan output: build/yams-release
  - Configure preset: yams-release
  - Inner CMake build dir: build/yams-release/build
- Debug:
  - Conan output: build/yams-debug
  - Configure preset: yams-debug
  - Inner build dir: build/yams-debug/build

Notes:
- Paths may differ slightly depending on the generator; use the preset’s inner build path for ctest and artifacts.
- Keep build/* out of version control.

## One-time setup

Detect a Conan profile for your host toolchain:

```bash
conan profile detect --force
```

## Install dependencies

Conan resolves and fetches third-party libraries and generates CMake presets.

Release:

```bash
conan install . --output-folder=build/yams-release -s build_type=Release --build=missing
```

Debug:

```bash
conan install . --output-folder=build/yams-debug -s build_type=Debug --build=missing
```

Tips:
- Append --build=missing to build packages not available precompiled.
- Use a custom profile via -pr:h=<host> -pr:b=<build> as needed.

ONNX embeddings (enablement and mapping)

- Conan option: `yams/*:enable_onnx` (default True in this repo’s profiles) pulls `onnxruntime` and sets the CMake cache var `YAMS_ENABLE_ONNX` via the generated toolchain.
- CMake option: `YAMS_ENABLE_ONNX` toggles ONNX support in `src/vector`. When ON and `onnxruntime` is found, the build defines `YAMS_USE_ONNX_RUNTIME`, enabling the ONNX-backed embedding path.
- Typical flows:
  - Conan: `conan install . -of build/yams-debug -s build_type=Debug -b missing -o yams/*:enable_onnx=True`
  - Plain CMake: `cmake -S . -B build -G Ninja -DYAMS_ENABLE_ONNX=ON -DCMAKE_PREFIX_PATH=/path/to/onnxruntime`
- Validation: configure logs should contain `ONNX Runtime found - enabling local embedding generation`. If you see `ONNX Runtime disabled`, check options and dependency visibility.

## Configure and build

CMake configure (from repo root):

```bash
cmake --preset yams-release
# or
cmake --preset yams-debug
```

Build:

```bash
cmake --build --preset yams-release -j
# or
cmake --build --preset yams-debug -j
```

Install (optional):

```bash
sudo cmake --install build/yams-release
# set a custom prefix with -DCMAKE_INSTALL_PREFIX=/opt/yams when configuring
```

## Tests

Run tests with ctest from the inner build directory:

```bash
# Debug
ctest --preset yams-debug --output-on-failure
# To parallelize: add -jN (e.g., -j$(nproc) on Linux)

# Release
ctest --preset yams-release --output-on-failure
# To parallelize: add -jN (e.g., -j$(nproc) on Linux)
```

For single-config Ninja (default here), CMake puts intermediate build files under build/yams-*/build; use the test presets, not raw paths.

### Managed plugin tests

- Build the plugin unit tests explicitly, then run with the test preset:
  - Build: `cmake --build --preset yams-debug --target s3_signer_tests object_storage_adapter_tests`
  - Run: `ctest --preset yams-debug -R S3SignerUnitTests --output-on-failure`
  - Run: `ctest --preset yams-debug -R ObjectStorageAdapterUnitTests --output-on-failure`

S3 plugin smoke test (optional, networked)
- Enable at configure time: `-DYAMS_TEST_S3_PLUGIN_INTEGRATION=ON`
- Build: `cmake --build --preset yams-debug --target s3_plugin_smoke_test`
- Environment required:
  - `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` (and optional `AWS_SESSION_TOKEN`)
  - `S3_TEST_BUCKET`
  - Optional for R2: `S3_TEST_ENDPOINT=<ACCOUNT>.r2.cloudflarestorage.com`, `S3_TEST_USE_PATH_STYLE=1`
- Run: `ctest --preset yams-debug -R s3_plugin_smoke_test --output-on-failure`

Discovery and paths
- The plugin loader checks `YAMS_PLUGIN_DIR` for discovery in addition to default paths. The smoke test sets this automatically to the built plugin path; you can also set it manually for custom runs.
- System prerequisites for S3 plugin tests: libcurl and OpenSSL development headers.

> **Note on Optional Tests:** Integration tests for plugins like S3 and ONNX are disabled by default. This is a common practice to prevent build and test failures for developers who may not have the required external dependencies (like S3 credentials or the ONNX Runtime library) configured in their local environment. It separates fast, local unit tests from slower integration tests. You can enable them via CMake options when you are specifically working on these components.

## Build options and toolchain notes

- Generator selection: set CMAKE_GENERATOR (e.g., Ninja) in your environment or profile.
- Install prefix: set CMAKE_INSTALL_PREFIX at configure time.
- Werror/code style: adhere to the project’s CMake defaults; do not relax warnings locally unless troubleshooting.
- Sanitizers: enable via your toolchain or add flags in a local cache:
  - Example (single-config): -DCMAKE_C_FLAGS_DEBUG="-fsanitize=address,undefined" -DCMAKE_CXX_FLAGS_DEBUG="-fsanitize=address,undefined"
  - For multi-config generators, use CMAKE_<LANG>_FLAGS_<CONFIG>.
- ccache: export CC="ccache clang" and CXX="ccache clang++" (or set via toolchain/profile) to speed up rebuilds.

## Dependency management (Conan)

- Dependencies are declared in the project’s conanfile (e.g., conanfile.py/conanfile.txt).
- Change dependencies by editing the conanfile; then re-run conan install for each configuration output-folder.
- Pinning: prefer exact versions and revisions where feasible; consider lockfiles for reproducible CI.
- Editable packages: use conan editable add for local development of dependent packages.

Example round-trip after editing dependencies:

```bash
# Release
conan install . --output-folder=build/yams-release -s build_type=Release --build=missing
cmake --preset yams-release
cmake --build --preset yams-release -j
```

## CI-friendly flow

```bash
conan profile detect --force
conan install . --output-folder=build/yams-release -s build_type=Release --build=missing
cmake --preset yams-release
cmake --build --preset yams-release -j$(nproc)
ctest --preset yams-release --output-on-failure -j$(nproc)
```

Cache:
- Cache ~/.conan and optionally the build/ directory (if your CI preserves workspaces) to reduce cold start time.

## Docker (optional)

Run the published image for CLI checks:

```bash
docker run --rm -it ghcr.io/trvon/yams:latest --version
```

Persist data with a bind mount:

```bash
mkdir -p $HOME/yams-data
docker run --rm -it -v $HOME/yams-data:/var/lib/yams ghcr.io/trvon/yams:latest yams init --non-interactive
```

For containerized builds, use a dev image with toolchains or mount your build cache and run the Conan/CMake flow inside.

## Troubleshooting

- Missing compiler/CMake/Ninja:
  - Ensure they’re on PATH; re-run conan profile detect.
- Preset not found:
  - Re-run conan install to regenerate presets.
- Link errors after dependency changes:
  - Clean or re-generate: delete the build/yams-*/CMakeCache.txt (inner build folder) or start with a fresh output-folder.
- Tests not discovered:
  - Run ctest from the correct inner build directory; confirm enable_testing/add_test in the codebase.
- ABI or stdlib mismatches:
  - Align compiler versions and libc++/libstdc++ across your profile and environment.

## Conventions

- Out-of-source builds only (build/*).
- Do not commit local cache or build artifacts.
- Keep Release builds for benchmarks and distribution; Debug for development.
- Prefer presets over ad-hoc build commands for repeatability.


## Toolchain & Sanitizers

- CMake modules under `cmake/` control portable defaults:
  - `ToolchainDetect.cmake`: prefers LLD and ThinLTO when supported; enables IPO/LTO portably.
  - `Sanitizers.cmake`: toggles `ENABLE_ASAN`, `ENABLE_UBSAN`, `ENABLE_TSAN` in Debug builds only.
- The default toolchain preference is LLVM (Clang + LLD) when present; otherwise the system toolchain is used.
- No changes to Conan profiles are required; CI passes settings/conf via CLI.
