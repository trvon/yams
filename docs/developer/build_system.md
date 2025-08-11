# YAMS Build System (CMake)
A concise guide to configuring, building, installing, and packaging YAMS.

## TL;DR
```bash
# Configure (Development profile + docs/manpages if pandoc is installed)
cmake -S . -B build -DYAMS_BUILD_PROFILE=dev -DYAMS_BUILD_DOCS=ON

# Build
cmake --build build -j

# (Optional) Install to a local prefix and put on PATH
cmake --install build --prefix "$PWD/.local"
export PATH="$PWD/.local/bin:$PATH"

# Verify
yams --version
yams --help
yams --help-all     # full verbose help if docs were built
```

## Build Profiles
Choose one:
- release
  - Lean runtime build; CLI and MCP server on; tests off
- dev (recommended when developing)
  - CLI, MCP, tests, (optionally) benchmarks; sanitizer support in Debug
- custom
  - Full manual control via options below

Example:
```bash
cmake -S . -B build -DYAMS_BUILD_PROFILE=release
```

## Common Options
Set with `-D<NAME>=<VALUE>` at configure time.

- YAMS_BUILD_CLI=ON|OFF
  - Build CLI tools (default depends on profile)
- YAMS_BUILD_MCP_SERVER=ON|OFF
  - Build MCP server (stdio/websocket) (default depends on profile)
- YAMS_BUILD_MAINTENANCE_TOOLS=ON|OFF
  - Build maintenance tools like gc/stats (default OFF)
- YAMS_BUILD_TESTS=ON|OFF
  - Build unit/integration tests (default depends on profile)
- YAMS_BUILD_BENCHMARKS=ON|OFF
  - Build benchmark executables under `src/**/benchmarks` (default OFF)
  - When ON, Google Benchmark is fetched and available as `benchmark::benchmark`
- YAMS_ENABLE_SANITIZERS=ON|OFF
  - Address/UB sanitizers in Debug (default ON)
- YAMS_ENABLE_COVERAGE=ON|OFF
  - Coverage instrumentation and helpers (default OFF)
- YAMS_BUILD_DOCS=ON|OFF
  - Generate manpages and embed verbose help into the CLI (default ON)
  - Requires pandoc; otherwise build proceeds with a clear warning and no embedding
- YAMS_USE_VCPKG=ON|OFF
  - Opt into vcpkg if desired (default OFF)
- YAMS_ENABLE_CONSUMER_TEST=ON|OFF
  - Enable downstream “consumer” CTest that verifies `find_package(Yams)` against a staged install (default ON when BUILD_TESTING)
- YAMS_VERSION=MAJOR.MINOR.PATCH
  - Override version (semver); otherwise derived from git tags (see “Versioning”)

Generator‑agnostic invocation:
```bash
cmake -S . -B build -DYAMS_BUILD_PROFILE=dev -DYAMS_BUILD_DOCS=ON
cmake --build build -j
ctest --test-dir build --output-on-failure
```

## Dependencies (Quick)
- C++20 toolchain (Clang 14+/GCC 11+/MSVC 2022)
- CMake 3.20+
- OpenSSL 3.x (headers/libs)
- SQLite3 (amalgamation is bundled automatically if needed)
- Optional: Protocol Buffers (if present)
- Optional: pandoc (for docs/manpages + embedded verbose help)

Platform tips:
- macOS (Homebrew):
  - `brew install cmake openssl@3 sqlite3 protobuf pandoc`
  - `export OPENSSL_ROOT_DIR="$(brew --prefix openssl@3)"`
- Ubuntu/Debian:
  - `sudo apt-get update && sudo apt-get install -y build-essential cmake git libssl-dev libsqlite3-dev protobuf-compiler pandoc`

## Versioning (Tag‑Driven)
- On configure, version is derived from `git describe --tags --match "v[0-9]*"`.
  - Example: tag `v1.2.3` → `PROJECT_VERSION=1.2.3`
- Override with `-DYAMS_VERSION=MAJOR.MINOR.PATCH` if building from a non‑tagged source or for custom builds.
- A generated header `generated/yams/version.hpp` provides:
  - `#define YAMS_VERSION_STRING "<MAJOR.MINOR.PATCH>"`
  - Used by the CLI `--version` flag.

## Docs & Manpages (Optional but Recommended)
- Enabled with `-DYAMS_BUILD_DOCS=ON` when pandoc is available.
- Effects:
  - Generates `yams(1)` manpage from `docs/user_guide/cli.md` and installs into `man1`.
  - Embeds a plain‑text copy of the full CLI reference and per‑command sections into the binary:
    - `yams --help-all` → full CLI reference
    - `yams --help --verbose` → full CLI reference
    - `yams <command> --help --verbose` → command‑specific section
- If pandoc is missing:
  - Build continues; verbose help falls back with a clear note; manpages are not produced.

## Targets & Layout (Orientation)
- Public headers: `include/yams/...`
- Core libraries: `src/{core,storage,compression,chunking,integrity,manifest,wal}`
- Metadata and DB: `src/metadata`
- Search/indexing/extraction: `src/{search,indexing,extraction}`
- Vector: `src/vector`
- Benchmarks:
  - Reusable framework: `src/benchmarks` → target alias `yams::benchmarks`
  - Module‑local executables: `src/<module>/benchmarks/*_bench.cpp`
  - Only configured when `YAMS_BUILD_BENCHMARKS=ON`
- CLI library: `src/cli` (linked to produce the `yams` tool)
- MCP server & transports: `src/mcp`
- Aggregating interface library: `yams_storage` (alias targets `YAMS::Storage`, `yams::storage`)

## Build, Install, Run
```bash
# Configure and build
cmake -S . -B build -DYAMS_BUILD_PROFILE=dev
cmake --build build -j

# Install to a prefix (staging)
cmake --install build --prefix "$PWD/.local"
export PATH="$PWD/.local/bin:$PATH"

# Run
yams --help
yams init --non-interactive
```

## Consumer Test (Downstream find_package)
When testing is enabled, CTest stages the current build to a temporary prefix and runs a minimal consumer project that uses:
```cmake
find_package(Yams)
target_link_libraries(consumer PRIVATE yams::storage)
```
This verifies installability and CMake package correctness.

Run:
```bash
ctest --test-dir build --output-on-failure
```

## Packaging (CPack)
CPack is configured to build redistributable packages:
```bash
# After building, from the build directory:
cpack
# Or specify a generator:
cpack -G ZIP
```
Components (typical):
- runtime: CLI/MCP and installed manpage(s)
- development: headers, CMake package files
- maintenance: optional tools (if enabled)

## RPATH & Install Notes
- Build and install RPATH are configured for macOS/Linux best practices.
- Installed binaries should resolve linked libs without manual `LD_LIBRARY_PATH` tweaks in typical setups.

## Troubleshooting (Fast)
- OpenSSL not found (macOS):
  - `export OPENSSL_ROOT_DIR="$(brew --prefix openssl@3)"`
- Pandoc missing warning:
  - Install pandoc, re‑configure, and rebuild to embed verbose help and produce manpages.
- Reconfigure clean:
  - `rm -rf build && cmake -S . -B build ...`
- Coverage tools not found:
  - Install `gcovr` (preferred) or `lcov` + `genhtml` for coverage targets.
- Windows:
  - Use the Visual Studio generator or Ninja; prefer `cmake --build` over `make`.

## See Also
- Developer Quick Start: `docs/developer/setup.md`
- CLI Reference (authoritative for help/manpages): `docs/user_guide/cli.md`
- Installation (user‑focused): `docs/user_guide/installation.md`
- API Docs: `docs/api/README.md`
- Architecture Overview: `docs/developer/architecture/README.md`

---
Keep this page short; rely on the CLI reference and per‑topic docs for exhaustive details and examples.