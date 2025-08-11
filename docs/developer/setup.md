# Developer Quick Start (Minimal)

A concise guide to get you building, running, and verifying YAMS quickly.

## Requirements

- C++20 compiler (Clang 14+/GCC 11+/MSVC 2022)
- CMake 3.20+
- Git
- OpenSSL 3.x headers/libs
- SQLite3 headers/libs
- Optional (recommended): Protocol Buffers 3.x (headers/libs)
- Optional (for docs/manpages + embedded verbose `--help`): Pandoc

Platform setup examples:
- macOS: `brew install cmake openssl@3 sqlite3 protobuf pandoc` and
  `export OPENSSL_ROOT_DIR="$(brew --prefix openssl@3)"`
- Ubuntu/Debian: `sudo apt-get update && sudo apt-get install -y build-essential cmake git libssl-dev libsqlite3-dev protobuf-compiler pandoc`
- Fedora: `sudo dnf install -y cmake gcc-c++ git openssl-devel sqlite-devel protobuf-compiler pandoc`

## TL;DR

```bash
# Clone
git clone https://github.com/trvon/yams.git
cd yams

# Configure (Development profile: tests, debug info, docs)
cmake -S . -B build \
  -DYAMS_BUILD_PROFILE=dev \
  -DYAMS_BUILD_DOCS=ON

# Build
cmake --build build -j

# Test (if enabled by profile)
ctest --test-dir build --output-on-failure

# (Optional) Install to a local prefix
cmake --install build --prefix "$PWD/.local"

# Initialize storage (after adding .local/bin to PATH)
export PATH="$PWD/.local/bin:$PATH"
yams init --non-interactive

# Verify
yams --version
yams --help
yams --help-all
yams search "test" --limit 1
```

## Build Profiles

- `-DYAMS_BUILD_PROFILE=release`
  - Lean build focused on runtime; CLI + MCP server on; tests off
- `-DYAMS_BUILD_PROFILE=dev` (recommended while developing)
  - CLI, MCP server, tests, benchmarks ON; Debug/RelWithDebInfo; sanitizers on in Debug
- `-DYAMS_BUILD_PROFILE=custom`
  - Mix your own flags (see common options below)

## Common CMake Options

- `-DYAMS_BUILD_CLI=ON|OFF`                Build CLI tools
- `-DYAMS_BUILD_MCP_SERVER=ON|OFF`         Build MCP server
- `-DYAMS_BUILD_TESTS=ON|OFF`              Unit/integration tests
- `-DYAMS_BUILD_BENCHMARKS=ON|OFF`         Benchmarks
- `-DYAMS_ENABLE_SANITIZERS=ON|OFF`        ASan/UBSan in Debug
- `-DYAMS_ENABLE_COVERAGE=ON|OFF`          Coverage instrumentation
- `-DYAMS_BUILD_DOCS=ON|OFF`               Generate manpages and embed verbose `--help`
- `-DYAMS_VERSION=MAJOR.MINOR.PATCH`       Override tag‑derived version

Tip: Prefer generator‑agnostic usage:
```bash
cmake -S . -B build -DYAMS_BUILD_PROFILE=dev
cmake --build build -j
```

## Docs & Manpages (Optional)

- If `YAMS_BUILD_DOCS=ON` and Pandoc is installed:
  - Generates `yams(1)` manpage at install
  - Embeds verbose help into the CLI for:
    - `yams --help-all`
    - `yams --help --verbose`
    - `yams <command> --help --verbose`
- If Pandoc is missing, build proceeds; verbose help falls back with a clear note.

## Run From Build or Install

- From install prefix:
  - Add `bin` to PATH: `export PATH="$PREFIX/bin:$PATH"`
  - Then `yams ...`
- From build tree (if your generator places executables there):
  - `./build/<subdir>/yams ...`
  - Or install to a staging prefix (recommended): `cmake --install build --prefix "$PWD/.local"` and use PATH as above.

## Storage & Config (XDG‑aligned)

Defaults:
- Data: `$XDG_DATA_HOME/yams` or `~/.local/share/yams`
- Config: `$XDG_CONFIG_HOME/yams` or `~/.config/yams`

Override:
- `--storage <path>` or `export YAMS_STORAGE="<path>"`

Initialize:
```bash
yams init --non-interactive
```

## Troubleshooting (Fast)

- OpenSSL not found (macOS):
  - `export OPENSSL_ROOT_DIR="$(brew --prefix openssl@3)"`
- Reconfigure clean:
  - `rm -rf build && cmake -S . -B build ...`
- Manpages/verbose help missing:
  - Ensure Pandoc is installed and `-DYAMS_BUILD_DOCS=ON`, reconfigure, rebuild
- Permissions:
  - Ensure your chosen `--storage` path is user‑writable (avoid sudo at runtime)

## Next Steps

- CLI Reference: `docs/user_guide/cli.md`
- Search guides: `docs/user_guide/search_guide.md`, `docs/user_guide/vector_search_guide.md`
- Admin/config: `docs/admin/configuration.md`
- Manpage (if built): `man yams`
