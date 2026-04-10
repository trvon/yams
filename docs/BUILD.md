# Building YAMS

Cross-platform build guide for Linux, macOS, and Windows.

## Quick Start

### Linux/macOS

```bash
# Portable optimized build (`-O2 -g`, `NDEBUG`)
./setup.sh Release
meson compile -C build/release
```

### Windows

```pwsh
./setup.ps1 Release
meson compile -C build/release
```

## Prerequisites

### Compilers

| Platform | Compiler | Minimum | Recommended |
|----------|----------|---------|-------------|
| Linux/macOS | GCC | 11+ | 13+ |
| Linux/macOS | Clang | 14+ | 16+ |
| Windows | MSVC | 193 (VS 2022) | 193+ |

All require C++20 support.

### Build Tools

| Tool | Required | Install |
|------|----------|---------|
| Meson | Yes | `pip install meson` |
| Ninja | Yes | `pip install ninja` |
| Conan | Default path only | `pip install conan` |
| CMake | Yes | Package manager |
| pkg-config | Linux/macOS | Package manager |

### System Libraries

**Ubuntu/Debian:**
```bash
sudo apt-get install -y build-essential cmake meson ninja-build pkg-config \
    libssl-dev libcurl4-openssl-dev libsqlite3-dev \
    protobuf-compiler libprotobuf-dev zlib1g-dev
```

**Fedora:**
```bash
sudo dnf install -y gcc-c++ cmake meson ninja-build pkg-config \
    openssl-devel libcurl-devel sqlite-devel protobuf-devel zlib-devel
```

**macOS:**
```bash
brew install cmake meson ninja pkg-config openssl sqlite protobuf
```

**Windows:**
- Visual Studio 2022 with "Desktop development with C++" workload
- Python 3.10+ (for Conan, Meson)
- CMake 3.23+

## Build Options

### Setup Script Options

| Variable | Default | Description |
|----------|---------|-------------|
| `YAMS_OFFLINE` | false | Disable network dependency resolution; use only local Conan cache and local sources |
| `YAMS_USE_SYSTEM_DEPS` | false | Skip Conan and resolve dependencies from system/pkg-config/CMake paths |
| `YAMS_COMPILER` | auto | Force `gcc` or `clang` |
| `YAMS_CPPSTD` | 20 | C++ standard (20 or 23) |
| `YAMS_DISABLE_ONNX` | false | Disable ONNX embeddings |
| `YAMS_DISABLE_SYMBOL_EXTRACTION` | false | Disable Tree-sitter symbol extraction |
| `YAMS_INSTALL_PREFIX` | /usr/local | Install location |
| `YAMS_PKG_CONFIG_PATH` | unset | Extra host `pkg-config` search path passed to Meson |
| `YAMS_BUILD_PKG_CONFIG_PATH` | unset | Extra build-machine `pkg-config` path for cross-build tools |
| `YAMS_CMAKE_PREFIX_PATH` | unset | Extra host CMake prefix path passed to Meson |
| `YAMS_BUILD_CMAKE_PREFIX_PATH` | unset | Extra build-machine CMake prefix path for cross-build tools |
| `YAMS_MESON_NATIVE_FILE` | unset | Use this Meson native file instead of Conan-generated metadata |
| `YAMS_MESON_CROSS_FILE` | unset | Use this Meson cross file instead of Conan-generated metadata |
| `SOURCE_DATE_EPOCH` | unset | If set, YAMS uses this timestamp for reproducible build metadata |

### Meson Options

| Option | Default | Description |
|--------|---------|-------------|
| `build-cli` | true | Build CLI binary |
| `build-mcp-server` | true | Build MCP server |
| `build-tests` | false | Build test suite |
| `enable-onnx` | enabled | ONNX embedding models |
| `plugin-symbols` | true | Tree-sitter symbol extraction plugin |

Configure after setup:
```bash
meson configure builddir -Dbuild-tests=true
```

## Manual Build

### GCC (Linux/macOS)

```bash
conan install . -of build/release -s build_type=Release \
  -s compiler.cppstd=20 --build=missing
meson setup build/release \
  --native-file build/release/build-release/conan/conan_meson_native.ini \
  --buildtype=debugoptimized \
  -Db_ndebug=true
meson compile -C build/release
```

### Clang (Linux/macOS)

```bash
conan install . -of build/release -s build_type=Release \
  -s compiler=clang -s compiler.cppstd=20 --build=missing
CC=clang CXX=clang++ meson setup build/release \
  --native-file build/release/build-release/conan/conan_meson_native.ini \
  --buildtype=debugoptimized \
  -Db_ndebug=true
meson compile -C build/release
```

### MSVC (Windows)

```pwsh
# Export local Conan recipes (required once)
conan export conan/qpdf --name=qpdf --version=11.9.0
conan export conan/onnxruntime --name=onnxruntime --version=1.23.0

conan install . -of build\release `
  -pr:h conan/profiles/host-windows-msvc -pr:b default `
  -s build_type=Release --build=missing

meson setup build\release `
  --native-file build\release\build-release\conan\conan_meson_native.ini `
  --buildtype=debugoptimized `
  -Db_ndebug=true

meson compile -C build\release
```

## Offline and System Dependency Builds

### Conan Cache Only (no network)

```bash
./setup.sh Release --offline
meson compile -C build/release
```

This mode uses Conan only from the local cache and disables Meson wrap downloads.

### System Dependencies Only (no Conan)

```bash
YAMS_USE_SYSTEM_DEPS=true \
YAMS_OFFLINE=true \
YAMS_PKG_CONFIG_PATH=/opt/yams-deps/lib/pkgconfig \
YAMS_CMAKE_PREFIX_PATH=/opt/yams-deps \
./setup.sh Release --system-deps --offline

meson compile -C build/release
```

This path is intended for distro/package-manager integration, sandboxed builders, and manually staged dependency prefixes.

### Direct Meson with Manual Prefixes

```bash
meson setup build/release \
  --buildtype=debugoptimized \
  -Db_ndebug=true \
  --wrap-mode=nofallback \
  --pkg-config-path=/opt/yams-deps/lib/pkgconfig \
  --cmake-prefix-path=/opt/yams-deps

meson compile -C build/release
```

### CMake Bootstrap

```bash
cmake -S . -B build/cmake-bootstrap \
  -DYAMS_PROFILE=Release \
  -DYAMS_OFFLINE=ON \
  -DYAMS_USE_SYSTEM_DEPS=ON

cmake --build build/cmake-bootstrap
```

This entrypoint is a thin wrapper around the repo's canonical Meson setup flow. It exists so integrators can start from source with CMake while keeping Meson as the maintained build graph.

### System-Deps Cross Build

```bash
YAMS_USE_SYSTEM_DEPS=true \
YAMS_OFFLINE=true \
YAMS_MESON_CROSS_FILE=/path/to/cross.ini \
YAMS_BUILD_PKG_CONFIG_PATH=/opt/build-tools/lib/pkgconfig \
YAMS_PKG_CONFIG_PATH=/opt/target-sysroot/usr/lib/pkgconfig \
./setup.sh Release --system-deps --offline
```

## Testing

```bash
# Build with tests
./setup.sh Debug
meson configure builddir -Dbuild-tests=true
meson compile -C builddir

# Run tests
meson test -C builddir --print-errorlogs
```

## Troubleshooting

### qpdf: "recompile with -fPIC"

```bash
conan remove 'qpdf/*' -c
./setup.sh Release
```

### Clang: "cannot find -lstdc++"

Install libstdc++ or use libc++:
```bash
sudo apt-get install libstdc++-13-dev
# Or switch to GCC:
YAMS_COMPILER=gcc ./setup.sh Release
```

### Windows: Boost build failures

Boost 1.85 requires VS 2022 toolset (v143). If using VS 2025:
1. Install v143 toolset via Visual Studio Installer → Individual Components
2. Clean cache: `conan remove 'boost/*' -c`
3. Re-run: `./setup.ps1 Release`

### Windows: Missing recipes

```pwsh
conan export conan/qpdf --name=qpdf --version=11.9.0
conan export conan/onnxruntime --name=onnxruntime --version=1.23.0
```

### Missing dependencies

```bash
pkg-config --list-all | grep -E "(sqlite|openssl|protobuf)"
```

Install missing development packages from Prerequisites.

## Compiler Comparison

| Feature | GCC 13+ | Clang 16+ | MSVC 193+ |
|---------|---------|-----------|-----------|
| C++20 coroutines | Yes | Yes | Yes |
| std::format | Yes | Yes | Yes |
| C++20 modules | Partial | Full | Full |
| Link time | Moderate | Fast (LLD) | Moderate |
| Platform | Linux, macOS | Linux, macOS | Windows |

## Advanced Options

### Code Coverage

```bash
./setup.sh Debug --coverage
meson test -C builddir
gcovr --html -o builddir/coverage.html
```

### LTO (Link Time Optimization)

```bash
CXXFLAGS="-flto" LDFLAGS="-flto" ./setup.sh Release
```

### Native Optimization

```bash
CXXFLAGS="-march=native" ./setup.sh Release
```

Use this only for local benchmarking. Do not ship or package binaries built with `-march=native`.

## Verification

```bash
./build/release/tools/yams-cli/yams --version
echo "test" | ./build/release/tools/yams-cli/yams add - --tags test
pkg-config --modversion yams
pkg-config --libs yams
```

## References

- [Conan Documentation](https://docs.conan.io/2/)
- [Meson Documentation](https://mesonbuild.com/)
- [GCC C++ Status](https://gcc.gnu.org/projects/cxx-status.html)
- [Clang C++ Status](https://clang.llvm.org/cxx_status.html)
- [MSVC C++ Status](https://docs.microsoft.com/cpp/overview/visual-cpp-language-conformance)
