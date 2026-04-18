# Building YAMS

Cross-platform build for Linux, macOS, and Windows. Canonical build graph is Meson; `setup.sh` / `setup.ps1` handle toolchain detection, Conan deps, and Meson configure in one step.

## Quick start

```bash
# Linux / macOS
./setup.sh Release
meson compile -C build/release

# Windows
./setup.ps1 Release
meson compile -C build/release
```

If `setup.sh` / `setup.ps1` reports that Conan supplied Meson/Ninja for the build directory, use the generated local wrapper (`build/.../mesonw` on Unix, `mesonw.ps1` on Windows) for subsequent `meson compile|install|test`.

Run `./setup.sh --help` (or `setup.ps1 -?`) for the full flag set.

## Prerequisites

| Platform       | Compiler (min / recommended) | Notes                                   |
|----------------|------------------------------|-----------------------------------------|
| Linux / macOS  | GCC 11+ / 13+                | C++20                                   |
| Linux / macOS  | Clang 14+ / 16+              | C++20                                   |
| Windows        | MSVC 193+ (VS 2022)          | "Desktop development with C++" workload |

Build tools: **Meson**, **Ninja**, **CMake 3.23+**, **pkg-config** (Linux/macOS), **Conan** (default dep path), **Python 3.10+**.

### System libraries

```bash
# Ubuntu / Debian
sudo apt-get install -y build-essential cmake meson ninja-build pkg-config \
    libssl-dev libcurl4-openssl-dev libsqlite3-dev protobuf-compiler libprotobuf-dev zlib1g-dev

# Fedora
sudo dnf install -y gcc-c++ cmake meson ninja-build pkg-config \
    openssl-devel libcurl-devel sqlite-devel protobuf-devel zlib-devel

# macOS
brew install cmake meson ninja pkg-config openssl sqlite protobuf
```

## Build options

### `setup.sh` environment variables

| Variable                        | Default    | Purpose                                                        |
|---------------------------------|------------|----------------------------------------------------------------|
| `YAMS_OFFLINE`                  | false      | Use only local Conan cache; disable wrap downloads             |
| `YAMS_USE_SYSTEM_DEPS`          | false      | Skip Conan; use system pkg-config / CMake prefixes             |
| `YAMS_COMPILER`                 | auto       | Force `gcc` or `clang`                                         |
| `YAMS_CPPSTD`                   | 20         | C++ standard (20 or 23)                                        |
| `YAMS_DISABLE_ONNX`             | false      | Disable ONNX plugin                                            |
| `YAMS_DISABLE_SYMBOL_EXTRACTION`| false      | Disable Tree-sitter symbols                                    |
| `YAMS_ONNX_GPU`                 | auto       | `auto\|cuda\|coreml\|directml\|migraphx\|none`                 |
| `YAMS_INSTALL_PREFIX`           | /usr/local | Install location                                               |
| `YAMS_PKG_CONFIG_PATH`          | unset      | Extra host pkg-config path                                     |
| `YAMS_CMAKE_PREFIX_PATH`        | unset      | Extra host CMake prefix                                        |
| `YAMS_MESON_NATIVE_FILE`        | unset      | Override Meson native file                                     |
| `YAMS_MESON_CROSS_FILE`         | unset      | Override Meson cross file                                      |
| `SOURCE_DATE_EPOCH`             | unset      | Reproducible build timestamp                                   |

Canonical sources: [`setup.sh`](../setup.sh), [`setup.ps1`](../setup.ps1).

### Meson options

Commonly used flags — see [`meson_options.txt`](../meson_options.txt) for the full list:

| Option              | Default  | Purpose                           |
|---------------------|----------|-----------------------------------|
| `build-cli`         | true     | Build CLI binary                  |
| `build-mcp-server`  | true     | Build MCP server                  |
| `build-tests`       | false    | Build test suite                  |
| `enable-onnx`       | enabled  | ONNX embedding models             |
| `plugin-symbols`    | true     | Tree-sitter symbol plugin         |
| `plugin-s3`         | true     | S3 object storage plugin          |

```bash
meson configure build/release -Dbuild-tests=true
```

Dependency versions live in [`conanfile.py`](../conanfile.py).

## Offline and system-deps builds

```bash
# Local Conan cache only
./setup.sh Release --offline

# System dependencies, no Conan
YAMS_USE_SYSTEM_DEPS=true YAMS_OFFLINE=true \
  YAMS_PKG_CONFIG_PATH=/opt/yams-deps/lib/pkgconfig \
  YAMS_CMAKE_PREFIX_PATH=/opt/yams-deps \
  ./setup.sh Release --system-deps --offline

# CMake wrapper (delegates to Meson)
cmake -S . -B build/cmake-bootstrap \
  -DYAMS_PROFILE=Release -DYAMS_OFFLINE=ON -DYAMS_USE_SYSTEM_DEPS=ON
cmake --build build/cmake-bootstrap
```

Cross builds: set `YAMS_MESON_CROSS_FILE`, `YAMS_BUILD_PKG_CONFIG_PATH`, and `YAMS_PKG_CONFIG_PATH`.

## Manual Meson setup

Skip `setup.sh` and drive Meson directly when you need custom profiles:

```bash
conan install . -of build/release -s build_type=Release -s compiler.cppstd=20 --build=missing
meson setup build/release \
  --native-file build/release/build-release/conan/conan_meson_native.ini \
  --buildtype=debugoptimized -Db_ndebug=true
meson compile -C build/release
```

Windows requires local Conan recipes once:

```pwsh
conan export conan/qpdf --name=qpdf --version=11.9.0
conan export conan/onnxruntime --name=onnxruntime --version=1.23.0
conan install . -of build\release -pr:h conan/profiles/host-windows-msvc -pr:b default `
  -s build_type=Release --build=missing
meson setup build\release --native-file build\release\build-release\conan\conan_meson_native.ini `
  --buildtype=debugoptimized -Db_ndebug=true
meson compile -C build\release
```

## Testing

```bash
./setup.sh Debug
meson configure build/debug -Dbuild-tests=true
meson compile -C build/debug
meson test -C build/debug --print-errorlogs
```

## Troubleshooting

| Symptom                                  | Fix                                                            |
|------------------------------------------|----------------------------------------------------------------|
| `qpdf: recompile with -fPIC`             | `conan remove 'qpdf/*' -c && ./setup.sh Release`               |
| Clang: `cannot find -lstdc++`            | `sudo apt install libstdc++-13-dev` or `YAMS_COMPILER=gcc ./setup.sh Release` |
| Windows Boost build failures (VS 2025)   | Install v143 toolset, `conan remove 'boost/*' -c`, rerun setup |
| Windows: missing qpdf/onnxruntime recipes| `conan export conan/qpdf …`, `conan export conan/onnxruntime …`|
| Missing system packages                  | `pkg-config --list-all \| grep -E "(sqlite\|openssl\|protobuf)"` |

## Advanced

```bash
# Coverage
./setup.sh Debug --coverage && meson test -C build/debug && gcovr --html -o build/debug/coverage.html

# LTO
CXXFLAGS="-flto" LDFLAGS="-flto" ./setup.sh Release

# Native tuning (local benchmarks only — do not ship)
CXXFLAGS="-march=native" ./setup.sh Release
```

## References

- [Conan docs](https://docs.conan.io/2/) · [Meson docs](https://mesonbuild.com/)
- [GCC C++ status](https://gcc.gnu.org/projects/cxx-status.html) · [Clang C++ status](https://clang.llvm.org/cxx_status.html) · [MSVC conformance](https://docs.microsoft.com/cpp/overview/visual-cpp-language-conformance)
