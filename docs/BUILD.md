# Building YAMS (GCC, Clang, or MSVC)

Cross-platform build guide for YAMS with GCC, Clang, or MSVC toolchains.

## Quick Start

**Linux/macOS:** Use `setup.sh` (auto-detects compiler, handles Conan + Meson):

```bash
# Release build (default: Clang if available, else GCC)
./setup.sh Release

# Debug build with tests
./setup.sh Debug

# Force specific compiler
YAMS_COMPILER=gcc ./setup.sh Release
YAMS_COMPILER=clang ./setup.sh Release
```

Then compile:
```bash
meson compile -C build/release  # or builddir for Debug
```

**Windows:** Use `setup.ps1` from any PowerShell prompt (MSVC + Conan + Meson):

```pwsh
# Release build (MSVC)
./setup.ps1 Release

# Debug build with tests
./setup.ps1 Debug
```

## Prerequisites

### Linux/macOS

**Compiler** (choose one):
- GCC 13+ (recommended) or GCC 11+ (C++20 minimum)
- Clang 16+ (recommended) or Clang 14+ (C++20 minimum)

**Build tools:**
```bash
# Ubuntu/Debian
sudo apt-get install -y build-essential cmake meson ninja-build pkg-config

# Optional but recommended for faster builds
sudo apt-get install -y ccache lld clang clang-tidy
```

**System dependencies:**
```bash
# Ubuntu/Debian
sudo apt-get install -y libssl-dev libcurl4-openssl-dev libsqlite3-dev \
    protobuf-compiler libprotobuf-dev zlib1g-dev

# Fedora
sudo dnf install -y openssl-devel libcurl-devel sqlite-devel protobuf-devel zlib-devel

# Arch
sudo pacman -S openssl curl sqlite protobuf zlib
```

### Windows

**Required:**
- **Visual Studio 2022** (or VS 2025 with v143 toolset) with "Desktop development with C++" workload
  - MSVC toolset version 193 (v143 - VS 2022) **required for Boost 1.85**
  - If using VS 2025: Install v143 toolset via Visual Studio Installer → Individual Components → "MSVC v143"
  - To check your VS version:
    ```powershell
    & "${env:ProgramFiles(x86)}\Microsoft Visual Studio\Installer\vswhere.exe" -latest -property installationPath
    # Path shows version: "18" = VS 2025, "17" = VS 2022
    ```
- Python 3.10+ with `pip` in PATH
- CMake 3.23+ in PATH

**Setup notes:**
- `setup.ps1` automatically uses VS 2022 toolset (v143/MSVC 193) for Boost compatibility
- Auto-installs Python packages (`conan`, `meson`, `ninja`, `numpy`)
- Auto-exports custom Conan recipes (`qpdf`, `onnxruntime`)
- Uses C++20 by default (C++23 currently incompatible with Boost on Windows)
- **PDF Plugin**: The `pdf_extractor` plugin is disabled by default on Windows due to compatibility issues with `qpdf` headers and MSVC. To attempt enabling it, pass `-Dplugin-pdf=true` to Meson, but be aware it may fail to compile.

### All Platforms

**Conan:**
```bash
pip install conan
conan profile detect --force
```

## Manual Build (without setup scripts)

### GCC

```bash
conan install . -of build/release -s build_type=Release \
  -s compiler.cppstd=20 -b missing --build=qpdf/*
meson setup build/release \
  --prefix /usr/local \
  --native-file build/release/build-release/conan/conan_meson_native.ini \
  --buildtype=release
meson compile -C build/release
```

### Clang

```bash
conan install . -of build/release -s build_type=Release \
  -s compiler=clang -s compiler.version=18 -s compiler.libcxx=libstdc++11 \
  -s compiler.cppstd=20 -b missing --build=qpdf/*
CC=clang CXX=clang++ meson setup build/release \
  --prefix /usr/local \
  --native-file build/release/build-release/conan/conan_meson_native.ini \
  --buildtype=release
meson compile -C build/release
```

### MSVC (Windows)

From any PowerShell prompt:

```pwsh
# Install Python tools (if not already installed)
python -m pip install --user conan meson ninja

# Ensure user scripts on PATH
$userBase = python -m site --user-base
$userBin = Join-Path $userBase 'Scripts'
if ($env:PATH -notlike "*$userBin*") { $env:PATH = "$userBin;$env:PATH" }

# Detect and configure Conan profile
conan profile detect --force

# Export local recipes (REQUIRED before first build)
conan export conan/qpdf --name=qpdf --version=11.9.0
conan export conan/onnxruntime --name=onnxruntime --version=1.23.2

# Option A: Use provided Windows profile (recommended)
conan install . -of build\release `
  -pr:h conan/profiles/host-windows-msvc -pr:b default `
  -s build_type=Release `
  --build=missing --build=qpdf/*

# Option B: Specify settings explicitly
conan install . -of build\release `
  -s os=Windows -s arch=x86_64 `
  -s compiler=msvc -s compiler.version=195 `
  -s compiler.runtime=dynamic -s compiler.cppstd=23 `
  -s build_type=Release `
  -c tools.microsoft.msbuild:vs_version=18 `
  --build=missing --build=qpdf/*

meson setup build\release `
  --native-file build\release\build-release\conan\conan_meson_native.ini `
  --buildtype=release

meson compile -C build\release
```

**Windows profile reference** (`conan/profiles/host-windows-msvc`):
```ini
[settings]
os=Windows
arch=x86_64
compiler=msvc
compiler.version=193  # VS 2022 (required for Boost 1.85 compatibility)
compiler.runtime=dynamic
compiler.cppstd=23    # C++23 works fine with MSVC 193
build_type=Release

[conf]
tools.cmake.cmaketoolchain:generator=Ninja
tools.microsoft.msbuild:vs_version=17  # VS 2022
```

**Note:** The project requires VS 2022 toolset (v143) for Boost compatibility, but C++23 features work correctly with MSVC 193.

Check compiler version: `cl.exe 2>&1 | Select-String "Version"`

## Common Options

**Enable tests:**
```bash
./setup.sh Debug
meson configure builddir -Dbuild-tests=true
meson test -C builddir
```

**Disable optional features:**
```bash
YAMS_DISABLE_ONNX=true ./setup.sh Release
YAMS_DISABLE_PDF=true ./setup.sh Release
YAMS_DISABLE_SYMBOL_EXTRACTION=true ./setup.sh Release
```

**Code coverage:**
```bash
./setup.sh Debug --coverage
meson test -C builddir
gcovr --html --html-details -o builddir/coverage.html
```

**libc++ hardening (Clang only):**
```bash
YAMS_LIBCXX_HARDENING=fast ./setup.sh Debug    # ~5% overhead
YAMS_LIBCXX_HARDENING=extensive ./setup.sh Debug  # ~10-20% overhead
```

## Platform-Specific Notes

### Windows API Compatibility

YAMS uses cross-platform abstractions for file I/O and system APIs. When porting code to Windows:

#### File I/O & Memory Mapping

| POSIX | Windows | Notes |
|-------|---------|-------|
| `open()` | `_wopen()` + `_O_BINARY` | Wide-char path required |
| `close()` | `_close()` | |
| `fstat()` | `_fstat64()` | 64-bit for large files |
| `ftruncate()` | `_chsize_s(fd, __int64)` | Explicit cast required |
| `fsync()` | `_commit()` | |
| `mmap()` | `CreateFileMapping()` + `MapViewOfFile()` | Two-step process |
| `munmap()` | `UnmapViewOfFile()` + `CloseHandle()` | Close both handles |
| `msync()` | `FlushViewOfFile(addr, size)` | Size parameter required |

#### File Open Flags

| POSIX | Windows | Notes |
|-------|---------|-------|
| `O_RDONLY` | `_O_RDONLY` | Requires `<fcntl.h>` |
| `O_RDWR` | `_O_RDWR` | |
| `O_CREAT` | `_O_CREAT` | |
| N/A | `_O_BINARY` | **Critical for Windows** |

#### Process & Environment

| POSIX | Windows | Notes |
|-------|---------|-------|
| `getpid()` | `_getpid()` | |
| `getuid()`, `getgid()` | N/A | Windows uses SIDs |
| `setenv()` | `_putenv_s()` | Different signature |
| `getenv()` | `getenv()` / `_dupenv_s()` | MSVC warns about security |

#### Common Patterns

**File operations template:**
```cpp
#ifdef _WIN32
#define NOMINMAX
#define _CRT_SECURE_NO_WARNINGS
#include <windows.h>
#include <io.h>
#include <fcntl.h>
#include <sys/stat.h>
#define fstat _fstat64
#define stat _stat64
#else
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#endif
```

**Memory mapping:**
```cpp
#ifdef _WIN32
HANDLE hFile = (HANDLE)_get_osfhandle(fd);
HANDLE mappingHandle = CreateFileMapping(hFile, NULL, PAGE_READWRITE, 0, size, NULL);
void* memory = MapViewOfFile(mappingHandle, FILE_MAP_WRITE, 0, 0, size);
// Use memory...
FlushViewOfFile(memory, size);
UnmapViewOfFile(memory);
CloseHandle(mappingHandle);
#else
void* memory = mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
// Use memory...
msync(memory, size, MS_SYNC);
munmap(memory, size);
#endif
```

**File truncation:**
```cpp
#ifdef _WIN32
_chsize_s(fd, static_cast<__int64>(newSize));
#else
ftruncate(fd, newSize);
#endif
```

**Pointer arithmetic with `void*`:**
```cpp
// Cast to uint8_t* required on all platforms for void* arithmetic
auto* bytePtr = static_cast<uint8_t*>(mappedMemory) + offset;
```

#### Windows Build Progress

**Completed migrations:**
- `src/wal/wal_file.cpp` - File I/O, memory mapping, truncation ✅

**In progress:**
- `src/daemon/daemon.cpp` - User/group management (needs SID abstraction)
- `src/daemon/components/ServiceManager.cpp` - File locking (different semantics)

**Current build state:** 55/222 files compiled (improved from 4/274)

**MSVC warnings:** Use `_CRT_SECURE_NO_WARNINGS` for cross-platform code to suppress security warnings on standard C functions.

## Troubleshooting

### qpdf linking error: "recompile with -fPIC"

**Error:**
```
/usr/bin/ld: libqpdf.a(...): relocation R_X86_64_PC32 against symbol `...` can not be used when making a shared object; recompile with -fPIC
```

**Fix:**
```bash
conan remove 'qpdf/*' -c
./setup.sh Release
```

### Clang: "cannot find -lstdc++"

**Fix (choose one):**

1. Switch to GCC:
   ```bash
   YAMS_COMPILER=gcc ./setup.sh Release
   ```

2. Use libc++:
   ```bash
   sudo apt-get install libc++-dev libc++abi-dev
   # Edit ~/.conan2/profiles/default:
   # compiler.libcxx=libstdc++11 → compiler.libcxx=libc++
   ```

3. Install libstdc++ dev files:
   ```bash
   sudo apt-get install libstdc++-13-dev
   ```

### Windows: VS non-existing installation error

**Fix:**
```pwsh
# Edit C:\Users\<YourUser>\.conan2\profiles\default
# Add to [conf] section:
[conf]
tools.microsoft.msbuild:vs_version=17
tools.cmake.cmaketoolchain:generator=Ninja
```

Or use `setup.ps1` which handles this automatically.

### Windows: Missing custom recipes

**Fix:**
```pwsh
conan export conan/qpdf --name=qpdf --version=11.9.0
conan export conan/onnxruntime --name=onnxruntime --version=1.23.2
./setup.ps1 Release
```

`setup.ps1` does this automatically.

### Windows: Boost build failures

**Error:**
```
...skipped libboost_*-variant-static.cmake for lack of...
boost/1.85.0: ERROR: Package build failed
```

**Cause:** Boost 1.85/1.86 requires VS 2022 toolset (v143). VS 2025 toolset (v145) is not supported.

**Solution:**

1. **Install VS 2022 v143 toolset (if using VS 2025):**
   - Open Visual Studio Installer
   - Click "Modify" on your VS installation
   - Go to "Individual Components" tab
   - Search for "v143"
   - Check "MSVC v143 - VS 2022 C++ x64/x86 build tools (Latest)"
   - Click "Modify" to install

2. **Clean Boost cache:**
   ```powershell
   conan remove 'boost/*' -c
   ```

3. **Re-run setup:**
   ```powershell
   .\setup.ps1 Release
   ```

The `setup.ps1` script automatically uses MSVC 193 (VS 2022) settings for Boost compatibility.

**Diagnostic tool:**
```pwsh
# Run the diagnostic script
.\fix-boost.ps1

# Clean Boost cache
.\fix-boost.ps1 -Clean
```

### Windows: Common compile errors

| Error | Fix |
|-------|-----|
| Missing `_O_RDONLY`, `_O_BINARY` | Add `#include <fcntl.h>` for Windows |
| `FlushViewOfFile` parameter count | Use 2-param form: `FlushViewOfFile(addr, size)` |
| `_chsize_s` parameter count | Use `_chsize_s(fd, static_cast<__int64>(size))` |
| Pointer arithmetic with `void*` | Cast to `uint8_t*` first |

### Missing dependencies (Linux/macOS)

**Check:**
```bash
pkg-config --list-all | grep -E "(sqlite|openssl|protobuf|libcurl)"
```

If missing, install development packages (see Prerequisites).

### Old compiler version

**Upgrade:**
```bash
# Ubuntu
sudo apt-get install gcc-13 g++-13 clang-18

# Update alternatives (optional)
sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-13 100
```

### ONNX disabled

**Fix:**
```bash
./setup.sh Release  # ONNX enabled by default
# Or explicitly:
conan install . -of build/release -s build_type=Release \
  -o yams/*:enable_onnx=True -b missing
```

## Compiler Comparison

| Feature | GCC 13+ | Clang 16+ | MSVC 193+ |
|---------|---------|-----------|-----------|
| C++20 coroutines | ✓ | ✓ | ✓ |
| std::format | ✓ | ✓ | ✓ |
| Default linker | GNU ld | LLD (faster) | MSVC linker |
| Hardening | `-D_GLIBCXX_DEBUG` | libc++ modes | `/sdl` |
| Diagnostics | Good | Better | Good |
| Platform | Linux, macOS | Linux, macOS, Windows | Windows only |

All three work well. Clang + LLD gives fastest link times on Linux/macOS.

## Advanced

**Custom C++ standard:**
```bash
# C++20 (default, widely supported)
YAMS_CPPSTD=20 ./setup.sh Release

# C++23 (requires GCC 13+, Clang 16+, or MSVC 193+)
YAMS_CPPSTD=23 ./setup.sh Release
```

**Note:** YAMS detects C++23 features at compile-time and uses them when available. C++20 is baseline.

**Custom install prefix:**
```bash
YAMS_INSTALL_PREFIX=/opt/yams ./setup.sh Release
```

**LTO:**
```bash
# GCC
CC=gcc CXX=g++ CXXFLAGS="-flto" LDFLAGS="-flto" ./setup.sh Release

# Clang (ThinLTO)
CC=clang CXX=clang++ CXXFLAGS="-flto=thin" LDFLAGS="-flto=thin" ./setup.sh Release
```

**Machine-specific optimizations:**
```bash
CXXFLAGS="-march=native -mtune=native" ./setup.sh Release
```
⚠️ Binary won't be portable to other CPUs.

## Verification

```bash
# Check version
./build/release/tools/yams-cli/yams --version

# Quick test
echo "Hello, YAMS!" | ./build/release/tools/yams-cli/yams add -

# After install
yams --version
```

## References

- [Developer Build System Guide](developer/build_system.md) - Detailed build options
- [Conan Documentation](https://docs.conan.io/2/)
- [Meson Documentation](https://mesonbuild.com/)
- [GCC C++20 Status](https://gcc.gnu.org/projects/cxx-status.html#cxx20)
- [Clang C++ Status](https://clang.llvm.org/cxx_status.html)
- [MSVC C++ Status](https://docs.microsoft.com/en-us/cpp/overview/visual-cpp-language-conformance)
