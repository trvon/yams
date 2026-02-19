# Developer Setup

Concise instructions to build, test, and develop YAMS locally.

## Supported Platforms

| Platform | Architecture | Status |
|----------|--------------|--------|
| Linux | x86_64, arm64 | Primary |
| macOS | Apple Silicon, x86_64 | Primary |
| Windows | x86_64 | Supported |

## Prerequisites

See `../BUILD.md#prerequisites` for compilers, build tools, and system package installs.

### Initialize Conan (one-time)

```bash
conan profile detect --force
```

## Quick Start

### Linux/macOS

```bash
# Release build
./setup.sh Release
meson compile -C build/release

# Debug build (includes tests)
./setup.sh Debug
meson compile -C builddir
```

### Windows

```pwsh
# Release build
./setup.ps1 Release
meson compile -C build/release

# Debug build (includes tests)
./setup.ps1 Debug
meson compile -C builddir
```

For manual (Conan + Meson) commands, see `../BUILD.md`.

## Build Options

For the full list of build variables and Meson options, see `../BUILD.md#build-options`.

### Developer convenience variables

| Variable | Default | Description |
|----------|---------|-------------|
| `YAMS_ONNX_GPU` | auto | GPU provider: `auto`, `cuda`, `coreml`, `directml`, `none` |
| `FAST_MODE` | 0 | Disable ONNX and tests for quick iteration |

Common post-setup tweak:
```bash
meson configure builddir -Dbuild-tests=true
```

## Testing

```bash
# Build with tests
./setup.sh Debug
meson configure builddir -Dbuild-tests=true
meson compile -C builddir

# Run all tests
meson test -C builddir --print-errorlogs

# Run specific suites
meson test -C builddir --suite unit:smoke      # Quick smoke tests
meson test -C builddir --suite unit            # All unit tests
meson test -C builddir --suite integration     # Integration tests
```

## Developer Loop

```bash
# Edit code, then rebuild (incremental)
meson compile -C builddir

# Smoke test CLI
./builddir/tools/yams-cli/yams --version

# Use temporary data directory
mkdir -p /tmp/yams-dev && export YAMS_STORAGE=/tmp/yams-dev
yams init --non-interactive
echo "test" | yams add - --tags test
yams search test
```

## Sanitizers (Debug)

```bash
CFLAGS='-fsanitize=address,undefined' CXXFLAGS='-fsanitize=address,undefined' \
  meson setup builddir --reconfigure
meson compile -C builddir
meson test -C builddir
```

## Static Analysis and Formatting

```bash
# clang-format
find src include -name '*.[ch]pp' -o -name '*.cc' -o -name '*.hh' | xargs clang-format -i

# clang-tidy (integrate via editor or invoke directly)
```

## CCache (optional)

Speed up rebuilds:
```bash
# Install
brew install ccache  # macOS
sudo apt install ccache  # Ubuntu

# Export wrappers
export CC="ccache gcc"
export CXX="ccache g++"
```

## Docker (optional)

```bash
# Quick CLI check
docker run --rm -it ghcr.io/trvon/yams:latest --version

# Persistent data
mkdir -p $HOME/yams-data
docker run --rm -it -v $HOME/yams-data:/var/lib/yams ghcr.io/trvon/yams:latest yams init --non-interactive
```

## GPU Acceleration

The ONNX plugin supports GPU acceleration for embedding generation. GPU support is **auto-detected** during build based on platform and available hardware.

### Supported Providers

| Platform | Provider | Hardware | Detection |
|----------|----------|----------|-----------|
| macOS | CoreML | Apple Neural Engine + GPU | Always enabled (built into ONNX Runtime) |
| Linux | CUDA | NVIDIA GPUs | Detected via `nvidia-smi` |
| Windows | DirectML | Any DirectX 12 GPU | Detected via Win32 API |
| Windows | CUDA | NVIDIA GPUs | Preferred over DirectML if detected |

### Build Configuration

```bash
# Auto-detect (default) - recommended
./setup.sh Release

# Force specific provider
YAMS_ONNX_GPU=cuda ./setup.sh Release      # NVIDIA CUDA
YAMS_ONNX_GPU=coreml ./setup.sh Release    # macOS CoreML
YAMS_ONNX_GPU=directml ./setup.ps1         # Windows DirectML

# Disable GPU (CPU-only)
YAMS_ONNX_GPU=none ./setup.sh Release
```

### Runtime Configuration

GPU is used automatically when the plugin loads. To enable/disable at runtime:

```toml
# ~/.config/yams/config.toml
[embeddings]
enable_gpu = true   # Use GPU if available (default)
# enable_gpu = false  # Force CPU-only
```

### Prerequisites

**CUDA (Linux/Windows):**
- NVIDIA GPU with CUDA Compute Capability 5.0+
- CUDA Toolkit 11.8+ and cuDNN 8.6+
- Driver version 525.60+

**DirectML (Windows):**
- Any DirectX 12 capable GPU (no additional drivers needed)

**CoreML (macOS):**
- macOS 11+ (Big Sur or later)
- No additional setup required; Neural Engine used on Apple Silicon

### Verification

```bash
# Check build configuration
yams plugin health

# Check ONNX provider in logs
yams daemon log | grep -i "execution provider"
# Should show: "[ONNX] CoreML execution provider enabled" or similar
```

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| Missing native file | Re-run Conan install |
| Link errors after dep change | Delete builddir and reconfigure |
| Tests not found | Ensure `-Dbuild-tests=true` and recompile |
| qpdf: "recompile with -fPIC" | `conan remove 'qpdf/*' -c` then re-setup |
| Clang: "cannot find -lstdc++" | Install libstdc++ or use `YAMS_COMPILER=gcc` |
| Windows: Boost build failures | Install v143 toolset, clean cache |
| Windows: Missing recipes | Export qpdf and onnxruntime recipes |

## Notes

- Use Release for benchmarking; Debug + sanitizers for development
- Keep build trees out of version control
- `mediainfo`/`libmediainfo-dev` enables richer video metadata
- Provide exact versions and commands when filing issues
