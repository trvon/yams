# Developer Setup

Concise instructions to build, test, and develop YAMS locally.

## Supported platforms
- macOS 12+ (Apple Silicon and x86_64)
- Linux x86_64 (glibc-based)
- Windows is not a primary target for development (WSL2 recommended)

## Prerequisites
- Git
- C++ compiler: Clang 14+ or GCC 11+
- CMake 3.25+ (Ninja recommended)
- Python 3.8+ and pip (for Conan)
- Conan 2.x (package/dependency manager)

Install examples:
- macOS (Homebrew)
  - brew install cmake ninja python
  - pip3 install --upgrade conan
  - xcode-select --install  (ensure Command Line Tools present)
- Ubuntu/Debian
  - sudo apt-get update
  - sudo apt-get install -y build-essential cmake ninja-build python3-pip
  - pip3 install --upgrade conan
- Fedora
  - sudo dnf groupinstall -y "Development Tools"
  - sudo dnf install -y cmake ninja-build python3-pip
  - pip3 install --upgrade conan

Initialize Conan profile (one-time):
```bash
conan profile detect --force
```

## Quick start (Release)
```bash
# From repo root
pip3 install --upgrade conan

# Resolve deps to a dedicated build folder
conan install . --output-folder=build/conan-release -s build_type=Release --build=missing

# Configure (Conan generates a CMake preset named "conan-release")
cmake --preset conan-release

# Build
cmake --build --preset conan-release

# Install (optional; may require sudo depending on CMAKE_INSTALL_PREFIX)
sudo cmake --install build/conan-release/build/Release
```

## Debug build
```bash
conan install . --output-folder=build/conan-debug -s build_type=Debug --build=missing
cmake --preset conan-debug
cmake --build --preset conan-debug
```

## Run unit tests
Common options:
- Run tests from the configured build directory with ctest
- Show output on failures; parallelize with -j

Examples:
```bash
# Debug tree (typical multi-config layout)
ctest --output-on-failure --test-dir build/conan-debug/build/Debug -j

# Release
ctest --output-on-failure --test-dir build/conan-release/build/Release -j
```

Notes:
- If your generator uses single-config (e.g., Ninja), the inner build dir path may differ (e.g., build/conan-debug/build).
- ctest will discover tests if the project enables testing (enable_testing/add_test).

## Developer loop
- Edit code
- Rebuild (incremental):
  - cmake --build --preset conan-debug -j
- Smoke test CLI (example):
  - ./yams --version (from install location) or from the built binary path inside the build tree
- Use a temporary data directory while developing:
  - mkdir -p /tmp/yams-dev && export YAMS_STORAGE=/tmp/yams-dev
  - yams init --non-interactive
  - echo "dev" | yams add - --tags dev
  - yams search dev

If YAMS_STORAGE is not supported in your version, run yams from a working directory you control (see docs for configuration and default paths).

## Address/UB Sanitizers (Debug)
Build with sanitizers for faster defect discovery:
```bash
# Example via CMake cache flags (per build directory)
cmake --preset conan-debug -DYA_ENABLE_ASAN=ON -DYA_ENABLE_UBSAN=ON
cmake --build --preset conan-debug -j
# Run tests to exercise instrumented code
ctest --output-on-failure --test-dir build/conan-debug/build/Debug -j
```
If your project doesn’t expose YA_ENABLE_ASAN/UBSAN, you can add compile/link flags via CMAKE_<LANG>_FLAGS_* cache entries or a toolchain overlay.

## Static analysis and formatting
- clang-format:
  ```bash
  find src include -name '*.[ch]pp' -o -name '*.cc' -o -name '*.hh' | xargs clang-format -i
  ```
- clang-tidy (example invocation):
  ```bash
  cmake --build --preset conan-debug --target clang-tidy
  ```
Adjust targets/paths to your repository layout. If a .clang-format or .clang-tidy exists at repo root, it will be used.

## CCache (optional)
Speed up rebuilds:
- Install ccache (brew install ccache or apt/dnf install ccache)
- Export CC/CXX wrappers or configure via CMake toolchain/profile as preferred

## Docker (optional)
Use the published image for quick CLI checks or isolation:
```bash
docker run --rm -it ghcr.io/trvon/yams:latest --version
```
Bind mount a host directory to persist data:
```bash
mkdir -p $HOME/yams-data
docker run --rm -it -v $HOME/yams-data:/var/lib/yams ghcr.io/trvon/yams:latest yams init --non-interactive
```

## Documentation (MkDocs)
If you need to preview docs locally:
```bash
# Requires Python 3 and mkdocs-material
pip3 install mkdocs mkdocs-material
mkdocs serve -f yams/mkdocs.yml
```

## Troubleshooting
- Compiler/toolchain not found: verify conan profile detect and PATH for compiler/cmake/ninja.
- Link errors/missing deps: re-run conan install with --build=missing; clean the build folder if needed.
- Tests not discovered: confirm enable_testing() and add_test() are present; run ctest from the correct inner build dir.
- Install prefix: set CMAKE_INSTALL_PREFIX when configuring if you don’t want system-wide install.

## Notes
- Use Release for benchmarking and production builds; Debug + sanitizers for local development.
- Keep your build trees (build/conan-debug, build/conan-release) out of version control.
- File issues with exact compiler/CMake/Conan versions and commands you ran for fast triage.