# Developer Setup

Concise instructions to build, test, and develop YAMS locally.

## Supported platforms
- macOS 12+ (Apple Silicon and x86_64)
- Linux x86_64 (glibc-based)
- Windows is not a primary target for development (WSL2 recommended)

## Prerequisites
- Git
- C++ compiler: Clang 14+ or GCC 11+
- Meson 1.4+ and Ninja
- Python 3.8+ and pip (for Conan)
- Conan 2.x (package/dependency manager)

Install examples:
- macOS (Homebrew)
  - brew install meson ninja python
  - pip3 install --upgrade conan
  - xcode-select --install  (ensure Command Line Tools present)
- Ubuntu/Debian
  - sudo apt-get update
  - sudo apt-get install -y build-essential meson ninja-build python3-pip
  - pip3 install --upgrade conan
- Fedora
  - sudo dnf groupinstall -y "Development Tools"
  - sudo dnf install -y meson ninja-build python3-pip
  - pip3 install --upgrade conan

Initialize Conan profile (one-time):
```bash
conan profile detect --force
```

## Quick start (Meson + Conan)
```bash
# Release
conan install . -of build/release -s build_type=Release -b missing
meson setup build/release \
  --prefix /usr/local \
  --native-file build/release/build-release/conan/conan_meson_native.ini \
  --buildtype=release
meson compile -C build/release

# Debug
conan install . -of build/debug -s build_type=Debug -b missing
meson setup build/debug \
  --prefix /usr/local \
  --native-file build/debug/build-debug/conan/conan_meson_native.ini \
  --buildtype=debug
meson compile -C build/debug
```

### Fast Mode
For quicker iteration (disables ONNX plugin/features & tests):
```bash
FAST_MODE=1 meson setup build/debug --reconfigure \
  --native-file build/debug/build-debug/conan/conan_meson_native.ini
```
or set `FAST_MODE=1` in CI (SourceHut manifest already respects it).

### Overriding Display Version
Inject a runtime/display version distinct from the static project version:
```bash
meson setup build/release --reconfigure \
  -Dyams-version="$(git describe --tags --always)" \
  --native-file build/release/build-release/conan/conan_meson_native.ini
```
The generated header `yams/version_generated.h` provides `YAMS_EFFECTIVE_VERSION`, git describe, and build timestamp.

## Run tests (Meson)
In Debug builds, tests are now enabled by default. For Release builds, pass `-Dbuild-tests=true` if you want to build tests.
```bash
meson compile -C build/debug
meson test -C build/debug                 # all suites
meson test -C build/debug -t unit         # unit only
meson test -C build/debug -t integration  # integration only
```
You can enable tests for Release builds by configuring that build dir with `-Dbuild-tests=true`.

## Strict local builds (warnings-as-errors)
To stabilize warnings locally without blocking CI, use the helper script (strict by default):
```bash
# Debug strict build (no tests compiled or run)
bash scripts/dev/tidy_build.sh --no-test

# If you already ran `conan install` and want to skip it:
bash scripts/dev/tidy_build.sh --no-conan --no-test

# Release strict build
bash scripts/dev/tidy_build.sh --release --no-test

# Disable strict (temporarily)
bash scripts/dev/tidy_build.sh --no-strict --no-test
```
Notes:
- CI remains non-strict for now; strict gating will be enabled once local builds are clean (see PBI 027).
- The script auto-detects the Conan Meson native file location.

## Developer loop
- Edit code
- Rebuild (incremental):
  - cmake --build --preset yams-debug -j
- Smoke test CLI (example):
  - ./yams --version (from install location) or from the built binary path inside the build tree
- Use a temporary data directory while developing:
  - mkdir -p /tmp/yams-dev && export YAMS_STORAGE=/tmp/yams-dev
  - yams init --non-interactive
  - echo "dev" | yams add - --tags dev
  - yams search dev

If YAMS_STORAGE is not supported in your version, run yams from a working directory you control (see docs for configuration and default paths).

## Address/UB Sanitizers (Debug)
You can inject sanitizer flags via environment or Meson options if/when exposed. Example with environment flags:
```bash
CFLAGS='-fsanitize=address,undefined' CXXFLAGS='-fsanitize=address,undefined' \
  meson setup build/debug --reconfigure
meson compile -C build/debug
meson test -C build/debug
```
If dedicated Meson options are added later, prefer those over raw flags.

## Static analysis and formatting
- clang-format:
  ```bash
  find src include -name '*.[ch]pp' -o -name '*.cc' -o -name '*.hh' | xargs clang-format -i
  ```
- clang-tidy: integrate via your editor or invoke directly on changed files.
Adjust targets/paths to your repository layout. If a .clang-format exists at repo root, it will be used.

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
- Compiler/toolchain not found: verify `conan profile detect` and PATH for meson/ninja.
- Link errors/missing deps: re-run `conan install` with `--build=missing`; clean the build folder if needed.
- Tests not found: ensure you configured with `-Dbuild-tests=true` and recompiled; list tests with `meson test -C build/debug -l`.
- Install prefix: use `meson install -C build/<dir>` and Meson install options as needed.

## Notes
- Use Release for benchmarking and production builds; Debug + sanitizers for local development.
- Keep your build trees (build/debug, build/release) out of version control.
- `mediainfo` + `libmediainfo-dev` (or distro equivalent) enables richer video metadata (`YAMS_HAVE_MEDIAINFO`).
- `ffprobe` (from FFmpeg) similarly enables `YAMS_HAVE_FFPROBE`; either tool enables video handler compilation.
- Provide exact compiler/Meson/Conan versions and commands you ran when filing issues for fast triage.
