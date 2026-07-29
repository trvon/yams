# Build YAMS

YAMS uses Meson as its canonical build graph. The setup scripts detect the host
toolchain, resolve dependencies with Conan, and configure the build directory.

## Requirements

- C++20 compiler: GCC 11+, Clang 14+, or MSVC 2022+
- Python 3.10+
- Meson, Ninja, CMake 3.23+, and Conan 2
- `pkg-config` on Linux and macOS

## Linux and macOS

```bash
./setup.sh Release
meson compile -C build/release
```

For a development build with tests:

```bash
./setup.sh Debug --with-tests
meson compile -C build/debug -j4
meson test -C build/debug --print-errorlogs
```

Use the build directory printed by `setup.sh`; existing local trees may use
`builddir` instead of `build/debug`.

## Windows

Run from a Visual Studio 2022 developer shell:

```powershell
./setup.ps1 Release
meson compile -C build/release
```

## Offline builds

```bash
./setup.sh Release --offline
```

This requires all Conan packages and Meson wraps to exist locally. To prefer
system dependencies:

```bash
YAMS_USE_SYSTEM_DEPS=true ./setup.sh Release --system-deps
```

## Common options

```bash
meson configure build/release
meson configure build/release -Dbuild-tests=true
meson compile -C build/release
meson install -C build/release
```

Run `./setup.sh --help`, `./setup.ps1 -?`, and inspect
[`meson_options.txt`](../meson_options.txt) for the current option set.

## Local quality gate

```bash
git config core.hooksPath .githooks
scripts/quality-gate.sh
```

The default pre-push hook runs the blocking Linux and macOS CI gate. Coverage is
explicit:

```bash
YAMS_PREPUSH_COVERAGE=1 git push
```

## Troubleshooting

- Reconfigure after changing compilers or dependency profiles.
- Do not run concurrent Meson compiles against the same build directory.
- If Conan supplies a local Meson wrapper, use the wrapper printed by setup.
- Use `meson test -C <build-dir> --print-errorlogs` for failing tests.
- Use `./setup.sh Debug --offline --with-tests` to validate against the local
  dependency cache.
