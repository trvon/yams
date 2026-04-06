# Mobile Build Pipeline

This repository builds `libyams_mobile` as a C ABI corpus library. The library is intended to be
embedded by higher-level app shells such as Flutter, not to host inference itself.

## Current CI Lane

The dedicated workflow is [mobile-artifacts.yml](/Users/trevon/work/apps/yams-app/yams/.github/workflows/mobile-artifacts.yml).

Current CI guarantees:

- Build `libyams_mobile` with `YAMS_ENABLE_MOBILE_BINDINGS=true`
- Keep the mobile lane library-only by disabling plugin, CLI, and MCP targets
- Compile the Catch2 mobile ABI and daemon protocol suites
- Run the mobile ABI suite and daemon protocol suite
- Verify that the shared library exports every `YAMS_MOBILE_API` symbol declared in
  [mobile_bindings.h](/Users/trevon/work/apps/yams-app/yams/include/yams/api/mobile_bindings.h)
- Package a downloadable artifact containing:
  - `lib/` with the built shared library
  - `include/yams/api/mobile_bindings.h`
  - `manifest.json`
- Validate all host/mobile Conan target profiles before building

Current artifact platforms:

- `linux-x86_64`
- `macos-arm64`

The reusable CI entrypoints are:

- [build_mobile_lane.sh](/Users/trevon/work/apps/yams-app/yams/scripts/ci/build_mobile_lane.sh)
- [run_mobile_lane.sh](/Users/trevon/work/apps/yams-app/yams/scripts/ci/run_mobile_lane.sh)
- [resolve_mobile_target.sh](/Users/trevon/work/apps/yams-app/yams/scripts/ci/resolve_mobile_target.sh)
- [check_mobile_profiles.sh](/Users/trevon/work/apps/yams-app/yams/scripts/ci/check_mobile_profiles.sh)

## Target Matrix

The mobile build lane now has named targets with separate Conan host profiles and separate default
build directories:

| Target | Conan host profile | Default build dir | Packaging role |
|--------|--------------------|-------------------|----------------|
| `host-linux-x86_64` | `conan/profiles/host-linux-clang` | `build/mobile-host-linux-x86_64` | Host-native CI artifact |
| `host-linux-arm64` | `conan/profiles/host-linux-clang-arm` | `build/mobile-host-linux-arm64` | Host-native packaging input |
| `host-macos-arm64` | `conan/profiles/host-macos-apple-clang` | `build/mobile-host-macos-arm64` | Host-native CI artifact |
| `host-macos-x86_64` | `conan/profiles/host-macos-apple-clang-x86` | `build/mobile-host-macos-x86_64` | Host-native packaging input |
| `ios-device-arm64` | `conan/profiles/host-ios-apple-clang-device-arm64` | `build/mobile-ios-device-arm64` | `.xcframework` device slice |
| `ios-sim-arm64` | `conan/profiles/host-ios-apple-clang-sim-arm64` | `build/mobile-ios-sim-arm64` | `.xcframework` simulator slice |
| `android-arm64` | `conan/profiles/host-android-clang-arm64` | `build/mobile-android-arm64` | `.aar` `arm64-v8a` slice |
| `android-x86_64` | `conan/profiles/host-android-clang-x86_64` | `build/mobile-android-x86_64` | `.aar` `x86_64` slice |

CI now validates all of those profiles with Conan, even though only the host-native targets are
built in the current workflow.

For platform targets, the shared build entrypoint now forwards machine-local toolchain paths into
Conan automatically:

- iOS targets infer `tools.apple:sdk_path` from `YAMS_IOS_DEVICE_SDK_PATH`,
  `YAMS_IOS_SIMULATOR_SDK_PATH`, or `xcrun`
- Android targets infer `tools.android:ndk_path` from `YAMS_ANDROID_NDK_PATH`,
  `ANDROID_NDK_ROOT`, or `ANDROID_NDK_HOME`, and require the selected NDK root to contain
  `build/cmake/android.toolchain.cmake`
- Cross-builds also resolve `protoc` from the build machine instead of the target protobuf package,
  so daemon IPC code generation no longer tries to execute Android/iOS binaries on the host

Manual workflow dispatch can now also attempt full platform packaging:

- `package_ios=true` builds `ios-device-arm64` and `ios-sim-arm64`, then assembles
  `YamsMobile.xcframework`
- `package_android=true` builds `android-arm64` and `android-x86_64`, then assembles a thin `.aar`

## Local Commands

Build the mobile lane locally:

```bash
scripts/ci/build_mobile_lane.sh Debug host-macos-arm64
```

Run verification:

```bash
scripts/ci/run_mobile_lane.sh build/mobile-host-macos-arm64
scripts/ci/check_mobile_abi.sh build/mobile-host-macos-arm64/src/mobile/libyams_mobile.dylib
```

Package a bundle:

```bash
scripts/ci/package_mobile_bindings.sh build/mobile-host-macos-arm64 build/mobile-host-macos-arm64/mobile-artifacts
```

Validate the full profile set:

```bash
scripts/ci/check_mobile_profiles.sh
```

Resolve a target to its profile/build-dir mapping:

```bash
scripts/ci/resolve_mobile_target.sh ios-device-arm64
```

Build an iOS xcframework locally:

```bash
scripts/ci/build_ios_xcframework.sh Release build/mobile-ios-packages
```

Build an Android AAR locally:

```bash
YAMS_ANDROID_NDK_PATH=/path/to/ndk scripts/ci/build_android_aar.sh Release build/mobile-android-packages
```

## Build Cleanup Direction

This lane is intentionally narrow. It validates the current corpus ABI and artifact packaging
without pretending the repo already ships native iOS/Android SDK bundles.

The wrapper boundary is documented in
[packaging/mobile/README.md](/Users/trevon/work/apps/yams-app/yams/packaging/mobile/README.md).

The next packaging layers should be built on top of this lane:

1. iOS: produce an `.xcframework` from device + simulator builds and publish it as a release
   artifact.
2. Android: produce a JNI/AAR wrapper containing the C ABI library for supported ABIs.
3. Flutter: consume those release artifacts from a thin plugin package rather than rebuilding the
   YAMS core inside the app repo.

That split keeps this repo responsible for the native corpus library and ABI validation, while the
Flutter repo owns Dart bindings, platform plugin glue, and UX.

Initial packaging helpers now exist for the wrapper layer:

- iOS cross-build + `.xcframework` orchestration:
  [build_ios_xcframework.sh](/Users/trevon/work/apps/yams-app/yams/scripts/ci/build_ios_xcframework.sh)
- iOS `.xcframework` assembly:
  [package_ios_xcframework.sh](/Users/trevon/work/apps/yams-app/yams/scripts/ci/package_ios_xcframework.sh)
- Android cross-build + `.aar` orchestration:
  [build_android_aar.sh](/Users/trevon/work/apps/yams-app/yams/scripts/ci/build_android_aar.sh)
- Android `.aar` assembly:
  [package_android_aar.sh](/Users/trevon/work/apps/yams-app/yams/scripts/ci/package_android_aar.sh)

The repo now has orchestration scripts for the actual iOS/Android target builds, but those still
depend on platform SDK availability in CI or on the local machine.

## Cross-Build Notes

Separate iOS and Android profiles now live in `conan/profiles/` so each platform can evolve
independently instead of sharing a generic mobile config.

iOS profiles:

- `conan/profiles/host-ios-apple-clang-device-arm64`
- `conan/profiles/host-ios-apple-clang-sim-arm64`

Android profiles:

- `conan/profiles/host-android-clang-arm64`
- `conan/profiles/host-android-clang-x86_64`

Those profiles are a clean source of truth for target settings, but real cross-builds still depend
on machine-local SDK wiring:

- iOS: Xcode plus any required Conan Apple SDK configuration such as `tools.apple:sdk_path`
- Android: Android NDK plus Conan/host configuration such as `tools.android:ndk_path`

The default CI path still keeps host-native builds green. Platform packaging is opt-in on manual
dispatch so failures in Apple/Android toolchain provisioning do not mask corpus ABI regressions.

Current validation state:

- the previous `fmt/10.2.1` simulator blocker was removed by forcing header-only `fmt` on iOS and
  Android builds in Conan
- mobile Conan builds now also force header-only `spdlog` to avoid Apple SDK-specific failures in
  the compiled `spdlog` + `fmt` path
- the shared mobile lane now disables plugins, CLI, and MCP builds so cross targets only configure
  the corpus library and its direct dependencies
- host-native mobile builds are green again with that narrower contract
- Android cross-builds now complete Meson setup, use native `/opt/homebrew/bin/protoc` for daemon
  IPC code generation, and proceed into real target compilation
- iOS simulator validation is now reaching later dependency builds under the real Apple SDK toolchain
- Android direct target builds now auto-forward the NDK path instead of only working through the
  AAR wrapper script
