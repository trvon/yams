# Mobile Build Pipeline

This repository builds `libyams_mobile` as a C ABI corpus library. The library is intended to be
embedded by higher-level app shells such as Flutter, not to host inference itself.

## Current CI Lane

The dedicated workflow is [mobile-artifacts.yml](/Users/trevon/work/apps/yams-app/yams/.github/workflows/mobile-artifacts.yml).

Current CI guarantees:

- Build `libyams_mobile` with `YAMS_ENABLE_MOBILE_BINDINGS=true`
- Compile the Catch2 mobile ABI and daemon protocol suites
- Run the mobile ABI suite and daemon protocol suite
- Verify that the shared library exports every `YAMS_MOBILE_API` symbol declared in
  [mobile_bindings.h](/Users/trevon/work/apps/yams-app/yams/include/yams/api/mobile_bindings.h)
- Package a downloadable artifact containing:
  - `lib/` with the built shared library
  - `include/yams/api/mobile_bindings.h`
  - `manifest.json`

Current artifact platforms:

- `linux-x86_64`
- `macos-arm64`

The reusable CI entrypoints are:

- [build_mobile_lane.sh](/Users/trevon/work/apps/yams-app/yams/scripts/ci/build_mobile_lane.sh)
- [run_mobile_lane.sh](/Users/trevon/work/apps/yams-app/yams/scripts/ci/run_mobile_lane.sh)

## Local Commands

Build the mobile lane locally:

```bash
scripts/ci/build_mobile_lane.sh Debug
```

Run verification:

```bash
scripts/ci/run_mobile_lane.sh builddir
scripts/ci/check_mobile_abi.sh builddir/src/mobile/libyams_mobile.dylib
```

Package a bundle:

```bash
scripts/ci/package_mobile_bindings.sh builddir builddir/mobile-artifacts
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
