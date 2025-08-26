/*
 * Fallback version header for YAMS
 *
 * This header provides default version macros so the codebase compiles even
 * when the generated header (typically located in the build directory under
 * generated/yams/version.hpp) is not present. The build system is expected to
 * generate and prefer that header; include directory ordering should ensure
 * the generated header takes precedence over this fallback.
 *
 * Macros mirror those in the generated header to avoid conditional code paths.
 */

#pragma once

// Semantic version components (fallback to 0.0.0)
#ifndef YAMS_VERSION_MAJOR
#define YAMS_VERSION_MAJOR 0
#endif

#ifndef YAMS_VERSION_MINOR
#define YAMS_VERSION_MINOR 0
#endif

#ifndef YAMS_VERSION_PATCH
#define YAMS_VERSION_PATCH 0
#endif

// Combined version string (fallback)
#ifndef YAMS_VERSION_STRING
#define YAMS_VERSION_STRING "0.0.0+dev"
#endif

// Extended metadata (fall back to unknowns if not provided by build system)
#ifndef YAMS_GIT_COMMIT
#define YAMS_GIT_COMMIT "unknown"
#endif

#ifndef YAMS_BUILD_DATE
#define YAMS_BUILD_DATE __DATE__ " " __TIME__
#endif

// Long version string: "X.Y.Z+qual (commit: abcdef1, built: YYYY-MM-DD HH:MM:SS)"
#ifndef YAMS_VERSION_LONG_STRING
#define YAMS_VERSION_LONG_STRING                                                                   \
    YAMS_VERSION_STRING " (commit: " YAMS_GIT_COMMIT ", built: " YAMS_BUILD_DATE ")"
#endif

// Optional C++ convenience constants (do not rely on these in C code)
#if defined(__cplusplus)
namespace yams {
namespace version {
constexpr int major_v = YAMS_VERSION_MAJOR;
constexpr int minor_v = YAMS_VERSION_MINOR;
constexpr int patch_v = YAMS_VERSION_PATCH;
constexpr const char* string_v = YAMS_VERSION_STRING;
constexpr const char* commit_v = YAMS_GIT_COMMIT;
constexpr const char* build_date_v = YAMS_BUILD_DATE;
constexpr const char* long_string_v = YAMS_VERSION_LONG_STRING;
} // namespace version
} // namespace yams
#endif