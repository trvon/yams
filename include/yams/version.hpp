/*
 * Fallback version header for YAMS
 *
 * This header provides default version macros so the codebase compiles even
 * when the generated header is not present. When the build system exposes
 * version_generated.h on the include path, we import it first and map the
 * generated metadata onto the legacy macros used throughout the codebase.
 *
 * Macros mirror those in the generated header to avoid conditional code paths.
 */

#pragma once

#if __has_include(<version_generated.h>)
#include <version_generated.h>
#endif

// Semantic version components (fallback to 0.0.0)
#ifndef YAMS_VERSION_MAJOR
#define YAMS_VERSION_MAJOR 0
#endif

#ifndef YAMS_VERSION_MINOR
#define YAMS_VERSION_MINOR 5
#endif

#ifndef YAMS_VERSION_PATCH
#define YAMS_VERSION_PATCH 0
#endif

// Combined version string (prefer generated effective version when available)
#ifndef YAMS_VERSION_STRING
#ifdef YAMS_EFFECTIVE_VERSION
#define YAMS_VERSION_STRING YAMS_EFFECTIVE_VERSION
#else
#define YAMS_VERSION_STRING "0.0.0-dev"
#endif
#endif

// Extended metadata (fall back to unknowns if not provided by build system)
#ifndef YAMS_GIT_COMMIT
#define YAMS_GIT_COMMIT "unknown"
#endif

#ifndef YAMS_BUILD_DATE
#ifdef YAMS_BUILD_TIMESTAMP
#define YAMS_BUILD_DATE YAMS_BUILD_TIMESTAMP
#else
#define YAMS_BUILD_DATE __DATE__ " " __TIME__
#endif
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
