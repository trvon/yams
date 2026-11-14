// Compile-time sanitizer detection for tests.
//
// Ask the compiler, not the environment: meson registrations export TSAN_OPTIONS in every lane
// (it only carries the suppressions path), so `getenv("TSAN_OPTIONS")` is true in non-TSan
// builds too and a skip keyed on it never runs the test anywhere.
#pragma once

#if defined(__SANITIZE_THREAD__)
#define YAMS_TEST_TSAN_ACTIVE 1
#elif defined(__has_feature)
#if __has_feature(thread_sanitizer)
#define YAMS_TEST_TSAN_ACTIVE 1
#endif
#endif

namespace yams::test {

#ifdef YAMS_TEST_TSAN_ACTIVE
inline constexpr bool kThreadSanitizerBuild = true;
#else
inline constexpr bool kThreadSanitizerBuild = false;
#endif

} // namespace yams::test
