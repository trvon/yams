#pragma once

#if defined(YAMS_TESTING) || defined(YAMS_DAEMON_TEST_HOOKS_IMPL)
#define YAMS_DAEMON_TEST_HOOKS_ENABLED 1
#if defined(__GNUC__) || defined(__clang__)
#define YAMS_DAEMON_TEST_HOOK [[gnu::visibility("hidden")]]
#else
#define YAMS_DAEMON_TEST_HOOK
#endif
#else
#define YAMS_DAEMON_TEST_HOOKS_ENABLED 0
#define YAMS_DAEMON_TEST_HOOK
#endif
