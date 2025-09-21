#if defined(TRACY_ENABLE)
#if __has_include(<tracy/TracyNewDelete.hpp>)
#include <tracy/TracyNewDelete.hpp>
#elif __has_include(<TracyNewDelete.hpp>)
#include <TracyNewDelete.hpp>
#else
// Fallback: header not available in this Tracy package; global new/delete hooks disabled.
// You can still see allocator graphs if you add explicit TracyAlloc/TracyFree in hot paths.
#endif
#endif

// This TU enables global new/delete tracking when TRACY_ENABLE is defined.
// It is intentionally tiny and linked only into the daemon.
