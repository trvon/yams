#pragma once

#if defined(__has_include)
#if __has_include(<expected>)
#define YAMS_HAS_STD_EXPECTED 1
#endif
#endif

#ifdef YAMS_HAS_STD_EXPECTED
#include <expected>
#else
#include <tl/expected.hpp>
#endif