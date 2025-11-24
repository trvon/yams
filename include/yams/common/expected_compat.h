#pragma once

#if defined(__has_include)
#if __has_include(<expected>)
  #if defined(_MSVC_LANG)
    #if _MSVC_LANG > 202002L
      #define YAMS_HAS_STD_EXPECTED 1
    #endif
  #elif __cplusplus > 202002L
    #define YAMS_HAS_STD_EXPECTED 1
  #endif
#endif
#endif

#ifdef YAMS_HAS_STD_EXPECTED
#include <expected>
#else
#include <tl/expected.hpp>
#endif