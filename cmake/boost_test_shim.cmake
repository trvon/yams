# Minimal shim to satisfy accidental transitive Boost.Test link references when
# using GoogleTest as the primary test framework. On some Conan/Boost setups,
# libboost_test_exec_monitor or libboost_prg_exec_monitor can appear on link
# lines via aggregate boosts. Providing empty objects avoids undefined symbol
# errors for test_main while keeping GoogleTest's main.

if(NOT TARGET yams::boost_test_shim)
    add_library(yams_boost_test_shim STATIC EXCLUDE_FROM_ALL
        ${CMAKE_CURRENT_LIST_DIR}/boost_test_shim.cpp)
    add_library(yams::boost_test_shim ALIAS yams_boost_test_shim)
    target_compile_features(yams_boost_test_shim PUBLIC cxx_std_20)
    set_target_properties(yams_boost_test_shim PROPERTIES POSITION_INDEPENDENT_CODE ON)
endif()
