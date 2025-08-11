if(EXISTS "/Volumes/picaso/work/tools/kronos/tests/unit/all_unit_tests")
  if(NOT EXISTS "/Volumes/picaso/work/tools/kronos/tests/unit/all_unit_tests[1]_tests.cmake" OR
     NOT "/Volumes/picaso/work/tools/kronos/tests/unit/all_unit_tests[1]_tests.cmake" IS_NEWER_THAN "/Volumes/picaso/work/tools/kronos/tests/unit/all_unit_tests" OR
     NOT "/Volumes/picaso/work/tools/kronos/tests/unit/all_unit_tests[1]_tests.cmake" IS_NEWER_THAN "${CMAKE_CURRENT_LIST_FILE}")
    include("/opt/homebrew/share/cmake/Modules/GoogleTestAddTests.cmake")
    gtest_discover_tests_impl(
      TEST_EXECUTABLE [==[/Volumes/picaso/work/tools/kronos/tests/unit/all_unit_tests]==]
      TEST_EXECUTOR [==[]==]
      TEST_WORKING_DIR [==[/Volumes/picaso/work/tools/kronos/tests/unit]==]
      TEST_EXTRA_ARGS [==[]==]
      TEST_PROPERTIES [==[LABELS;unit;TIMEOUT;30]==]
      TEST_PREFIX [==[]==]
      TEST_SUFFIX [==[]==]
      TEST_FILTER [==[]==]
      NO_PRETTY_TYPES [==[FALSE]==]
      NO_PRETTY_VALUES [==[FALSE]==]
      TEST_LIST [==[all_unit_tests_TESTS]==]
      CTEST_FILE [==[/Volumes/picaso/work/tools/kronos/tests/unit/all_unit_tests[1]_tests.cmake]==]
      TEST_DISCOVERY_TIMEOUT [==[5]==]
      TEST_DISCOVERY_EXTRA_ARGS [==[]==]
      TEST_XML_OUTPUT_DIR [==[]==]
    )
  endif()
  include("/Volumes/picaso/work/tools/kronos/tests/unit/all_unit_tests[1]_tests.cmake")
else()
  add_test(all_unit_tests_NOT_BUILT all_unit_tests_NOT_BUILT)
endif()
