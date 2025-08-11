if(EXISTS "/Volumes/picaso/work/tools/kronos/tests/unit/integrity/integrity_unit_tests")
  if(NOT EXISTS "/Volumes/picaso/work/tools/kronos/tests/unit/integrity/integrity_unit_tests[1]_tests.cmake" OR
     NOT "/Volumes/picaso/work/tools/kronos/tests/unit/integrity/integrity_unit_tests[1]_tests.cmake" IS_NEWER_THAN "/Volumes/picaso/work/tools/kronos/tests/unit/integrity/integrity_unit_tests" OR
     NOT "/Volumes/picaso/work/tools/kronos/tests/unit/integrity/integrity_unit_tests[1]_tests.cmake" IS_NEWER_THAN "${CMAKE_CURRENT_LIST_FILE}")
    include("/opt/homebrew/share/cmake/Modules/GoogleTestAddTests.cmake")
    gtest_discover_tests_impl(
      TEST_EXECUTABLE [==[/Volumes/picaso/work/tools/kronos/tests/unit/integrity/integrity_unit_tests]==]
      TEST_EXECUTOR [==[]==]
      TEST_WORKING_DIR [==[/Volumes/picaso/work/tools/kronos/tests/unit/integrity]==]
      TEST_EXTRA_ARGS [==[]==]
      TEST_PROPERTIES [==[]==]
      TEST_PREFIX [==[]==]
      TEST_SUFFIX [==[]==]
      TEST_FILTER [==[]==]
      NO_PRETTY_TYPES [==[FALSE]==]
      NO_PRETTY_VALUES [==[FALSE]==]
      TEST_LIST [==[integrity_unit_tests_TESTS]==]
      CTEST_FILE [==[/Volumes/picaso/work/tools/kronos/tests/unit/integrity/integrity_unit_tests[1]_tests.cmake]==]
      TEST_DISCOVERY_TIMEOUT [==[5]==]
      TEST_DISCOVERY_EXTRA_ARGS [==[]==]
      TEST_XML_OUTPUT_DIR [==[]==]
    )
  endif()
  include("/Volumes/picaso/work/tools/kronos/tests/unit/integrity/integrity_unit_tests[1]_tests.cmake")
else()
  add_test(integrity_unit_tests_NOT_BUILT integrity_unit_tests_NOT_BUILT)
endif()
