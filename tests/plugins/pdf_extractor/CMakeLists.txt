cmake_minimum_required(VERSION 3.16)

project(YamsPdfExtractorPluginTests LANGUAGES CXX)

# Optionally build the real PDF extractor plugin from plugins/pdf_extractor for the test harness
option(YAMS_TEST_BUILD_PDF_EXTRACTOR_PLUGIN "Build PDF extractor plugin for tests" ON)
if(YAMS_TEST_BUILD_PDF_EXTRACTOR_PLUGIN AND EXISTS ${CMAKE_SOURCE_DIR}/plugins/pdf_extractor/CMakeLists.txt)
    # Avoid duplicate target if the plugin was already added elsewhere
    if(NOT TARGET yams_pdf_extractor)
        add_subdirectory(${CMAKE_SOURCE_DIR}/plugins/pdf_extractor ${CMAKE_BINARY_DIR}/plugins/pdf_extractor_build)
    endif()
endif()

# Only define the test if a source is present (keeps the tree buildable without the test file)
if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/pdf_extractor_plugin_test.cpp")
    add_executable(pdf_extractor_plugin_test
        pdf_extractor_plugin_test.cpp
    )
    if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/content_extractor_v1_test.cpp")
        target_sources(pdf_extractor_plugin_test PRIVATE content_extractor_v1_test.cpp)
    endif()

    target_compile_features(pdf_extractor_plugin_test PRIVATE cxx_std_20)

    target_link_libraries(pdf_extractor_plugin_test
        PRIVATE
            test_utils
            gtest_main
            # ABI plugin loader and helpers live here
            yams::daemon
            yams::app_services
    )

    # On Linux we may need libdl for dlopen/dlsym within the tests
    if(UNIX AND NOT APPLE)
        target_link_libraries(pdf_extractor_plugin_test PRIVATE dl)
    endif()

    # Allow tests to find the freshly built plugin artifacts at runtime (rpath)
    if(TARGET yams_pdf_extractor)
        # Ensure the test can locate the plugin .so in the build tree
        set_target_properties(pdf_extractor_plugin_test PROPERTIES
            BUILD_RPATH "${CMAKE_BINARY_DIR}/plugins/pdf_extractor_build"
            INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_LIBDIR}/yams/plugins"
        )
    endif()

    # Register with CTest using GoogleTest discovery
    include(GoogleTest)
    gtest_discover_tests(pdf_extractor_plugin_test
        DISCOVERY_MODE PRE_TEST
        DISCOVERY_TIMEOUT ${YAMS_CTEST_DISCOVERY_TIMEOUT}
        PROPERTIES
            LABELS "plugins;pdf;content;integration"
            TIMEOUT 120
    )
endif()
