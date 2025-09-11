cmake_minimum_required(VERSION 3.16)

project(YamsS3PluginSmoke LANGUAGES CXX)

option(YAMS_TEST_S3_PLUGIN_INTEGRATION "Build S3 plugin smoke test" OFF)

if(YAMS_TEST_S3_PLUGIN_INTEGRATION)
    add_executable(s3_plugin_smoke_test s3_plugin_smoke_test.cpp)
    target_compile_features(s3_plugin_smoke_test PRIVATE cxx_std_20)
    target_link_libraries(s3_plugin_smoke_test
        PRIVATE
            yams::core
            yams::storage_engine
            spdlog::spdlog
    )

    if(UNIX AND NOT APPLE)
        target_link_libraries(s3_plugin_smoke_test PRIVATE dl)
    endif()

    add_test(NAME s3_plugin_smoke_test COMMAND s3_plugin_smoke_test)
    # Help the loader find the freshly built plugin
    set_tests_properties(s3_plugin_smoke_test PROPERTIES
        ENVIRONMENT "YAMS_PLUGIN_DIR=${CMAKE_BINARY_DIR}/plugins/object_storage_s3"
    )
endif()
