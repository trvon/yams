add_subdirectory(mock_model_provider)
add_subdirectory(mock_content_extractor)
add_subdirectory(pdf_extractor)
add_subdirectory(s3)

# Optionally build the real ONNX plugin from plugins/onnx for test harness
option(YAMS_TEST_BUILD_ONNX_PLUGIN "Build ONNX plugin for tests" ON)
if(YAMS_TEST_BUILD_ONNX_PLUGIN AND EXISTS ${CMAKE_SOURCE_DIR}/plugins/onnx/CMakeLists.txt)
    if(NOT TARGET yams_onnx_plugin)
        add_subdirectory(${CMAKE_SOURCE_DIR}/plugins/onnx ${CMAKE_BINARY_DIR}/plugins/onnx_build)
    endif()
endif()
