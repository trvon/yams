cmake_minimum_required(VERSION 3.16)

project(yams_onnx_plugin LANGUAGES C CXX)

option(YAMS_BUILD_ONNX_PLUGIN "Build the YAMS ONNX plugin" ON)
set(YAMS_PLUGIN_DIR "lib/yams/plugins" CACHE STRING "Install directory for YAMS plugins (relative to CMAKE_INSTALL_PREFIX)")

if(NOT YAMS_BUILD_ONNX_PLUGIN)
  message(STATUS "YAMS_BUILD_ONNX_PLUGIN=OFF — skipping yams_onnx_plugin")
  return()
endif()

# Require ONNX Runtime headers/libs to build the plugin
find_package(onnxruntime QUIET)
if(NOT TARGET onnxruntime::onnxruntime)
  message(STATUS "ONNX Runtime not found — skipping yams_onnx_plugin")
  return()
endif()

add_library(yams_onnx_plugin SHARED
  onnx_plugin.cpp
  model_provider.cpp
  onnx_model_pool.cpp
)

target_compile_features(yams_onnx_plugin PRIVATE cxx_std_17)

# Include the project headers (expects repo layout: ./include)
target_include_directories(yams_onnx_plugin
  PRIVATE
    ${CMAKE_SOURCE_DIR}/include
)

set_target_properties(yams_onnx_plugin PROPERTIES
  OUTPUT_NAME "yams_onnx_plugin"
  PREFIX ""
  C_VISIBILITY_PRESET default
  CXX_VISIBILITY_PRESET default
  VISIBILITY_INLINES_HIDDEN OFF
)

# Link ONNX Runtime and internal vector utilities if available.
target_link_libraries(yams_onnx_plugin PRIVATE onnxruntime::onnxruntime)
if(TARGET yams::vector)
  target_link_libraries(yams_onnx_plugin PRIVATE yams::vector)
endif()
if(TARGET spdlog::spdlog)
  target_link_libraries(yams_onnx_plugin PRIVATE spdlog::spdlog)
endif()

install(
  TARGETS yams_onnx_plugin
  LIBRARY DESTINATION ${YAMS_PLUGIN_DIR}
  RUNTIME DESTINATION ${YAMS_PLUGIN_DIR}
  ARCHIVE DESTINATION ${YAMS_PLUGIN_DIR}
)
