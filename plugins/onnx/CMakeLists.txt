cmake_minimum_required(VERSION 3.22)

# Consolidated CMake for ONNX runtime plugin.
project(yams_onnx_plugin LANGUAGES CXX)

option(YAMS_BUILD_ONNX_PLUGIN "Build the YAMS ONNX plugin" ON)
set(YAMS_PLUGIN_DIR "lib/yams/plugins" CACHE STRING "Install directory for YAMS plugins (relative to CMAKE_INSTALL_PREFIX)")

# Avoid duplicate definition if this file is processed twice (some multi-config generators or re-configures)
if(TARGET yams_onnx_plugin)
  message(STATUS "yams_onnx_plugin target already exists — skipping redefinition")
  return()
endif()

if(NOT YAMS_BUILD_ONNX_PLUGIN)
  message(STATUS "YAMS_BUILD_ONNX_PLUGIN=OFF — skipping yams_onnx_plugin")
  return()
endif()

find_package(onnxruntime QUIET)
if(NOT TARGET onnxruntime::onnxruntime)
  message(STATUS "ONNX Runtime not found — skipping yams_onnx_plugin")
  return()
endif()

find_package(spdlog QUIET)

add_library(yams_onnx_plugin MODULE
  onnx_plugin.cpp
  model_provider.cpp
  onnx_model_pool.cpp
)

# nlohmann_json is already a Conan dependency for the main project, but this plugin
# may be built in a context where the transitive usage requirements from yams::core
# do not propagate private includes. We explicitly link the header-only target if present.
find_package(nlohmann_json QUIET)

target_link_libraries(yams_onnx_plugin
  PRIVATE
    yams::core
    yams::vector
    $<TARGET_NAME_IF_EXISTS:onnxruntime::onnxruntime>
    $<TARGET_NAME_IF_EXISTS:spdlog::spdlog>
    $<TARGET_NAME_IF_EXISTS:nlohmann_json::nlohmann_json>
)

# Some Conan packages expose include dirs only on their imported targets; for safety,
# propagate their interface include directories if they exist.
foreach(dep IN ITEMS onnxruntime::onnxruntime spdlog::spdlog nlohmann_json::nlohmann_json)
  if(TARGET ${dep})
    get_target_property(_inc ${dep} INTERFACE_INCLUDE_DIRECTORIES)
    if(_inc)
      target_include_directories(yams_onnx_plugin PRIVATE ${_inc})
    endif()
  endif()
endforeach()

target_compile_features(yams_onnx_plugin PUBLIC cxx_std_20)
set_target_properties(yams_onnx_plugin PROPERTIES
  CXX_VISIBILITY_PRESET hidden
  VISIBILITY_INCLUDES_HIDDEN ON
)

# macOS: set robust rpaths so the plugin can locate its deps when loaded by the daemon
if(APPLE)
  # Always allow relative resolution from the module location and via @rpath
  set_target_properties(yams_onnx_plugin PROPERTIES
    BUILD_RPATH "@loader_path;@rpath"
    INSTALL_RPATH "@loader_path;@rpath"
  )
  # If onnxruntime is a shared library with a known location, add its dir to rpath
  if(TARGET onnxruntime::onnxruntime)
    get_target_property(_ort_type onnxruntime::onnxruntime TYPE)
    if(_ort_type STREQUAL "SHARED_LIBRARY" OR _ort_type STREQUAL "MODULE_LIBRARY")
      get_target_property(_ort_loc onnxruntime::onnxruntime LOCATION)
      if(_ort_loc)
        get_filename_component(_ort_dir "${_ort_loc}" DIRECTORY)
        set_property(TARGET yams_onnx_plugin APPEND PROPERTY BUILD_RPATH "${_ort_dir}")
        set_property(TARGET yams_onnx_plugin APPEND PROPERTY INSTALL_RPATH "${_ort_dir}")
        # Copy onnxruntime dylibs next to the plugin to avoid DYLD issues (best-effort)
        add_custom_command(TARGET yams_onnx_plugin POST_BUILD
          COMMAND ${CMAKE_COMMAND} -E make_directory "$<TARGET_FILE_DIR:yams_onnx_plugin>/onnxruntime"
          COMMAND ${CMAKE_COMMAND} -E copy_directory "${_ort_dir}" "$<TARGET_FILE_DIR:yams_onnx_plugin>/onnxruntime"
          COMMENT "Bundling onnxruntime dylibs with ONNX plugin"
        )
        # Add the bundled subdir to rpath
        set_property(TARGET yams_onnx_plugin APPEND PROPERTY BUILD_RPATH "$<TARGET_FILE_DIR:yams_onnx_plugin>/onnxruntime")
        set_property(TARGET yams_onnx_plugin APPEND PROPERTY INSTALL_RPATH "@loader_path/onnxruntime")
        # Install the bundled dylibs alongside the plugin
        install(DIRECTORY "${_ort_dir}/" DESTINATION ${CMAKE_INSTALL_LIBDIR}/yams/plugins/onnxruntime FILES_MATCHING PATTERN "*.dylib")
      endif()
    endif()
  endif()

  # Bundle PDFium if present on the system to avoid DYLD lookup issues
  find_library(PDFIUM_LIB NAMES pdfium libpdfium.dylib)
  if(PDFIUM_LIB)
    get_filename_component(PDFIUM_DIR "${PDFIUM_LIB}" DIRECTORY)
    get_filename_component(PDFIUM_NAME "${PDFIUM_LIB}" NAME)
    # Ensure plugin rpath includes the bundled pdfium subdir
    set_property(TARGET yams_onnx_plugin APPEND PROPERTY BUILD_RPATH "@loader_path/pdfium")
    set_property(TARGET yams_onnx_plugin APPEND PROPERTY INSTALL_RPATH "@loader_path/pdfium")
    # Copy libpdfium.dylib into a local subdir next to the plugin
    add_custom_command(TARGET yams_onnx_plugin POST_BUILD
      COMMAND ${CMAKE_COMMAND} -E make_directory "$<TARGET_FILE_DIR:yams_onnx_plugin>/pdfium"
      COMMAND ${CMAKE_COMMAND} -E copy "${PDFIUM_LIB}" "$<TARGET_FILE_DIR:yams_onnx_plugin>/pdfium/${PDFIUM_NAME}"
      COMMAND /usr/bin/install_name_tool -id @rpath/libpdfium.dylib "$<TARGET_FILE_DIR:yams_onnx_plugin>/pdfium/${PDFIUM_NAME}"
      COMMENT "Bundling PDFium dylib with ONNX plugin"
    )
    # Install the bundled PDFium alongside the plugin
    install(FILES "${PDFIUM_LIB}" DESTINATION ${CMAKE_INSTALL_LIBDIR}/yams/plugins/pdfium)
  endif()
endif()

# Backward compatibility alias (legacy name used previously in this directory)
add_library(yams_onnx ALIAS yams_onnx_plugin)

install(TARGETS yams_onnx_plugin
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/yams/plugins
)
