#[[
OnnxRuntimeProvider.cmake (simplified)
-------------------------------------
Provider now only reuses an existing ONNX Runtime target (from Conan/package
manager or parent project). Internal source build logic was removed.

Resulting behavior:
 - If target onnxruntime::onnxruntime already exists -> reuse.
 - Else attempt find_package(ONNXRuntime) quietly.
 - If still absent we record provider strategy "none" and higher level
   code (guarded by YAMS_ENABLE_ONNX) will compile stubs.
]]#

if(TARGET onnxruntime::onnxruntime)
  message(STATUS "[OnnxRuntimeProvider] Reusing existing onnxruntime::onnxruntime target")
  set(YAMS_ORT_PROVIDER "existing" CACHE INTERNAL "ORT provider strategy")
  # Do not return yet; we may still need to provide GenAI extension headers transparently.
endif()

if(NOT TARGET onnxruntime::onnxruntime)
  find_package(ONNXRuntime QUIET)
  if(TARGET onnxruntime::onnxruntime)
    set(YAMS_ORT_PROVIDER "package-find" CACHE INTERNAL "ORT provider strategy")
    message(STATUS "[OnnxRuntimeProvider] Found ONNX Runtime via find_package")
  else()
    message(STATUS "[OnnxRuntimeProvider] No ONNX Runtime target found (no internal build fallback enabled)")
    set(YAMS_ORT_PROVIDER "none" CACHE INTERNAL "ORT provider strategy")
  endif()
endif()

# ---------------------------------------------------------------------------
# Lightweight automatic fetch of onnxruntime-genai headers (no new options)
# Triggered only when ONNX is enabled at higher level and runtime target exists
# but required GenAI headers are missing.
# ---------------------------------------------------------------------------
if(DEFINED YAMS_ENABLE_ONNX AND YAMS_ENABLE_ONNX AND TARGET onnxruntime::onnxruntime)
  # Gather exported include dirs and scan for a known GenAI header marker.
  get_target_property(_ort_inc onnxruntime::onnxruntime INTERFACE_INCLUDE_DIRECTORIES)
  # The standalone onnxruntime-genai repo (v0.9.x) places public headers in its 'src' directory
  # (e.g. ort_genai.h) rather than an 'include/' prefix. We therefore look for that file first.
  set(_genai_marker_headers
      "ort_genai.h"                 # layout used by onnxruntime-genai repo
      "onnxruntime/genai/embedding.h" # hypothetical packaged layout (future?)
  )
  set(_have_genai FALSE)
  foreach(_d IN LISTS _ort_inc)
    foreach(_marker IN LISTS _genai_marker_headers)
      if(EXISTS "${_d}/${_marker}")
        set(_have_genai TRUE)
        break()
      endif()
    endforeach()
    if(_have_genai)
      break()
    endif()
  endforeach()

  if(NOT _have_genai AND NOT TARGET onnxruntime::genai_headers)
    set(_GENAI_VERSION "0.9.1")
    set(_GENAI_URL "https://github.com/microsoft/onnxruntime-genai/archive/refs/tags/v${_GENAI_VERSION}.tar.gz")
    set(_GENAI_ARCHIVE "${CMAKE_BINARY_DIR}/_deps/onnxruntime-genai-${_GENAI_VERSION}.tar.gz")
    set(_GENAI_SRC_DIR "${CMAKE_BINARY_DIR}/_deps/onnxruntime-genai-${_GENAI_VERSION}")
    file(MAKE_DIRECTORY "${CMAKE_BINARY_DIR}/_deps")
    if(NOT EXISTS "${_GENAI_ARCHIVE}")
      message(STATUS "[OnnxRuntimeProvider] Fetching onnxruntime-genai v${_GENAI_VERSION} (headers only)")
      file(DOWNLOAD "${_GENAI_URL}" "${_GENAI_ARCHIVE}" SHOW_PROGRESS STATUS _dl_status)
      list(GET _dl_status 0 _dl_code)
      if(NOT _dl_code EQUAL 0)
        list(GET _dl_status 1 _dl_msg)
        message(WARNING "[OnnxRuntimeProvider] Failed to download onnxruntime-genai archive: ${_dl_msg}")
      endif()
    endif()
    if(EXISTS "${_GENAI_ARCHIVE}" AND NOT EXISTS "${_GENAI_SRC_DIR}")
      execute_process(
        COMMAND ${CMAKE_COMMAND} -E tar xzf "${_GENAI_ARCHIVE}"
        WORKING_DIRECTORY "${CMAKE_BINARY_DIR}/_deps"
        RESULT_VARIABLE _genai_extract_res
      )
      if(NOT _genai_extract_res EQUAL 0)
        message(WARNING "[OnnxRuntimeProvider] Extraction of onnxruntime-genai archive failed (code ${_genai_extract_res})")
      endif()
    endif()
    # Provide an interface target pointing at the repo's src directory (acts as public include dir)
    if(EXISTS "${_GENAI_SRC_DIR}/src/ort_genai.h")
      message(STATUS "[OnnxRuntimeProvider] Preparing interface target for onnxruntime-genai headers (src layout)")
      add_library(onnxruntime_genai_headers INTERFACE)
      target_include_directories(onnxruntime_genai_headers INTERFACE "${_GENAI_SRC_DIR}/src")
      add_library(onnxruntime::genai_headers ALIAS onnxruntime_genai_headers)
      set(YAMS_GENAI_HEADER_DIR "${_GENAI_SRC_DIR}/src" CACHE INTERNAL "GenAI header include dir")
      message(STATUS "[OnnxRuntimeProvider] onnxruntime-genai headers provided from src/ (v${_GENAI_VERSION}) -> ${YAMS_GENAI_HEADER_DIR}")
    elseif(EXISTS "${_GENAI_SRC_DIR}/include")
      add_library(onnxruntime_genai_headers INTERFACE)
      target_include_directories(onnxruntime_genai_headers INTERFACE "${_GENAI_SRC_DIR}/include")
      add_library(onnxruntime::genai_headers ALIAS onnxruntime_genai_headers)
      message(STATUS "[OnnxRuntimeProvider] onnxruntime-genai headers provided (v${_GENAI_VERSION})")
    else()
      message(STATUS "[OnnxRuntimeProvider] onnxruntime-genai headers still unavailable after fetch attempt (no src/ort_genai.h)")
    endif()
  elseif(_have_genai)
    message(STATUS "[OnnxRuntimeProvider] onnxruntime distribution already includes GenAI headers")
  endif()
      if(TARGET onnxruntime::genai_headers)
        get_target_property(_yams_genai_inc onnxruntime::genai_headers INTERFACE_INCLUDE_DIRECTORIES)
        message(STATUS "[OnnxRuntimeProvider] GenAI interface include dirs: ${_yams_genai_inc}")
      endif()
endif()

