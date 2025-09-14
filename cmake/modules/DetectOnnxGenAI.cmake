# DetectOnnxGenAI.cmake
#
# Performs a lightweight feature probe to see if ONNX Runtime GenAI C++ headers
# are available. Defines:
#   YAMS_GENAI_RUNTIME_PRESENT (CACHE BOOL) - TRUE if headers usable
#
# Strategy:
#   1. Collect ONNX runtime include dirs from target onnxruntime::onnxruntime
#   2. If no target, abort (leave FALSE)
#   3. try_compile a tiny translation unit including a representative GenAI header.
#      (Header path may evolve; we test several candidate includes.)
#
# This module is safe to include multiple times; it will short-circuit if the
# cache variable is already defined.

if(DEFINED YAMS_GENAI_RUNTIME_PRESENT)
    return()
endif()

set(YAMS_GENAI_RUNTIME_PRESENT FALSE CACHE BOOL "ONNX Runtime GenAI headers available")

if(NOT TARGET onnxruntime::onnxruntime)
    message(STATUS "[GenAI] onnxruntime target not present; skipping GenAI header probe")
    return()
endif()

set(_genai_search_includes)
get_target_property(_ort_includes onnxruntime::onnxruntime INTERFACE_INCLUDE_DIRECTORIES)
if(_ort_includes)
    list(APPEND _genai_search_includes ${_ort_includes})
endif()
if(TARGET onnxruntime::genai_headers)
    get_target_property(_genai_inc onnxruntime::genai_headers INTERFACE_INCLUDE_DIRECTORIES)
    if(_genai_inc)
        list(APPEND _genai_search_includes ${_genai_inc})
    endif()
endif()
list(REMOVE_DUPLICATES _genai_search_includes)
if(NOT _genai_search_includes)
    message(STATUS "[GenAI] No include dirs available for GenAI probe")
    return()
endif()

# Candidate header names (adjust as ONNX Runtime GenAI API stabilizes)
set(_GENAI_HEADER_CANDIDATES
    "ort_genai.h"                      # layout in standalone onnxruntime-genai repo (src/)
    "onnxruntime/genai/ort_genai.h"    # potential packaged future layout
    "onnxruntime/genai/embedding.h"
    "onnxruntime/genai/genai_api.h"
    "onnxruntime/genai/pipeline.h"
)

set(_found_candidate_header "")
foreach(_inc IN LISTS _genai_search_includes)
    foreach(_hdr IN LISTS _GENAI_HEADER_CANDIDATES)
        if(EXISTS "${_inc}/${_hdr}")
            set(_found_candidate_header "${_hdr}")
            break()
        endif()
    endforeach()
    if(_found_candidate_header)
        break()
    endif()
endforeach()

if(NOT _found_candidate_header)
    message(STATUS "[GenAI] No candidate ONNX GenAI headers found in exported include dirs")
    return()
endif()

message(STATUS "[GenAI] Candidate GenAI header found: ${_found_candidate_header}; probing usability")

file(WRITE ${CMAKE_BINARY_DIR}/genai_probe.cpp "#include <${_found_candidate_header}>\nint main(){return 0;}")

# Use check_cxx_source_compiles which respects CMAKE_REQUIRED_INCLUDES
set(_saved_required_includes ${CMAKE_REQUIRED_INCLUDES})
set(CMAKE_REQUIRED_INCLUDES ${_genai_search_includes})
include(CheckCXXSourceCompiles)
check_cxx_source_compiles("#include <${_found_candidate_header}>\nint main(){return 0;}" _genai_tc)
set(CMAKE_REQUIRED_INCLUDES ${_saved_required_includes})

if(_genai_tc)
    set(YAMS_GENAI_RUNTIME_PRESENT TRUE CACHE BOOL "ONNX Runtime GenAI headers available" FORCE)
    message(STATUS "[GenAI] ONNX GenAI headers usable (probe succeeded)")
else()
    message(STATUS "[GenAI] Probe compile failed; treating GenAI headers as unavailable")
endif()
