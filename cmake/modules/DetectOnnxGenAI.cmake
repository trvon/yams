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

get_target_property(_ort_includes onnxruntime::onnxruntime INTERFACE_INCLUDE_DIRECTORIES)
if(NOT _ort_includes)
    message(STATUS "[GenAI] No include dirs exported by onnxruntime target; skipping probe")
    return()
endif()

# Candidate header names (adjust as ONNX Runtime GenAI API stabilizes)
set(_GENAI_HEADER_CANDIDATES
    "onnxruntime/genai/embedding.h"
    "onnxruntime/genai/genai_api.h"
    "onnxruntime/genai/pipeline.h"
)

set(_found_candidate_header "")
foreach(_inc IN LISTS _ort_includes)
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

try_compile(_genai_tc
    ${CMAKE_BINARY_DIR}/genai_probe_build
    ${CMAKE_BINARY_DIR}/genai_probe.cpp
    CMAKE_FLAGS
      -DCMAKE_CXX_STANDARD=20
      -DCMAKE_CXX_STANDARD_REQUIRED=ON
      -DCMAKE_CXX_EXTENSIONS=OFF
    INCLUDE_DIRECTORIES ${_ort_includes}
)

if(_genai_tc)
    set(YAMS_GENAI_RUNTIME_PRESENT TRUE CACHE BOOL "ONNX Runtime GenAI headers available" FORCE)
    message(STATUS "[GenAI] ONNX GenAI headers usable (probe succeeded)")
else()
    message(STATUS "[GenAI] Probe compile failed; treating GenAI headers as unavailable")
endif()
