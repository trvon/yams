# OnnxRuntimeProvider.cmake
# Unified acquisition of onnxruntime::onnxruntime target.
# Behavior:
# 1. If onnxruntime::onnxruntime already exists, do nothing.
# 2. Else try find_package(onnxruntime CONFIG QUIET)
# 3. If still missing and YAMS_BUILD_INTERNAL_ONNXRUNTIME=ON, FetchContent a chosen ORT release
#    (new enough to include GenAI headers) and create imported target onnxruntime::onnxruntime.
# 4. Expose variable YAMS_ORt_PROVIDER (external|internal) for logging/diagnostics.
#
# NOTE: Full source build of ONNX Runtime is heavy; for now we only scaffold logic and
#       emit a message guiding manual upgrade if OFF. Actual FetchContent can be added later.
#
# Inputs:
#   YAMS_BUILD_INTERNAL_ONNXRUNTIME (option)
# Outputs:
#   onnxruntime::onnxruntime target (if found/built)
#   YAMS_ORT_PROVIDER (string)

if(TARGET onnxruntime::onnxruntime)
    set(YAMS_ORT_PROVIDER "existing")
    return()
endif()

find_package(onnxruntime QUIET)
if(TARGET onnxruntime::onnxruntime)
    set(YAMS_ORT_PROVIDER "external")
    message(STATUS "[ORT] Found external ONNX Runtime package")
    return()
endif()

if(NOT YAMS_BUILD_INTERNAL_ONNXRUNTIME)
    set(YAMS_ORT_PROVIDER "missing")
    message(STATUS "[ORT] External ONNX Runtime not found and internal build disabled (YAMS_BUILD_INTERNAL_ONNXRUNTIME=OFF)")
    return()
endif()

# Placeholder for future internal build. For now, just emit guidance.
set(YAMS_ORT_PROVIDER "internal-disabled")
message(WARNING "[ORT] Internal build requested but provider implementation not yet implemented. Provide newer ORT manually or extend OnnxRuntimeProvider.cmake.")
