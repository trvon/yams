#pragma once

// DLL export/import macros for yams_onnx_resource shared library
// On Windows, symbols must be explicitly exported from DLLs and imported by consumers.
// On other platforms, symbols are visible by default.

#if defined(_WIN32) || defined(_WIN64)
#ifdef YAMS_ONNX_RESOURCE_BUILDING
#define YAMS_ONNX_RESOURCE_API __declspec(dllexport)
#else
#define YAMS_ONNX_RESOURCE_API __declspec(dllimport)
#endif
#else
#define YAMS_ONNX_RESOURCE_API
#endif
