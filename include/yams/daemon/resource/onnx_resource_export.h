#pragma once

// DLL export/import macros for yams_onnx_resource shared library
// On Windows, symbols must be explicitly exported from DLLs and imported by consumers.
// Elsewhere the library builds with -fvisibility=hidden and exports only this API.

#if defined(_WIN32) || defined(_WIN64)
#ifdef YAMS_ONNX_RESOURCE_BUILDING
#define YAMS_ONNX_RESOURCE_API __declspec(dllexport)
#else
#define YAMS_ONNX_RESOURCE_API __declspec(dllimport)
#endif
#elif defined(__GNUC__) || defined(__clang__)
// The library builds with hidden visibility; only annotated API is exported.
#define YAMS_ONNX_RESOURCE_API __attribute__((visibility("default")))
#else
#define YAMS_ONNX_RESOURCE_API
#endif
