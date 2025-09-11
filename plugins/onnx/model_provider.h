// Minimal ONNX model provider adapter exposing yams_model_provider_v1

#pragma once

#include <cstddef>
#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_set>
#include <vector>

extern "C" {
#include "yams/plugins/model_provider_v1.h"
}

struct OnnxProviderContext {
    std::mutex mu;
    yams_model_load_progress_cb progress_cb = nullptr;
    void* progress_user = nullptr;

    // For a future implementation, track loaded models.
    std::unordered_set<std::string> loaded_models;
};

// Returns a singleton vtable for the ONNX provider and ensures its context is initialized.
// The returned pointer is owned by the plugin and remains valid for the plugin lifetime.
extern "C" yams_model_provider_v1* yams_onnx_get_model_provider();
