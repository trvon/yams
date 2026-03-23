// Compatibility alias to the canonical GenAI adapter.
// Prefer including <yams/genai/onnx_genai_adapter.h> in new code.
#pragma once
#include <yams/genai/onnx_genai_adapter.h>
namespace yams::daemon {
using OnnxGenAIAdapter = yams::genai::OnnxGenAIAdapter;
}
