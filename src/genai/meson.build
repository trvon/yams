# GenAI helpers are header/inline-only for Meson purposes; vector library
# pulls in onnx_genai_adapter.cpp directly.

yams_genai_dep = declare_dependency(
  include_directories: include_directories('../../include'),
)

meson.override_dependency('yams_genai', yams_genai_dep)
