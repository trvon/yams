Hound Graph Adapter (Skeleton)
==============================

This directory will host a GraphAdapter provider that bridges Hound projects
to YAMS graphs. Initial skeleton only; implementation will map Hound
`graphs/graph_*.json` to GraphJSON v1.

Native C-ABI entry points (see include/yams/plugins/graph_adapter_v1.h):
```
extern "C" const char* getGraphAdapterName();
extern "C" yams::daemon::IGraphAdapter* createGraphAdapterV1();
```

WASM alternative: implement the WIT world `graph-adapter-v1` using docs/spec/wit/graph_adapter_v1.wit
and export `graph_adapter_v1_*` functions that the host will call via WasmRuntime JSON bridge.

