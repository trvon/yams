#pragma once

// Optional C-ABI surface for native GraphAdapter providers.
// Plugins implementing GraphAdapter v1 may export the following symbols so the
// daemon can discover and register them via AbiPluginLoader:
//
//   extern "C" const char* getGraphAdapterName();
//   extern "C" yams::daemon::IGraphAdapter* createGraphAdapterV1();
//
// Versioned variants are preferred; the loader also probes a generic
// createGraphAdapter() as a fallback.

#include <yams/daemon/resource/graph_adapter.h>

extern "C" {

// Required: return a stable adapter name (e.g., "graphjson", "hound-graph")
const char* getGraphAdapterName();

// Preferred: create adapter implementing GraphAdapter v1
yams::daemon::IGraphAdapter* createGraphAdapterV1();

// Optional legacy: non-versioned factory
yams::daemon::IGraphAdapter* createGraphAdapter();

// Optional: return adapter version string (semantic version)
const char* getGraphAdapterVersion();
}
