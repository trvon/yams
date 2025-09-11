# YAMS Ghidra External Plugin (Experimental)

This example provides an External (Python) plugin for YAMS that uses PyGhidra to analyze binaries.
It speaks newline‑delimited JSON‑RPC over stdio using the SDK in `external/yams-sdk`.

Prerequisites
- Ghidra 12.0+ installed
- Python 3.8+
- PyGhidra: `pip install pyghidra` (or from your Ghidra install as per PyGhidra README)
- Set `GHIDRA_INSTALL_DIR` or provide `ghidra_install` in the plugin init config

Location and install
- This plugin now lives under `plugins/yams-ghidra-plugin`.
- Install is opt-in: build with `-DYAMS_INSTALL_GHIDRA_PLUGIN=ON` to install into `${CMAKE_INSTALL_LIBDIR}/yams/plugins/yams-ghidra-plugin`.

Quick run (standalone)
```
# From repo root
export PYTHONPATH="$PWD/external/yams-sdk/python:$PYTHONPATH"
python examples/plugins/yams-ghidra-plugin/plugin.py

# In another shell: send handshake + init + a test call
printf '%s
' '{"id":1,"method":"handshake.manifest"}' | python examples/plugins/yams-ghidra-plugin/plugin.py
```

JSON‑RPC methods
- handshake.manifest → returns {name, version, interfaces: ["ghidra_analysis_v1"]}
- plugin.init → input {ghidra_install?: "/path/to/ghidra", project_dir?: "/tmp/ghidra", project_name?: "YamsGhidra"}
- plugin.health → {status:"ok"}
- ghidra.analyze → input {source:{type:"path", path:"/path/to/bin"}}, output {arch, functions:[{name, addr}], count}
- ghidra.decompile_function → stub (to be implemented)

Notes
- This example uses PyGhidra’s high‑level helpers and may be refined to use ProgramLoader and DecompInterface directly.
- For end‑to‑end with YAMS, integrate with the future ExternalPluginHost.
