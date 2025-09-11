# YAMS External Plugins (Examples)

This directory contains example scaffolding you can copy into an out‑of‑tree repository (e.g., `git@git.sr.ht:~trvon/yams-plugins`).

- See `docs/guide/external_dr_plugins.md` for usage and load instructions.
- Keep this out of the default build; these are examples only.

Tree:

```
s3/
  c_abi/
    CMakeLists.txt
    src/s3_stub_plugin.c
    plugin_manifest.json
LICENSE-GPL-3.0.txt   # Placeholder; use full GPLv3 text in your plugin repo
```
