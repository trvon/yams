# YAMS Ecosystem: Plugins and Projects

This page tracks plugins and projects that build on YAMS. See `registry.yaml` for a machine‑readable list.

- Maintained plugins: developed in or by the YAMS project
- Community plugins: third‑party, built against YAMS interfaces
- Projects built on YAMS: end‑user apps or services using YAMS

## Maintained Plugins
- onnx-model-provider (transport: ABI) — `model_provider_v1`
  - Repo: this repo (`plugins/onnx`)
  - Status: alpha

- yams-ghidra-plugin (transport: External) — `object_storage_s3`
  - Repo: this repo (`plugins/object_storage_s3`)
  - Status: alpha

- yams-ghidra-plugin (transport: External) — `ghidra_analysis_v1`
  - Repo: this repo (`plugins/yams-ghidra-plugin`)
  - Status: alpha

## Community Plugins

## Projects Built on YAMS

## Contributing
- Add your plugin/project by opening a PR that updates both:
  - `docs/ecosystem/registry.yaml` (authoritative, machine‑readable)
  - `docs/ecosystem/README.md` (human‑friendly summary)
- Please include: name, repo URL, transport (abi/wasm/external), interfaces provided, license, and status.
