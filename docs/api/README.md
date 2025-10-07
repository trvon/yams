# YAMS API Documentation

A concise index for YAMS HTTP API docs. Start here, open the spec, and generate a client if you need one.

## What’s here

- OpenAPI spec (authoritative):
  - [openapi.yaml](./openapi.yaml)
- Topical guides:
  - [Search API](./search_api.md)
  - [Vector Search API](./vector_search_api.md)
  - [Tree Diff API](./tree_diff_api.md) (PBI-043)

If you’re exploring or automating against YAMS via HTTP, read the guides and treat `openapi.yaml` as the source of truth.

## TL;DR

- View the API quickly:
  - Drag-and-drop `openapi.yaml` into https://editor.swagger.io/
  - Or use Redocly locally (if installed): `redocly preview-docs docs/api/openapi.yaml`
- Generate a client (examples):
  - With openapi-generator (Homebrew):  
    `brew install openapi-generator`
  - TypeScript (fetch):  
    `openapi-generator generate -i docs/api/openapi.yaml -g typescript-fetch -o ./api-clients/ts`
  - Python (requests):  
    `openapi-generator generate -i docs/api/openapi.yaml -g python -o ./api-clients/python`
  - C++ (cpp-httplib, experimental):  
    `openapi-generator generate -i docs/api/openapi.yaml -g cpp-httplib -o ./api-clients/cpp`
- Base URL:
  - During development, the HTTP server (if enabled) typically runs on localhost. Check your server configuration for the exact host/port.
- Auth:
  - See the “security”/“components.securitySchemes” section in `openapi.yaml` for the current scheme(s).

## Quick usage patterns

- cURL shape (consult endpoint/params in the spec or topical guides):
  - GET with query params:  
    `curl -sS "<BASE_URL>/api/v1/<resource>?q=..." | jq`
  - POST with JSON body:  
    `curl -sS -X POST "<BASE_URL>/api/v1/<resource>" -H "Content-Type: application/json" -d @payload.json | jq`
  - Auth header (if required):  
    `-H "Authorization: Bearer <TOKEN>"` or `-H "X-API-Key: <KEY>"` (see spec)

For concrete examples, see:
- [Search API](./search_api.md)
- [Vector Search API](./vector_search_api.md)
- [Tree Diff API](./tree_diff_api.md)

## Development notes

- The HTTP API surface may evolve with releases; `openapi.yaml` in your branch/tag matches that build.
- If you don’t see a running HTTP server, use:
  - CLI: `docs/user_guide/cli.md`
  - MCP server (for AI tools/workflows): `docs/user_guide/mcp.md`

## See also

- CLI Reference: `../user_guide/cli.md`
- Installation: `../user_guide/installation.md`
- Troubleshooting search: `../troubleshooting/search_issues.md`

---
This page is an index stub intended to be brief and link-rich. For authoritative details, rely on the OpenAPI spec and topical guides.