# OpenGrep audit scan triage - 2026-05-02

- Source JSON: `.artifacts/opengrep/opengrep-yams-audit.json`
- Total JSON findings after tuning/fixes: 435
- Runner: `OPENGREP_BIN=/Users/trevon/.local/bin/opengrep scripts/lint.sh opengrep --all-files`

## By rule

- 295: `tools.opengrep.rules.audit.yams.cpp.result-value-without-guard`
- 140: `tools.opengrep.rules.audit.yams.cpp.raw-yams-getenv`

## Top files

- 103: `src/metadata/knowledge_graph_store_sqlite.cpp`
- 24: `src/daemon/components/ConfigResolver.cpp`
- 15: `src/api/metadata_api.cpp`
- 15: `src/daemon/client/daemon_client.cpp`
- 15: `src/search/symbol_enrichment.cpp`
- 14: `src/cli/yams_cli.cpp`
- 13: `src/vector/embedding_service.cpp`
- 12: `src/metadata/connection_pool.cpp`
- 10: `src/app/services/session_service.cpp`
- 10: `src/repair/embedding_repair_util.cpp`
- 9: `src/daemon/components/DatabaseManager.cpp`
- 9: `src/daemon/components/VectorSystemManager.cpp`
- 8: `src/cli/commands/update_command.cpp`
- 8: `src/search/search_lexical_pipeline.cpp`
- 7: `src/metadata/repository/search_ops.cpp`
- 6: `include/yams/common/name_resolver.h`
- 6: `src/cli/commands/session_command.cpp`
- 6: `src/search/simeon_lexical_backend.cpp`
- 5: `include/yams/mcp/tool_registry.h`
- 5: `include/yams/metadata/metadata_repository.h`
- 5: `src/cli/tune_runner.cpp`
- 5: `src/daemon/components/PluginManager.cpp`
- 5: `src/plugins/plugin_installer.cpp`
- 5: `src/search/kg_scorer_simple.cpp`
- 4: `src/app/services/retrieval_service.cpp`

## Completed this pass

- Fixed lint wrapper UX: non-strict OpenGrep runs now warn when JSON findings exist instead of reporting clean.
- Tuned byte-cast rule to ignore char/byte view casts and focus on typed container reinterpretation.
- Eliminated `vector-data-reinterpret-struct` findings by rewriting WAL encode paths to construct POD payloads and append byte spans instead of writing through reinterpret-cast pointers.
- Centralized WAL POD decoding through `readObject<T>()` with explicit trivially-copyable static assertions.
- Refactored message framing and TurboQuant memcpy patterns to avoid low-signal destination forms; documented remaining intentional POD decode sites.
- Documented numeric PRAGMA dynamic-SQL findings as non-injection paths using nosemgrep rationales.

## Remaining backlog

- `result-value-without-guard` remains noisy (295). Most samples are guarded by prior `if (!r) return` or inline `if (r && r.value())`; the rule needs deeper guard recognition or a narrower target set.
- `raw-yams-getenv` remains policy backlog (140). Next pass should classify env reads into typed config migration, central compatibility wrappers, and test/debug-only overlays.
