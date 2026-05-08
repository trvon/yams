# OpenGrep empty-catch triage checkpoint

After fixing high-signal TLS/shell/remove_all findings and adding targeted comments/logging, the audit profile reports only `yams.cpp.empty-catch-block` findings.

Total remaining empty-catch findings: 209

## config/env parse fallback: 64

- `src/daemon/components/ConfigResolver.cpp` (16): [337, 772, 778, 784, 820, 826, 849, 1036, 1042, 1048, 1054, 1064]...
- `src/api/content_store_builder.cpp` (8): [172, 182, 190, 198, 207, 217, 225, 234]
- `src/daemon/components/VectorSystemManager.cpp` (8): [150, 163, 173, 282, 290, 300, 324, 356]
- `src/cli/yams_cli.cpp` (5): [502, 1123, 1129, 1140, 1147]
- `src/daemon/components/DatabaseManager.cpp` (5): [41, 226, 234, 293, 302]
- `include/yams/daemon/resource/gpu_info.h` (4): [169, 176, 273, 433]
- `src/daemon/components/DaemonMetrics.cpp` (3): [831, 837, 1834]
- `src/daemon/components/dispatcher/request_dispatcher_models.cpp` (2): [83, 90]
- `src/vector/embedding_service.cpp` (2): [772, 948]
- `src/cli/commands/repair_command.cpp` (1): [275]
- `src/cli/commands/serve_command.cpp` (1): [90]
- `src/cli/commands/session_command.cpp` (1): [587]

## metrics best-effort collection: 53

- `src/daemon/components/DaemonMetrics.cpp` (48): [562, 603, 911, 1003, 1034, 1085, 1090, 1109, 1214, 1314, 1326, 1339]...
- `src/daemon/components/SearchComponent.cpp` (2): [185, 207]
- `src/daemon/components/SocketServer.cpp` (2): [234, 498]
- `include/yams/daemon/components/admission_control.h` (1): [51]

## needs manual review: 33

- `include/yams/daemon/resource/gpu_info.h` (4): [42, 138, 242, 308]
- `src/daemon/components/dispatcher/request_dispatcher_embeddings.cpp` (4): [184, 210, 216, 249]
- `src/daemon/components/EmbeddingLifecycleManager.cpp` (3): [66, 75, 149]
- `src/daemon/components/PluginManager.cpp` (3): [689, 1184, 1189]
- `src/vector/embedding_service.cpp` (3): [117, 146, 605]
- `src/app/services/retrieval_service.cpp` (2): [774, 797]
- `src/daemon/components/TuningManager.cpp` (2): [171, 176]
- `src/daemon/components/dispatcher/request_dispatcher_models.cpp` (2): [109, 116]
- `src/repair/embedding_repair_util.cpp` (2): [94, 123]
- `include/yams/cli/cli_sync.h` (1): [57]
- `src/cli/commands/repair_command.cpp` (1): [135]
- `src/cli/commands/update_command.cpp` (1): [634]

## logger guard not matched by rule exemption: 18

- `src/daemon/components/VectorSystemManager.cpp` (9): [64, 93, 106, 122, 133, 243, 428, 434, 446]
- `src/daemon/components/PluginManager.cpp` (2): [844, 886]
- `src/daemon/components/SocketServer.cpp` (2): [437, 786]
- `src/daemon/components/RequestExecutor.cpp` (1): [66]
- `src/daemon/components/WorkCoordinator.cpp` (1): [356]
- `src/daemon/ipc/streaming_processor.cpp` (1): [292]
- `src/repair/embedding_repair_util.cpp` (1): [225]
- `src/vector/embedding_service.cpp` (1): [754]

## cleanup/noexcept best-effort: 16

- `src/cli/yams_cli.cpp` (3): [273, 283, 291]
- `src/daemon/components/ServiceManagerFsm.cpp` (3): [224, 311, 319]
- `src/daemon/components/WorkCoordinator.cpp` (3): [51, 374, 399]
- `src/daemon/components/DatabaseManager.cpp` (2): [128, 146]
- `src/daemon/components/SocketServer.cpp` (2): [110, 529]
- `src/daemon/components/PluginManager.cpp` (1): [228]
- `src/daemon/components/RequestExecutor.cpp` (1): [30]
- `src/vector/embedding_generator.cpp` (1): [253]

## FSM no-throw boundary: 13

- `src/daemon/components/ServiceManagerFsm.cpp` (9): [144, 154, 164, 174, 184, 194, 204, 214, 234]
- `src/daemon/components/SearchEngineManager.cpp` (3): [320, 364, 375]
- `src/daemon/components/VectorSystemManager.cpp` (1): [457]

## best-effort parse fallback: 12

- `src/cli/yams_cli.cpp` (3): [782, 1162, 1233]
- `include/yams/cli/plugin_helpers.h` (2): [62, 85]
- `src/cli/commands/session_command.cpp` (1): [606]
- `src/cli/vector_db_util.cpp` (1): [86]
- `src/daemon/components/ConfigResolver.cpp` (1): [467]
- `src/daemon/components/VectorSystemManager.cpp` (1): [214]
- `src/daemon/components/dispatcher/request_dispatcher_models.cpp` (1): [106]
- `src/daemon/components/dispatcher/request_dispatcher_prune.cpp` (1): [65]
- `src/daemon/resource/abi_model_provider_adapter.cpp` (1): [321]

## Recommended next pass

- Treat `config/env parse fallback` as candidates for typed parse helpers that log invalid config at debug level once per field.
- Treat `FSM no-throw boundary` and `cleanup/noexcept` as acceptable only with explicit comments or a helper that centralizes logging/fallback behavior.
- Keep `metrics best-effort collection` as INFO unless it hides failed metrics publication in hot paths; prefer best-effort comments over noisy logs.
- Manually inspect `needs manual review` before adding suppressions.
