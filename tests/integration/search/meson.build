# Hybrid search integration tests - Catch2 tests requiring full daemon with embeddings
# Covers semantic search, hybrid search, RRF fusion with real infrastructure

if not catch2_dep.found()
  message('Catch2 not found; skipping hybrid search integration tests.')
else
  hybrid_search_integration_srcs = files(
    'hybrid_search_integration_test.cpp',
  )

  symbol_search_integration_srcs = files(
    'symbol_search_integration_test.cpp',
  )

  fsm_tuning_integration_srcs = files(
    'fsm_tuning_benchmark_integration_test.cpp',
  )

  dbg = (get_option('buildtype') == 'debug')
  if (get_option('enable-integration-tests') or dbg) and (get_option('enable-daemon-integration') or dbg)
    # Catch2 needs its main library
    catch2_main_dep = dependency('catch2-with-main', required: false)
    if not catch2_main_dep.found()
      catch2_main_dep = dependency('Catch2WithMain', required: false, method: 'cmake')
    endif

    hybrid_search_integration_exe = executable('yams_hybrid_search_integration',
      hybrid_search_integration_srcs,
      include_directories: incs,
      dependencies: [catch2_dep, catch2_main_dep, test_deps, catch2_trailing_link_deps],
      cpp_args: ['-DYAMS_TESTING=1'],
      install: false,
    )

    test('hybrid_search_integration',
      hybrid_search_integration_exe,
      suite: ['integration', 'search', 'hybrid', 'semantic', 'catch2'],
      env: {
        'YAMS_TEST_SAFE_SINGLE_INSTANCE': '1',
        'TSAN_OPTIONS': _test_tsan_options,
        # Note: Do NOT set YAMS_DISABLE_VECTORS - we need vector search for hybrid tests
      },
      is_parallel: false,
      timeout: 300,  # 5 minutes - allows time for daemon startup + embedding model load
      # Allow exit 0 when all tests skip (Windows daemon shutdown not yet implemented)
      args: ['--allow-running-no-tests'],
    )

    # Symbol search integration tests
    symbol_search_integration_exe = executable('yams_symbol_search_integration',
      symbol_search_integration_srcs,
      include_directories: incs,
      dependencies: [catch2_dep, catch2_main_dep, test_deps, catch2_trailing_link_deps],
      cpp_args: ['-DYAMS_TESTING=1'],
      install: false,
    )

    test('symbol_search_integration',
      symbol_search_integration_exe,
      suite: ['integration', 'search', 'symbols', 'kg', 'catch2'],
      env: {
        'YAMS_TEST_SAFE_SINGLE_INSTANCE': '1',
        'YAMS_DISABLE_VECTORS': '1',  # Don't need embeddings for symbol tests
        'TSAN_OPTIONS': _test_tsan_options,
      },
      is_parallel: false,
      timeout: 120,  # 2 minutes - symbol extraction can be slow
      # Allow exit 0 when all tests skip (Windows daemon shutdown not yet implemented)
      args: ['--allow-running-no-tests'],
    )

    # FSM Tuning integration tests (Phase 4 of yams-7ez4)
    fsm_tuning_integration_exe = executable('yams_fsm_tuning_integration',
      fsm_tuning_integration_srcs,
      include_directories: incs,
      dependencies: [catch2_dep, catch2_main_dep, test_deps, catch2_trailing_link_deps],
      cpp_args: ['-DYAMS_TESTING=1'],
      install: false,
    )

    test('fsm_tuning_integration',
      fsm_tuning_integration_exe,
      suite: ['integration', 'search', 'fsm', 'tuning', 'benchmark', 'catch2'],
      env: {
        'YAMS_TEST_SAFE_SINGLE_INSTANCE': '1',
        'TSAN_OPTIONS': _test_tsan_options,
        # Some tests need embeddings for hybrid search validation
      },
      is_parallel: false,
      timeout: 180,  # 3 minutes - FSM tests run multiple search operations
      # Allow exit 0 when all tests skip (Windows daemon shutdown not yet implemented)
      args: ['--allow-running-no-tests'],
    )
  else
    message('hybrid_search_integration test disabled (enable with -Denable-daemon-integration=true).')
  endif
endif
