# Test Coverage Report

Test infrastructure and strategy for YAMS. Coverage metrics pending measurement.

## Build Status

**Status**: All modules compile on Windows, Linux, and macOS.

## Test Infrastructure

| Location | Type | Description |
|----------|------|-------------|
| `tests/unit/` | Unit | Isolated component tests with mocks |
| `tests/integration/` | Integration | Component workflow tests |
| `tests/stress/` | Stress | Load and concurrency tests |
| `tests/benchmarks/` | Benchmarks | Performance measurement |

## Running Tests

```bash
# Run all tests
meson test -C builddir --print-errorlogs

# Run specific suite
meson test -C builddir --suite unit
meson test -C builddir --suite integration

# Generate coverage (requires gcovr)
meson configure builddir -Db_coverage=true
meson test -C builddir
gcovr -r . builddir -e tests/ --html-details coverage.html
```

## Coverage Measurement

Coverage metrics not yet measured. To establish baseline:

1. Build with coverage enabled
2. Run full test suite
3. Generate gcovr report
4. Update this document with actual metrics

## Known Limitations

- **Windows AF_UNIX**: Some integration tests skip due to socket limitations
- **Plugins**: Plugin tests require specific binaries to be built

## Related Documentation

- `tests/README.md` - Test directory structure
- `tests/CATCH2_PATTERNS.md` - Test writing guidelines