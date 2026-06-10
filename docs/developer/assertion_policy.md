# Assertion Policy

YAMS uses a project-local assertion model for internal invariants and programmer
errors. This complements sanitizers and tests; it does **not** replace normal
runtime error handling.

## Core rule

Assertions are for **bugs**, not expected failures.

Use ordinary errors / `Result<T>` / exceptions for:
- invalid user input
- missing files or plugins
- network / database failures
- environment drift
- other failures the program can report coherently

Use assertions for:
- violated invariants
- impossible control-flow paths
- invalid lifecycle transitions
- broken ownership / concurrency assumptions
- preconditions and postconditions that represent programmer misuse

## Design principles

- Prefer the YAMS assertion layer over raw `assert(...)` for project invariants; raw `assert(...)` is globally controlled by `NDEBUG` and may vanish in release-like builds.
- Disabled debug checks must not evaluate expensive or side-effecting expressions. Compute side effects before the assertion and assert on the resulting value.
- Include a message that explains the invariant, not just the boolean expression.
- Choose the cheapest assertion tier that preserves safety: always-on for corruption/UB-adjacent invariants, debug-only for expensive consistency checks.
- Treat preconditions/postconditions as design documentation: they should state what the caller/provider has already promised, not validate ordinary user input.

## Assertion tiers

### Always-on hard assertions
These survive optimized and shipping builds.

- `YAMS_ASSERT(condition, message)`
- `YAMS_PRECONDITION(condition, message)`
- `YAMS_POSTCONDITION(condition, message)`
- `YAMS_UNREACHABLE(message)`

These abort the process after printing file, line, function, and (when
available) a stacktrace.

Use them for corruption-risk and UB-adjacent invariants such as:
- shutdown sequencing
- callback / executor affinity
- impossible FSM transitions
- counter / cache consistency
- ownership transfer assumptions

### Debug-only checks
These are for expensive or redundant sanity checks.

- `YAMS_DCHECK(condition, message)`

Default behavior:
- enabled when `NDEBUG` is **not** set
- disabled when `NDEBUG` **is** set

Override for canary / hardened builds:

```bash
meson setup build-canary \
  --buildtype=debugoptimized \
  -Db_ndebug=true \
  -Denable-dcheck=true
```

`-Denable-dcheck=true` adds `-DYAMS_ENABLE_DCHECK=1`, keeping
`YAMS_DCHECK` active even in `debugoptimized` builds that also define
`NDEBUG`.

## Why plain `assert(...)` is not enough

Standard `assert(...)` is controlled by `NDEBUG`. In YAMS release-like builds,
`Release` currently maps to `debugoptimized` and sets `b_ndebug=true`, so plain
`assert(...)` may disappear.

Therefore:
- plain `assert(...)` is acceptable only for checks that may vanish in release
- important invariants must use the YAMS assertion layer

## Design rules

1. **No side effects in assertion expressions**

Bad:
```cpp
assert(queue.pop());
```

Good:
```cpp
const bool popped = queue.pop();
YAMS_ASSERT(popped, "queue must contain wake token");
```

2. **Prefer termination on invariant failure**

If an invariant violation implies corrupted state, continuing is usually less
safe than aborting.

3. **Keep recoverable failures out of assertions**

Bad:
```cpp
YAMS_ASSERT(std::filesystem::exists(configPath), "config file must exist");
```

Good:
```cpp
if (!std::filesystem::exists(configPath)) {
    return Error{ErrorCode::NotFound, "config file missing"};
}
```

4. **Treat `YAMS_UNREACHABLE` as a proof obligation**

Use it only after real exhaustiveness or invariant reasoning. Do not use it to
silence compiler warnings on paths that might still occur.

## Suggested rollout order

The assertion rollout is test-first: add or identify behavior coverage before hardening a path, then introduce assertions as executable invariants. Death tests are useful only when the failure mode is stable across platforms; otherwise prefer normal behavior tests around the code that prevents the invariant breach.

1. Introduce the shared assertion layer.
2. Harden lifecycle / shutdown invariants first.
3. Migrate raw `assert(...)` in non-generated code.
4. Add debug-only consistency checks where they improve diagnosability.
5. Document subsystem-specific assertion expectations in code reviews.

## Initial YAMS priorities

Highest-value adoption surfaces:
- `PostIngestQueue`
- `ServiceManager`
- daemon lifecycle / FSM wrappers
- metadata counters, path-tree consistency, and batch write accounting
- non-generated raw `assert(...)` sites such as `src/vector/turboquant.cpp`

## References

- Jonathan Müller, "How do I implement assertions?" — limitations of global `assert`, assertion levels, delayed evaluation, and custom handlers: https://www.foonathan.net/2016/09/assertions/
- cppreference, `assert` / `NDEBUG` semantics: https://en.cppreference.com/cpp/error/assert
- C++ Core Guidelines — contracts, preconditions, postconditions, and invariants
