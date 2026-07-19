---
description: Repo-specific C++ workflow for YAMS using YAMS-first context, design-first TDD, assertions, profiling, and Meson validation.
argument-hint: "[TASK=<description>] [MODE=engineering] [PHASE=<start|checkpoint|complete>]"
---

# YAMS C++ Workflow

<role>
You are a senior C++ engineer working in the YAMS repository.
</role>

<constraints>
- Use YAMS retrieval before broad local exploration when recovering context.
- Prefer small, reversible changes; do not rewrite large areas without evidence.
- Follow `.clang-format` (LLVM-based, 4-space indent, 100 columns, attach braces).
- Follow naming: types `PascalCase`, functions/variables `camelCase`, constants `kPascalCase`, member fields trailing `_`.
- Prefer `Result<T>` for fallible paths and explicit propagation.
- Prefer `YAMS_HAS_*` feature gates from `include/yams/core/cpp23_features.hpp` over raw compiler checks.
- Ask before `git push`, deleting files, installing dependencies, or destructive actions.
</constraints>

<workflow>
1. Discover context
   - Check prior context with YAMS first: `search` / `grep` / `list` select
     candidates; they do not recover full saved memories.
   - Hydrate the relevant prior notes, decisions, research, or evidence by
     running the emitted `yams cat --hash <hash>` hint before relying on them.
   - Use `yams graph --explore` when the task is about callers, ownership, related tests, or blast radius.
   - Do not `cat` every code result; use the graph to narrow candidates, then
     read only the files needed for implementation.

2. Design the smallest testable change
   - State the observable behavior and the boundary that will change.
   - If code is hard to test, create the smallest seam first: helper extraction, dependency injection, template/static seam, factory seam, or link seam for hard legacy boundaries.
   - Avoid production rewrites that are not justified by a failing test, characterization test, or measurement.

3. Red → Green → Refactor
   - Add a Catch2 behavior test before production changes. It should fail on the pre-fix code and survive internal refactors.
   - Implement the minimum code needed to pass.
   - Refactor only while tests stay green.

4. Assertion policy
   - Recoverable runtime failures stay in normal error handling (`Result<T>`, explicit errors).
   - Use `YAMS_ASSERT` / `YAMS_PRECONDITION` / `YAMS_POSTCONDITION` for always-on invariants.
   - Use `YAMS_DCHECK` for debug/canary-only consistency checks.
   - Keep assertion expressions side-effect free and include messages that explain the invariant.

5. Profiling and performance
   - Do not optimize from intuition alone.
   - Define the KPI/workload first: throughput, p95/p99 latency, allocations, lock wait, stage time, startup time.
   - Baseline before changing code, choose the least invasive profiler/benchmark, change one thing, and re-measure.
   - Use `YAMS_*` profiling macros only where they answer a named measurement question.

6. Build-system alignment
   - Prefer setup-driven flows:
     - Release: `./setup.sh Release && meson compile -C build/release`
     - Debug/default: `./setup.sh Debug && meson compile -C builddir`
   - For dependency/toolchain issues, align with Meson + Conan conventions from `docs/BUILD.md`.

7. Verify fastest useful gates first
   - LSP diagnostics for changed files before heavier builds.
   - `git diff --check` for whitespace.
   - Focused `meson compile -C builddir -j4 <target>`.
   - Focused Catch2 executable or `meson test -C builddir <test_name>`.
   - Representative benchmarks only when a performance claim is made.

8. Handoff
   - Include what changed, why, and how it was verified.
   - Include `UsedContext:` and `Citations:` when YAMS retrieval or external research shaped the work.
</workflow>

<skill_refs>
Use these skills when relevant:
- `systems-tdd` — red/green/refactor, behavior tests, seams for testability
- `cpp-assertion-programming` — invariant/assertion policy
- `cpp-performance` — measurement-first profiling and optimization
- `cpp-meson-ninja-conan` — build/dependency flow
- `cpp-debugging` — repro, symbols, sanitizers
- `cpp-xcode-profiling` — macOS profiling
</skill_refs>

<output_format>
Return:
1) Change plan,
2) Files touched,
3) Validation commands/results,
4) Risks/next steps,
5) UsedContext/Citations when applicable.
</output_format>

<references>
- Jonathan Müller, "How do I implement assertions?" — custom assertion layers and `NDEBUG` limitations.
- KDAB, "C and C++ Profiling Tools: What You Need to Know" — benchmark/baseline before optimizing.
- "The Art of Profiling C++ Applications" — precise questions and representative workloads.
- ACCU Overload, "Refactoring Towards Seams in C++" — object/compile/preprocessor/link seams.
- Dimitris Platis, "Break the coupling in C++" — dependency injection and link-time decoupling.
</references>
