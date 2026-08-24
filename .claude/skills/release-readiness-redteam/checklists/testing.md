# Test System Audit Checklist

> Load at Phase 8. Question is never "what's the coverage?" — it's "do these tests prove the system handles its real risks?"

## Risk Coverage (not line coverage)

- [ ] Map: top 5 system risks (from Phases 6–7) → test that would catch each. Unmapped risk = finding.
- [ ] Layer mix assessed: unit / integration / E2E / contract / regression — what exists vs what the architecture demands.
- [ ] Boundary tests exist for every input-matrix edge in `bugs-edge-cases.md`.
- [ ] Failure-injection tests present: dependency down, timeout, partial write, restart mid-operation.
- [ ] Concurrency tests exist for every claim in `concurrency-lifecycle.md` (stress loops, not single-shot).

## Test Honesty

- [ ] Happy-path-only suites flagged; ratio of failure-path tests counted.
- [ ] Mock overuse audited: tests mocking the very behavior under test = theater.
- [ ] Implementation coupling: tests reaching privates/whitebox such that refactoring breaks them without behavior change.
- [ ] Tests that structurally cannot fail (assert nothing, assert constants, try/catch-swallow) hunted and listed.
- [ ] Determinism: no `Thread.sleep`-based timing assertions; time/random injected; tests pass with `-Dsurefire.rerunFailingTestsCount=3` style flake probing.
- [ ] Coverage number reported honestly WITH its blind spots named (coverage ≠ correctness).

## CI Enforcement

- [ ] Which jobs actually BLOCK merge? (Advisory checks are decoration.)
- [ ] Build + full suite green on clean checkout right now — verified this session.
- [ ] Examples/docs snippets compiled or executed in CI, not rotting unchecked.

## Exit Bar

Scores ≥8 only if top risks map to executable failing-on-bug tests AND failure/concurrency paths are covered AND CI gates actually gate.
