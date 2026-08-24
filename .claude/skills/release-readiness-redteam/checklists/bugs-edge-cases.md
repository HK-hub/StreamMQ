# Bug & Edge-Case Hunting Checklist

> Load at Phase 6a. Hunt by executing against the real artifact where possible.

## Input Matrix

For every public entry point, feed:

```text
null · empty · blank/whitespace · zero · negative · Integer.MAX_VALUE
oversized (1MB string, 1M elements) · malformed encoding · wrong type
unicode/boundary chars · concurrent duplicates of the same input
```

Record behavior: handled gracefully / informative error / crash / silent corruption. Silent corruption = automatic P0/P1.

## Lifecycle Abuse Matrix

```text
initialize twice        start twice          stop twice
close twice             use after close      use before init
partial initialization failure   restart after failure
close while work in flight
```

Each cell must have defined, tested behavior. "Undefined" = finding.

## Error Propagation

- [ ] Failures at layer N surface correctly at N+1 (not transformed into misleading success).
- [ ] Async failures delivered (callback/future/logs), never silently dropped.
- [ ] Partial-failure semantics defined: which side effects persist after a mid-operation crash?

## Resource Leak Sweep

Enumerate and verify cleanup for:

```text
memory (caches, listeners registered but never removed)
threads (created per-request? daemon? named?)
connections · file descriptors · executors
temp files/buffers · native/off-heap allocations
```

Method: run representative workload, then measure (thread count, fd count, heap) before vs after. Growth = finding.

## Exit Bar

Scores ≥8 only if input matrix executed on core entry points, lifecycle abuse cells defined-and-tested, and leak sweep measured rather than asserted.
