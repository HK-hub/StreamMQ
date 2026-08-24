# Performance Audit Checklist

> Load at Phase 7a. Never claim "slow" without a measurement. Protocol: Hypothesis → Benchmark → Measurement → Evidence → Conclusion.

## Static Hot-Path Scan

- [ ] Core-path algorithmic complexity audited (hidden O(n²) in loops containing IO/lookups).
- [ ] Repeated work flagged: same parse/IO/serialization executed multiple times per logical operation.
- [ ] Allocation churn on hot paths (object-per-event, boxing, substring/regex recompiles, stream pipelines per message).
- [ ] Excessive copying: byte[]/String round-trips where zero-copy or views suffice.
- [ ] Lock breadth on hot paths: coarse monitors guarding whole operations vs fine-grained needs.
- [ ] Blocking calls inside async contexts (blocking IO on event-loop threads = automatic finding).
- [ ] GC pressure points identified (allocation rate estimate at target throughput stated numerically).
- [ ] Cache misuse: unbounded caches, wrong keys, no hit-rate observability.

## Measurement

- [ ] Throughput/latency measured at realistic AND 10× load; p99/p999 recorded, not just mean.
- [ ] Memory footprint under sustained load measured (steady-state after GC, not startup snapshot).
- [ ] Results compared against the project's OWN claimed numbers in docs — mismatch = finding.
- [ ] Benchmarks reproducible (JMH or equivalent; warmup, isolation from JIT noise documented).

## Missing Instrumentation

- [ ] List benchmarks that SHOULD exist for this project but don't. Each becomes a P2/P3 finding with suggested protocol.

## Exit Bar

Scores ≥8 only if hot paths carry measurement-backed evidence and every performance claim in docs is either verified or removed.
