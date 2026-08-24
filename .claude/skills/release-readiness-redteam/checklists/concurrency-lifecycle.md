# Concurrency & Lifecycle Audit Checklist

> Load at Phase 6b. Static reading proves nothing here — unverifiable claims get a minimal repro test written.

## Thread Safety

- [ ] Every shared mutable field accounted for: lock, CAS, or confinement — with the mechanism named in code/docs.
- [ ] Safe publication verified: no `this` escape during construction; immutable types truly final-field-based.
- [ ] Compound operations atomic end-to-end (check-then-act races hunted: `contains→add`, `get→put`, double-checked locking without volatile).
- [ ] Collections exposed publicly are concurrent-safe or defensively copied.
- [ ] Visibility: state written by thread A read correctly by B (final/volatile/locks — not hope).

## Liveness

- [ ] Lock ordering global and acyclic → deadlock impossible by construction; otherwise justify.
- [ ] No unbounded waits on external resources without timeout; interruption honored everywhere (no swallowed `InterruptedException`).
- [ ] Starvation scenarios examined: can a fair-share of work be denied indefinitely?

## Thread Pools & Async Lifecycle

- [ ] Pool sizing justified (CPU-bound vs IO-bound math shown).
- [ ] Queue bounds + rejection policy explicit and documented.
- [ ] Shutdown ordering: producers stopped → queues drained (or explicitly dropped) → workers joined → resources released. Timeout-bounded at every stage.
- [ ] Tasks submitted during shutdown have defined behavior (reject loudly, never silently vanish).
- [ ] Callbacks/completions never run on surprising threads or after owner closed.

## Domain-Specific: Messaging / Streaming Systems

Apply if applicable (e.g., queue/broker/stream projects):

- [ ] Ordering guarantees stated precisely (per-partition? per-key?) and actually hold under concurrency.
- [ ] Delivery semantics named (at-most-once / at-least-once / exactly-once) and matching implementation reality — including duplicate suppression on redelivery.
- [ ] Backpressure defined end-to-end: slow consumer → producer behavior specified (block/drop/fail?) — unbounded buffering = finding.
- [ ] Overflow policy per queue/topic explicit and observable.
- [ ] Reconnect/rebalance recovery: no loss, no duplication beyond declared semantics.
- [ ] Persistence boundaries honest: "durable" means fsync'd where claimed, not page-cache-hopeful.

## Verification Rule

For each claim above that cannot be proven statically: **write the minimal repro/concurrency test** and execute it. Record VERIFIED/LIKELY accordingly. jcstress or stress-test loops count; eyeballing does not.

## Exit Bar

Scores ≥8 only if shared-state map complete, shutdown ordering timeout-bounded, and every messaging-semantics claim matches executable evidence.
