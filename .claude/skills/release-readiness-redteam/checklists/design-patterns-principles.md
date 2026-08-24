# Design Pattern & Principle Audit Checklist

> Load at Phase 3. First build a pattern census, then interrogate each entry.

## Pattern Census

List every design pattern actually used. For EACH:

```text
Pattern:
Purpose (claimed):
Actual problem it solves (evidenced):
Necessity (what breaks if removed?):
Complexity introduced (files, indirection hops):
Simpler alternative:
Verdict: JUSTIFIED / SPECULATIVE / HARMFUL
```

Primary hunt target: **patterns used for the pattern's sake.**

- [ ] Interfaces with exactly one production implementation AND no test/second consumer → candidate for deletion.
- [ ] Factories wrapping `new` with no selection logic.
- [ ] Abstraction layers whose only caller is one class away.
- [ ] Event/callback machinery where a direct call is clearer.

Rule: if a direct implementation is simpler, **recommend deleting the abstraction** — deletion is a valid review outcome.

## Principle Spot-Checks

Do NOT apply principles mechanically. For each suspected violation run:

```text
Violation → Intentional trade-off? → Reasonable? → Actual impact?
```

- [ ] SOLID: SRP violations that hurt; DIP inverted where it matters; OCP claims real vs rhetorical.
- [ ] DRY vs False-DRY: wrong abstraction is worse than duplication — flag premature unification of two genuinely different things.
- [ ] KISS: simplest thing that could work, shipped.
- [ ] YAGNI: configurability/hooks nobody asked for yet.
- [ ] Information hiding: internals not visible through public surface; exceptions don't escape their layer.
- [ ] Composition over inheritance: no inheritance hierarchies >2 levels used for code reuse.
- [ ] Fail fast: invalid states crash at construction/config time, not 3 hours later downstream.
- [ ] Least surprise: behavior matches what the name and signature promise.

## Exit Bar

Scores ≥8 only if every surviving pattern has an evidenced necessity answer and zero interface-with-one-impl cases remain unexplained.
