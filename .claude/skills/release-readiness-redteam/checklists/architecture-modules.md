# Architecture & Module Red Team Checklist

> Load at Phase 2. Draw the dependency graph before judging anything.

## Dependency Structure

- [ ] Full module dependency graph drawn; zero circular dependencies (compile-time AND runtime/init-order).
- [ ] Dependency direction follows intent: stable abstractions are not depending on volatile details.
- [ ] No hidden dependencies — module usable without reading a sibling's source or init order.
- [ ] Layer violations listed: who reaches past a boundary and why.

## Complexity Honesty

Core question: **does this architecture reduce complexity, or relocate it into indirection?**

Hunt specifically for:

```text
God Module / God Service / God Class      Interface Explosion
Premature Abstraction                     Factory Explosion
Excessive Indirection / deep call chains  Manager Explosion
Leaky Abstractions                        DTO Explosion
Utility Explosion                         Event Explosion
Premature Optimization                    Configuration Explosion
```

For each hit: evidence + what simpler shape replaces it.

## Module Interrogation (per module)

Answer all five, in writing:

```text
Why does this module exist?
Can it be removed?
Can it be merged?
Should it be split?
Is its boundary correct?
```

Plus: cohesion (one reason to change?), testability seams (can it be instantiated in isolation?), replaceability (is swapping it a rewrite?).

## System-Level Concerns

- [ ] Lifecycle ownership clear: exactly one owner of init/start/stop/shutdown semantics; ordering defined.
- [ ] State ownership unambiguous — no "who mutates this?" ambiguity across modules.
- [ ] Single points of failure identified; behavior under partial failure specified.
- [ ] Error flow designed (where do failures surface?) not accidental.
- [ ] Extension points are real (≥2 implementations or a documented consumer) — otherwise they're speculation tax.
- [ ] Bottlenecks under 10× load identified with mechanism, not vibes.

## Evolution Safety

Verdict required: `SAFE TO EVOLVE` or `UNSAFE TO EVOLVE`.
If UNSAFE → escalate via `templates/architecture-rewrite.md`. Do not offer patches for a wrong direction.

## Exit Bar

Scores ≥8 only if graph is acyclic, no god-module survives interrogation, and lifecycle/state ownership has single-owner answers.
