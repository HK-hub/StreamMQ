# API / SDK / Maven Audit Checklist

> Load at Phase 5. Simulate a developer who knows nothing about this project. JVM/Maven-first; substitute ecosystem equivalents for other stacks.

## First-Use Simulation (execute, don't imagine)

Walk the full path and record wall-clock time:

```text
Search/discover → read README → add Maven dependency → init → configure
→ first API call → trigger an error path → clean shutdown
```

- [ ] Time-to-first-success ≤ 10 minutes using ONLY the README. Record where it breaks.
- [ ] Copy-paste-able quickstart exists: coordinates + minimal code, no placeholders.
- [ ] A runnable example module/example dir compiles and runs as part of the build.
- [ ] First error a newcomer typically makes produces a message that tells them the fix.

## Public API Surface

- [ ] Every public class/method has Javadoc including thread-safety and nullness contracts.
- [ ] Naming consistent across surface (verbs, nouns, no `Manager`/`Helper`/`Util` dumping grounds).
- [ ] Sensible defaults — zero-config path works for the primary use case.
- [ ] Escape hatches present for the 20% case (raw access, custom implementations) without leaking internals.
- [ ] Resource management idiom clear: `AutoCloseable`/builder/lifecycle — documented who owns what.
- [ ] Exceptions public, typed, hierarchical; internal exceptions never leak to callers.
- [ ] Semver exposure audit: list everything public today that WILL need to break later. Each item either hidden now or scheduled with deprecation policy.

## Build & Maven Engineering

- [ ] groupId/artifactId/version sane; coordinates in docs == actual deployed artifacts.
- [ ] Java compatibility declared (`maven.compiler.release`) matches bytecode reality (verify with `javap -verbose` or similar).
- [ ] Dependency scopes correct: compile vs provided vs optional vs test — nothing leaked that users shouldn't get transitively.
- [ ] Transitive dependency count justified; every compile-scope dep defensible in a review comment.
- [ ] No snapshot dependencies; version ranges absent (reproducible builds).
- [ ] Central-publishing completeness: `name`, `description`, `url`, `licenses`, `developers`, `scm` all present — Central rejects otherwise.
- [ ] Sources + Javadoc jars produced and signed; GPG signing verified end-to-end.
- [ ] Dependency convergence enforced (enforcer plugin or equivalent); conflicting-version surprises listed.
- [ ] `module-info` / Multi-Release-JAR claims actually tested if present.

## Exit Bar

Scores ≥8 only if first-use simulation completed under 10 min unaided AND POM passes Central requirements AND semver exposure audit has no unhidden time bombs.
