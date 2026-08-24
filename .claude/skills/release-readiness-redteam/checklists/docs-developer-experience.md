# Documentation & Developer Experience Checklist

> Load at Phase 9. Every doc claim is verified against code behavior — docs that lie are worse than docs that are missing.

## Doc–Reality Conformance (verify each against source/build)

- [ ] README quickstart: every command executed; every snippet compiles/runs.
- [ ] Maven coordinates in docs == actual POM coordinates.
- [ ] Documented defaults == actual code defaults (config keys, timeouts, batch sizes).
- [ ] Version requirements accurate (min JDK/runtime claims tested).
- [ ] Parameter descriptions match actual semantics (units! ms vs s, bytes vs elements).
- [ ] Examples directory maintained — no example referencing deleted APIs.

## Completeness

```text
README · Quick Start · Installation · Configuration reference · API reference
Examples · FAQ · Troubleshooting · Architecture overview · CONTRIBUTING · Changelog
```

- [ ] Every public symbol has reference documentation.
- [ ] Troubleshooting section answers the top 5 errors users will actually hit (harvested from Phase 6 findings).
- [ ] Error messages are grep-able and each has a doc entry or actionable message inline.

## DX Journey Friction Audit

Walk the journey as a stranger; log friction per stage:

```text
Discover → Install → Understand → Configure → Run → Debug → Integrate → Upgrade → Contribute
```

- [ ] Debug experience: can a user turn on verbose logging and localize a failure without reading source?
- [ ] IDE experience: Javadoc renders on hover in IntelliJ/Eclipse; auto-complete suggests the intended path first (facade discoverability).
- [ ] Upgrade story: breaking changes listed, migration notes exist for anything renamed since any earlier tag.

## Exit Bar

Scores ≥8 only if zero doc-reality contradictions found AND quickstart executed clean AND the debug journey works without reading source.
