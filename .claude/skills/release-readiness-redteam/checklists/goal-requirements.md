# Goal & Requirements Checklist

> Load at Phase 1. Mark each item PASS / FAIL(+finding) / N/A(justify).

## Positioning

- [ ] A one-sentence pitch exists and a stranger can repeat it after reading the README once.
- [ ] The 3 closest existing alternatives are named; differentiation claims are specific, not hand-waved ("faster", "simpler" without numbers = FAIL).
- [ ] The answer to "why build this instead of using `<alternative>`" survives a hostile HN comment.
- [ ] Target audience implied by docs matches audience implied by API design complexity.

## Goal Traceability

- [ ] Every shipped feature traces to the stated goal. List features with no declared reason (Feature Bloat).
- [ ] No feature exists purely as speculative future-proofing (YAGNI violation).
- [ ] The capability users MOST need is actually the most polished one (Missing Core Capability check — polish ≠ count of features).
- [ ] Feature list in README/docs matches what actually ships. Untested promise = broken promise.

## Requirements Integrity

- [ ] Non-functional requirements are stated where they matter: performance targets, memory budget, compatibility matrix (OS/JDK/runtime), scalability ceiling.
- [ ] Explicit non-goals are documented ("this does NOT do X").
- [ ] Technical constraints (min JDK, native deps, network assumptions) are discoverable BEFORE first run, not discovered by crash.
- [ ] Success criteria exist — how would anyone know the project achieves its goal?

## Exit Bar

Dimension scores ≥8 only if positioning survives the "why not X" attack AND no orphan features exist AND NFRs are stated and measurable.
