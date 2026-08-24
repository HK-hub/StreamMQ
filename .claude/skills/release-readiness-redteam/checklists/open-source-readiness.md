# Open Source Readiness Checklist

> Load at Phase 10. You are about to hand this to anonymous strangers and promote it publicly. Everything below is judged as "will it survive day one of attention?"

## Legal & Governance

- [ ] LICENSE present, correct SPDX, matches what POM claims.
- [ ] Third-party license compatibility verified for ALL dependencies (no GPL-in-MIT surprises).
- [ ] CONTRIBUTING.md with working dev-env instructions (executed).
- [ ] CODE_OF_CONDUCT.md, SECURITY.md (private disclosure channel), issue templates, PR template present.
- [ ] DCO/CLA decision made and stated.

## Release Engineering

- [ ] CHANGELOG.md maintained per release (human-written, not diff-dump).
- [ ] Semver tag discipline: tags match released versions; no retagged versions in history.
- [ ] Maven Central publication verified end-to-end from a CLEAN machine: `mvn archetype:generate`-style consumer resolves artifacts, javadoc, sources.
- [ ] POM metadata completeness re-checked at `templates/release-gate.md` time (Central rejects incomplete).
- [ ] Git history hygiene: no secrets, no "final-final" chaos, sensible commit granularity for public archaeology.

## Public-Facing Assets (for the promotion you're planning)

- [ ] Repo social preview image set; logo/icon exists.
- [ ] Demo artifact ready: GIF/screenshot/asciinema for README + Product Hunt/X posts.
- [ ] "Why not X?" FAQ pre-written for the 3 closest alternatives (feeds Phase 11 persona #3).
- [ ] Badges honest: CI/coverage/version badges reflect reality.
- [ ] Support capacity honesty: who answers issues, expected response time — written down somewhere public.

## Day-One Simulation

- [ ] Simulate: repo hits GitHub trending → 500 stars in a week. What breaks first? (issue triage absent? discussions disabled? no good-first-issues?) List the first three failures.

## Exit Bar

Scores ≥8 only if governance files complete, clean-machine consumption verified, and day-one simulation answered concretely.
