# Final Review Report Template

> Mandatory output structure for Phase 13. Emit exactly these sections, in this order.

```markdown
# Release Readiness Review — <project> v<version>
Date · Commit SHA · Reviewer: release-readiness-redteam

## 1. Verdict: GO | CONDITIONAL GO | NO-GO
One line. Gate rule that decided it. Bolded single most important action.

## 2. Executive Damage Report
≤10 lines, most damaging findings first, one line each.

## 3. Project Understanding
What we believe this project is and who it serves. Wrong assumptions here invalidate downstream sections — state them so they can be corrected.

## 4. Scope & Method (Coverage Honesty)
What was examined, executed (commands + outcomes), skipped and why. Silent gaps forbidden.

## 5. Scorecard
| Dimension | Score /10 |
15 dimensions (per SKILL.md §7) + Overall = round(sum÷15×10).

## 6. Findings
Grouped P0 → P4. Each finding uses `finding.md` format verbatim.
A dimension with nothing found still reports: "No material findings after active attack: <what was tried>."

## 7. Architecture Evolution Safety
SAFE TO EVOLVE / UNSAFE TO EVOLVE (+ link to architecture-rewrite record if escalated).

## 8. Predicted Public Reactions
Top 5 posts from the Persona Gauntlet, verbatim, each tagged with the finding IDs that evidence them.

## 9. Fix Roadmap
Ordered phases: Blockers → Architecture → Core Quality (impl/API/tests) → DX (docs/examples/errors) → Perf & Sec → Polish.
Each item: finding IDs · effort S/M/L · dependency order.

## 10. Release Gate Record
Link/embed completed `release-gate.md`.

## Appendix A — Commands Executed
Raw command list with pass/fail outcome per command.

## Appendix B — UNKNOWN Register
Every UNKNOWN with reason and what evidence would resolve it.
```

Rules: never close with encouragement. The report ends on §Appendix B — evidence and unknowns, not motivation.
