# Release Gate Record Template

> Fill at Phase 12, after all findings classified and confidence rules enforced.

```markdown
# Release Gate — <project> v<version>
Date: YYYY-MM-DD · Reviewer: release-readiness-redteam

## Finding Counts
| P0 | P1 | P2 | P3 | P4 |
|---:|---:|---:|---:|---:|

## Blockers (VERIFIED P0/P1 only)
- F-XX-NN — title — one-line impact

## Confidence Enforcement
- [ ] Zero P0/P1 remain HYPOTHESIS (each verified or downgraded, reason logged)
- [ ] Build & tests executed this session: <pass/fail + command>

## Gate Rule Application
Any VERIFIED P0 → NO-GO ............ : <triggered/not>
Any unresolved P1 → NO-GO default .. : <triggered/not>
≥5 open P2 → CONDITIONAL recommended : <triggered/not>

## Verdict: GO | CONDITIONAL GO | NO-GO

## Risk Acceptances (required for CONDITIONAL GO with open P1)
### Risk #1
- Risk: <what remains broken/dangerous>
- Reason accepted: <why shipping anyway>
- Owner: <name>
- Mitigation: <monitoring/rollback/docs warning>
- Re-evaluate at: vX.Y

## Pre-Promotion Conditions (for CONDITIONAL GO)
Ordered list of what must land before the public announcement posts go out.
```

Rules: verdicts are one of the three tokens — never prose like "publish after optimizing". Every CONDITIONAL GO carries at least one completed Risk Acceptance. NO-GO lists the exact findings that must flip to unblock.
