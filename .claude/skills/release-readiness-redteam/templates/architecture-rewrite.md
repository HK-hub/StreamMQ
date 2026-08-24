# Architecture Rewrite Escalation Template

> Trigger ONLY when the architecture's direction itself is wrong — not when taste differs. Test: can the stated product goals be met by the current structure within ~2 releases of incremental work? If no, escalate. A wrong-direction patch list is malpractice.

```markdown
# Architecture Escalation — <project>

## Architecture Status: UNSAFE TO EVOLVE | SAFE WITH CONSTRAINTS

## Root Problems
1. <structural defect #1 — with evidence>
2. <…>
3. <…>

## Why Incremental Fixes Are Insufficient
<The mechanism: what property of the structure makes patches converge back to the same wall. Cite Phase 2 evidence.>

## Recommended Architecture
<Target shape: modules, dependency directions, lifecycle ownership, data flow. One diagram-worth of description, not a fantasy redesign.>

## Migration Strategy
Stepwise strangler path:
1. <step — independently shippable>
2. <step>
3. …

## Migration Risks
| Risk | Likelihood | Mitigation |
|---|---|---|

## Estimated Complexity
Total effort: <S/M/L/XL> · Critical path: <which steps serialize> · Can ship v1 on current architecture at all? <yes-with-constraints / no>

## Constraint on This Review
List P0/P1 findings OUTSIDE the architectural scope that still gate release regardless of rewrite decision.
```

Discipline: the recommended architecture must be **simpler or equal** in total moving parts unless complexity increase is justified by a named requirement. Rewrites that add indirection to escape indirection are auto-rejected.
