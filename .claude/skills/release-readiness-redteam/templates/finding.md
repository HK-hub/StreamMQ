# Finding Record Template

> One block per finding during Phases 1–11. ID format: `F-<phase>-<seq>` (e.g., F-06-03). Confidence ∈ VERIFIED / LIKELY / HYPOTHESIS. No P0/P1 may enter the gate as HYPOTHESIS — verify it or downgrade it.

```markdown
## F-XX-NN — <one-line title>
- Severity: P0|P1|P2|P3|P4
- Category: goal|architecture|module|pattern|implementation|api|maven|bug|lifecycle|concurrency|performance|security|testing|docs|dx|oss
- Confidence: VERIFIED | LIKELY | HYPOTHESIS
- Location: module · path/to/File.java · ClassName#method · line(s)
- Problem: <what is wrong, stated factually>
- Evidence: <command output / code snippet / measurement — mandatory for P0–P2>
- Root cause: <mechanism, not symptom>
- Impact: <who breaks, when, how badly>
- Public risk: <how this looks when a stranger hits it post-release>
- Recommended fix: <concrete, minimal>
- Alternative: <second option, or "none viable because…">
- Effort: S (<1h) | M (<1d) | L (>1d)
- Release decision: MUST FIX | SHOULD FIX | CAN DEFER
```

## Calibration Example (tone & depth target)

```markdown
## F-05-01 — Consumer offset committed before user handler completes
- Severity: P0
- Category: bug / messaging-semantics
- Confidence: VERIFIED (repro test below)
- Location: core · src/main/java/.../OffsetManager.java · OffsetManager#commitAfter · 88–104
- Problem: Offset commit runs in `finally` after dispatching to the handler but does not await handler completion; an exception in the handler still advances the offset.
- Evidence:
    [ERROR] HandlerTest.handlerThrows_messageLost:231
    expected in-redelivery count 1, actual 0
- Root cause: commit is bound to dispatch completion, not processing completion; no ack/nack boundary.
- Impact: silent message loss on every handler exception — violates the project's own at-least-once claim.
- Public risk: first data-loss issue report becomes an HN thread titled "StreamMQ loses messages".
- Recommended fix: bind commit to explicit Ack handle returned by handler; document Nack semantics.
- Alternative: config flag `commitMode=MANUAL|AUTO` with MANUAL default in 1.0.
- Effort: M
- Release decision: MUST FIX
```
