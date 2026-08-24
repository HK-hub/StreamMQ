# Implementation Quality Checklist

> Load at Phase 4. Read the core paths line-by-line; sample the rest.

## Class & Method Hygiene

- [ ] No class juggling multiple responsibilities with excessive mutable state.
- [ ] Lifecycle within classes coherent (construction → ready → close) without half-initialized windows.
- [ ] Methods >50 LOC or >4 params flagged; deep nesting (>3 levels) flagged.
- [ ] Hidden side effects: getters that mutate, "check" methods that write state.
- [ ] Null-handling strategy coherent project-wide (`Optional`, annotations, or contract — pick one story).

## Exception Handling

- [ ] Zero swallowed exceptions (`catch {}`, catch-log-continue without rethrow/decision).
- [ ] Exception types semantically correct (no `Exception`/`RuntimeException` blanket throws in public API).
- [ ] Cause chains preserved — no `new X(e.getMessage())` stack-trace amputation.
- [ ] Public exception hierarchy documented and meaningful to callers.
- [ ] Error messages actionable: say what failed, why, and what to try next.

## Logging

- [ ] Levels sane (ERROR = needs action, WARN = degraded, INFO = operational events, DEBUG = diagnostics).
- [ ] Context sufficiency: an operator can diagnose from logs alone (ids, sizes, endpoints).
- [ ] No sensitive data (secrets, tokens, payloads with PII) at INFO+.
- [ ] Hot-path logging doesn't allocate/string-concat per event unconditionally.

## Code Cleanliness

- [ ] Magic values extracted or configured; constants named by meaning.
- [ ] Dead code, commented-out blocks, unreachable branches: zero tolerance.
- [ ] TODO/FIXME density counted — each one either resolved or turned into a tracked issue before release.
- [ ] Naming consistent: same concept = same word everywhere (class/method/var/param/package/API).
- [ ] Resource handling via try-with-resources/equivalent everywhere; no manual close() without finally.
- [ ] `equals`/`hashCode` contracts correct wherever used as map keys; immutability claims verified against final fields and defensive copies.

## Exit Bar

Scores ≥8 only if zero swallowed exceptions, zero dead code, and error messages pass the "stranger can act on this" test.
