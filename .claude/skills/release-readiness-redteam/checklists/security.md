# Security Audit Checklist

> Load at Phase 7b. Mindset: an attacker reads your source and your dependency tree before using your library.

## Dependency & Supply Chain

- [ ] Known CVEs scanned (`mvn org.owasp:dependency-check-maven:check` / `versions:display-dependency-updates`); results recorded even when clean.
- [ ] Transitive risk reviewed: which transitive deps could a maintainer abandon tomorrow?
- [ ] Build plugins/sources trusted; no snapshot repos or unknown repositories in build config.
- [ ] Library risk propagation answered explicitly: **does a flaw here become every downstream user's flaw?** Which components run with elevated privilege?

## Attack Surface

- [ ] Deserialization surface mapped — any `ObjectInputStream`/`readObject` of external data = finding until justified.
- [ ] Injection surfaces checked (command, path traversal via user-controlled filenames, expression/template evaluation).
- [ ] SSRF potential wherever URLs/endpoints are user-configurable.
- [ ] Input validation boundary defined: where does untrusted data first enter, and what gates it?
- [ ] Reflection/classloading: dynamic loading of user-specified classes sandboxed or documented as trusted-only.
- [ ] File access scoped; no writes outside intended directories; symlink handling considered.
- [ ] Management/admin endpoints (JMX, HTTP) default-off or authenticated.

## Secrets & Data

- [ ] Git history scanned for secrets (`git log -p` + secret patterns) — including pre-rewrite history.
- [ ] Logging verified free of secrets/tokens/credentials at all levels.
- [ ] Crypto used only via standard, current primitives; zero home-rolled crypto.
- [ ] Defaults are safe defaults: insecure option requires explicit opt-in, never the reverse.

## Exit Bar

Scores ≥8 only if CVE scan clean-or-triaged, deserialization surface empty-or-justified, history secret-free, and the downstream-propagation question has a written answer.
