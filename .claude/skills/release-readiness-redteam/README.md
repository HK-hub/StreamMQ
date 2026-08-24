# release-readiness-redteam

Adversarial pre-release review skill for open-source projects. A hostile review board tries to **prove your project must not ship yet** — so anonymous developers on GitHub/HN/Product Hunt never get the chance to first.

Runs a phased audit (goals → architecture → patterns → implementation → API/Maven → bugs/concurrency → performance/security → tests → docs/DX → OSS readiness), verifies findings by **executing builds/tests/examples**, classifies them P0–P4 with confidence labels, simulates five hostile personas, and issues a **GO / CONDITIONAL GO / NO-GO** gate with a prioritized fix roadmap.

## Install

| Agent | Location | Notes |
|---|---|---|
| Claude Code | `.claude/skills/release-readiness-redteam/` (project) or `~/.claude/skills/…` (global) | invoke `/release-readiness-redteam`, or ask naturally ("发布前红队审查") |
| Codex CLI | `~/.codex/prompts/release-readiness-redteam.md` | strip YAML frontmatter; invoke `/release-readiness-redteam` |
| OpenCode | `~/.config/opencode/command/release-readiness-redteam.md` | keep frontmatter `description`; becomes slash command |
| pi / Cursor / others | merge into `AGENTS.md` / `.cursor/rules` | paste SKILL.md body minus frontmatter |

Follows the open Agent Skills layout (`SKILL.md` + supporting files), so other spec-compliant agents load it directly.

## Design: progressive disclosure

`SKILL.md` is deliberately lean — identity, hard rules, phase orchestration, gate logic. Domain detail lives in 12 checklists loaded **at phase entry**, not upfront, so one invocation doesn't burn the context window on all 12 domains at once.

## Layout

```text
release-readiness-redteam/
├── SKILL.md                            # orchestration & constraints
├── checklists/                         # one per review dimension
│   ├── goal-requirements.md            ├── performance.md
│   ├── architecture-modules.md         ├── security.md
│   ├── design-patterns-principles.md   ├── testing.md
│   ├── implementation-quality.md       ├── docs-developer-experience.md
│   ├── api-sdk-maven.md                └── open-source-readiness.md
│   ├── bugs-edge-cases.md
│   └── concurrency-lifecycle.md        # incl. messaging/streaming-specific items
└── templates/
    ├── finding.md                      # evidence-first issue record + calibration example
    ├── release-gate.md                 # GO / CONDITIONAL GO / NO-GO record
    ├── architecture-rewrite.md         # escalation when direction itself is wrong
    └── review-report.md                # mandatory final output skeleton
```

## Customization

- **Other stacks**: `api-sdk-maven.md` is JVM-first — swap registry/signing/transitive-dep items for npm/PyPI/Cargo equivalents; everything else is language-neutral.
- **Domain depth**: add project-specific items under "Domain-Specific" sections (e.g., delivery semantics for queues).
- **Stricter gates**: tighten `SKILL.md` §6 thresholds; the P0/P1 rules are intentionally conservative defaults.

## Philosophy

> Finding a defect today costs hours. Letting the internet find it after launch costs the launch.

Don't prove the feature works — find when it fails. Don't ask why the author built it this way — ask whether you'd build it this way from zero today.
