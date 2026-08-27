## What Type of Change is This?

- [ ] Bug fix (non-breaking change which fixes an issue)
- [ ] New feature (non-breaking change which adds functionality)
- [ ] Breaking change (fix or feature that would cause existing functionality to not work as expected)
- [ ] This change requires a documentation update

## What is the Current Behavior?

Please describe the current behavior that you are modifying, or link to a relevant issue.

Fixes # (issue)

## What is the New Behavior?

Please describe the new behavior or feature introduced by this PR.

## Does This Introduce a Breaking Change?

- [ ] Yes
- [ ] No

If yes, please describe the impact and migration path for existing applications:

## How Has This Been Tested?

Please describe the tests that you ran to verify your changes. Provide instructions
so we can reproduce. Please also list any relevant details for your test configuration.

- [ ] `mvn test` (unit tests pass)
- [ ] `mvn verify`（本地运行需本地 Redis） (integration tests pass)
- [ ] `mvn spotless:check` (code formatting check passes)
- [ ] `mvn enforcer:enforce` (enforcer rules pass)

**Test Configuration**:
- JDK: 21
- OS: [e.g., Linux, Windows]
- Redis: [e.g., 7.2]

## Screenshots (If Applicable)

Add screenshots to help explain your changes.

## Checklist

- [ ] My code follows the style guidelines of this project (Google Java Format via Spotless)
- [ ] I have performed a self-review of my own code
- [ ] I have commented my code, particularly in hard-to-understand areas
- [ ] I have made corresponding changes to the documentation
- [ ] My changes generate no new warnings
- [ ] I have added tests that prove my fix is effective or that my feature works
- [ ] New and existing unit tests pass locally with my changes
- [ ] Any dependent changes have been merged and published in downstream modules

## Additional Notes

Add any other context or screenshots about the pull request here.