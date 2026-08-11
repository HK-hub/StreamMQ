---
name: Bug Report
about: Create a bug report to help us improve StreamMQ
title: "[BUG] "
labels: bug
assignees: ""
---

## Describe the Bug

A clear and concise description of what the bug is.

**Steps to Reproduce**

```java
// Minimal reproducible code
Message<String> message = MessageBuilder.<String>withTopic("test-topic")
        .body("test")
        .build();
template.syncSend(message);
```

**Expected Behavior**

A clear and concise description of what you expected to happen.

**Actual Behavior**

A clear and concise description of what actually happened.

## Environment

- **StreamMQ Version**: [e.g., 0.1.0]
- **Java Version**: [e.g., 21]
- **Spring Boot Version**: [e.g., 3.3.5]
- **Redis Version**: [e.g., 7.2]
- **Redisson Version**: [e.g., 3.34.1]
- **OS**: [e.g., Linux, Windows, macOS]
- **Deployment**: [e.g., Docker, Kubernetes, bare metal]

## Stack Trace

```
Paste the full stack trace or error log here.
```

## Additional Context

Add any other context about the problem here. Screenshots, configuration files, or
related issues are all helpful.