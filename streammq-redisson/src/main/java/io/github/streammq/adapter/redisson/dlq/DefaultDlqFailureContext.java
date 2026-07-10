package io.github.streammq.adapter.redisson.dlq;

import io.github.streammq.core.policy.DlqFailureContext;
import io.github.streammq.core.policy.DlqConfig;

import java.util.Collections;
import java.util.Map;

/**
 * {@link DlqFailureContext} 默认实现。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class DefaultDlqFailureContext implements DlqFailureContext {

    private final int dlqAttempts;
    private final String dlqReason;
    private final String originalTopic;
    private final String originalMessageId;
    private final Throwable lastFailureCause;
    private final Map<String, String> dlqFields;
    private final int maxDlqRetryAttempts;
    private final long dlqRetryDelayMs;

    public DefaultDlqFailureContext(int dlqAttempts, String dlqReason, String originalTopic,
                                    String originalMessageId, Throwable lastFailureCause,
                                    Map<String, String> dlqFields, int maxDlqRetryAttempts, long dlqRetryDelayMs) {
        this.dlqAttempts = dlqAttempts;
        this.dlqReason = dlqReason;
        this.originalTopic = originalTopic;
        this.originalMessageId = originalMessageId;
        this.lastFailureCause = lastFailureCause;
        this.dlqFields = dlqFields;
        this.maxDlqRetryAttempts = maxDlqRetryAttempts;
        this.dlqRetryDelayMs = dlqRetryDelayMs;
    }

    @Override public int dlqAttempts() { return dlqAttempts; }
    @Override public int maxDlqRetryAttempts() { return maxDlqRetryAttempts; }
    @Override public String dlqReason() { return dlqReason; }
    @Override public String originalTopic() { return originalTopic; }
    @Override public String originalMessageId() { return originalMessageId; }
    @Override public Throwable lastFailureCause() { return lastFailureCause; }
    @Override public long dlqRetryDelayMs() { return dlqRetryDelayMs; }
    @Override
    public Map<String, String> dlqFields() {
        return dlqFields != null ? Collections.unmodifiableMap(dlqFields) : Collections.emptyMap();
    }
}
