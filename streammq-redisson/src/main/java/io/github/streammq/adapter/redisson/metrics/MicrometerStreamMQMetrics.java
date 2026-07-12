package io.github.streammq.adapter.redisson.metrics;

import io.github.streammq.core.metrics.StreamMQMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;

import java.time.Duration;
import java.util.Objects;

/**
 * StreamMQ Micrometer 指标收集器。
 *
 * <p>指标列表：
 * <ul>
 *   <li>streammq_send_total{topic,success} — 发送总数</li>
 *   <li>streammq_send_duration{topic} — 发送耗时</li>
 *   <li>streammq_consume_total{topic,group,success} — 消费总数</li>
 *   <li>streammq_consume_duration{topic,group} — 消费耗时</li>
 *   <li>streammq_retry_total{topic,group} — 重试总数</li>
 *   <li>streammq_dlq_total{topic,group} — 死信总数</li>
 *   <li>streammq_delay_total{level} — 延时消息投递总数</li>
 *   <li>streammq_transaction_commit_total{group} — 事务提交总数</li>
 *   <li>streammq_transaction_rollback_total{group} — 事务回滚总数</li>
 *   <li>streammq_transaction_check_total{group,result} — 事务回查总数</li>
 * </ul>
 */
public class MicrometerStreamMQMetrics implements StreamMQMetrics {
    public static final String METRIC_SEND_TOTAL = "streammq.send.total";
    public static final String METRIC_SEND_DURATION = "streammq.send.duration";
    public static final String METRIC_CONSUME_TOTAL = "streammq.consume.total";
    public static final String METRIC_CONSUME_DURATION = "streammq.consume.duration";
    public static final String METRIC_RETRY_TOTAL = "streammq.retry.total";
    public static final String METRIC_DLQ_TOTAL = "streammq.dlq.total";
    public static final String METRIC_DELAY_TOTAL = "streammq.delay.total";
    public static final String METRIC_TX_COMMIT_TOTAL = "streammq.transaction.commit.total";
    public static final String METRIC_TX_ROLLBACK_TOTAL = "streammq.transaction.rollback.total";
    public static final String METRIC_TX_CHECK_TOTAL = "streammq.transaction.check.total";

    private final MeterRegistry registry;

    public MicrometerStreamMQMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    @Override
    public void recordSend(String topic, boolean success, Duration duration) {
        if (Objects.isNull(registry)) return;
        registry.counter(METRIC_SEND_TOTAL, Tags.of("topic", topic, "success", String.valueOf(success))).increment();
        registry.timer(METRIC_SEND_DURATION, Tags.of("topic", topic)).record(duration);
    }

    @Override
    public void recordConsume(String topic, String group, boolean success, Duration duration) {
        if (Objects.isNull(registry)) return;
        registry.counter(METRIC_CONSUME_TOTAL, Tags.of("topic", topic, "group", group, "success", String.valueOf(success))).increment();
        registry.timer(METRIC_CONSUME_DURATION, Tags.of("topic", topic, "group", group)).record(duration);
    }

    @Override
    public void recordRetry(String topic, String group) {
        if (Objects.isNull(registry)) return;
        registry.counter(METRIC_RETRY_TOTAL, Tags.of("topic", topic, "group", group)).increment();
    }

    @Override
    public void recordDlq(String topic, String group) {
        if (Objects.isNull(registry)) return;
        registry.counter(METRIC_DLQ_TOTAL, Tags.of("topic", topic, "group", group)).increment();
    }

    @Override
    public void recordDelayDelivery(String level) {
        if (Objects.isNull(registry)) return;
        registry.counter(METRIC_DELAY_TOTAL, Tags.of("level", level)).increment();
    }

    @Override
    public void recordTransactionCommit(String group) {
        if (Objects.isNull(registry)) return;
        registry.counter(METRIC_TX_COMMIT_TOTAL, Tags.of("group", group)).increment();
    }

    @Override
    public void recordTransactionRollback(String group) {
        if (Objects.isNull(registry)) return;
        registry.counter(METRIC_TX_ROLLBACK_TOTAL, Tags.of("group", group)).increment();
    }

    @Override
    public void recordTransactionCheck(String group, String result) {
        if (Objects.isNull(registry)) return;
        registry.counter(METRIC_TX_CHECK_TOTAL, Tags.of("group", group, "result", result)).increment();
    }
}
