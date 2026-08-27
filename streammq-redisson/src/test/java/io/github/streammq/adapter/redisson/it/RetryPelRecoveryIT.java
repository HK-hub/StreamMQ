/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.streammq.adapter.redisson.converter.DefaultMessageConverter;
import io.github.streammq.adapter.redisson.scheduler.PelClaimScheduler;
import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import io.github.streammq.core.enums.DlqReason;
import io.github.streammq.core.message.MessageBuilder;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.redisson.api.RStream;
import org.redisson.api.StreamMessageId;
import org.redisson.api.stream.StreamAddArgs;
import org.redisson.api.stream.StreamCreateGroupArgs;
import org.redisson.api.stream.StreamReadGroupArgs;

/**
 * RETRY/DLQ PEL 恢复回归测试（发布前红队审查 R2）。
 *
 * <p>历史缺陷：PelClaim 仅扫描业务流；绑定器跳过 DLQ 组、未注册重试流目标；且消费者名含容器随机 token， 实例重启后自身 PEL 排空读不到遗留条目——重试流与死信流的
 * pending 永久卡死。
 *
 * <p>本测试以合成消费者名（模拟崩溃实例的遗留）制造 pending，验证调度器对三类流的恢复。
 */
@DisplayName("Retry/DLQ PEL 恢复集成测试")
class RetryPelRecoveryIT extends AbstractRedisIT {

    private Map<String, String> entryFields(String body, int retryTimes) {
        Map<String, String> fields =
                converter.toStreamFields(MessageBuilder.<String>withTopic("t").body(body).build());
        fields.put(DefaultMessageConverter.FIELD_RETRY_TIMES, Integer.toString(retryTimes));
        return fields;
    }

    /** 在指定流上以合成消费者制造一条不 ACK 的 pending（模拟崩溃实例遗留）。 */
    private void seedPending(
            String streamKey, String group, String ghostConsumer, Map<String, String> fields) {
        RStream<String, String> stream = redisson.getStream(streamKey);
        stream.add(StreamAddArgs.entries(fields));
        stream.createGroup(
                StreamCreateGroupArgs.name(group).makeStream().id(new StreamMessageId(0, 0)));
        stream.readGroup(group, ghostConsumer, StreamReadGroupArgs.neverDelivered().count(10));
    }

    @Test
    @DisplayName("RETRY 种类：滞留条目尾部复制重投，旧条目 ACK，计数字段保持")
    void retryPending_copiedToTailAndAcked() {
        String topic = "rp-topic";
        String group = "rp-group";
        String retryKey = StreamMQKeys.retryStream(namespace, topic, group);
        RStream<String, String> retryStream = redisson.getStream(retryKey);

        seedPending(retryKey, group, "ghost-c1", entryFields("retry-body", 1));

        PelClaimScheduler scheduler = new PelClaimScheduler(redisson, namespace, 100, 32, 50);
        scheduler.registerRetryStreamTarget(topic, group, 16);
        scheduler.start();
        try {
            // 尾部复制：流中出现第二条（新 ID），旧条目被 ACK 移出 PEL
            await().atMost(10, TimeUnit.SECONDS).until(() -> retryStream.size() == 2);

            await().atMost(5, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    assertThat(
                                                    retryStream.listPending(
                                                            group,
                                                            StreamMessageId.MIN,
                                                            StreamMessageId.MAX,
                                                            100))
                                            .isEmpty());

            // 复制出的条目字段原样保留：body 与 retryTimes 计数不变（消费循环按 '>' 重新处理）
            var all = retryStream.range(StreamMessageId.MIN, StreamMessageId.MAX);
            assertThat(all).hasSize(2);
            Map<String, String> copied =
                    all.values().stream()
                            .filter(f -> decodedBody(f).equals("retry-body"))
                            .findFirst()
                            .orElseThrow();
            assertThat(copied.get(DefaultMessageConverter.FIELD_RETRY_TIMES)).isEqualTo("1");
        } finally {
            scheduler.stop();
        }
    }

    @Test
    @DisplayName("RETRY 种类：超过 maxReconsumeTimes 的滞留条目转投 DLQ 并 ACK")
    void retryPendingOverMax_landsInDlq() {
        String topic = "rp-max-topic";
        String group = "rp-max-group";
        String retryKey = StreamMQKeys.retryStream(namespace, topic, group);
        String dlqKey = StreamMQKeys.dlqStream(namespace, group);
        RStream<String, String> retryStream = redisson.getStream(retryKey);
        RStream<String, String> dlqStream = redisson.getStream(dlqKey);

        seedPending(retryKey, group, "ghost-c2", entryFields("doomed-body", 16));

        PelClaimScheduler scheduler = new PelClaimScheduler(redisson, namespace, 100, 32, 50);
        scheduler.registerRetryStreamTarget(topic, group, 16);
        scheduler.start();
        try {
            await().atMost(10, TimeUnit.SECONDS).until(() -> dlqStream.size() == 1);
            await().atMost(5, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    assertThat(
                                                    retryStream.listPending(
                                                            group,
                                                            StreamMessageId.MIN,
                                                            StreamMessageId.MAX,
                                                            100))
                                            .isEmpty());

            Map<String, String> dlqEntry =
                    dlqStream
                            .range(StreamMessageId.MIN, StreamMessageId.MAX)
                            .values()
                            .iterator()
                            .next();
            assertThat(dlqEntry.get(RetrySchedulerField.DLQ_REASON))
                    .isEqualTo(DlqReason.MAX_RETRY.getCode());
            assertThat(decodedBody(dlqEntry)).isEqualTo("doomed-body");
        } finally {
            scheduler.stop();
        }
    }

    @Test
    @DisplayName("DLQ 种类：滞留死信条目尾部复制重投并 ACK（终局语义，无上限判定）")
    void dlqPending_copiedToTailAndAcked() {
        String group = "rpd-group";
        String dlqKey = StreamMQKeys.dlqStream(namespace, group);
        RStream<String, String> dlqStream = redisson.getStream(dlqKey);

        // 死信条目带任意计数字段也一律原样复制（失败策略字段随行约束循环）
        Map<String, String> fields = entryFields("dead-body", 99);
        fields.put(RetrySchedulerField.ORIGINAL_RETRY_COUNT, "99");
        seedPending(dlqKey, group, "ghost-c3", fields);

        PelClaimScheduler scheduler = new PelClaimScheduler(redisson, namespace, 100, 32, 50);
        scheduler.registerDlqTarget(group, group);
        scheduler.start();
        try {
            await().atMost(10, TimeUnit.SECONDS).until(() -> dlqStream.size() == 2);
            await().atMost(5, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    assertThat(
                                                    dlqStream.listPending(
                                                            group,
                                                            StreamMessageId.MIN,
                                                            StreamMessageId.MAX,
                                                            100))
                                            .isEmpty());
        } finally {
            scheduler.stop();
        }
    }

    /** 字段名常量中转（避免测试类与主代码常量来源纠缠）。 */
    private static final class RetrySchedulerField {
        static final String DLQ_REASON = io.github.streammq.core.StreamMQConstants.FIELD_DLQ_REASON;
        static final String ORIGINAL_RETRY_COUNT = "originalRetryCount";
    }

    /**
     * 解码流条目的 body 字段：转换器以 Base64(Jackson JSON) 存储，Jackson 对 String 序列化带引号 （如 {@code
     * ImRvb21lZC1ib2R5Ig==} → {@code "doomed-body"}），此处还原为裸业务值。
     */
    private static String decodedBody(Map<String, String> fields) {
        String encoded = fields.get("body");
        if (encoded == null) {
            return null;
        }
        String json =
                new String(
                        java.util.Base64.getDecoder().decode(encoded),
                        java.nio.charset.StandardCharsets.UTF_8);
        return json.startsWith("\"") && json.endsWith("\"") && json.length() >= 2
                ? json.substring(1, json.length() - 1)
                : json;
    }
}
