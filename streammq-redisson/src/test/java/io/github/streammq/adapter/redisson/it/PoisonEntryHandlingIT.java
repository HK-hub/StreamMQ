/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.it;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.streammq.adapter.redisson.converter.DefaultMessageConverter;
import io.github.streammq.adapter.redisson.listener.RedissonStreamListener;
import io.github.streammq.adapter.redisson.producer.RedissonStreamProducer;
import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import io.github.streammq.core.enums.ConsumeFromWhere;
import io.github.streammq.core.enums.DlqReason;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageBuilder;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.redisson.api.RMap;
import org.redisson.api.RStream;
import org.redisson.api.RedissonClient;
import org.redisson.api.StreamMessageId;
import org.redisson.api.stream.StreamAddArgs;
import org.redisson.api.stream.StreamCreateGroupArgs;
import org.redisson.client.codec.StringCodec;

/**
 * 毒丸条目（poison entry）隔离集成测试（红队 U-03 缺口）。
 *
 * <p>毒丸产生条件：Entry 由非 StreamMQ 发送方直接写入（绕过 producer 的 {@code toStreamFields}）， 使 {@code
 * converter.fromStreamFields} 抛运行时异常——本测试用<b>类型错误</b>的 {@code retryTimes} 字段制造该异常 （{@code
 * Integer.parseInt("not-a-number")} → {@code SerializationException}）。
 *
 * <p>覆盖 {@code RedissonStreamListener#handlePoisonEntry} 的两条设计语义：
 *
 * <ul>
 *   <li>转存 DLQ 成功：毒丸携带 {@code dlqReason=deserialize} 进入 DLQ Stream 并被 ACK，后续合法消息不受阻断
 *   <li>转存 DLQ 失败：毒丸<b>不 ACK</b>，保留在 PEL 等待重投（宁可重复隔离也不静默丢失）
 * </ul>
 */
@DisplayName("毒丸条目隔离集成测试")
class PoisonEntryHandlingIT extends AbstractRedisIT {

    private static final String TOPIC = "poison-entry-topic";
    private static final String GROUP = "poison-entry-group";
    private static final String CONSUMER_NAME = "poison-entry-consumer";

    @Test
    @DisplayName("毒丸转存 DLQ 成功：合法消息仍被消费，毒丸已 ACK 且 DLQ 记录原因与原始 entryId")
    void poisonEntry_routedToDlq_validMessageStillConsumed() {
        RedissonStreamListener consumer =
                new RedissonStreamListener(
                        redisson, namespace, TOPIC, GROUP, CONSUMER_NAME, converter);
        RedissonStreamProducer producer =
                new RedissonStreamProducer(
                        redisson, namespace, GROUP + "-p", converter, 3000L, 0, 0, 0);
        try {
            createConsumerGroup(TOPIC, GROUP);
            RStream<String, String> topicStream = topicStream();

            // 1) 绕过 producer 直写毒丸：retryTimes 非数字 → fromStreamFields 抛 SerializationException
            StreamMessageId poisonId = addPoisonEntry(topicStream);

            // 2) 同一流内随后写入一条合法消息
            producer.syncSend(MessageBuilder.<String>withTopic(TOPIC).body("valid-body").build());

            List<Message<?>> messages = consumer.pull(10);

            // 毒丸不混入消费结果，合法消息在同一读循环内被正常投递（读循环未被阻断）
            assertThat(messages).hasSize(1);
            assertThat(messages.get(0).getBody()).isEqualTo("valid-body");

            // 毒丸被转存 DLQ，携带原因码与原始 entryId
            RStream<String, String> dlqStream =
                    redisson.getStream(StreamMQKeys.dlqStream(namespace, GROUP));
            Map<StreamMessageId, Map<String, String>> dlqEntries =
                    dlqStream.range(StreamMessageId.MIN, StreamMessageId.MAX);
            assertThat(dlqEntries).hasSize(1);
            Map<String, String> dlqFields = dlqEntries.values().iterator().next();
            assertThat(dlqFields)
                    .containsEntry("dlqReason", DlqReason.DESERIALIZE.getCode())
                    .containsEntry("dlqEntryId", poisonId.toString())
                    .containsEntry(DefaultMessageConverter.FIELD_RETRY_TIMES, "not-a-number");

            // 转存成功后毒丸被 ACK：仅合法消息留在 PEL（本条 pull 尚未 ACK）
            assertThat(
                            topicStream.listPending(
                                    GROUP, StreamMessageId.MIN, StreamMessageId.MAX, 100))
                    .hasSize(1);

            consumer.ackBatch(List.of(messages.get(0).getMessageId()));
            assertThat(
                            topicStream.listPending(
                                    GROUP, StreamMessageId.MIN, StreamMessageId.MAX, 100))
                    .isEmpty();
        } finally {
            consumer.close();
            producer.close();
        }
    }

    @Test
    @DisplayName("毒丸转存 DLQ 失败：毒丸不 ACK、保留在 PEL，合法消息不受影响")
    @SuppressWarnings("unchecked")
    void poisonEntry_dlqWriteFails_entryStaysInPel() {
        String dlqKey = StreamMQKeys.dlqStream(namespace, GROUP);
        // 仅让 DLQ Stream 的写入失败：其它 key 仍走真实 Redis
        RedissonClient dlqFailingClient = Mockito.spy(redisson);
        RStream<String, String> failingDlqStream = Mockito.mock(RStream.class);
        Mockito.doThrow(new RuntimeException("simulated DLQ write failure"))
                .when(failingDlqStream)
                .add(Mockito.any(StreamAddArgs.class));
        Mockito.doReturn(failingDlqStream)
                .when(dlqFailingClient)
                .getStream(dlqKey, StringCodec.INSTANCE);

        RedissonStreamListener consumer =
                new RedissonStreamListener(
                        dlqFailingClient, namespace, TOPIC, GROUP, CONSUMER_NAME, converter);
        RedissonStreamProducer producer =
                new RedissonStreamProducer(
                        redisson, namespace, GROUP + "-p", converter, 3000L, 0, 0, 0);
        try {
            createConsumerGroup(TOPIC, GROUP);
            RStream<String, String> topicStream = topicStream();
            StreamMessageId poisonId = addPoisonEntry(topicStream);
            producer.syncSend(MessageBuilder.<String>withTopic(TOPIC).body("valid-body").build());

            List<Message<?>> messages = consumer.pull(10);

            assertThat(messages).hasSize(1);
            assertThat(messages.get(0).getBody()).isEqualTo("valid-body");

            // 毒丸未 ACK：仍在流中且留在 PEL（未静默丢失），等待下一轮重投/人工排查
            assertThat(topicStream.size()).isEqualTo(2L);
            assertThat(
                            topicStream.listPending(
                                    GROUP, StreamMessageId.MIN, StreamMessageId.MAX, 100))
                    // 毒丸未 ACK + 本条 pull 返回的合法消息也未 ACK
                    .hasSize(2)
                    .anySatisfy(
                            pending ->
                                    assertThat(pending.getId().toString())
                                            .isEqualTo(poisonId.toString()));
            // DLQ 未写入任何内容
            assertThat(redisson.getStream(dlqKey).size()).isZero();
        } finally {
            consumer.close();
            producer.close();
        }
    }

    // ===================== 红队第六轮 R1-3：DLQ 模式毒丸不得裸 ACK =====================

    @Test
    @DisplayName("R1-3:DLQ 模式毒丸先隔离落盘(原始字段可恢复)再 ACK，合法消息继续消费")
    void dlqModePoison_isQuarantinedBeforeAck() {
        RStream<String, String> dlqStream =
                redisson.getStream(StreamMQKeys.dlqStream(namespace, GROUP));
        dlqStream.createGroup(
                StreamCreateGroupArgs.name(GROUP).makeStream().id(new StreamMessageId(0, 0)));

        Map<String, String> poisonFields = new LinkedHashMap<>();
        poisonFields.put(DefaultMessageConverter.FIELD_BODY, "dlq-poison-body");
        poisonFields.put(DefaultMessageConverter.FIELD_RETRY_TIMES, "not-a-number");
        StreamMessageId poisonId = dlqStream.add(StreamAddArgs.entries(poisonFields));

        Map<String, String> validFields = new LinkedHashMap<>();
        validFields.put(DefaultMessageConverter.FIELD_BODY, "dlq-valid-body");
        dlqStream.add(StreamAddArgs.entries(validFields));

        // DLQ 模式监听器：topic=group，其余参数按默认值处理
        RedissonStreamListener consumer =
                new RedissonStreamListener(
                        redisson,
                        namespace,
                        GROUP,
                        GROUP,
                        CONSUMER_NAME,
                        converter,
                        true, // dlqMode
                        false,
                        false,
                        null,
                        ConsumeFromWhere.DEFAULT,
                        null,
                        null);
        try {
            List<Message<?>> messages = consumer.pull(10);

            // 合法消息仍被正常投递（毒丸不阻断读循环）
            assertThat(messages).hasSize(1);
            assertThat(messages.get(0).getBody()).isEqualTo("dlq-valid-body");

            // 毒丸进入二级隔离区：原始字段可恢复（不再是"只剩字段名的日志 + 裸 ACK 丢弃"）
            RMap<String, String> quarantine =
                    redisson.getMap(
                            StreamMQKeys.quarantinePayloadHash(
                                    namespace, GROUP, poisonId.toString()),
                            StringCodec.INSTANCE);
            assertThat(quarantine.readAllMap())
                    .as("隔离区必须保留死信原始字段（可排查/可重放）")
                    .containsEntry(DefaultMessageConverter.FIELD_BODY, "dlq-poison-body")
                    .containsEntry(DefaultMessageConverter.FIELD_RETRY_TIMES, "not-a-number")
                    .containsEntry("dlqReason", DlqReason.DESERIALIZE.getCode())
                    .containsEntry("dlqEntryId", poisonId.toString());
            assertThat(
                            redisson.getScoredSortedSet(
                                            StreamMQKeys.quarantineZset(namespace, "dlq-poison"),
                                            StringCodec.INSTANCE)
                                    .size())
                    .as("隔离区 ZSet 必须登记该死信")
                    .isGreaterThanOrEqualTo(1);

            // 隔离落盘成功后才 ACK：PEL 只剩本条 pull 返回、尚未 ACK 的合法消息
            assertThat(dlqStream.listPending(GROUP, StreamMessageId.MIN, StreamMessageId.MAX, 100))
                    .hasSize(1)
                    .noneSatisfy(
                            pending ->
                                    assertThat(pending.getId().toString())
                                            .isEqualTo(poisonId.toString()));
            consumer.ackBatch(List.of(messages.get(0).getMessageId()));
            assertThat(dlqStream.listPending(GROUP, StreamMessageId.MIN, StreamMessageId.MAX, 100))
                    .isEmpty();
        } finally {
            consumer.close();
        }
    }

    // ===================== 夹具 =====================

    private RStream<String, String> topicStream() {
        return redisson.getStream(StreamMQKeys.topicStream(namespace, TOPIC));
    }

    /**
     * 直写一条"类型错误"的毒丸 Entry（绕过 producer）。
     *
     * <p>{@code retryTimes} 非数字是 {@code fromStreamFields} 中最确定的抛错点之一： 缺失字段会被容错跳过（不构成毒丸），而类型错误 无法降级
     * → 抛出 {@code SerializationException}（继承自 RuntimeException）→ 命中毒丸分支。
     */
    private StreamMessageId addPoisonEntry(RStream<String, String> topicStream) {
        Map<String, String> poisonFields = new LinkedHashMap<>();
        poisonFields.put(DefaultMessageConverter.FIELD_BODY, "poison-body");
        poisonFields.put(DefaultMessageConverter.FIELD_RETRY_TIMES, "not-a-number");
        return topicStream.add(StreamAddArgs.entries(poisonFields));
    }
}
