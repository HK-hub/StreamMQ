/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.message;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.streammq.core.enums.DelayLevel;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/** {@link Message} 单元测试：不可变值对象语义，覆盖构造、派生（withXxx/addXxx）、防御性拷贝、 延时/事务判断、属性视图与 toString。 */
@DisplayName("Message 消息载体测试")
class MessageTest {

    /** 标准测试消息工厂。 */
    private Message<String> sample() {
        return new Message<>(
                "topic",
                "tag",
                "keys",
                "shard",
                null,
                null,
                "body",
                null,
                null,
                1234567890L,
                "host:8080",
                null,
                0);
    }

    @Nested
    @DisplayName("全参构造")
    class FullConstructor {

        @Test
        @DisplayName("全参构造正确赋值所有字段")
        void fullConstructor_assignsAllFields() {
            Map<String, String> props = new HashMap<>();
            props.put("traceId", "t-1");
            Map<String, String> userProps = new HashMap<>();
            userProps.put("userKey", "userVal");

            Message<String> message =
                    new Message<>(
                            "topic",
                            "tag",
                            "keys",
                            "shard",
                            props,
                            userProps,
                            "body",
                            DelayLevel.SECOND_1,
                            100L,
                            1234567890L,
                            "host:8080",
                            "tx-1",
                            0);

            assertThat(message.getTopic()).isEqualTo("topic");
            assertThat(message.getTag()).isEqualTo("tag");
            assertThat(message.getKeys()).isEqualTo("keys");
            assertThat(message.getShardingKey()).isEqualTo("shard");
            assertThat(message.getBody()).isEqualTo("body");
            assertThat(message.getDelayLevel()).isEqualTo(DelayLevel.SECOND_1);
            assertThat(message.getDelayTimeMillis()).isEqualTo(100L);
            assertThat(message.getBornTimestamp()).isEqualTo(1234567890L);
            assertThat(message.getBornHost()).isEqualTo("host:8080");
            assertThat(message.getTransactionId()).isEqualTo("tx-1");
            assertThat(message.getProperties()).containsEntry("traceId", "t-1");
            assertThat(message.getUserProperties()).containsEntry("userKey", "userVal");
        }

        @Test
        @DisplayName("messageId 初始为 null，发送结果承载真实 ID")
        void fullConstructor_messageIdNull() {
            assertThat(sample().getMessageId()).isNull();
        }

        @Test
        @DisplayName("全参构造对 null properties/userProperties 创建空 Map")
        void fullConstructor_nullPropertiesBecomesEmpty() {
            Message<String> message =
                    new Message<>(
                            "topic", null, null, null, null, null, "body", null, null, 0L, null,
                            null, 0);
            assertThat(message.getProperties()).isEmpty();
            assertThat(message.getUserProperties()).isEmpty();
        }

        @Test
        @DisplayName("全参构造拷贝 properties，原始 Map 修改不影响消息")
        void fullConstructor_copiesPropertiesMap() {
            Map<String, String> props = new HashMap<>();
            props.put("k", "v");
            Message<String> message =
                    new Message<>(
                            "topic", null, null, null, props, null, "body", null, null, 0L, null,
                            null, 0);
            props.put("k2", "v2");
            assertThat(message.getProperties()).hasSize(1).containsEntry("k", "v");
        }

        @Test
        @DisplayName("topic 为 null 抛 NPE，空字符串抛 IAE")
        void fullConstructor_topicValidation() {
            assertThatThrownBy(
                            () ->
                                    new Message<>(
                                            null, null, null, null, null, null, "body", null, null,
                                            0L, null, null, 0))
                    .isInstanceOf(NullPointerException.class);
            assertThatThrownBy(
                            () ->
                                    new Message<>(
                                            " ", null, null, null, null, null, "body", null, null,
                                            0L, null, null, 0))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Nested
    @DisplayName("withXxx 派生方法")
    class DerivedInstances {

        @Test
        @DisplayName("每个 withXxx 返回新实例且只改变目标字段")
        void derivedMethods_returnNewInstanceAndChangeOnlyTargetField() {
            Message<String> base = sample();

            assertThat(base.withTopic("t2").getTopic()).isEqualTo("t2");
            assertThat(base.withTag("tag2").getTag()).isEqualTo("tag2");
            assertThat(base.withKeys("k2").getKeys()).isEqualTo("k2");
            assertThat(base.withShardingKey("s2").getShardingKey()).isEqualTo("s2");
            assertThat(base.withBody("b2").getBody()).isEqualTo("b2");
            assertThat(base.withDelayLevel(DelayLevel.MINUTE_1).getDelayLevel())
                    .isEqualTo(DelayLevel.MINUTE_1);
            assertThat(base.withDelayTimeMillis(500L).getDelayTimeMillis()).isEqualTo(500L);
            assertThat(base.withBornTimestamp(999L).getBornTimestamp()).isEqualTo(999L);
            assertThat(base.withBornHost("h2").getBornHost()).isEqualTo("h2");
            assertThat(base.withReconsumeTimes(3).getReconsumeTimes()).isEqualTo(3);
            assertThat(base.withTransactionId("tx").getTransactionId()).isEqualTo("tx");

            MessageId id = new MessageId("100-0");
            assertThat(base.withMessageId(id).getMessageId()).isEqualTo(id);

            // 原实例不受影响
            assertThat(base.getTag()).isEqualTo("tag");
            assertThat(base.getBody()).isEqualTo("body");
        }

        @Test
        @DisplayName("派生实例保留原实例的全部其他字段")
        void derived_preservesOtherFields() {
            Message<String> derived = sample().withTag("changed").withReconsumeTimes(5);
            assertThat(derived.getTopic()).isEqualTo("topic");
            assertThat(derived.getKeys()).isEqualTo("keys");
            assertThat(derived.getShardingKey()).isEqualTo("shard");
            assertThat(derived.getBornHost()).isEqualTo("host:8080");
            assertThat(derived.getReconsumeTimes()).isEqualTo(5);
        }
    }

    @Nested
    @DisplayName("isDelayMessage 延时消息判断")
    class IsDelayMessage {

        @Test
        @DisplayName("delayLevel 派生后返回 true")
        void withDelayLevel() {
            assertThat(sample().withDelayLevel(DelayLevel.SECOND_5).isDelayMessage()).isTrue();
        }

        @Test
        @DisplayName("delayTimeMillis 派生后返回 true")
        void withDelayTimeMillis() {
            assertThat(sample().withDelayTimeMillis(2000L).isDelayMessage()).isTrue();
        }

        @Test
        @DisplayName("两者均未设置时返回 false")
        void withoutAnyDelay() {
            assertThat(sample().isDelayMessage()).isFalse();
        }
    }

    @Nested
    @DisplayName("isTransactionMessage 事务消息判断")
    class IsTransactionMessage {

        @Test
        @DisplayName("transactionId 为 null 时返回 false")
        void nullTransactionId() {
            assertThat(sample().isTransactionMessage()).isFalse();
        }

        @Test
        @DisplayName("transactionId 为空字符串时返回 false")
        void emptyTransactionId() {
            assertThat(sample().withTransactionId("").isTransactionMessage()).isFalse();
        }

        @Test
        @DisplayName("transactionId 为非空字符串时返回 true")
        void nonEmptyTransactionId() {
            assertThat(sample().withTransactionId("tx-001").isTransactionMessage()).isTrue();
        }
    }

    @Nested
    @DisplayName("addProperty / addUserProperty 校验")
    class AddPropertyValidation {

        @Test
        @DisplayName("addProperty key 为 null 抛 NPE")
        void addProperty_nullKey() {
            assertThatThrownBy(() -> sample().addProperty(null, "v"))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("property key");
        }

        @Test
        @DisplayName("addUserProperty value 为 null 抛 NPE")
        void addUserProperty_nullValue() {
            assertThatThrownBy(() -> sample().addUserProperty("k", null))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("userProperty value");
        }

        @Test
        @DisplayName("addProperty 返回携带新属性的派生实例，原实例不变")
        void addProperty_normal() {
            Message<String> enriched = sample().addProperty("traceId", "t-001");
            assertThat(enriched.getProperties()).containsEntry("traceId", "t-001");
            assertThat(sample().getProperties()).isEmpty();
        }

        @Test
        @DisplayName("addUserProperty 正常写入")
        void addUserProperty_normal() {
            Message<String> enriched = sample().addUserProperty("bizKey", "bizVal");
            assertThat(enriched.getUserProperties()).containsEntry("bizKey", "bizVal");
        }
    }

    @Nested
    @DisplayName("不可修改视图")
    class UnmodifiableView {

        @Test
        @DisplayName("getProperties 返回不可修改视图，写入抛 UnsupportedOperationException")
        void getProperties_unmodifiable() {
            Message<String> message = sample().addProperty("k", "v");
            assertThatExceptionOfType(UnsupportedOperationException.class)
                    .isThrownBy(() -> message.getProperties().put("new", "val"));
        }

        @Test
        @DisplayName("getUserProperties 返回不可修改视图，写入抛 UnsupportedOperationException")
        void getUserProperties_unmodifiable() {
            Message<String> message = sample().addUserProperty("k", "v");
            assertThatExceptionOfType(UnsupportedOperationException.class)
                    .isThrownBy(() -> message.getUserProperties().put("new", "val"));
        }
    }

    @Nested
    @DisplayName("toString")
    class ToString {

        @Test
        @DisplayName("toString 包含 topic/tag/keys/messageId/bornTimestamp 等关键字段")
        void toString_containsKeyFields() {
            Message<String> message =
                    new Message<>(
                            "order-topic",
                            "created",
                            "order-123",
                            null,
                            null,
                            null,
                            "hello",
                            null,
                            null,
                            1000L,
                            null,
                            "tx-1",
                            0);

            String str = message.toString();
            assertThat(str).contains("topic='order-topic'");
            assertThat(str).contains("tag='created'");
            assertThat(str).contains("keys='order-123'");
            assertThat(str).contains("bornTimestamp=1000");
            assertThat(str).contains("transactionId='tx-1'");
            assertThat(str).startsWith("Message{");
        }
    }
}
