/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.message;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.streammq.core.enums.DelayLevel;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/** {@link MessageBuilder} 单元测试，覆盖静态工厂、链式方法、with* 别名、必填校验与默认值。 */
@DisplayName("MessageBuilder 流式构造器测试")
class MessageBuilderTest {

    @Nested
    @DisplayName("静态工厂方法")
    class StaticFactories {

        @Test
        @DisplayName("withTopic 创建带 topic 的 Builder 并可链式构造消息")
        void withTopic_chain() {
            Message<String> message =
                    MessageBuilder.<String>withTopic("order-topic")
                            .tag("created")
                            .keys("order-123")
                            .body("payload")
                            .build();
            assertThat(message.getTopic()).isEqualTo("order-topic");
            assertThat(message.getTag()).isEqualTo("created");
            assertThat(message.getKeys()).isEqualTo("order-123");
            assertThat(message.getBody()).isEqualTo("payload");
        }

        @Test
        @DisplayName("withPayload 创建带 body 的 Builder")
        void withPayload() {
            Message<String> message =
                    MessageBuilder.<String>withPayload("body-content").topic("topic").build();
            assertThat(message.getBody()).isEqualTo("body-content");
            assertThat(message.getTopic()).isEqualTo("topic");
        }

        @Test
        @DisplayName("create 创建空 Builder")
        void create() {
            MessageBuilder<String> builder = MessageBuilder.create();
            Message<String> message = builder.topic("t").body("b").build();
            assertThat(message.getTopic()).isEqualTo("t");
            assertThat(message.getBody()).isEqualTo("b");
        }
    }

    @Nested
    @DisplayName("with* 别名方法")
    class WithAliasMethods {

        @Test
        @DisplayName("tag 设置标签")
        void withTag() {
            Message<String> message =
                    MessageBuilder.<String>withTopic("t").tag("tag1").body("b").build();
            assertThat(message.getTag()).isEqualTo("tag1");
        }

        @Test
        @DisplayName("keys 设置业务键")
        void withKeys() {
            Message<String> message =
                    MessageBuilder.<String>withTopic("t").keys("k1").body("b").build();
            assertThat(message.getKeys()).isEqualTo("k1");
        }

        @Test
        @DisplayName("shardingKey 设置分片键")
        void withShardingKey() {
            Message<String> message =
                    MessageBuilder.<String>withTopic("t").shardingKey("shard-1").body("b").build();
            assertThat(message.getShardingKey()).isEqualTo("shard-1");
        }

        @Test
        @DisplayName("body 设置消息体")
        void withBody() {
            Message<String> message =
                    MessageBuilder.<String>withTopic("t").body("body-val").build();
            assertThat(message.getBody()).isEqualTo("body-val");
        }

        @Test
        @DisplayName("withProperty 添加单条系统属性")
        void withProperty() {
            Message<String> message =
                    MessageBuilder.<String>withTopic("t")
                            .withProperty("traceId", "t-1")
                            .body("b")
                            .build();
            assertThat(message.getProperties()).containsEntry("traceId", "t-1");
        }

        @Test
        @DisplayName("withUserProperty 添加单条用户属性")
        void withUserProperty() {
            Message<String> message =
                    MessageBuilder.<String>withTopic("t")
                            .withUserProperty("biz", "val")
                            .body("b")
                            .build();
            assertThat(message.getUserProperties()).containsEntry("biz", "val");
        }

        @Test
        @DisplayName("delayLevel 设置延时级别")
        void withDelayLevel() {
            Message<String> message =
                    MessageBuilder.<String>withTopic("t")
                            .delayLevel(DelayLevel.SECOND_10)
                            .body("b")
                            .build();
            assertThat(message.getDelayLevel()).isEqualTo(DelayLevel.SECOND_10);
            assertThat(message.isDelayMessage()).isTrue();
        }

        @Test
        @DisplayName("delayTimeMillis 设置延时毫秒")
        void withDelayTimeMillis() {
            Message<String> message =
                    MessageBuilder.<String>withTopic("t").delayTimeMillis(3000L).body("b").build();
            assertThat(message.getDelayTimeMillis()).isEqualTo(3000L);
            assertThat(message.isDelayMessage()).isTrue();
        }
    }

    @Nested
    @DisplayName("properties 批量设置")
    class WithProperties {

        @Test
        @DisplayName("properties 批量设置系统属性")
        void withProperties_batch() {
            Map<String, String> props = new LinkedHashMap<>();
            props.put("a", "1");
            props.put("b", "2");
            Message<String> message =
                    MessageBuilder.<String>withTopic("t").properties(props).body("b").build();
            assertThat(message.getProperties())
                    .containsEntry("a", "1")
                    .containsEntry("b", "2")
                    .hasSize(2);
        }

        @Test
        @DisplayName("properties 传入 null 不影响已有属性")
        void properties_nullIgnored() {
            Message<String> message =
                    MessageBuilder.<String>withTopic("t")
                            .withProperty("k", "v")
                            .properties(null)
                            .body("b")
                            .build();
            assertThat(message.getProperties()).containsOnlyKeys("k");
        }
    }

    @Nested
    @DisplayName("build 必填校验")
    class BuildValidation {

        @Test
        @DisplayName("topic 为 null 抛 NPE")
        void build_nullTopic() {
            assertThatThrownBy(() -> MessageBuilder.<String>create().body("b").build())
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("topic");
        }

        @Test
        @DisplayName("topic 为空字符串抛 IllegalArgumentException")
        void build_emptyTopic() {
            assertThatThrownBy(() -> MessageBuilder.<String>create().topic("").body("b").build())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("topic must not be empty");
        }

        @Test
        @DisplayName("body 为 null 抛 NPE")
        void build_nullBody() {
            assertThatThrownBy(() -> MessageBuilder.<String>create().topic("t").build())
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("body");
        }
    }

    @Nested
    @DisplayName("topic / tag 处理")
    class TopicTagHandling {

        @Test
        @DisplayName("topic() 自动 trim 去除前后空白")
        void topic_trims() {
            Message<String> message =
                    MessageBuilder.<String>create().topic("  spaced-topic  ").body("b").build();
            assertThat(message.getTopic()).isEqualTo("spaced-topic");
        }

        @Test
        @DisplayName("tag() 传 null 时为 null")
        void tag_nullStaysNull() {
            Message<String> message =
                    MessageBuilder.<String>withTopic("t").tag(null).body("b").build();
            assertThat(message.getTag()).isNull();
        }

        @Test
        @DisplayName("tag() 传非 null 时自动 trim")
        void tag_trims() {
            Message<String> message =
                    MessageBuilder.<String>withTopic("t").tag("  tag-trimmed  ").body("b").build();
            assertThat(message.getTag()).isEqualTo("tag-trimmed");
        }
    }

    @Nested
    @DisplayName("delayTimeMillis 校验")
    class DelayTimeMillisValidation {

        @Test
        @DisplayName("delayTimeMillis 传 0 抛 IllegalArgumentException")
        void delayTimeMillis_zero() {
            assertThatThrownBy(() -> MessageBuilder.<String>withTopic("t").delayTimeMillis(0))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("must be positive");
        }

        @Test
        @DisplayName("delayTimeMillis 传负数抛 IllegalArgumentException")
        void delayTimeMillis_negative() {
            assertThatThrownBy(() -> MessageBuilder.<String>withTopic("t").delayTimeMillis(-100))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("must be positive");
        }
    }

    @Nested
    @DisplayName("默认值填充")
    class Defaults {

        @Test
        @DisplayName("bornTimestamp 未设置时自动填入当前时间")
        void bornTimestamp_autoFill() {
            long before = System.currentTimeMillis();
            Message<String> message = MessageBuilder.<String>withTopic("t").body("b").build();
            long after = System.currentTimeMillis();
            assertThat(message.getBornTimestamp()).isBetween(before, after);
        }

        @Test
        @DisplayName("bornHost 未设置时为 unknown")
        void bornHost_defaultUnknown() {
            Message<String> message = MessageBuilder.<String>withTopic("t").body("b").build();
            assertThat(message.getBornHost()).isEqualTo("unknown");
        }

        @Test
        @DisplayName("bornTimestamp 显式设置时不被覆盖")
        void bornTimestamp_explicitKept() {
            Message<String> message =
                    MessageBuilder.<String>withTopic("t").bornTimestamp(42L).body("b").build();
            assertThat(message.getBornTimestamp()).isEqualTo(42L);
        }

        @Test
        @DisplayName("bornHost 显式设置时不被覆盖")
        void bornHost_explicitKept() {
            Message<String> message =
                    MessageBuilder.<String>withTopic("t").bornHost("my-host").body("b").build();
            assertThat(message.getBornHost()).isEqualTo("my-host");
        }

        @Test
        @DisplayName("transactionId 设置后透传到消息")
        void transactionId_passedThrough() {
            Message<String> message =
                    MessageBuilder.<String>withTopic("t").transactionId("tx-100").body("b").build();
            assertThat(message.getTransactionId()).isEqualTo("tx-100");
            assertThat(message.isTransactionMessage()).isTrue();
        }

        @Test
        @DisplayName("messageId 设置后透传到消息；未设置时保持 null")
        void messageId_passedThrough() {
            Message<String> without = MessageBuilder.<String>withTopic("t").body("b").build();
            assertThat(without.getMessageId()).isNull();

            MessageId id = new MessageId("100-0");
            Message<String> with =
                    MessageBuilder.<String>withTopic("t").body("b").messageId(id).build();
            assertThat(with.getMessageId()).isEqualTo(id);
        }
    }

    @Nested
    @DisplayName("from(Message) 复制")
    class FromMessage {

        @Test
        @DisplayName("from 复制全部字段，包括 messageId（F-06 回归）")
        void fromCopiesAllFieldsIncludingMessageId() {
            MessageId id = new MessageId("1234567890-1");
            Message<String> original =
                    MessageBuilder.<String>withTopic("order-topic")
                            .tag("created")
                            .keys("order-123")
                            .shardingKey("order-123")
                            .transactionId("tx-1")
                            .reconsumeTimes(2)
                            .bornTimestamp(42L)
                            .bornHost("host-a")
                            .withProperty("traceId", "t-1")
                            .withUserProperty("biz", "v")
                            .messageId(id)
                            .body("payload")
                            .build();

            Message<String> copy = MessageBuilder.from(original).build();

            assertThat(copy.getTopic()).isEqualTo(original.getTopic());
            assertThat(copy.getTag()).isEqualTo(original.getTag());
            assertThat(copy.getKeys()).isEqualTo(original.getKeys());
            assertThat(copy.getShardingKey()).isEqualTo(original.getShardingKey());
            assertThat(copy.getBody()).isEqualTo(original.getBody());
            assertThat(copy.getBornTimestamp()).isEqualTo(original.getBornTimestamp());
            assertThat(copy.getBornHost()).isEqualTo(original.getBornHost());
            assertThat(copy.getReconsumeTimes()).isEqualTo(original.getReconsumeTimes());
            assertThat(copy.getTransactionId()).isEqualTo(original.getTransactionId());
            assertThat(copy.getProperties()).isEqualTo(original.getProperties());
            assertThat(copy.getUserProperties()).isEqualTo(original.getUserProperties());
            // 关键回归：messageId 不再丢失
            assertThat(copy.getMessageId()).isEqualTo(id);
            assertThat(copy).isEqualTo(original);
        }

        @Test
        @DisplayName("from 源消息无 messageId 时副本同样为 null")
        void fromKeepsNullMessageId() {
            Message<String> original = MessageBuilder.<String>withTopic("t").body("b").build();
            assertThat(MessageBuilder.from(original).build().getMessageId()).isNull();
        }
    }
}
