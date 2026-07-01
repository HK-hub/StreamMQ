package io.github.streammq.core.message;

import io.github.streammq.core.enums.DelayLevel;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 * {@link Message} 单元测试，覆盖构造、getter/setter、延时/事务判断、属性视图与 toString。
 */
@DisplayName("Message 消息载体测试")
class MessageTest {

    @Nested
    @DisplayName("默认构造")
    class DefaultConstructor {

        @Test
        @DisplayName("默认构造时 properties 与 userProperties 为空 Map")
        void defaultConstructor_shouldInitEmptyProperties() {
            Message<String> message = new Message<>();
            assertThat(message.getProperties()).isEmpty();
            assertThat(message.getUserProperties()).isEmpty();
        }

        @Test
        @DisplayName("默认构造时引用类型字段为 null，基本类型为零值")
        void defaultConstructor_fieldsAreDefault() {
            Message<String> message = new Message<>();
            assertThat(message.getTopic()).isNull();
            assertThat(message.getTag()).isNull();
            assertThat(message.getKeys()).isNull();
            assertThat(message.getShardingKey()).isNull();
            assertThat(message.getBody()).isNull();
            assertThat(message.getDelayLevel()).isNull();
            assertThat(message.getDelayTimeMillis()).isNull();
            assertThat(message.getMessageId()).isNull();
            assertThat(message.getBornTimestamp()).isZero();
            assertThat(message.getBornHost()).isNull();
            assertThat(message.getReconsumeTimes()).isZero();
            assertThat(message.getTransactionId()).isNull();
        }
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

            Message<String> message = new Message<>("topic", "tag", "keys", "shard",
                props, userProps, "body", DelayLevel.SECOND_1, 100L,
                1234567890L, "host:8080", "tx-1");

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
        @DisplayName("全参构造对 null properties/userProperties 创建空 Map")
        void fullConstructor_nullPropertiesBecomesEmpty() {
            Message<String> message = new Message<>("topic", null, null, null,
                null, null, "body", null, null, 0L, null, null);
            assertThat(message.getProperties()).isEmpty();
            assertThat(message.getUserProperties()).isEmpty();
        }

        @Test
        @DisplayName("全参构造拷贝 properties，原始 Map 修改不影响消息")
        void fullConstructor_copiesPropertiesMap() {
            Map<String, String> props = new HashMap<>();
            props.put("k", "v");
            Message<String> message = new Message<>("topic", null, null, null,
                props, null, "body", null, null, 0L, null, null);
            props.put("k2", "v2");
            assertThat(message.getProperties()).hasSize(1).containsEntry("k", "v");
        }
    }

    @Nested
    @DisplayName("Getter/Setter")
    class GetterSetter {

        @Test
        @DisplayName("所有 setter 正确写入，getter 正确读取")
        void settersAndGetters() {
            Message<String> message = new Message<>();
            message.setTopic("topic");
            message.setTag("tag");
            message.setKeys("keys");
            message.setShardingKey("shard");
            message.setBody("body");
            message.setDelayLevel(DelayLevel.MINUTE_1);
            message.setDelayTimeMillis(500L);
            message.setBornTimestamp(999L);
            message.setBornHost("host");
            message.setReconsumeTimes(3);
            message.setTransactionId("tx");

            assertThat(message.getTopic()).isEqualTo("topic");
            assertThat(message.getTag()).isEqualTo("tag");
            assertThat(message.getKeys()).isEqualTo("keys");
            assertThat(message.getShardingKey()).isEqualTo("shard");
            assertThat(message.getBody()).isEqualTo("body");
            assertThat(message.getDelayLevel()).isEqualTo(DelayLevel.MINUTE_1);
            assertThat(message.getDelayTimeMillis()).isEqualTo(500L);
            assertThat(message.getBornTimestamp()).isEqualTo(999L);
            assertThat(message.getBornHost()).isEqualTo("host");
            assertThat(message.getReconsumeTimes()).isEqualTo(3);
            assertThat(message.getTransactionId()).isEqualTo("tx");
        }

        @Test
        @DisplayName("setMessageId 正确设置与读取")
        void messageIdSetter() {
            Message<String> message = new Message<>();
            MessageId id = new MessageId("100-0");
            message.setMessageId(id);
            assertThat(message.getMessageId()).isEqualTo(id);
        }

        @Test
        @DisplayName("setProperties 传入 null 时清空为空 Map")
        void setProperties_nullBecomesEmpty() {
            Message<String> message = new Message<>();
            message.putProperty("k", "v");
            message.setProperties(null);
            assertThat(message.getProperties()).isEmpty();
        }

        @Test
        @DisplayName("setUserProperties 传入 null 时清空为空 Map")
        void setUserProperties_nullBecomesEmpty() {
            Message<String> message = new Message<>();
            message.putUserProperty("k", "v");
            message.setUserProperties(null);
            assertThat(message.getUserProperties()).isEmpty();
        }
    }

    @Nested
    @DisplayName("isDelayMessage 延时消息判断")
    class IsDelayMessage {

        @Test
        @DisplayName("设置 delayLevel 时返回 true")
        void withDelayLevel() {
            Message<String> message = new Message<>();
            message.setDelayLevel(DelayLevel.SECOND_5);
            assertThat(message.isDelayMessage()).isTrue();
        }

        @Test
        @DisplayName("设置 delayTimeMillis 时返回 true")
        void withDelayTimeMillis() {
            Message<String> message = new Message<>();
            message.setDelayTimeMillis(2000L);
            assertThat(message.isDelayMessage()).isTrue();
        }

        @Test
        @DisplayName("两者均未设置时返回 false")
        void withoutAnyDelay() {
            Message<String> message = new Message<>();
            assertThat(message.isDelayMessage()).isFalse();
        }
    }

    @Nested
    @DisplayName("isTransactionMessage 事务消息判断")
    class IsTransactionMessage {

        @Test
        @DisplayName("transactionId 为 null 时返回 false")
        void nullTransactionId() {
            Message<String> message = new Message<>();
            message.setTransactionId(null);
            assertThat(message.isTransactionMessage()).isFalse();
        }

        @Test
        @DisplayName("transactionId 为空字符串时返回 false")
        void emptyTransactionId() {
            Message<String> message = new Message<>();
            message.setTransactionId("");
            assertThat(message.isTransactionMessage()).isFalse();
        }

        @Test
        @DisplayName("transactionId 为非空字符串时返回 true")
        void nonEmptyTransactionId() {
            Message<String> message = new Message<>();
            message.setTransactionId("tx-001");
            assertThat(message.isTransactionMessage()).isTrue();
        }
    }

    @Nested
    @DisplayName("putProperty / putUserProperty 校验")
    class PutPropertyValidation {

        @Test
        @DisplayName("putProperty key 为 null 抛 NPE")
        void putProperty_nullKey() {
            Message<String> message = new Message<>();
            assertThatThrownBy(() -> message.putProperty(null, "v"))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("property key");
        }

        @Test
        @DisplayName("putProperty value 为 null 抛 NPE")
        void putProperty_nullValue() {
            Message<String> message = new Message<>();
            assertThatThrownBy(() -> message.putProperty("k", null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("property value");
        }

        @Test
        @DisplayName("putUserProperty key 为 null 抛 NPE")
        void putUserProperty_nullKey() {
            Message<String> message = new Message<>();
            assertThatThrownBy(() -> message.putUserProperty(null, "v"))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("userProperty key");
        }

        @Test
        @DisplayName("putUserProperty value 为 null 抛 NPE")
        void putUserProperty_nullValue() {
            Message<String> message = new Message<>();
            assertThatThrownBy(() -> message.putUserProperty("k", null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("userProperty value");
        }

        @Test
        @DisplayName("putProperty 正常写入")
        void putProperty_normal() {
            Message<String> message = new Message<>();
            message.putProperty("traceId", "t-001");
            assertThat(message.getProperties()).containsEntry("traceId", "t-001");
        }

        @Test
        @DisplayName("putUserProperty 正常写入")
        void putUserProperty_normal() {
            Message<String> message = new Message<>();
            message.putUserProperty("bizKey", "bizVal");
            assertThat(message.getUserProperties()).containsEntry("bizKey", "bizVal");
        }
    }

    @Nested
    @DisplayName("不可修改视图")
    class UnmodifiableView {

        @Test
        @DisplayName("getProperties 返回不可修改视图，写入抛 UnsupportedOperationException")
        void getProperties_unmodifiable() {
            Message<String> message = new Message<>();
            message.putProperty("k", "v");
            assertThatExceptionOfType(UnsupportedOperationException.class)
                .isThrownBy(() -> message.getProperties().put("new", "val"));
        }

        @Test
        @DisplayName("getUserProperties 返回不可修改视图，写入抛 UnsupportedOperationException")
        void getUserProperties_unmodifiable() {
            Message<String> message = new Message<>();
            message.putUserProperty("k", "v");
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
            Message<String> message = new Message<>();
            message.setTopic("order-topic");
            message.setTag("created");
            message.setKeys("order-123");
            message.setBornTimestamp(1000L);
            message.setTransactionId("tx-1");
            message.setBody("hello");

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
