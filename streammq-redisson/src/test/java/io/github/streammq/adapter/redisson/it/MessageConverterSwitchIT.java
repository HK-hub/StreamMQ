package io.github.streammq.adapter.redisson.it;

import io.github.streammq.adapter.redisson.converter.DefaultMessageConverter;
import io.github.streammq.adapter.redisson.listener.RedissonStreamListener;
import io.github.streammq.adapter.redisson.producer.RedissonStreamProducer;
import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageBuilder;
import io.github.streammq.core.converter.MessageConverter;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.redisson.api.RStream;
import org.redisson.api.StreamMessageId;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 消息转换器切换(DefaultMessageConverter vs 自定义 PassThroughMessageConverter)
 * 端到端 Redis 联动集成测试。
 *
 * <p>验证 {@link DefaultMessageConverter} 与自定义 {@link PassThroughMessageConverter} 在
 * 生产-消费全链路下的正确性,并对比两者产生的 Stream Entry 字段格式差异。
 */
@DisplayName("消息转换器切换集成测试")
class MessageConverterSwitchIT extends AbstractRedisIT {

    /**
     * 自定义直通消息转换器:body 直接以字符串形式存入 Stream Entry,
     * 不经过序列化器与 Base64 编码。仅支持 String body。
     *
     * <p>与 {@link DefaultMessageConverter} 的差异:
     * <ul>
     *   <li>{@code body} 字段为原始字符串,而非 Base64(序列化字节)</li>
     *   <li>不写入 {@code bodyType} 之外的序列化相关字段(保留 bodyType 以便消费端识别类型)</li>
     * </ul>
     */
    static class PassThroughMessageConverter implements MessageConverter {

        @Override
        public Map<String, String> toStreamFields(Message<?> message) {
            Objects.requireNonNull(message, "message");
            Map<String, String> fields = new HashMap<>(8);

            Object body = message.getBody();
            if (body != null) {
                // body 直接以字符串形式存储,不 Base64,不序列化
                fields.put(DefaultMessageConverter.FIELD_BODY, body.toString());
                fields.put(DefaultMessageConverter.FIELD_BODY_TYPE, body.getClass().getName());
            }

            if (message.getTag() != null) {
                fields.put(DefaultMessageConverter.FIELD_TAG, message.getTag());
            }
            if (message.getKeys() != null) {
                fields.put(DefaultMessageConverter.FIELD_KEYS, message.getKeys());
            }
            if (message.getShardingKey() != null) {
                fields.put(DefaultMessageConverter.FIELD_SHARDING_KEY, message.getShardingKey());
            }
            fields.put(DefaultMessageConverter.FIELD_BORN_TS, Long.toString(message.getBornTimestamp()));
            if (message.getBornHost() != null) {
                fields.put(DefaultMessageConverter.FIELD_BORN_HOST, message.getBornHost());
            }
            return fields;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> Message<T> fromStreamFields(Map<String, String> fields, Class<T> targetType) {
            Objects.requireNonNull(fields, "fields");
            Objects.requireNonNull(targetType, "targetType");
            Message<T> message = new Message<>();

            String bodyStr = fields.get(DefaultMessageConverter.FIELD_BODY);
            if (bodyStr != null && !bodyStr.isEmpty()) {
                // 直接将字符串作为 body 返回(仅支持 String 类型)
                message.setBody((T) bodyStr);
            }

            if (fields.containsKey(DefaultMessageConverter.FIELD_TAG)) {
                message.setTag(fields.get(DefaultMessageConverter.FIELD_TAG));
            }
            if (fields.containsKey(DefaultMessageConverter.FIELD_KEYS)) {
                message.setKeys(fields.get(DefaultMessageConverter.FIELD_KEYS));
            }
            if (fields.containsKey(DefaultMessageConverter.FIELD_SHARDING_KEY)) {
                message.setShardingKey(fields.get(DefaultMessageConverter.FIELD_SHARDING_KEY));
            }
            if (fields.containsKey(DefaultMessageConverter.FIELD_BORN_HOST)) {
                message.setBornHost(fields.get(DefaultMessageConverter.FIELD_BORN_HOST));
            }

            String bornTs = fields.get(DefaultMessageConverter.FIELD_BORN_TS);
            if (bornTs != null && !bornTs.isEmpty()) {
                message.setBornTimestamp(Long.parseLong(bornTs));
            }
            return message;
        }

        @Override
        public String name() {
            return "passthrough";
        }
    }

    @Test
    @DisplayName("DefaultMessageConverter:发送 String body 后消费还原内容一致")
    void defaultMessageConverter_stringBody_roundTrip() {
        String topic = "conv-default-str-topic";
        String group = "conv-default-str-group";

        // 基类已使用 DefaultMessageConverter + JacksonJsonSerializer
        RedissonStreamProducer producer =
            new RedissonStreamProducer(redisson, namespace, group + "-p", converter, 3000L, 0, 0);
        RedissonStreamListener consumer =
            new RedissonStreamListener(redisson, namespace, topic, group, "c1", converter);
        createConsumerGroup(topic, group);

        try {
            producer.syncSend(MessageBuilder.<String>withTopic(topic)
                .tag("default-tag")
                .keys("default-key")
                .body("default-body")
                .build());

            List<Message<?>> messages = consumer.pull(1);
            assertThat(messages).hasSize(1);
            Message<?> received = messages.get(0);
            assertThat(received.getBody()).isEqualTo("default-body");
            assertThat(received.getTag()).isEqualTo("default-tag");
            assertThat(received.getKeys()).isEqualTo("default-key");
        } finally {
            producer.close();
            consumer.close();
        }
    }

    @Test
    @DisplayName("PassThroughMessageConverter:body 直接以字符串存储,消费还原内容一致")
    void passThroughMessageConverter_stringBody_roundTrip() {
        String topic = "conv-passthrough-str-topic";
        String group = "conv-passthrough-str-group";

        MessageConverter passThroughConverter = new PassThroughMessageConverter();

        RedissonStreamProducer producer =
            new RedissonStreamProducer(redisson, namespace, group + "-p", passThroughConverter, 3000L, 0, 0);
        RedissonStreamListener consumer =
            new RedissonStreamListener(redisson, namespace, topic, group, "c1", passThroughConverter);
        createConsumerGroup(topic, group);

        try {
            producer.syncSend(MessageBuilder.<String>withTopic(topic)
                .tag("pass-tag")
                .keys("pass-key")
                .body("pass-body")
                .build());

            List<Message<?>> messages = consumer.pull(1);
            assertThat(messages).hasSize(1);
            Message<?> received = messages.get(0);
            assertThat(received.getBody()).isEqualTo("pass-body");
            assertThat(received.getTag()).isEqualTo("pass-tag");
            assertThat(received.getKeys()).isEqualTo("pass-key");
        } finally {
            producer.close();
            consumer.close();
        }
    }

    @Test
    @DisplayName("两种 converter 产生 Stream Entry body 字段格式不同但都能正确还原")
    void bothConverters_differentBodyFormat_bothRestored() {
        String topicDefault = "conv-cmp-default-topic";
        String topicPass = "conv-cmp-passthrough-topic";
        String group = "conv-cmp-group";

        MessageConverter defaultConverter = converter;
        MessageConverter passThroughConverter = new PassThroughMessageConverter();

        RedissonStreamProducer defaultProducer =
            new RedissonStreamProducer(redisson, namespace, group + "-dp", defaultConverter, 3000L, 0, 0);
        RedissonStreamProducer passProducer =
            new RedissonStreamProducer(redisson, namespace, group + "-pp", passThroughConverter, 3000L, 0, 0);

        try {
            defaultProducer.syncSend(MessageBuilder.<String>withTopic(topicDefault).body("same-text").build());
            passProducer.syncSend(MessageBuilder.<String>withTopic(topicPass).body("same-text").build());

            // 读取两个 Stream 的 entry 字段,对比 body 字段格式
            RStream<String, String> defaultStream =
                redisson.getStream(StreamMQKeys.topicStream(namespace, topicDefault));
            RStream<String, String> passStream =
                redisson.getStream(StreamMQKeys.topicStream(namespace, topicPass));

            Map<StreamMessageId, Map<String, String>> defaultRange =
                defaultStream.range(1, StreamMessageId.MIN, StreamMessageId.MAX);
            Map<StreamMessageId, Map<String, String>> passRange =
                passStream.range(1, StreamMessageId.MIN, StreamMessageId.MAX);

            assertThat(defaultRange).hasSize(1);
            assertThat(passRange).hasSize(1);

            String defaultBody = defaultRange.values().iterator().next().get(DefaultMessageConverter.FIELD_BODY);
            String passBody = passRange.values().iterator().next().get(DefaultMessageConverter.FIELD_BODY);

            // body 字段都非空
            assertThat(defaultBody).isNotEmpty();
            assertThat(passBody).isNotEmpty();

            // DefaultMessageConverter 的 body 是 Base64 编码,PassThrough 的 body 是原始字符串
            // 两者格式不同
            assertThat(defaultBody).isNotEqualTo(passBody);
            // PassThrough 的 body 应直接等于原始字符串
            assertThat(passBody).isEqualTo("same-text");
            // Default 的 body 应为 Base64 编码,不等于原始字符串
            assertThat(defaultBody).isNotEqualTo("same-text");

            // 分别用对应 converter 消费,都能正确还原 body
            createConsumerGroup(topicDefault, group + "-c");
            createConsumerGroup(topicPass, group + "-c");

            RedissonStreamListener defaultConsumer =
                new RedissonStreamListener(redisson, namespace, topicDefault, group + "-c", "c1", defaultConverter);
            RedissonStreamListener passConsumer =
                new RedissonStreamListener(redisson, namespace, topicPass, group + "-c", "c1", passThroughConverter);
            try {
                List<Message<?>> defaultMessages = defaultConsumer.pull(1);
                List<Message<?>> passMessages = passConsumer.pull(1);

                assertThat(defaultMessages).hasSize(1);
                assertThat(passMessages).hasSize(1);
                assertThat(defaultMessages.get(0).getBody()).isEqualTo("same-text");
                assertThat(passMessages.get(0).getBody()).isEqualTo("same-text");
            } finally {
                defaultConsumer.close();
                passConsumer.close();
            }
        } finally {
            defaultProducer.close();
            passProducer.close();
        }
    }

    @Test
    @DisplayName("PassThroughMessageConverter:带 tag/keys/shardingKey 的完整消息往返一致")
    void passThroughMessageConverter_fullMessage_roundTrip() {
        String topic = "conv-passthrough-full-topic";
        String group = "conv-passthrough-full-group";

        MessageConverter passThroughConverter = new PassThroughMessageConverter();

        RedissonStreamProducer producer =
            new RedissonStreamProducer(redisson, namespace, group + "-p", passThroughConverter, 3000L, 0, 0);
        RedissonStreamListener consumer =
            new RedissonStreamListener(redisson, namespace, topic, group, "c1", passThroughConverter);
        createConsumerGroup(topic, group);

        try {
            producer.syncSend(MessageBuilder.<String>withTopic(topic)
                .tag("full-tag")
                .keys("full-key")
                .shardingKey("full-shard")
                .body("full-body")
                .build());

            List<Message<?>> messages = consumer.pull(1);
            assertThat(messages).hasSize(1);
            Message<?> received = messages.get(0);
            assertThat(received.getBody()).isEqualTo("full-body");
            assertThat(received.getTag()).isEqualTo("full-tag");
            assertThat(received.getKeys()).isEqualTo("full-key");
            assertThat(received.getShardingKey()).isEqualTo("full-shard");
            assertThat(received.getTopic()).isEqualTo(topic);
            assertThat(received.getMessageId()).isNotNull();
        } finally {
            producer.close();
            consumer.close();
        }
    }
}
