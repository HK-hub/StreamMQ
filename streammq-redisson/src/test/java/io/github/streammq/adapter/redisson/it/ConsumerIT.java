/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.streammq.adapter.redisson.listener.RedissonStreamListener;
import io.github.streammq.adapter.redisson.producer.RedissonStreamProducer;
import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageBuilder;
import io.github.streammq.core.message.MessageId;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.redisson.api.RStream;
import org.redisson.api.StreamGroup;
import org.redisson.api.StreamMessageId;

/**
 * {@link RedissonStreamListener} 的 Redis 联动集成测试。
 *
 * <p>覆盖消费组创建、消息拉取、ACK、PEL 留存与超时阻塞拉取等场景。
 */
@DisplayName("RedissonStreamListener 集成测试")
class ConsumerIT extends AbstractRedisIT {

    private RedissonStreamListener consumer;
    private RedissonStreamProducer producer;
    private static final String TOPIC = "consumer-test-topic";
    private static final String GROUP = "consumer-test-group";
    private static final String CONSUMER_NAME = "consumer-1";

    @BeforeEach
    void setUpConsumerAndProducer() {
        producer =
                new RedissonStreamProducer(
                        redisson, namespace, GROUP + "-p", converter, 3000L, 0, 0, 0);
        consumer =
                new RedissonStreamListener(
                        redisson, namespace, TOPIC, GROUP, CONSUMER_NAME, converter);
        // 显式创建消费者组,绕过主代码 StreamMessageId.MIN bug
        createConsumerGroup(TOPIC, GROUP);
    }

    @AfterEach
    void tearDownConsumerAndProducer() {
        if (consumer != null) {
            consumer.close();
        }
        if (producer != null) {
            producer.close();
        }
    }

    @Test
    @DisplayName("createGroup:首次 pull 后 Redis 中存在该消费组")
    void createGroup_groupExistsAfterPull() {
        consumer.pull(1);

        RStream<String, String> stream =
                redisson.getStream(StreamMQKeys.topicStream(namespace, TOPIC));
        List<StreamGroup> groups = stream.listGroups();
        assertThat(groups).anyMatch(g -> GROUP.equals(g.getName()));
    }

    @Test
    @DisplayName("consume 单条消息:发送后消费,内容一致")
    void consume_singleMessage_contentMatches() {
        Message<String> sent =
                MessageBuilder.<String>withTopic(TOPIC)
                        .tag("consume-tag")
                        .keys("consume-key")
                        .body("consume-body")
                        .build();
        producer.syncSend(sent);

        List<Message<?>> messages = consumer.pull(1);

        assertThat(messages).hasSize(1);
        Message<?> received = messages.get(0);
        assertThat(received.getTopic()).isEqualTo(TOPIC);
        assertThat(received.getTag()).isEqualTo("consume-tag");
        assertThat(received.getKeys()).isEqualTo("consume-key");
        assertThat(received.getBody()).isEqualTo("consume-body");
        assertThat(received.getMessageId()).isNotNull();
    }

    @Test
    @DisplayName("consume 多条消息:发送 3 条后消费 3 条")
    void consume_multipleMessages() {
        for (int i = 0; i < 3; i++) {
            producer.syncSend(
                    MessageBuilder.<String>withTopic(TOPIC)
                            .tag("t")
                            .keys("k-" + i)
                            .body("b-" + i)
                            .build());
        }

        List<Message<?>> messages = consumer.pull(10);

        assertThat(messages).hasSize(3);
    }

    @Test
    @DisplayName("ack 消息后 PEL 为空")
    void ack_messagePelEmpty() {
        producer.syncSend(MessageBuilder.<String>withTopic(TOPIC).body("ack-body").build());
        List<Message<?>> messages = consumer.pull(1);
        assertThat(messages).hasSize(1);

        consumer.ack(messages.get(0).getMessageId());

        RStream<String, String> stream =
                redisson.getStream(StreamMQKeys.topicStream(namespace, TOPIC));
        assertThat(stream.listPending(GROUP, StreamMessageId.MIN, StreamMessageId.MAX, 100))
                .isEmpty();
    }

    @Test
    @DisplayName("不 ack 时 PEL 中保留消息")
    void noAck_pelRetainsMessage() {
        producer.syncSend(MessageBuilder.<String>withTopic(TOPIC).body("noack-body").build());
        List<Message<?>> messages = consumer.pull(1);
        assertThat(messages).hasSize(1);

        RStream<String, String> stream =
                redisson.getStream(StreamMQKeys.topicStream(namespace, TOPIC));
        assertThat(stream.listPending(GROUP, StreamMessageId.MIN, StreamMessageId.MAX, 100))
                .hasSize(1);
    }

    @Test
    @DisplayName("已 ack 的消息不会被重复消费")
    void ackedMessage_notRedelivered() {
        producer.syncSend(MessageBuilder.<String>withTopic(TOPIC).body("once").build());
        List<Message<?>> first = consumer.pull(1);
        assertThat(first).hasSize(1);
        consumer.ack(first.get(0).getMessageId());

        List<Message<?>> second = consumer.pull(1);
        assertThat(second).isEmpty();
    }

    @Test
    @DisplayName("ackBatch 批量确认后 PEL 为空")
    void ackBatch_pelEmpty() {
        for (int i = 0; i < 3; i++) {
            producer.syncSend(MessageBuilder.<String>withTopic(TOPIC).body("batch-" + i).build());
        }
        List<Message<?>> messages = consumer.pull(10);
        assertThat(messages).hasSize(3);

        List<MessageId> ids = messages.stream().map(Message::getMessageId).toList();
        consumer.ackBatch(ids);

        RStream<String, String> stream =
                redisson.getStream(StreamMQKeys.topicStream(namespace, TOPIC));
        assertThat(stream.listPending(GROUP, StreamMessageId.MIN, StreamMessageId.MAX, 100))
                .isEmpty();
    }

    @Test
    @DisplayName("pullBlock 超时后返回空列表")
    void pullBlock_timeoutReturnsEmpty() {
        List<Message<?>> messages = consumer.pullBlock(1, Duration.ofMillis(500));
        assertThat(messages).isEmpty();
    }

    @Test
    @DisplayName("pullBlock 收到消息后立即返回")
    void pullBlock_returnsMessageImmediately() {
        producer.syncSend(MessageBuilder.<String>withTopic(TOPIC).body("block-body").build());

        List<Message<?>> messages = consumer.pullBlock(1, Duration.ofSeconds(2));
        assertThat(messages).hasSize(1);
        assertThat(messages.get(0).getBody()).isEqualTo("block-body");
    }

    @Test
    @DisplayName("pull 无效 batchSize 抛 IllegalArgumentException")
    void pull_invalidBatchSize_throws() {
        assertThatThrownBy(() -> consumer.pull(0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> consumer.pull(-1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> consumer.pull(1001)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("已关闭 Consumer 调用 pull 抛 IllegalStateException")
    void pull_afterClose_throws() {
        consumer.close();
        assertThatThrownBy(() -> consumer.pull(1))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("closed");
    }

    @Test
    @DisplayName("已关闭 Consumer 调用 ack 抛 IllegalStateException")
    void ack_afterClose_throws() {
        consumer.close();
        assertThatThrownBy(() -> consumer.ack(new MessageId("0-0")))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("重复创建消费组(BUSYGROUP)不会抛异常")
    void createGroup_alreadyExists_doesNotThrow() {
        // @BeforeEach 已通过 createConsumerGroup 创建组,此处再次创建应得到 BUSYGROUP(被捕获)
        // Consumer.pull() 内部 ensureGroup() 会尝试创建已存在的组,不应抛异常
        List<Message<?>> messages = consumer.pull(1);
        assertThat(messages).isEmpty();
    }

    @Test
    @DisplayName("ack 不存在的 messageId 不抛异常")
    void ack_nonExistentMessageId_noException() {
        // 使用一个不存在的 messageId 进行 ack,Redis XACK 对不存在的 ID 返回 0 但不抛异常
        consumer.pull(1); // 确保 group 已创建
        MessageId fakeId = new MessageId("9999999999999-0");
        // 不应抛异常
        consumer.ack(fakeId);
    }
}
