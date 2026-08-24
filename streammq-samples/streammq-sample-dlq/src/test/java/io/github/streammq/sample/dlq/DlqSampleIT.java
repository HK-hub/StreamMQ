package io.github.streammq.sample.dlq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.streammq.adapter.redisson.retry.FixedIntervalRetryPolicy;
import io.github.streammq.core.annotation.StreamMQConsumer;
import io.github.streammq.core.annotation.StreamMQDlqConsumer;
import io.github.streammq.core.consumer.AbstractDlqMessageConsumer;
import io.github.streammq.core.consumer.ConsumeContext;
import io.github.streammq.core.consumer.StreamMessageConcurrentlyConsumer;
import io.github.streammq.core.enums.ConsumeAction;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.SendResult;
import io.github.streammq.core.policy.RetryPolicy;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

/**
 * StreamMQ 死信队列（DLQ）示例集成测试。
 *
 * <p>启动完整 Spring Boot 上下文，通过 {@link OrderProducer} 发送消息， 由独立的测试消费者组接收并验证，覆盖生产→存储→消费→死信全链路。
 *
 * <p>测试场景：
 *
 * <ul>
 *   <li>{@code normalMessageDelivery} 正常消息投递 → 测试消费者接收并验证消息内容
 *   <li>{@code failedMessageTriggersDlq} 消息消费失败超过重试次数 → 进入死信队列
 *   <li>{@code dlqConsumerReceivesDeadLetter} 死信消费者从 DLQ Stream 接收到死信消息
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@SpringBootTest(classes = DlqSampleApplication.class)
@ActiveProfiles("it")
@Import({
    DlqSampleIT.TestMessageCollector.class,
    DlqSampleIT.TestFailConsumer.class,
    DlqSampleIT.TestDlqConsumer.class,
    DlqSampleIT.TestConfig.class
})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@TestPropertySource(properties = {"spring.redis.host=127.0.0.1", "spring.redis.port=6379"})
@DisplayName("DLQ 示例集成测试")
class DlqSampleIT {

    private static final String TEST_CONSUMER_GROUP = "test-collector-group";
    private static final String TEST_FAIL_CONSUMER_GROUP = "test-fail-group";
    private static final String NAMESPACE = SampleConstants.NAMESPACE;
    private static final String TOPIC = SampleConstants.TOPIC;

    @Autowired private OrderProducer orderProducer;

    @Autowired private TestMessageCollector testCollector;

    @Autowired private TestDlqConsumer testDlqConsumer;

    @Autowired private RedissonClient redissonClient;

    @BeforeEach
    void clearReceivedMessages() {
        testCollector.receivedMessages.clear();
        testDlqConsumer.receivedDlqMessages.clear();
        cleanStreams();
    }

    private void cleanStreams() {
        try {
            String topicKey = "streammq:" + NAMESPACE + ":msg:" + TOPIC;
            String retryKey =
                    "streammq:"
                            + NAMESPACE
                            + ":retry:msg:"
                            + TOPIC
                            + ":"
                            + TEST_FAIL_CONSUMER_GROUP;
            String dlqKey = "streammq:" + NAMESPACE + ":dlq:" + TEST_FAIL_CONSUMER_GROUP;
            System.out.println(
                    "=== Cleaning streams: " + topicKey + ", " + retryKey + ", " + dlqKey);
            long deleted = redissonClient.getKeys().delete(topicKey, retryKey, dlqKey);
            System.out.println("=== Deleted keys count: " + deleted);
        } catch (Exception e) {
            System.out.println("=== Clean streams error: " + e.getMessage());
        }
    }

    // ===================== 测试场景 =====================

    /** 验证正常消息投递后，测试消费者能正确接收并验证消息的 keys、body、tag 属性。 */
    @Test
    @DisplayName("正常消息投递 - 消费者接收验证")
    void normalMessageDelivery() {
        String orderId = "IT-NORMAL-001";
        String content = "normal-order-content";

        System.out.println("=== TestCollector instance: " + testCollector.hashCode());
        System.out.println(
                "=== TestCollector.receivedMessages before send: "
                        + testCollector.receivedMessages.size());

        SendResult result = orderProducer.sendOrder(orderId, content);

        assertThat(result).isNotNull();
        assertThat(result.isSuccess()).isTrue();

        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            System.out.println(
                                    "=== TestCollector.receivedMessages in await: "
                                            + testCollector.receivedMessages.size());
                            assertThat(testCollector.receivedMessages).hasSize(1);
                            Message<String> received = testCollector.receivedMessages.peek();
                            assertThat(received).isNotNull();
                            assertThat(received.getKeys()).isEqualTo(orderId);
                            assertThat(received.getBody()).isEqualTo(content);
                            assertThat(received.getTag()).isEqualTo("dlq-test");
                        });
    }

    /**
     * 验证消息消费失败超过 maxReconsumeTimes 后，消息会进入死信队列。
     *
     * <p>使用 {@link TestFailConsumer}（consumerGroup = test-fail-group）始终抛出异常， 触发 DLQ 机制。通过 {@link
     * TestDlqConsumer} 验证死信消息被接收。
     */
    @Test
    @DisplayName("消息消费失败触发死信队列")
    void failedMessageTriggersDlq() {
        String orderId = "IT-DLQ-001";
        String content = "dlq-order-content";

        SendResult result = orderProducer.sendOrder(orderId, content);

        assertThat(result).isNotNull();
        assertThat(result.isSuccess()).isTrue();

        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            assertThat(testDlqConsumer.receivedDlqMessages).isNotEmpty();
                            Message<String> received =
                                    testDlqConsumer.receivedDlqMessages.stream()
                                            .filter(m -> orderId.equals(m.getKeys()))
                                            .findFirst()
                                            .orElse(null);
                            assertThat(received).isNotNull();
                            assertThat(received.getBody()).isEqualTo(content);
                        });
    }

    /** 验证死信消费者能从 DLQ Stream 接收到死信消息， 并验证死信消息的元数据正确性。 */
    @Test
    @DisplayName("死信消费者收到死信消息并验证元数据")
    void dlqConsumerReceivesDeadLetter() {
        String orderId = "IT-DLQ-002";
        String content = "dlq-order-content-002";

        SendResult result = orderProducer.sendOrder(orderId, content);

        assertThat(result).isNotNull();
        assertThat(result.isSuccess()).isTrue();

        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            assertThat(testDlqConsumer.receivedDlqMessages).isNotEmpty();
                            // 查找当前测试发送的消息（可能有之前测试的残留消息）
                            Message<String> received =
                                    testDlqConsumer.receivedDlqMessages.stream()
                                            .filter(m -> orderId.equals(m.getKeys()))
                                            .findFirst()
                                            .orElse(null);
                            assertThat(received).isNotNull();
                            assertThat(received.getBody()).isEqualTo(content);
                            assertThat(received.getUserProperties())
                                    .containsEntry("source", "dlq-sample");
                        });
    }

    // ===================== 测试配置 =====================

    /** 测试专用配置：覆盖全局 {@link RetryPolicy} 使用短间隔重试策略， 使 DLQ 测试在秒级完成而非分钟级。 */
    @Configuration
    static class TestConfig {
        @Bean
        public RetryPolicy streamMQRetryPolicy() {
            return new FixedIntervalRetryPolicy(100L, 3);
        }
    }

    // ===================== 测试消息收集器 =====================

    /**
     * 测试专用消息收集器（正常消费）。
     *
     * <p>通过 {@link StreamMQConsumer} 注解注册为 {@code order-topic} 的消费者， 使用独立的测试消费者组（{@value
     * #TEST_CONSUMER_GROUP}）， 避免与生产环境消费者组冲突。
     */
    @StreamMQConsumer(topic = SampleConstants.TOPIC, consumerGroup = TEST_CONSUMER_GROUP)
    static class TestMessageCollector implements StreamMessageConcurrentlyConsumer<String> {

        final ConcurrentLinkedQueue<Message<String>> receivedMessages =
                new ConcurrentLinkedQueue<>();

        @Override
        public ConsumeAction onMessage(Message<String> message, ConsumeContext context) {
            System.out.println(
                    "=== TestMessageCollector.onMessage called: keys="
                            + message.getKeys()
                            + ", body="
                            + message.getBody()
                            + ", instance="
                            + this.hashCode());
            receivedMessages.add(message);
            System.out.println(
                    "=== TestMessageCollector.receivedMessages size=" + receivedMessages.size());
            return ConsumeAction.SUCCESS;
        }
    }

    /**
     * 测试专用失败消费者（始终抛异常触发 DLQ）。
     *
     * <p>使用独立的测试消费者组（{@value #TEST_FAIL_CONSUMER_GROUP}）， 确保不会与生产消费者组冲突。始终抛出 {@link
     * RuntimeException}， 模拟消费失败场景，验证 DLQ 机制。
     */
    @StreamMQConsumer(
            topic = SampleConstants.TOPIC,
            consumerGroup = TEST_FAIL_CONSUMER_GROUP,
            maxReconsumeTimes = 3)
    static class TestFailConsumer implements StreamMessageConcurrentlyConsumer<String> {

        @Override
        public ConsumeAction onMessage(Message<String> message, ConsumeContext context) {
            throw new RuntimeException(
                    "Test intentional failure for DLQ: orderId=" + message.getKeys());
        }
    }

    /**
     * 测试专用死信消费者。
     *
     * <p>通过 {@link StreamMQDlqConsumer} 注解注册为 DLQ 消费者， 监听 {@value #TEST_FAIL_CONSUMER_GROUP}
     * 消费者组的死信队列， 收集死信消息供测试验证。
     */
    @StreamMQDlqConsumer(consumerGroup = TEST_FAIL_CONSUMER_GROUP, namespace = SampleConstants.NAMESPACE)
    static class TestDlqConsumer extends AbstractDlqMessageConsumer<String> {

        final ConcurrentLinkedQueue<Message<String>> receivedDlqMessages =
                new ConcurrentLinkedQueue<>();

        @Override
        public void onDlqMessage(Message<String> message, ConsumeContext context) {
            receivedDlqMessages.add(message);
        }
    }
}
