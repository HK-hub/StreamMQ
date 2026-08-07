package io.github.streammq.sample.quickstart;

import io.github.streammq.core.annotation.StreamMQConsumer;
import io.github.streammq.core.consumer.ConsumeContext;
import io.github.streammq.core.consumer.StreamMessageConcurrentlyConsumer;
import io.github.streammq.core.enums.ConsumeAction;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.SendResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * QuickStart 示例集成测试。
 *
 * <p>启动完整 Spring Boot 上下文，通过 {@link OrderProducer} 发送消息，
 * 由 {@link TestMessageCollector} 接收并验证，覆盖生产→存储→消费全链路。
 *
 * <p>测试场景：
 * <ul>
 *   <li>{@code createOrder} 同步发送 → 消费者接收并验证消息内容</li>
 *   <li>{@code createOrderWithBuilder} Builder 模式发送 → 验证 tag/keys/userProps 正确性</li>
 *   <li>{@code createOrderAsync} 异步发送 → 验证 CompletableFuture 完成与消息接收</li>
 *   <li>{@code createOrdersBatch} 批量发送 → 验证多条消息全部接收</li>
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@SpringBootTest(classes = QuickStartApplication.class)
@Import(QuickStartSampleIT.TestMessageCollector.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@TestPropertySource(properties = {
        "spring.redis.host=127.0.0.1",
        "spring.redis.port=6379"
})
@DisplayName("QuickStart 示例集成测试")
class QuickStartSampleIT {

    /** 测试消费者组，与生产消费者组隔离 */
    private static final String TEST_CONSUMER_GROUP = "test-consumer-group";

    @Autowired
    private OrderProducer orderProducer;

    @Autowired
    private TestMessageCollector testCollector;

    /**
     * 每个测试前清空已接收消息队列，避免测试间干扰。
     */
    @BeforeEach
    void clearReceivedMessages() {
        testCollector.receivedMessages.clear();
    }

    // ===================== 测试场景 =====================

    /**
     * 验证 {@link OrderProducer#createOrder(String, String)} 同步发送消息后，
     * 测试消费者能正确接收并验证消息的 keys、body、tag 属性。
     */
    @Test
    @DisplayName("createOrder 发送消息后消费者接收")
    void createOrder_consumerReceivesMessage() {
        String orderId = "IT-ORDER-001";
        String content = "order-content-001";

        SendResult result = orderProducer.createOrder(orderId, content);

        assertThat(result).isNotNull();
        assertThat(result.isSuccess()).isTrue();

        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    assertThat(testCollector.receivedMessages).hasSize(1);
                    Message<String> received = testCollector.receivedMessages.peek();
                    assertThat(received).isNotNull();
                    assertThat(received.getKeys()).isEqualTo(orderId);
                    assertThat(received.getBody()).isEqualTo(content);
                    assertThat(received.getTag()).isEqualTo("created");
                });
    }

    /**
     * 验证 {@link OrderProducer#createOrderWithBuilder(String, String)} 通过
     * {@link io.github.streammq.core.message.MessageBuilder} 发送的消息包含
     * 正确的 tag、keys 和 userProperties。
     */
    @Test
    @DisplayName("createOrderWithBuilder 发送带正确 tag/keys/userProps 的消息")
    void createOrderWithBuilder_sendsWithCorrectMetadata() {
        String orderId = "IT-ORDER-002";
        String content = "order-content-002";

        SendResult result = orderProducer.createOrderWithBuilder(orderId, content);

        assertThat(result).isNotNull();
        assertThat(result.isSuccess()).isTrue();

        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    assertThat(testCollector.receivedMessages).hasSize(1);
                    Message<String> received = testCollector.receivedMessages.peek();
                    assertThat(received).isNotNull();
                    assertThat(received.getKeys()).isEqualTo(orderId);
                    assertThat(received.getBody()).isEqualTo(content);
                    assertThat(received.getTag()).isEqualTo("created");
                    assertThat(received.getUserProperties())
                            .containsEntry("source", "quickstart-sample");
                });
    }

    /**
     * 验证 {@link OrderProducer#createOrderAsync(String, String)} 异步发送消息后，
     * 返回的 {@link java.util.concurrent.CompletableFuture} 正常完成，
     * 且消费者能接收到对应消息。
     */
    @Test
    @DisplayName("createOrderAsync 异步发送并完成")
    void createOrderAsync_sendsAndCompletes() {
        String orderId = "IT-ORDER-003";
        String content = "order-content-003";

        AtomicReference<SendResult> resultRef = new AtomicReference<>();
        orderProducer.createOrderAsync(orderId, content)
                .thenAccept(resultRef::set);

        await().atMost(10, TimeUnit.SECONDS)
                .until(() -> resultRef.get() != null && resultRef.get().isSuccess());

        assertThat(resultRef.get()).isNotNull();
        assertThat(resultRef.get().isSuccess()).isTrue();

        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    assertThat(testCollector.receivedMessages).hasSize(1);
                    Message<String> received = testCollector.receivedMessages.peek();
                    assertThat(received).isNotNull();
                    assertThat(received.getKeys()).isEqualTo(orderId);
                    assertThat(received.getBody()).isEqualTo(content);
                    assertThat(received.getTag()).isEqualTo("async");
                });
    }

    /**
     * 验证 {@link OrderProducer#createOrdersBatch(List, List)} 批量发送多条消息后，
     * 所有消息均被消费者接收，且 keys 与 tag 属性正确。
     */
    @Test
    @DisplayName("createOrdersBatch 批量发送多条消息")
    void createOrdersBatch_sendsMultipleMessages() {
        List<String> orderIds = List.of("IT-BATCH-001", "IT-BATCH-002", "IT-BATCH-003");
        List<String> contents = List.of("batch-content-001", "batch-content-002", "batch-content-003");

        List<SendResult> results = orderProducer.createOrdersBatch(orderIds, contents);

        assertThat(results).hasSize(3);
        assertThat(results).allMatch(SendResult::isSuccess);

        await().atMost(15, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    assertThat(testCollector.receivedMessages).hasSize(3);
                    assertThat(testCollector.receivedMessages)
                            .extracting(Message::getKeys)
                            .containsExactlyInAnyOrderElementsOf(orderIds);
                    assertThat(testCollector.receivedMessages)
                            .extracting(Message::getTag)
                            .allMatch(tag -> tag.equals("batch"));
                });
    }

    // ===================== 测试消息收集器 =====================

    /**
     * 测试专用消息收集器。
     *
     * <p>通过 {@link StreamMQConsumer} 注解注册为 {@code order-topic} 的消费者，
     * 使用独立的测试消费者组（{@value #TEST_CONSUMER_GROUP}），
     * 避免与生产环境的 {@link OrderConsumer} 消费组冲突。
     *
     * <p>将所有接收到的消息存入 {@link ConcurrentLinkedQueue}，
     * 供测试方法通过 Awaitility + AssertJ 进行验证。
     */
    @StreamMQConsumer(topic = "order-topic", consumerGroup = TEST_CONSUMER_GROUP)
    static class TestMessageCollector implements StreamMessageConcurrentlyConsumer<String> {

        /** 已接收的消息集合，线程安全 */
        final ConcurrentLinkedQueue<Message<String>> receivedMessages = new ConcurrentLinkedQueue<>();

        /**
         * 处理接收到的消息，将其加入收集队列。
         *
         * @param message 消息载体
         * @param context 消费上下文
         * @return 消费成功动作
         */
        @Override
        public ConsumeAction onMessage(Message<String> message, ConsumeContext context) {
            receivedMessages.add(message);
            return ConsumeAction.SUCCESS;
        }
    }
}