/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.sample.interceptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.streammq.core.annotation.StreamMQConsumer;
import io.github.streammq.core.consumer.ConsumeContext;
import io.github.streammq.core.consumer.StreamMessageConcurrentlyConsumer;
import io.github.streammq.core.enums.ConsumeAction;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.SendResult;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

/**
 * Interceptor 示例集成测试。
 *
 * <p>启动完整 Spring Boot 上下文，通过 {@link OrderProducer} 发送消息， 由 {@link TestMessageCollector} 接收并验证拦截器注入的
 * traceId/spanId， 覆盖生产者拦截器 → 存储 → 消费者拦截器全链路。
 *
 * <p>测试场景：
 *
 * <ul>
 *   <li>{@code sendMessage_traceIdInjectedByInterceptor} 发送消息 → 验证 traceId 被
 *       TraceProducerInterceptor 注入并被消费者接收
 *   <li>{@code sendMessage_spanIdInjectedByInterceptor} 发送消息 → 验证 spanId 被正确注入
 *   <li>{@code sendMultipleMessages_allReceived} 批量发送 → 验证多条消息全部被消费者接收
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@SpringBootTest(classes = InterceptorSampleApplication.class)
@ActiveProfiles("it")
@Import(InterceptorSampleIT.TestMessageCollector.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@TestPropertySource(properties = {"spring.redis.host=127.0.0.1", "spring.redis.port=6379"})
@DisplayName("Interceptor 示例集成测试")
@EnabledIf(
        value = "io.github.streammq.test.util.RedisAvailability#localhostAvailable",
        disabledReason = "Redis not available at localhost:6379")
class InterceptorSampleIT {

    private static final String TEST_CONSUMER_GROUP = "test-interceptor-consumer-group";

    @Autowired private OrderProducer orderProducer;

    @Autowired private TestMessageCollector testCollector;

    @BeforeEach
    void clearReceivedMessages() {
        testCollector.receivedMessages.clear();
    }

    @Test
    @DisplayName("发送消息后 traceId 被生产者拦截器注入并被消费者接收")
    void sendMessage_traceIdInjectedByInterceptor() {
        String orderId = "IT-INTERCEPTOR-001";
        String content = "interceptor-test-content-001";

        SendResult result = orderProducer.sendOrder(orderId, content);

        assertThat(result).isNotNull();
        assertThat(result.isSuccess()).isTrue();

        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            assertThat(testCollector.receivedMessages).hasSize(1);
                            Message<String> received = testCollector.receivedMessages.peek();
                            assertThat(received).isNotNull();
                            assertThat(received.getKeys()).isEqualTo(orderId);
                            assertThat(received.getBody()).isEqualTo(content);
                            assertThat(received.getTag()).isEqualTo("order");
                            assertThat(received.getUserProperties()).containsKey("traceId");
                            assertThat(
                                            received.getUserProperties()
                                                    .get(SampleConstants.PROP_TRACE_ID))
                                    .isNotNull();
                            assertThat(
                                            received.getUserProperties()
                                                    .get(SampleConstants.PROP_TRACE_ID))
                                    .isNotEmpty();
                        });
    }

    @Test
    @DisplayName("发送消息后 spanId 被生产者拦截器注入")
    void sendMessage_spanIdInjectedByInterceptor() {
        String orderId = "IT-INTERCEPTOR-002";
        String content = "interceptor-test-content-002";

        SendResult result = orderProducer.sendOrder(orderId, content);

        assertThat(result).isNotNull();
        assertThat(result.isSuccess()).isTrue();

        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            assertThat(testCollector.receivedMessages).hasSize(1);
                            Message<String> received = testCollector.receivedMessages.peek();
                            assertThat(received).isNotNull();
                            assertThat(received.getUserProperties()).containsEntry("spanId", "1");
                        });
    }

    @Test
    @DisplayName("发送多条消息均被消费者接收")
    void sendMultipleMessages_allReceived() {
        String[] orderIds = {"IT-BATCH-001", "IT-BATCH-002", "IT-BATCH-003"};
        String[] contents = {"batch-content-001", "batch-content-002", "batch-content-003"};

        for (int i = 0; i < orderIds.length; i++) {
            SendResult result = orderProducer.sendOrder(orderIds[i], contents[i]);
            assertThat(result.isSuccess()).isTrue();
        }

        await().atMost(15, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            assertThat(testCollector.receivedMessages).hasSize(3);
                            assertThat(testCollector.receivedMessages)
                                    .extracting(Message::getKeys)
                                    .containsExactlyInAnyOrder(orderIds);
                        });
    }

    @StreamMQConsumer(topic = SampleConstants.TOPIC, consumerGroup = TEST_CONSUMER_GROUP)
    static class TestMessageCollector implements StreamMessageConcurrentlyConsumer<String> {

        final ConcurrentLinkedQueue<Message<String>> receivedMessages =
                new ConcurrentLinkedQueue<>();

        @Override
        public ConsumeAction onMessage(Message<String> message, ConsumeContext context) {
            receivedMessages.add(message);
            return ConsumeAction.SUCCESS;
        }
    }
}
