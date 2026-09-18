/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.sample.tracing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.streammq.core.annotation.StreamMQConsumer;
import io.github.streammq.core.consumer.ConsumeContext;
import io.github.streammq.core.consumer.StreamMessageConcurrentlyConsumer;
import io.github.streammq.core.enums.ConsumeAction;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.SendResult;
import io.github.streammq.tracing.StreamMQTracing;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * Tracing 示例集成测试。
 *
 * <p>验证发送消息后消费者能收到带 W3C traceparent 属性的消息。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@SpringBootTest(classes = {TracingApplication.class, TracingSampleIT.TestConfig.class})
@Import(TracingSampleIT.TestEventCollector.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@DisplayName("Tracing 示例集成测试")
@EnabledIf(
        value = "io.github.streammq.test.util.RedisAvailability#localhostAvailable",
        disabledReason = "Redis not available at localhost:6379")
class TracingSampleIT {

    private static final String TEST_CONSUMER_GROUP = "tracing-test-consumer";
    private static final String TRACEPARENT_REGEX = "00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}";

    /** 每次运行的唯一后缀：命名空间与载荷均带此后缀，避免跨运行残留数据污染断言 */
    private static final String RUN_ID = UUID.randomUUID().toString().substring(0, 8);

    /** 本次运行的专属命名空间（覆写 streammq.namespace），配合 {@link #cleanupNamespace()} 实现跨运行隔离 */
    private static final String IT_NAMESPACE = "tracing-it-" + RUN_ID;

    /** 覆写全局命名空间，避免与历史运行/其它示例共享 streammq:tracing-sample:* 键 */
    @DynamicPropertySource
    static void overrideNamespace(DynamicPropertyRegistry registry) {
        registry.add("streammq.namespace", () -> IT_NAMESPACE);
    }

    /**
     * 清理本次运行命名空间下的全部键。
     *
     * <p>使用独立客户端：{@code @DirtiesContext(AFTER_EACH_TEST_METHOD)} 在每个方法后关闭上下文， 注入的 RedissonClient 在
     * {@code @AfterAll} 阶段已被 shutdown，无法复用于清理。
     */
    @AfterAll
    static void cleanupNamespace() {
        Config config = new Config();
        config.useSingleServer().setAddress("redis://127.0.0.1:6379").setDatabase(0);
        config.setCodec(StringCodec.INSTANCE);
        RedissonClient cleanupClient = Redisson.create(config);
        try {
            cleanupClient.getKeys().deleteByPattern("streammq:" + IT_NAMESPACE + ":*");
        } finally {
            cleanupClient.shutdown();
        }
    }

    @Autowired private EventProducer eventProducer;

    @Autowired private TestEventCollector testCollector;

    @BeforeEach
    void clear() {
        testCollector.receivedMessages.clear();
        testCollector.lastTraceparent.set(null);
    }

    @Test
    @DisplayName("发送事件后消费者应收到带 traceparent 的消息")
    void shouldPropagateTraceContext() {
        String eventId = "EVT-001-" + RUN_ID;
        SendResult result = eventProducer.emitEvent(eventId, "event-payload-" + RUN_ID);
        assertThat(result.isSuccess()).isTrue();

        await().atMost(15, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            // DemoRunner 会在上下文启动时发送 trace-demo-001 演示事件，
                            // 断言必须针对本测试自己的 EVT-001 消息（keys 带 RUN_ID），而非队列总量
                            Message<String> target =
                                    testCollector.receivedMessages.stream()
                                            .filter(m -> eventId.equals(m.getKeys()))
                                            .findFirst()
                                            .orElse(null);
                            assertThat(target).isNotNull();
                            String traceparent =
                                    target.getUserProperties()
                                            .get(
                                                    io.github.streammq.tracing.StreamMQTracing
                                                            .TRACEPARENT_KEY);
                            assertThat(traceparent).isNotNull().matches(TRACEPARENT_REGEX);
                        });
    }

    /** 测试配置类，提供真实 OpenTelemetry SDK 实例。 */
    static class TestConfig {

        @Bean
        public OpenTelemetry streamMQOpenTelemetry() {
            SdkTracerProvider tracerProvider =
                    SdkTracerProvider.builder().setSampler(Sampler.alwaysOn()).build();
            return OpenTelemetrySdk.builder().setTracerProvider(tracerProvider).build();
        }
    }

    /** 测试专用消息收集器，记录 traceparent 属性。 */
    @StreamMQConsumer(topic = SampleConstants.TOPIC, consumerGroup = TEST_CONSUMER_GROUP)
    static class TestEventCollector implements StreamMessageConcurrentlyConsumer<String> {

        final ConcurrentLinkedQueue<Message<String>> receivedMessages =
                new ConcurrentLinkedQueue<>();
        final AtomicReference<String> lastTraceparent = new AtomicReference<>();

        @Override
        public ConsumeAction onMessage(Message<String> message, ConsumeContext context) {
            receivedMessages.add(message);
            String tp = message.getUserProperties().get(StreamMQTracing.TRACEPARENT_KEY);
            if (tp != null) {
                lastTraceparent.set(tp);
            }
            return ConsumeAction.SUCCESS;
        }
    }
}
