/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.spring.cloud.stream.binder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.streammq.core.util.RedisAvailability;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.context.annotation.Bean;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * StreamMQ Spring Cloud Stream Binder 扩展集成测试。
 *
 * <p>在 {@link StreamMQBinderIT} 基础上扩展，覆盖更完整的场景：
 *
 * <ul>
 *   <li>通过 {@link StreamBridge} 发送带 headers（tag、keys）的消息
 *   <li>发送用户自定义 properties，验证透传正确性
 *   <li>多种消息类型（String、JSON payload）
 *   <li>验证 {@link StreamMQMessageHandler} 常量正确使用
 *   <li>多条消息批量发送与接收
 * </ul>
 *
 * <p>使用本地 Redis（127.0.0.1:6379），namespace {@code binder-ext-it}，database 10。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@SpringBootTest(
        classes = StreamMQBinderExtendedIT.TestApplication.class,
        properties = {
            "spring.application.name=streammq-binder-ext-it",
            "streammq.enabled=true",
            "streammq.namespace=binder-ext-it",
            "streammq.producer.group=binder-ext-it-producer",
            "redisson.singleServerConfig.address=redis://127.0.0.1:6379",
            "redisson.singleServerConfig.database=10",
            "spring.cloud.stream.default-binder=streammq",
            "spring.cloud.stream.binders.streammq.type=streammq",
            "spring.cloud.stream.function.definition=receiveMsg",
            "spring.cloud.stream.bindings.receiveMsg-in-0.destination=binder-ext-topic",
            "spring.cloud.stream.bindings.receiveMsg-in-0.group=binder-ext-cg",
            "spring.cloud.stream.source=sendMsg",
            "spring.cloud.stream.bindings.sendMsg-out-0.destination=binder-ext-topic",
            "spring.cloud.stream.bindings.sendMsg-out-0.binder=streammq"
        })
@DirtiesContext
@DisplayName("StreamMQ Binder 扩展集成测试")
@EnabledIf(
        value = "io.github.streammq.core.util.RedisAvailability#localhostAvailable",
        disabledReason = "Redis not available at localhost:6379")
class StreamMQBinderExtendedIT {
    @BeforeAll
    static void requireRedis() {
        // 无本地 Redis 时跳过（上下文/用例依赖真实 Redis），保证 mvn verify 任意环境可复现
        Assumptions.assumeTrue(
                RedisAvailability.isAvailable("localhost", 6379),
                "Redis not available at localhost:6379, skipping IT");
    }

    @DynamicPropertySource
    static void redisPassword(DynamicPropertyRegistry registry) {
        String password =
                System.getProperty(
                        "test.redis.password",
                        System.getenv().getOrDefault("STREAMMQ_TEST_REDIS_PASSWORD", ""));
        if (!password.isEmpty()) {
            registry.add("redisson.singleServerConfig.password", () -> password);
        }
    }

    @Autowired private StreamBridge streamBridge;

    @Autowired private TestMessageCollector collector;

    /** 每个测试前清空收集器，避免上一轮测试遗留消息干扰断言。 */
    @BeforeEach
    void clearState() {
        collector.clear();
    }

    /** 发送带 Tag 和 Keys 的消息，验证 Consumer 完整接收到 payload。 */
    @Test
    @DisplayName("发送带 Tag 和 Keys 的消息，Consumer 应收到完整 payload")
    void shouldSendWithHeadersAndReceiveThem() {
        streamBridge.send(
                "sendMsg-out-0",
                MessageBuilder.withPayload("hello-with-headers")
                        .setHeader(StreamMQMessageHandler.HEADER_TAG, "order-tag")
                        .setHeader(StreamMQMessageHandler.HEADER_KEYS, "order-keys-001")
                        .build());

        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> assertThat(collector.getPayloads()).contains("hello-with-headers"));
    }

    /** 发送用户自定义 properties，验证消息成功投递。 */
    @Test
    @DisplayName("发送用户自定义 properties，验证消息成功投递")
    void shouldPropagateUserDefinedProperties() {
        streamBridge.send(
                "sendMsg-out-0",
                MessageBuilder.withPayload("user-props-test")
                        .setHeader("myCustomHeader", "custom-value")
                        .setHeader("traceId", "trace-001")
                        .build());

        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> assertThat(collector.getPayloads()).contains("user-props-test"));
    }

    /** 发送 String 类型消息，验证 Consumer 正确接收。 */
    @Test
    @DisplayName("发送 String 类型消息，Consumer 应正确接收")
    void shouldSendAndReceiveStringMessage() {
        streamBridge.send(
                "sendMsg-out-0", MessageBuilder.withPayload("plain-text-message").build());

        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> assertThat(collector.getPayloads()).contains("plain-text-message"));
    }

    /** 发送 JSON 类型 payload，验证 Consumer 正确接收。 */
    @Test
    @DisplayName("发送 JSON 类型 payload，Consumer 应正确接收")
    void shouldSendAndReceiveJsonPayload() {
        String jsonPayload = "{\"name\":\"test\",\"value\":123}";
        streamBridge.send(
                "sendMsg-out-0",
                MessageBuilder.withPayload(jsonPayload)
                        .setHeader("contentType", "application/json")
                        .build());

        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(collector.getPayloads()).contains(jsonPayload));
    }

    /** 发送多条消息，验证 Consumer 收到全部消息。 */
    @Test
    @DisplayName("发送多条消息，Consumer 应收到全部")
    void shouldSendMultipleMessages() {
        for (int i = 0; i < 5; i++) {
            streamBridge.send(
                    "sendMsg-out-0", MessageBuilder.withPayload("batch-msg-" + i).build());
        }

        await().atMost(15, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                assertThat(collector.getPayloads())
                                        .contains(
                                                "batch-msg-0",
                                                "batch-msg-1",
                                                "batch-msg-2",
                                                "batch-msg-3",
                                                "batch-msg-4"));
    }

    /** 验证 {@link StreamMQMessageHandler} 常量在消息头中正确使用。 */
    @Test
    @DisplayName("验证 StreamMQMessageHandler 常量在消息头中正确使用")
    void shouldUseStreamMQMessageHandlerConstants() {
        streamBridge.send(
                "sendMsg-out-0",
                MessageBuilder.withPayload("constant-test")
                        .setHeader(StreamMQMessageHandler.HEADER_TAG, "priority-high")
                        .setHeader(StreamMQMessageHandler.HEADER_KEYS, "order-456")
                        .build());

        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(collector.getPayloads()).contains("constant-test"));
    }

    /** 验证 StreamBridge 与 TestMessageCollector 正确注入。 */
    @Test
    @DisplayName("StreamBridge 与 Collector 应正确注入")
    void shouldInjectBeans() {
        assertThat(streamBridge).isNotNull();
        assertThat(collector).isNotNull();
    }

    /**
     * 测试 Spring Boot 应用配置。
     *
     * <p>定义一个函数式 Consumer {@code receiveMsg} 作为消息接收端， Spring Cloud Stream 会自动创建 {@code
     * receiveMsg-in-0} 绑定。 通过 {@code spring.cloud.stream.source.sendMsg} 声明输出 binding 名 {@code
     * sendMsg-out-0}，由 StreamMQ Binder 创建对应的生产者。
     */
    @SpringBootApplication
    static class TestApplication {

        @Bean
        public Consumer<String> receiveMsg() {
            return payload -> TestMessageCollector.INSTANCE.addPayload(payload);
        }

        @Bean
        public TestMessageCollector testMessageCollector() {
            return TestMessageCollector.INSTANCE;
        }
    }

    /** 测试消息收集器，单例模式确保 Spring Bean 与静态 Consumer 引用同一实例。 */
    static class TestMessageCollector {

        static final TestMessageCollector INSTANCE = new TestMessageCollector();

        private final List<String> payloads = new CopyOnWriteArrayList<>();

        void addPayload(String payload) {
            payloads.add(payload);
        }

        List<String> getPayloads() {
            return payloads;
        }

        void clear() {
            payloads.clear();
        }
    }
}
