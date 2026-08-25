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
 * StreamMQ Spring Cloud Stream Binder 真实集成测试。
 *
 * <p>使用本地 Redis（127.0.0.1:6379）进行真实的消息收发， 验证通过标准 Spring Cloud Stream API（StreamBridge + Consumer）与
 * StreamMQ Binder 的完整链路。
 *
 * <p>测试场景：
 *
 * <ul>
 *   <li>通过 {@link StreamBridge} 发送消息到 Spring Cloud Stream binding
 *   <li>StreamMQ Binder 将消息写入 Redis Stream
 *   <li>StreamMQ Consumer 从 Redis Stream 消费消息
 *   <li>Binder 将消息转换为 Spring Messaging 消息并传递给 {@code Consumer<String>} Bean
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@SpringBootTest(
        classes = StreamMQBinderIT.TestApplication.class,
        properties = {
            "spring.application.name=streammq-binder-it",
            "streammq.enabled=true",
            "streammq.namespace=binder-it",
            "streammq.producer.group=binder-it-producer",
            "redisson.singleServerConfig.address=redis://127.0.0.1:6379",
            "redisson.singleServerConfig.database=9",
            "spring.cloud.stream.default-binder=streammq",
            "spring.cloud.stream.binders.streammq.type=streammq",
            "spring.cloud.stream.function.definition=receiveMsg",
            "spring.cloud.stream.bindings.receiveMsg-in-0.destination=binder-it-topic",
            "spring.cloud.stream.bindings.receiveMsg-in-0.group=binder-it-cg",
            "spring.cloud.stream.source=sendMsg",
            "spring.cloud.stream.bindings.sendMsg-out-0.destination=binder-it-topic",
            "spring.cloud.stream.bindings.sendMsg-out-0.binder=streammq"
        })
@DirtiesContext
@DisplayName("StreamMQ Binder 真实集成测试")
@EnabledIf(
        value = "io.github.streammq.core.util.RedisAvailability#localhostAvailable",
        disabledReason = "Redis not available at localhost:6379")
class StreamMQBinderIT {
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

    /**
     * 每个测试前清空收集器，避免上一轮测试遗留消息干扰断言。
     *
     * <p><b>不使用 {@code flushdb()} 清空 Redis</b>：监听器容器在 Spring 上下文启动时即创建消费组， 此时调用 {@code flushdb()}
     * 会删除消费组但 {@code RedissonStreamListener.groupCreated} 标志仍为 true， 导致后续 {@code readGroup}
     * 持续失败。消费组通过 {@code XREADGROUP >} 只投递从未投递过的新消息， 已消费并 ack 的消息不会被重复投递，因此仅清空收集器即可保证测试隔离。
     */
    @BeforeEach
    void clearState() {
        collector.clear();
    }

    @Test
    @DisplayName("通过 StreamBridge 发送消息，Consumer 应收到完整内容")
    void shouldSendAndReceiveViaSpringCloudStream() {
        streamBridge.send("sendMsg-out-0", MessageBuilder.withPayload("hello-streammq").build());

        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> assertThat(collector.getReceived()).contains("hello-streammq"));
    }

    @Test
    @DisplayName("发送多条消息，Consumer 应收到全部")
    void shouldSendAndReceiveMultipleMessages() {
        for (int i = 0; i < 5; i++) {
            streamBridge.send("sendMsg-out-0", MessageBuilder.withPayload("msg-" + i).build());
        }

        await().atMost(15, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                assertThat(collector.getReceived())
                                        .contains("msg-0", "msg-1", "msg-2", "msg-3", "msg-4"));
    }

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
     * sendMsg-out-0}， 由 StreamMQ Binder 创建对应的生产者。
     */
    @SpringBootApplication
    static class TestApplication {

        @Bean
        public Consumer<String> receiveMsg() {
            return payload -> TestMessageCollector.INSTANCE.add(payload);
        }

        @Bean
        public TestMessageCollector testMessageCollector() {
            return TestMessageCollector.INSTANCE;
        }
    }

    /** 测试消息收集器，单例模式确保 Spring Bean 与静态 Consumer 引用同一实例。 */
    static class TestMessageCollector {
        static final TestMessageCollector INSTANCE = new TestMessageCollector();
        private final List<String> received = new CopyOnWriteArrayList<>();

        void add(String message) {
            received.add(message);
        }

        List<String> getReceived() {
            return received;
        }

        void clear() {
            received.clear();
        }
    }
}
