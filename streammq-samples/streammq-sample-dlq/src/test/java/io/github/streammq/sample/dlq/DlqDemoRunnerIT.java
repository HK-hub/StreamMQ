/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.sample.dlq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.streammq.adapter.redisson.retry.FixedIntervalRetryPolicy;
import io.github.streammq.core.policy.RetryPolicy;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.redisson.Redisson;
import org.redisson.api.RStream;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;

/**
 * DLQ 示例「默认演示」闭环集成测试。
 *
 * <p><b>为什么不激活 {@code it} profile：</b>{@link DemoRunner} 标注了 {@code @Profile("!it")}， 本测试验证的正是用户执行
 * {@code mvn spring-boot:run} 时走的默认路径——启动即发送一条必然失败的消息。 <b>若 DemoRunner 只发送「注定成功」的消息（历史缺陷），DLQ
 * 演示就是空转</b>：默认运行既不产生重试，也不产生死信。
 *
 * <p>测试断言（真实 Redis，全程使用本次运行专属命名空间）：
 *
 * <ul>
 *   <li>示例内置的 {@link OrderDlqConsumer} 收到至少一条死信（重试 → DLQ → DLQ 消费闭环可达）
 *   <li>DLQ Stream {@code streammq:{ns}:dlq:order-consumer-group} 中存在死信条目（路由确实发生）
 * </ul>
 *
 * <p>重试策略被测试覆写为 100ms 固定间隔（与示例默认的 2s 数组等价但更快），保证秒级完成。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@SpringBootTest(classes = DlqSampleApplication.class)
@Import(DlqDemoRunnerIT.TestConfig.class)
@TestPropertySource(
        properties = {"spring.data.redis.host=127.0.0.1", "spring.data.redis.port=6379"})
@DisplayName("DLQ 默认演示（DemoRunner）集成测试")
@EnabledIf(
        value = "io.github.streammq.test.util.RedisAvailability#localhostAvailable",
        disabledReason = "Redis not available at localhost:6379")
class DlqDemoRunnerIT {

    /** 每次运行的唯一后缀：命名空间与载荷均带此后缀，避免跨运行残留数据污染断言 */
    private static final String RUN_ID = UUID.randomUUID().toString().substring(0, 8);

    /** 本次运行的专属命名空间（覆写 streammq.namespace），配合 {@link #cleanupNamespace()} 实现跨运行隔离 */
    private static final String IT_NAMESPACE = "dlq-demo-it-" + RUN_ID;

    /** 覆写全局命名空间：示例消费者与 DLQ 消费者全部继承该值（不再有硬编码 namespace） */
    @DynamicPropertySource
    static void overrideNamespace(DynamicPropertyRegistry registry) {
        registry.add("streammq.namespace", () -> IT_NAMESPACE);
    }

    @Autowired private OrderDlqConsumer orderDlqConsumer;

    @Autowired private RedissonClient redissonClient;

    /**
     * 清理本次运行命名空间下的全部键。
     *
     * <p>使用独立客户端：注入的 RedissonClient 在上下文关闭后不可复用。
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

    @Test
    @DisplayName("DemoRunner 默认消息重试耗尽后进入 DLQ 并被 DLQ 消费者接收")
    void demoRunnerMessageEntersDlqAndIsConsumed() {
        // ① 默认演示在启动时已发送必失败消息；重试 3 次后应被示例内置的 DLQ 消费者接收
        await().atMost(30, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                assertThat(orderDlqConsumer.getReceivedDlqMessageCount())
                                        .as("DemoRunner 发送的失败消息应经重试后进入 DLQ 并被消费")
                                        .isGreaterThanOrEqualTo(1));

        // ② DLQ Stream 中确实存在死信条目：证明失败消息被路由，而不是被消费者静默 ACK 丢弃
        RStream<String, String> dlqStream =
                redissonClient.getStream(
                        "streammq:" + IT_NAMESPACE + ":dlq:" + SampleConstants.CONSUMER_GROUP,
                        StringCodec.INSTANCE);
        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(dlqStream.size()).isGreaterThanOrEqualTo(1));
    }

    /** 测试专用配置：短间隔重试策略，使「重试 → DLQ」在秒级完成而非示例默认的分钟级。 */
    @Configuration
    static class TestConfig {
        @Bean
        public RetryPolicy streamMQRetryPolicy() {
            return new FixedIntervalRetryPolicy(100L, 3);
        }
    }
}
