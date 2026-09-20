/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.sample.delay;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.streammq.adapter.redisson.retry.FixedIntervalRetryPolicy;
import io.github.streammq.core.enums.DelayLevel;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.SendResult;
import io.github.streammq.core.policy.RetryPolicy;
import java.util.List;
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
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * 延时消息示例集成测试，基于真实本地 Redis 连接验证延时消息的完整收发链路。
 *
 * <p>测试场景覆盖：
 *
 * <ul>
 *   <li>{@link #sendCustomDelayMessage()} — 发送自定义延时消息（5 秒）并验证消费
 *   <li>{@link #sendFixedDelayMessage()} — 发送固定延时消息（{@link DelayLevel#SECOND_10}）并验证消费
 *   <li>{@link #failedDelayMessageEntersDlqAfterRetries()} — 消费失败 → 重试耗尽 → 最终进入 DLQ（不丢消息）
 * </ul>
 *
 * <p>本测试使用独立的测试消费者组（{@code delay-order-consumer-group-it}）， 通过 {@link DelayMessageTestConsumer}
 * 收集消息，配合 Awaitility 进行超时等待。
 *
 * <p>前置条件：本地 Redis 已启动（默认地址 {@code 127.0.0.1:6379}）。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@SpringBootTest(classes = DelaySampleApplication.class)
@ActiveProfiles("it")
@Import(DelaySampleIT.TestConfig.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@DisplayName("延时消息示例集成测试")
@EnabledIf(
        value = "io.github.streammq.test.util.RedisAvailability#localhostAvailable",
        disabledReason = "Redis not available at localhost:6379")
class DelaySampleIT {

    /** 每次运行的唯一后缀：命名空间与载荷均带此后缀，避免跨运行残留数据污染断言 */
    private static final String RUN_ID = UUID.randomUUID().toString().substring(0, 8);

    /** 本次运行的专属命名空间（覆写 streammq.namespace），配合 {@link #cleanupNamespace()} 实现跨运行隔离 */
    private static final String IT_NAMESPACE = "delay-it-" + RUN_ID;

    /** 覆写全局命名空间，避免与历史运行/其它示例共享 streammq:delay:* 键 */
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

    @Autowired private DelayMessageProducer producer;

    /** 示例自带的消费者：用于注入失败，验证失败消息最终进入 DLQ 而不是被静默 ACK 吞掉 */
    @Autowired private DelayMessageConsumer delayMessageConsumer;

    @Autowired private RedissonClient redissonClient;

    /**
     * 发送自定义延时消息（5 秒延时时长），验证消息在延时期满后被正确消费。
     *
     * <p>验证要点：
     *
     * <ul>
     *   <li>消息发送成功，返回非空 MessageId
     *   <li>消息在超时时间内（15 秒）被测试消费者接收
     *   <li>接收到的消息包含正确的 orderId、body 和 tag（{@code custom-delay}）
     * </ul>
     */
    @Test
    @DisplayName("sendCustomDelayMessage — 自定义延时消息（5 秒）")
    void sendCustomDelayMessage() throws InterruptedException {
        DelayMessageTestConsumer.reset();

        String orderId = "order-custom-" + System.currentTimeMillis();
        String content = "自定义延时消息测试内容";

        SendResult result = producer.sendCustomDelayMessage(orderId, content, 5000L);

        assertThat(result.isSuccess()).as("自定义延时消息发送应成功").isTrue();
        assertThat(result.getMessageId()).as("发送结果应包含 MessageId").isNotNull();

        DelayMessageTestConsumer.awaitMessages(1, 15_000L);

        List<Message<String>> messages = DelayMessageTestConsumer.getReceivedMessages();
        assertThat(messages).as("应接收到 1 条延时消息").hasSize(1);
        assertThat(messages.get(0).getKeys()).as("消息 keys 应匹配 orderId").isEqualTo(orderId);
        assertThat(messages.get(0).getBody()).as("消息 body 应匹配发送内容").isEqualTo(content);
        assertThat(messages.get(0).getTag()).as("消息 tag 应为 custom-delay").isEqualTo("custom-delay");
    }

    /**
     * 发送固定延时消息（{@link DelayLevel#SECOND_10}，10 秒延时时长）， 验证消息在延时期满后被正确消费。
     *
     * <p>验证要点：
     *
     * <ul>
     *   <li>消息发送成功，返回非空 MessageId
     *   <li>消息在超时时间内（20 秒）被测试消费者接收
     *   <li>接收到的消息包含正确的 orderId、body 和 tag（{@code delay}）
     * </ul>
     */
    @Test
    @DisplayName("sendFixedDelayMessage — 固定延时消息（10 秒级别）")
    void sendFixedDelayMessage() throws InterruptedException {
        DelayMessageTestConsumer.reset();

        String orderId = "order-fixed-" + System.currentTimeMillis();
        String content = "固定延时消息测试内容";

        SendResult result = producer.sendFixedDelayMessage(orderId, content, DelayLevel.SECOND_10);

        assertThat(result.isSuccess()).as("固定延时消息发送应成功").isTrue();
        assertThat(result.getMessageId()).as("发送结果应包含 MessageId").isNotNull();

        DelayMessageTestConsumer.awaitMessages(1, 20_000L);

        List<Message<String>> messages = DelayMessageTestConsumer.getReceivedMessages();
        assertThat(messages).as("应接收到 1 条延时消息").hasSize(1);
        assertThat(messages.get(0).getKeys()).as("消息 keys 应匹配 orderId").isEqualTo(orderId);
        assertThat(messages.get(0).getBody()).as("消息 body 应匹配发送内容").isEqualTo(content);
        assertThat(messages.get(0).getTag()).as("消息 tag 应为 delay").isEqualTo("delay");
    }

    /**
     * 验证延时消息消费失败不会被静默 ACK 吞掉：重试耗尽（{@code maxReconsumeTimes=3}）后由框架路由到 DLQ Stream {@code
     * streammq:{ns}:dlq:{consumerGroup}}，供 DLQ 消费者 / 运维处理。
     *
     * <p>该用例守护「重试用尽即 SUCCESS 吞消息」这一历史反模式：失败消息必须最终出现在 DLQ 中。
     */
    @Test
    @DisplayName("sendCustomDelayMessage — 消费失败的消息重试耗尽后进入 DLQ")
    void failedDelayMessageEntersDlqAfterRetries() {
        String orderId = "order-fail-" + System.currentTimeMillis();
        String content = "延时失败消息（验证 DLQ 路由）";

        delayMessageConsumer.setFailOrderId(orderId);
        try {
            SendResult result = producer.sendCustomDelayMessage(orderId, content, 1000L);
            assertThat(result.isSuccess()).as("延时消息发送应成功").isTrue();

            RStream<String, String> dlqStream =
                    redissonClient.getStream(
                            "streammq:" + IT_NAMESPACE + ":dlq:" + SampleConstants.CONSUMER_GROUP,
                            StringCodec.INSTANCE);
            await().atMost(30, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    assertThat(dlqStream.size())
                                            .as("失败消息应在重试耗尽后进入 DLQ，而不是被静默丢弃")
                                            .isGreaterThanOrEqualTo(1));
        } finally {
            delayMessageConsumer.clearFailOrderId();
        }
    }

    // ===================== 测试配置 =====================

    /** 测试专用配置：短间隔重试策略，使「失败 → 重试耗尽 → DLQ」在秒级完成（示例默认退避为分钟级）。 */
    @Configuration
    static class TestConfig {
        @Bean
        public RetryPolicy streamMQRetryPolicy() {
            return new FixedIntervalRetryPolicy(100L, 3);
        }
    }
}
