/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.sample.delay;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.streammq.core.enums.DelayLevel;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.SendResult;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

/**
 * 延时消息示例集成测试，基于真实本地 Redis 连接验证延时消息的完整收发链路。
 *
 * <p>测试场景覆盖：
 *
 * <ul>
 *   <li>{@link #sendCustomDelayMessage()} — 发送自定义延时消息（5 秒）并验证消费
 *   <li>{@link #sendFixedDelayMessage()} — 发送固定延时消息（{@link DelayLevel#SECOND_10}）并验证消费
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
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@DisplayName("延时消息示例集成测试")
@EnabledIf(
        value = "io.github.streammq.test.util.RedisAvailability#localhostAvailable",
        disabledReason = "Redis not available at localhost:6379")
class DelaySampleIT {

    @Autowired private DelayMessageProducer producer;

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
}
