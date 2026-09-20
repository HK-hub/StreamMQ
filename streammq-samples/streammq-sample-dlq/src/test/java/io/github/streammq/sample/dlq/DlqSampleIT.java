/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
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
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
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
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
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
 *   <li>{@code assertNamespaceConsistencyThenCleanupNamespace} 示例 DLQ 消费者与生产端同命名空间， 且运行后示例默认命名空间
 *       {@code streammq:dlq:*} 不新增键（注解继承全局命名空间，收发不分家）
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
@TestPropertySource(
        properties = {"spring.data.redis.host=127.0.0.1", "spring.data.redis.port=6379"})
@DisplayName("DLQ 示例集成测试")
@EnabledIf(
        value = "io.github.streammq.test.util.RedisAvailability#localhostAvailable",
        disabledReason = "Redis not available at localhost:6379")
class DlqSampleIT {

    private static final String TEST_CONSUMER_GROUP = "test-collector-group";
    private static final String TEST_FAIL_CONSUMER_GROUP = "test-fail-group";
    private static final String TOPIC = SampleConstants.TOPIC;

    /** 每次运行的唯一后缀：命名空间与载荷均带此后缀，避免跨运行残留数据污染断言 */
    private static final String RUN_ID = UUID.randomUUID().toString().substring(0, 8);

    /**
     * 本次运行的专属命名空间（覆写 streammq.namespace），配合 {@link
     * #assertNamespaceConsistencyThenCleanupNamespace()} 实现跨运行隔离
     */
    private static final String IT_NAMESPACE = "dlq-it-" + RUN_ID;

    /** 覆写全局命名空间，避免与历史运行/其它示例共享 streammq:dlq:* 键 */
    @DynamicPropertySource
    static void overrideNamespace(DynamicPropertyRegistry registry) {
        registry.add("streammq.namespace", () -> IT_NAMESPACE);
    }

    /** 示例 application.yml 中配置的默认命名空间（streammq.namespace: dlq） */
    private static final String SAMPLE_DEFAULT_NAMESPACE = "dlq";

    /** IT 开始前示例默认命名空间下已存在的键快照：用于断言本次运行没有在该命名空间留下任何新键 */
    private static Set<String> sampleDefaultNamespaceKeysBeforeIt;

    /**
     * 在任何 Spring 上下文创建之前，快照示例默认命名空间（{@code streammq:dlq:*}）下的键。
     *
     * <p>上下文创建发生在测试实例注入阶段（晚于本回调），因此快照能覆盖「消费者注册时建组」这一时刻。
     */
    @BeforeAll
    static void snapshotSampleDefaultNamespaceKeys() {
        sampleDefaultNamespaceKeysBeforeIt = keysOfSampleDefaultNamespace();
    }

    /** 读取示例默认命名空间下的键（自建客户端，不依赖上下文生命周期）。 */
    private static Set<String> keysOfSampleDefaultNamespace() {
        RedissonClient client = Redisson.create(newCleanupConfig());
        try {
            return listKeys(client, "streammq:" + SAMPLE_DEFAULT_NAMESPACE + ":*");
        } finally {
            client.shutdown();
        }
    }

    /** 按 pattern 读取键集合。 */
    private static Set<String> listKeys(RedissonClient client, String pattern) {
        Set<String> keys = new LinkedHashSet<>();
        for (String key : client.getKeys().getKeysByPattern(pattern, 100)) {
            keys.add(key);
        }
        return keys;
    }

    /** 清理用 Redisson 配置：直连本地 Redis，字符串编解码。 */
    private static Config newCleanupConfig() {
        Config config = new Config();
        config.useSingleServer().setAddress("redis://127.0.0.1:6379").setDatabase(0);
        config.setCodec(StringCodec.INSTANCE);
        return config;
    }

    /**
     * 断言「消费端与生产端命名空间一致」且没有跨命名空间残留，然后清理本次运行命名空间下的全部键。
     *
     * <p><b>为什么断言这两点：</b>IT 通过 {@link #overrideNamespace} 把全局命名空间覆写为 {@link
     * #IT_NAMESPACE}，示例中的注解（{@code @StreamMQConsumer} / {@code @StreamMQDlqConsumer}）统一继承该全局值。
     * 若某个注解把 namespace 硬编码为编译期常量（历史上 {@code OrderDlqConsumer} 就是这样），会同时出现两个症状：
     *
     * <ul>
     *   <li>示例内置 DLQ 消费者的 DLQ Stream 建在错误命名空间（{@code streammq:dlq:*}）而非生产端写入死信的 {@code
     *       streammq:{IT_NAMESPACE}:dlq:*}——永远收不到死信；
     *   <li>每次门禁都在示例默认命名空间残留空 DLQ 流与心跳键。
     * </ul>
     *
     * <p>使用独立客户端：{@code @DirtiesContext(AFTER_EACH_TEST_METHOD)} 在每个方法后关闭上下文， 注入的 RedissonClient 在
     * {@code @AfterAll} 阶段已被 shutdown，无法复用于清理。
     */
    @AfterAll
    static void assertNamespaceConsistencyThenCleanupNamespace() {
        RedissonClient client = Redisson.create(newCleanupConfig());
        try {
            // ① 示例内置 DLQ 消费者跟随全局命名空间：其 DLQ Stream 必须建在 IT 专属命名空间下
            String sampleDlqStreamKey =
                    "streammq:" + IT_NAMESPACE + ":dlq:" + SampleConstants.CONSUMER_GROUP;
            assertThat(client.getKeys().countExists(sampleDlqStreamKey))
                    .as("示例 DLQ 消费者必须继承全局命名空间（与生产端一致），应存在 %s", sampleDlqStreamKey)
                    .isEqualTo(1L);

            // ② 示例默认命名空间不得新增键：硬编码 namespace 的残留检测
            Set<String> newKeysInDefaultNamespace =
                    listKeys(client, "streammq:" + SAMPLE_DEFAULT_NAMESPACE + ":*");
            newKeysInDefaultNamespace.removeAll(sampleDefaultNamespaceKeysBeforeIt);
            assertThat(newKeysInDefaultNamespace)
                    .as(
                            "示例默认命名空间 streammq:%s:* 在 IT 期间不得新增键"
                                    + "（新增说明注解把 namespace 硬编码，DLQ 收发命名空间分离）",
                            SAMPLE_DEFAULT_NAMESPACE)
                    .isEmpty();

            // ③ 清理本次运行的专属命名空间
            client.getKeys().deleteByPattern("streammq:" + IT_NAMESPACE + ":*");
        } finally {
            client.shutdown();
        }
    }

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
            // 注意：不得删除 topic 流（streammq:{ns}:msg:{topic}）。
            // Redis 的 DEL 会连带销毁该流上的全部消费者组；监听器随后以 NEWEST（默认位点）重建组，
            // 导致本次测试发送的消息（在组重建之前写入）永远被错过，表现为 receivedMessages 恒为 0。
            // 正确做法：保留 topic 流与消费者组（组位点随消费推进，已 ACK 消息不会重复投递，天然隔离），
            // 仅清理 retry / dlq 流以避免跨测试污染。
            String retryKey =
                    "streammq:"
                            + IT_NAMESPACE
                            + ":retry:msg:"
                            + TOPIC
                            + ":"
                            + TEST_FAIL_CONSUMER_GROUP;
            String dlqKey = "streammq:" + IT_NAMESPACE + ":dlq:" + TEST_FAIL_CONSUMER_GROUP;
            System.out.println("=== Cleaning streams: " + retryKey + ", " + dlqKey);
            long deleted = redissonClient.getKeys().delete(retryKey, dlqKey);
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
        String orderId = "IT-NORMAL-001-" + RUN_ID;
        String content = "normal-order-content-" + RUN_ID;

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
        String orderId = "IT-DLQ-001-" + RUN_ID;
        String content = "dlq-order-content-" + RUN_ID;

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
        String orderId = "IT-DLQ-002-" + RUN_ID;
        String content = "dlq-order-content-002-" + RUN_ID;

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
     *
     * <p>不显式声明 {@code namespace}：注解值只能取编译期常量，无法携带每次运行的 {@link #RUN_ID}， 留空即回退到被 {@link
     * #overrideNamespace} 覆写的全局命名空间，与 {@link TestFailConsumer} 写入死信所用的命名空间保持一致。
     */
    @StreamMQDlqConsumer(consumerGroup = TEST_FAIL_CONSUMER_GROUP)
    static class TestDlqConsumer extends AbstractDlqMessageConsumer<String> {

        final ConcurrentLinkedQueue<Message<String>> receivedDlqMessages =
                new ConcurrentLinkedQueue<>();

        @Override
        public void onDlqMessage(Message<String> message, ConsumeContext context) {
            receivedDlqMessages.add(message);
        }
    }
}
