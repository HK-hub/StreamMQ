/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import io.github.streammq.adapter.redisson.producer.RedissonStreamProducer;
import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import io.github.streammq.core.exception.StreamMQException;
import io.github.streammq.core.message.MessageBuilder;
import io.github.streammq.core.message.SendResult;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;

/**
 * Broker 不可用时的降级语义（故障注入）。
 *
 * <p>此前"依赖不可用"这一最高风险场景只由 {@code Assumptions} 跳过覆盖——测试套件从未验证过 Broker 不可达时 SDK
 * 的行为。本类用一个指向未监听端口的客户端注入故障，断言：
 *
 * <ul>
 *   <li>发送以类型化异常失败（不静默吞掉、不返回假的成功结果）；
 *   <li>不产生"半写入"（失败路径不得在健康 Broker 上留下残缺条目）；
 *   <li>故障链路不影响健康链路（故障是隔离的）。
 * </ul>
 */
@DisplayName("Broker 不可用降级（故障注入）")
class BrokerUnavailableIT extends AbstractRedisIT {

    /** 未监听端口：连接必定失败 */
    private static final String DEAD_ADDRESS = "redis://127.0.0.1:6399";

    private RedissonClient deadClient;
    private RedissonStreamProducer deadProducer;

    @BeforeEach
    void setUpDeadClient() {
        Config config = new Config();
        // lazyInitialization：Redisson 默认在 create() 阶段就建连校验，指向未监听端口会直接抛异常；
        // 置为惰性初始化后，连接失败延迟到首次命令，正好用于验证"首次命令失败"的降级语义。
        config.setLazyInitialization(true);
        config.useSingleServer()
                .setAddress(DEAD_ADDRESS)
                .setConnectionMinimumIdleSize(0)
                .setConnectionPoolSize(1)
                .setConnectTimeout(500)
                .setTimeout(500)
                .setRetryAttempts(0)
                .setRetryInterval(100);
        deadClient = Redisson.create(config);
        deadProducer =
                new RedissonStreamProducer(
                        deadClient, namespace, "dead-group", converter, 1000L, 0, 0, 0);
    }

    @AfterEach
    void tearDownDeadClient() {
        if (deadProducer != null) {
            deadProducer.close();
        }
        if (deadClient != null && !deadClient.isShutdown()) {
            // 指向未监听端口的客户端可能留有连接重试定时器：用无静默期/无等待超时的 shutdown，
            // 确保其线程被尽快回收（否则 surefire fork 可能迟迟不退出）。
            deadClient.shutdown(0, 0, TimeUnit.SECONDS);
        }
    }

    @Test
    @DisplayName("syncSend 在 Broker 不可达时抛出 StreamMQException，且不留下半写入")
    void syncSend_brokerUnreachable_throwsAndLeavesNoPartialWrite() {
        String topic = "unavailable-topic";
        assertThatThrownBy(
                        () ->
                                deadProducer.syncSend(
                                        MessageBuilder.<String>withTopic(topic)
                                                .body("payload")
                                                .build()))
                .isInstanceOf(StreamMQException.class);

        // 健康客户端视角：该 topic 上没有任何条目（失败路径未产生半写入）
        assertThat(
                        redisson.getStream(
                                        StreamMQKeys.topicStream(namespace, topic),
                                        StringCodec.INSTANCE)
                                .size())
                .isZero();
    }

    @Test
    @DisplayName("asyncSend 在 Broker 不可达时以异常完成，且健康链路不受影响")
    void asyncSend_brokerUnreachable_completesExceptionally() {
        String topic = "unavailable-async-topic";
        CompletableFuture<SendResult> future =
                deadProducer.asyncSend(
                        MessageBuilder.<String>withTopic(topic).body("payload").build());

        await().atMost(20, TimeUnit.SECONDS).until(future::isDone);
        assertThat(future).isCompletedExceptionally();

        // 故障链路与健康链路隔离：健康客户端仍可正常发送
        RedissonStreamProducer healthyProducer =
                new RedissonStreamProducer(
                        redisson, namespace, "healthy-group", converter, 3000L, 0, 0, 0);
        try {
            assertThat(
                            healthyProducer.syncSend(
                                    MessageBuilder.<String>withTopic("available-topic")
                                            .body("payload")
                                            .build()))
                    .isNotNull();
        } finally {
            healthyProducer.close();
        }
    }
}
