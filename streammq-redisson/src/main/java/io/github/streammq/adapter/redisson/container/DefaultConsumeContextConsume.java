/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.container;

import io.github.streammq.core.consumer.ConsumeOrderlyContext;
import io.github.streammq.core.listener.ListenerRegistration;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageId;
import java.util.Map;
import java.util.Objects;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 默认 {@link ConsumeOrderlyContext} 实现，同时兼容普通消费场景。
 *
 * <p>由容器在 {@code handleMessage} 中创建，传给 Consumer 的 {@code onMessage} 方法。
 * 封装当前消息与注册信息，提供消息元数据访问与顺序消费分片信息。
 *
 * <p>消费结果由 {@code onMessage} 返回值表达，本上下文不再提供手动 ACK/nack/defer 调用。
 *
 * <p>线程安全：每个 {@code handleMessage} 调用创建独立实例，无需考虑并发。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@RequiredArgsConstructor
public class DefaultConsumeContextConsume implements ConsumeOrderlyContext {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultConsumeContextConsume.class);

    private final Message<?> message;
    private final ListenerRegistration<?> registration;

    /** 本消息实际使用的 Redis 消费者名（容器注入，与 XREADGROUP 使用的名称一致） */
    private final String actualConsumerName;

    /** 兼容构造器：未提供实际消费者名时按容器命名规则推导 */
    public DefaultConsumeContextConsume(Message<?> message, ListenerRegistration<?> registration) {
        this(message, registration, null);
    }

    @Override
    public String topic() {
        return message.getTopic();
    }

    @Override
    public String consumerGroup() {
        return registration.getGroup();
    }

    @Override
    public String consumerName() {
        // 返回真实参与消费者组协议的名称（XREADGROUP 使用的 consumerName），
        // 而非虚构的 "{group}-consumer"——业务基于此名称做运维排查时必须与 Redis 中一致
        if (actualConsumerName != null) {
            return actualConsumerName;
        }
        return registration.getGroup() + "-" + instanceTokenOrPlaceholder();
    }

    @Override
    public int reconsumeTimes() {
        return message.getReconsumeTimes();
    }

    @Override
    public long bornTimestamp() {
        return message.getBornTimestamp();
    }

    @Override
    public String bornHost() {
        return message.getBornHost();
    }

    @Override
    public Map<String, String> messageTrack() {
        return message.getProperties();
    }

    @Override
    public String ext(String key) {
        return message.getProperties().get(key);
    }

    @Override
    public String shardingKey() {
        return message.getShardingKey();
    }

    @Override
    public int shardId() {
        // 与 RedissonOrderlyShardLockManager 的分片路由保持同一公式：
        // (shardingKey.hashCode() & 0x7fffffff) % shardCount；非顺序消费（shardCount<=0）返回 0
        int shardCount = registration.getShardCount();
        if (shardCount <= 0) {
            return 0;
        }
        String shardingKey = message.getShardingKey();
        if (Objects.isNull(shardingKey)) {
            shardingKey = "";
        }
        return (shardingKey.hashCode() & 0x7fffffff) % shardCount;
    }

    @Override
    public MessageId queueOffset() {
        return message.getMessageId();
    }

    @Override
    public long backlog() {
        // 真实堆积量需每次调用 XPENDING，逐消息调用代价过高；当前实现固定返回 0，
        // 堆积监控请使用管理端点 /actuator/streammq/{group}/pending/{topic} 或诊断模块。
        return 0;
    }

    /** 实例标识占位：容器未注入时的降级值（与容器命名规则一致的提示性后缀）。 */
    private String instanceTokenOrPlaceholder() {
        return "unknown-instance";
    }
}
