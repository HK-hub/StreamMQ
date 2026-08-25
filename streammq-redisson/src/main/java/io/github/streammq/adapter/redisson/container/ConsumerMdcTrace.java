/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.container;

import io.github.streammq.adapter.redisson.support.MdcKeys;
import io.github.streammq.core.listener.ListenerRegistration;
import io.github.streammq.core.message.Message;
import java.util.Objects;
import org.slf4j.MDC;

/**
 * 消费侧 MDC 结构化日志上下文管理（策略类）。
 *
 * <p>在 {@code handleMessage} 入口注入 topic、consumerGroup、messageId、shardingKey、reconsumeTimes 到 SLF4J
 * {@link MDC}，出口清理，使消费日志可通过 MDC 占位符输出结构化上下文。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public final class ConsumerMdcTrace {

    private ConsumerMdcTrace() {}

    /**
     * 注入消费侧 MDC 上下文。
     *
     * @param message 待消费消息
     * @param reg Listener 注册信息
     */
    public static void inject(Message<?> message, ListenerRegistration<?> reg) {
        MDC.put(MdcKeys.TOPIC, reg.getTopic());
        MDC.put(MdcKeys.CONSUMER_GROUP, reg.getGroup());
        if (Objects.nonNull(message.getMessageId())) {
            MDC.put(MdcKeys.MSG_ID, String.valueOf(message.getMessageId()));
        }
        if (Objects.nonNull(message.getShardingKey())) {
            MDC.put(MdcKeys.SHARDING_KEY, message.getShardingKey());
        }
        MDC.put(MdcKeys.RECONSUME_TIMES, String.valueOf(message.getReconsumeTimes()));
    }

    /** 清理消费侧 MDC 上下文。 */
    public static void clear() {
        MDC.remove(MdcKeys.TOPIC);
        MDC.remove(MdcKeys.CONSUMER_GROUP);
        MDC.remove(MdcKeys.MSG_ID);
        MDC.remove(MdcKeys.SHARDING_KEY);
        MDC.remove(MdcKeys.RECONSUME_TIMES);
    }
}
