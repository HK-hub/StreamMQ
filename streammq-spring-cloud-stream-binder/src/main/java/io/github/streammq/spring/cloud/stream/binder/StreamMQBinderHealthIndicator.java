/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.spring.cloud.stream.binder;

import io.github.streammq.core.listener.StreamMQListenerContainer;
import java.util.Objects;
import org.springframework.boot.actuate.health.AbstractHealthIndicator;
import org.springframework.boot.actuate.health.Health;

/**
 * StreamMQ Binder 健康检查指标。
 *
 * <p>继承 {@link AbstractHealthIndicator}，通过检查 {@link StreamMQListenerContainer#isRunning()} 判断
 * Binder 健康状态，并报告当前已注册的消费者数量。
 *
 * <p>当 Listener 容器未运行时报告 DOWN，同时附带容器状态详情。 仅当 Spring Boot Actuator 在 classpath 时生效。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class StreamMQBinderHealthIndicator extends AbstractHealthIndicator {

    private final StreamMQListenerContainer listenerContainer;

    /**
     * 构造健康检查指标。
     *
     * @param listenerContainer StreamMQ Listener 容器（可为 null，表示未装配）
     */
    public StreamMQBinderHealthIndicator(StreamMQListenerContainer listenerContainer) {
        this.listenerContainer = listenerContainer;
    }

    @Override
    protected void doHealthCheck(Health.Builder builder) throws Exception {
        if (Objects.isNull(listenerContainer)) {
            builder.down()
                    .withDetail(
                            StreamMQBinderConstants.HEALTH_DETAIL_ERROR,
                            "StreamMQListenerContainer is not configured");
            return;
        }
        boolean running = listenerContainer.isRunning();
        // 只判 isRunning() 会漏掉"容器在跑但消费循环启动失败"——那些人消费者在注册表可见却永不消费，
        // 健康面却报 UP（假健康，与 starter 指示器的判据不一致）。消费循环健康必须一并纳入。
        boolean consumeLoopsHealthy = listenerContainer.isConsumeLoopsHealthy();
        int consumerCount = listenerContainer.getConsumers().size();
        if (running && consumeLoopsHealthy) {
            builder.up();
        } else {
            builder.down();
        }
        builder.withDetail(StreamMQBinderConstants.HEALTH_DETAIL_LC_RUNNING, running);
        builder.withDetail("consumeLoopsHealthy", consumeLoopsHealthy);
        if (!consumeLoopsHealthy) {
            builder.withDetail("consumeLoopFailures", listenerContainer.getConsumeLoopFailures());
        }
        builder.withDetail(StreamMQBinderConstants.HEALTH_DETAIL_LC_CONSUMER_COUNT, consumerCount);
    }
}
