package io.github.streammq.spring.cloud.stream.binder;

import io.github.streammq.core.listener.StreamMQListenerContainer;

import org.springframework.boot.actuate.health.AbstractHealthIndicator;
import org.springframework.boot.actuate.health.Health;

import java.util.Objects;

/**
 * StreamMQ Binder 健康检查指标。
 *
 * <p>继承 {@link AbstractHealthIndicator}，通过检查 {@link StreamMQListenerContainer#isRunning()}
 * 判断 Binder 健康状态，并报告当前已注册的消费者数量。
 *
 * <p>当 Listener 容器未运行时报告 DOWN，同时附带容器状态详情。
 * 仅当 Spring Boot Actuator 在 classpath 时生效。
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
            builder.down().withDetail("error", "StreamMQListenerContainer is not configured");
            return;
        }
        boolean running = listenerContainer.isRunning();
        int consumerCount = listenerContainer.getConsumers().size();
        if (running) {
            builder.up();
        } else {
            builder.down();
        }
        builder.withDetail("listenerContainer.running", running);
        builder.withDetail("listenerContainer.consumerCount", consumerCount);
    }
}
