package io.github.streammq.spring.boot.autoconfigure;

import io.github.streammq.core.consumer.StreamMqListenerContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.context.SmartLifecycle;

/**
 * 将 {@link StreamMqListenerContainer} 适配为 Spring {@link SmartLifecycle}，
 * 让 Spring 容器统一管理 Listener 的启动与停止。
 *
 * <p>启动相位 {@link #getPhase} 设为较低值（{@code Integer.MAX_VALUE - 200}），
 * 确保在调度器（{@code Integer.MAX_VALUE - 100}）之后启动、之前停止。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class StreamMqListenerContainerLifecycle implements SmartLifecycle {

    private static final Logger LOG = LoggerFactory.getLogger(StreamMqListenerContainerLifecycle.class);

    /** 启动相位：低于调度器，确保调度器先就绪 */
    public static final int PHASE = Integer.MAX_VALUE - 200;

    private final StreamMqListenerContainer listenerContainer;
    private volatile boolean running = false;

    /**
     * 构造 Lifecycle。
     *
     * @param listenerContainer Listener 容器
     */
    public StreamMqListenerContainerLifecycle(StreamMqListenerContainer listenerContainer) {
        this.listenerContainer = listenerContainer;
    }

    @Override
    public void start() {
        if (running) {
            return;
        }
        LOG.info("Starting StreamMqListenerContainer (SmartLifecycle phase={})", PHASE);
        listenerContainer.start();
        running = true;
    }

    @Override
    public void stop() {
        if (!running) {
            return;
        }
        LOG.info("Stopping StreamMqListenerContainer");
        listenerContainer.stop();
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return PHASE;
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }
}
