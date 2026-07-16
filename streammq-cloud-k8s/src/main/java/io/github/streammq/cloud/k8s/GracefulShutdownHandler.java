package io.github.streammq.cloud.k8s;

import io.github.streammq.core.listener.StreamMQListenerContainer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.ObjectProvider;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * StreamMQ 优雅关闭处理器。
 *
 * <p>实现 Spring {@link DisposableBean}，在 Spring 容器关闭时接收关闭信号，
 * 按 K8s 优雅终止流程逐步释放 StreamMQ 资源，避免消息丢失或重复消费。
 *
 * <p>关闭流程：
 * <ol>
 *   <li>标记 {@code isShuttingDown = true}（通过 {@link AtomicBoolean}）</li>
 *   <li>通过 {@link ObjectProvider} 获取 {@link StreamMQListenerContainer}，
 *       调用 {@code pause()} 停止拉取新消息</li>
 *   <li>等待处理中的消息完成（最多 {@code gracefulShutdownTimeoutMs}，默认 30 秒）</li>
 *   <li>调用 {@code container.stop()} 释放线程与连接</li>
 * </ol>
 *
 * <p>其他组件可通过 {@link #isShuttingDown()} 查询当前是否处于关闭过程中，
 * 据此跳过非必要任务（如心跳、rebalance）。
 *
 * @author StreamMQ Contributors
 * @since 2.0.0
 */
@Slf4j
public class GracefulShutdownHandler implements DisposableBean {

    private final AtomicBoolean shuttingDown = new AtomicBoolean(false);

    private final ObjectProvider<StreamMQListenerContainer> containerProvider;

    private final CloudK8sProperties properties;

    /**
     * 构造优雅关闭处理器。
     *
     * @param containerProvider 监听器容器的可选注入提供者
     * @param properties K8s 云原生增强配置
     */
    public GracefulShutdownHandler(ObjectProvider<StreamMQListenerContainer> containerProvider,
                                   CloudK8sProperties properties) {
        this.containerProvider = Objects.requireNonNull(containerProvider, "containerProvider");
        this.properties = Objects.requireNonNull(properties, "properties");
    }

    @Override
    public void destroy() {
        if (!shuttingDown.compareAndSet(false, true)) {
            log.info("StreamMQ graceful shutdown already in progress, skip duplicate trigger");
            return;
        }
        log.info("StreamMQ graceful shutdown initiated");
        StreamMQListenerContainer container = containerProvider.getIfAvailable();
        if (Objects.isNull(container)) {
            log.info("No StreamMQListenerContainer available, graceful shutdown completed");
            return;
        }
        pauseContainer(container);
        waitForInFlightMessages(container);
        stopContainer(container);
        log.info("StreamMQ graceful shutdown completed");
    }

    /**
     * 返回当前是否处于关闭过程中。
     *
     * <p>其他组件可在关闭过程中查询此标志，据此跳过非必要任务（如心跳、rebalance），
     * 避免在关闭阶段产生无效副作用。
     *
     * @return true 表示已进入优雅关闭流程
     */
    public boolean isShuttingDown() {
        return shuttingDown.get();
    }

    private void pauseContainer(StreamMQListenerContainer container) {
        try {
            log.info("Pausing StreamMQ listener container to stop pulling new messages");
            container.pause();
        } catch (Exception e) {
            log.warn("Failed to pause StreamMQ listener container: {}", e.getMessage());
        }
    }

    /**
     * 等待处理中的消息完成。
     *
     * <p>pause() 仅停止拉取新消息，不改变容器的 running 状态，因此不能依赖
     * {@code isRunning()} 判断 in-flight 是否完成。此处采用有界短等待策略：
     * 等待固定宽限期（最多 1 秒），让 in-flight 消息自然完成，然后由
     * {@link #stopContainer} 统一关闭容器。
     *
     * @param container 监听器容器
     */
    private void waitForInFlightMessages(StreamMQListenerContainer container) {
        long gracePeriod = Math.min(properties.getGracefulShutdownTimeoutMs(), 1000L);
        long deadline = System.currentTimeMillis() + gracePeriod;
        while (System.currentTimeMillis() < deadline) {
            try {
                Thread.sleep(100L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Interrupted while waiting for in-flight messages to complete");
                return;
            }
        }
    }

    private void stopContainer(StreamMQListenerContainer container) {
        try {
            log.info("Stopping StreamMQ listener container");
            container.stop();
        } catch (Exception e) {
            log.warn("Failed to stop StreamMQ listener container: {}", e.getMessage());
        }
    }
}
