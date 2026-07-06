package io.github.streammq.core.scheduler;

/**
 * StreamMQ 调度器公共接口。
 *
 * <p>所有调度器（如 {@code DelayMessageScheduler}、{@code RetryScheduler}、
 * {@code TransactionScanner}）均实现本接口，便于上层（如 Spring {@code SmartLifecycle}）
 * 以统一方式管理启停，遵循"依赖接口而非实现"原则。
 *
 * <p>实现类可能提供额外的业务方法（如注册重试目标、注册半消息等），
 * 这些方法不属于本公共接口。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface StreamMQScheduler {

    /**
     * 启动调度器。
     *
     * <p>实现应保证幂等：重复调用不应产生副作用。
     */
    void start();

    /**
     * 停止调度器，释放线程池等资源。
     *
     * <p>实现应保证幂等：重复调用不应产生副作用。
     */
    void stop();

    /**
     * 返回调度器是否正在运行。
     *
     * @return true 如果调度器已启动且未停止
     */
    boolean isRunning();
}
