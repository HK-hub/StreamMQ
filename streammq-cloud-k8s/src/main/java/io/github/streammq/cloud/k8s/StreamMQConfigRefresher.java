package io.github.streammq.cloud.k8s;

/**
 * StreamMQ 配置热更新接口，支持在运行时动态刷新关键配置项。
 *
 * <p>在 K8s 环境中，配置变更（如 ConfigMap 滚动更新）可通过实现本接口将新配置
 * 推送到运行中的 {@link io.github.streammq.core.listener.StreamMQListenerContainer}，
 * 避免重启 Pod 即可生效。
 *
 * <p>限制说明：
 * <ul>
 *   <li>不支持运行时变更 backend type（如 redis 切换到 rocketmq），此类变更需重启 Pod</li>
 *   <li>刷新重试间隔时，需保证 {@code retryIntervals} 长度与 {@code maxReconsumeTimes} 一致</li>
 *   <li>消费线程数刷新仅影响后续创建的消费线程，不会中断正在执行的任务</li>
 * </ul>
 *
 * <p>默认实现 {@link NoopConfigRefresher} 为空操作，不产生任何副作用。
 * 用户可提供自定义实现并注册为 Bean 以覆盖默认行为。
 *
 * @author StreamMQ Contributors
 * @since 2.0.0
 */
public interface StreamMQConfigRefresher {

    /**
     * 刷新重试策略配置。
     *
     * <p>运行时调整最大重试次数与各次重试之间的等待间隔。
     * {@code retryIntervals} 的长度应与 {@code maxReconsumeTimes} 保持一致，
     * 每个元素对应该次重试的等待毫秒数。
     *
     * @param maxReconsumeTimes 最大重试次数
     * @param retryIntervals 各次重试间隔毫秒数组，长度应与 maxReconsumeTimes 一致
     */
    void refreshRetryPolicy(int maxReconsumeTimes, long[] retryIntervals);

    /**
     * 刷新消费线程池大小范围。
     *
     * <p>动态调整消费线程池的最小与最大线程数。仅影响后续创建的消费线程，
     * 不会中断当前正在执行的消息处理任务。
     *
     * @param min 最小消费线程数
     * @param max 最大消费线程数
     */
    void refreshConsumerThreads(int min, int max);

    /**
     * 刷新重试与延时消息扫描间隔。
     *
     * <p>调整后台扫描线程拉取重试消息与延时消息的频率。
     * 不能运行时变更 backend type，扫描间隔变更不影响已分配的消息分片。
     *
     * @param retryScanMs 重试消息扫描间隔（毫秒）
     * @param delayScanMs 延时消息扫描间隔（毫秒）
     */
    void refreshScanInterval(long retryScanMs, long delayScanMs);
}
