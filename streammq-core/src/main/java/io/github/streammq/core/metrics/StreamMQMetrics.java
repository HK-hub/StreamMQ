package io.github.streammq.core.metrics;

import java.time.Duration;

/**
 * StreamMQ 指标收集接口。
 *
 * <p>定义了 StreamMQ 各核心环节的指标埋点契约，由具体的实现（如基于 Micrometer 的 {@code
 * MicrometerStreamMQMetrics}）提供实际的指标上报能力。
 *
 * <p>实现类应保证线程安全，并允许在指标注册表不可用时以 no-op 方式安全降级， 即所有 {@code record*} 方法都不得抛出异常影响业务主流程。
 *
 * <p>指标覆盖范围：
 *
 * <ul>
 *   <li>发送：{@link #recordSend}
 *   <li>消费：{@link #recordConsume}
 *   <li>重试：{@link #recordRetry}
 *   <li>死信：{@link #recordDlq}
 *   <li>延时投递：{@link #recordDelayDelivery}
 *   <li>事务提交 / 回滚 / 回查：{@link #recordTransactionCommit} / {@link #recordTransactionRollback} /
 *       {@link #recordTransactionCheck}
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface StreamMQMetrics {

    /**
     * 记录一次消息发送结果。
     *
     * @param topic 消息主题
     * @param success 是否发送成功
     * @param duration 发送耗时
     */
    void recordSend(String topic, boolean success, Duration duration);

    /**
     * 记录一次消息消费结果。
     *
     * @param topic 消息主题
     * @param group 消费者组
     * @param success 是否消费成功
     * @param duration 消费耗时
     */
    void recordConsume(String topic, String group, boolean success, Duration duration);

    /**
     * 记录一次消息重试。
     *
     * @param topic 消息主题
     * @param group 消费者组
     */
    void recordRetry(String topic, String group);

    /**
     * 记录一条消息进入死信队列。
     *
     * @param topic 消息主题
     * @param group 消费者组
     */
    void recordDlq(String topic, String group);

    /**
     * 记录一次延时消息投递。
     *
     * @param level 延时等级
     */
    void recordDelayDelivery(String level);

    /**
     * 记录一次事务消息提交。
     *
     * @param group 消费者组
     */
    void recordTransactionCommit(String group);

    /**
     * 记录一次事务消息回滚。
     *
     * @param group 消费者组
     */
    void recordTransactionRollback(String group);

    /**
     * 记录一次事务消息回查。
     *
     * @param group 消费者组
     * @param result 回查结果
     */
    void recordTransactionCheck(String group, String result);
}
