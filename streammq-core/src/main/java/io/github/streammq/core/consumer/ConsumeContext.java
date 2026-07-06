package io.github.streammq.core.consumer;

import io.github.streammq.core.enums.AcknowledgeMode;

import java.time.Duration;
import java.util.Map;

/**
 * 消费上下文，封装消费过程中的运行时信息与操作接口。
 *
 * <p>框架在调用 Consumer 时构造此对象，注入到 {@code onMessage} 第二参数。
 *
 * <p>关键能力：
 * <ul>
 *   <li>获取消息元信息：topic, consumerGroup, consumerName, reconsumeTimes, bornTimestamp, bornHost</li>
 *   <li>获取 ACK 操作接口：{@link #acknowledge()} 返回 {@link Acknowledgment}（AUTO 模式下返回值可能为 null 或不可用）</li>
 *   <li>标记消息已 ACK：{@link #markAcked()}（由 Consumer 在 AUTO 模式下返回 {@code ConsumeAction.SUCCESS} 时由框架调用，
 *       或由 {@link Acknowledgment#acknowledge()} 内部调用）</li>
 *   <li>查询是否已 ACK：{@link #isAcked()}</li>
 *   <li>暂停消费（顺序消费专用）：{@link #suspend(Duration)}</li>
 *   <li>获取扩展属性：{@link #ext(String)}</li>
 *   <li>获取 ACK 模式：{@link #ackMode()}</li>
 * </ul>
 *
 * <p>ACK 模式说明：
 * <ul>
 *   <li>{@link AcknowledgeMode#AUTO} - 不应调用 {@link #acknowledge()}，
 *       由 Consumer 返回 {@code ConsumeAction.SUCCESS} 时框架调用 {@link #markAcked()} 标记已 ACK</li>
 *   <li>{@link AcknowledgeMode#MANUAL} - Consumer 通过 {@link #acknowledge()} 显式控制 ACK，
 *       {@code onMessage} 返回值被忽略；若退出时未 ACK，框架视为失败进入重试</li>
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface ConsumeContext {

    /**
     * 返回当前消息所属 Topic。
     *
     * @return Topic
     */
    String topic();

    /**
     * 返回当前消费者组名。
     *
     * @return 消费者组名
     */
    String consumerGroup();

    /**
     * 返回当前 Consumer 实例名。
     *
     * @return Consumer 实例名
     */
    String consumerName();

    /**
     * 返回当前消息已重试消费次数（首次消费为 0）。
     *
     * @return 重试次数
     */
    int reconsumeTimes();

    /**
     * 返回消息出生时间戳（毫秒）。
     *
     * @return 出生时间戳
     */
    long bornTimestamp();

    /**
     * 返回消息出生主机（host:port）。
     *
     * @return 出生主机
     */
    String bornHost();

    /**
     * 返回消息追踪信息（含 traceId、spanId 等）。
     *
     * @return 不可修改的追踪信息 Map
     */
    Map<String, String> messageTrack();

    /**
     * 获取扩展属性。
     *
     * @param key 属性键
     * @return 属性值，不存在则返回 null
     */
    String ext(String key);

    /**
     * 返回 ACK 模式。
     *
     * @return ACK 模式
     */
    AcknowledgeMode ackMode();

    /**
     * 返回 ACK 操作接口。
     *
     * <p>AUTO 模式下不应调用此方法（返回值可能为 null 或不可用）；
     * MANUAL 模式下返回可用 {@link Acknowledgment} 实例。
     *
     * @return {@link Acknowledgment} 实例，AUTO 模式下可能为 null
     */
    Acknowledgment acknowledge();

    /**
     * 标记当前消息为已 ACK。
     *
     * <p>由框架在 AUTO 模式下 Consumer 返回 {@code ConsumeAction.SUCCESS} / {@code OrderlyAction.SUCCESS}
     * 时调用，或由 {@link Acknowledgment#acknowledge()} 内部调用。
     * 业务代码通常无需直接调用此方法。
     */
    void markAcked();

    /**
     * 返回当前消息是否已被标记为已 ACK。
     *
     * @return true 如果已通过 {@link #markAcked()} 或 {@link Acknowledgment#acknowledge()} 标记
     */
    boolean isAcked();

    /**
     * 暂停当前消费一段时间（顺序消费专用，用于 {@code OrderlyAction.SUSPEND_CURRENT_QUEUE_A_MOMENT} 的延后实现）。
     * 并发模式下调用此方法等同于返回 {@code ConsumeAction.RECONSUME_LATER}。
     *
     * @param duration 暂停时长
     */
    void suspend(Duration duration);
}
