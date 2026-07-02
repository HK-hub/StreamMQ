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
 *   <li>获取 ACK 操作接口：{@link #acknowledge()} 返回 {@link Acknowledgment}</li>
 *   <li>暂停消费（顺序消费专用）：{@link #suspend(Duration)}</li>
 *   <li>获取扩展属性：{@link #ext(String)}</li>
 *   <li>获取 ACK 模式：{@link #ackMode()}</li>
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface ConsumeConcurrentlyContext {

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
     * 返回 ACK 操作接口（仅在 MANUAL 模式下使用，AUTO 模式下调用将被忽略）。
     *
     * @return {@link Acknowledgment} 实例
     */
    Acknowledgment acknowledge();

    /**
     * 暂停当前消费一段时间（顺序消费专用，用于 {@code Action.SUSPEND_CURRENT_QUEUE_A_MOMENT} 的延后实现）。
     * 并发模式下调用此方法等同于返回 {@code Action.RECONSUME_LATER}。
     *
     * @param duration 暂停时长
     */
    void suspend(Duration duration);
}
