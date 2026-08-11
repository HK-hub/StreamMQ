package io.github.streammq.core.trace;

import java.util.List;

/**
 * 追踪查询服务 SPI，提供按消息 ID、Topic、消费组维度的追踪记录查询能力。
 *
 * <p>对齐 PRD §6.4.4，默认实现 {@code RedisStreamMQTraceService} 将追踪数据存储在 Redis Stream 中。
 *
 * <p>使用示例：
 *
 * <pre>{@code
 * // 查询某条消息的完整链路
 * List<TraceRecord> records = traceService.queryByMessageId("123-0");
 *
 * // 查询某 Topic 在时间范围内的所有事件
 * List<TraceRecord> records = traceService.queryByTopic("order-topic", startMs, endMs);
 * }</pre>
 *
 * @author StreamMQ Contributors
 * @since 1.0.0
 */
public interface StreamMQTraceService {

    /**
     * 按消息 ID 查询追踪记录。
     *
     * @param messageId 消息 ID
     * @return 匹配的追踪记录列表，按时间升序排列
     */
    List<TraceRecord> queryByMessageId(String messageId);

    /**
     * 按主题和时间范围查询追踪记录。
     *
     * @param topic 主题
     * @param startTimeMs 起始时间戳（毫秒，包含）
     * @param endTimeMs 结束时间戳（毫秒，包含）
     * @return 匹配的追踪记录列表，按时间升序排列
     */
    List<TraceRecord> queryByTopic(String topic, long startTimeMs, long endTimeMs);

    /**
     * 按消费组和时间范围查询追踪记录。
     *
     * @param group 消费者组名
     * @param startTimeMs 起始时间戳（毫秒，包含）
     * @param endTimeMs 结束时间戳（毫秒，包含）
     * @return 匹配的追踪记录列表，按时间升序排列
     */
    List<TraceRecord> queryByGroup(String group, long startTimeMs, long endTimeMs);
}
