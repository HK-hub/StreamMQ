package io.github.streammq.diagnostics.model;

import java.util.List;

/**
 * 消息完整生命周期画像，聚合一条消息从发送到消费（含重试）的全部信息。
 *
 * <p>由 {@link io.github.streammq.diagnostics.MessageProfileService} 基于 {@link
 * io.github.streammq.core.trace.StreamMQTraceService} 的追踪记录构建， 用于消息链路可视化与问题排查。
 *
 * <p>典型字段说明：
 *
 * <ul>
 *   <li>{@link #sendDurationMillis} - 发送耗时（从 bornTimestamp 到发送成功的时间差）
 *   <li>{@link #consumeHistory} - 消费历史，按时间升序排列，每次重试生成一条记录
 *   <li>{@link #retryCount} - 重试次数（consumeHistory.size - 1）
 *   <li>{@link #finalStatus} - 最终状态（SUCCESS / FAILED / DLQ / PROCESSING / UNKNOWN）
 *   <li>{@link #routePath} - 路由路径，记录消息经过的主题流转（如 topic -> retry-topic -> dlq-topic）
 * </ul>
 *
 * @param messageId 消息 ID
 * @param topic 主题
 * @param tag 消息标签
 * @param keys 业务键
 * @param bornTimestamp 出生时间戳（毫秒）
 * @param sendDurationMillis 发送耗时（毫秒）
 * @param consumeHistory 消费历史，按时间升序排列
 * @param retryCount 重试次数
 * @param finalStatus 最终状态
 * @param routePath 路由路径（消息经过的主题流转列表）
 * @param bodyType 消息体类型名
 * @param bornHost 出生主机（发送端 host:port）
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public record MessageProfile(
        String messageId,
        String topic,
        String tag,
        String keys,
        long bornTimestamp,
        long sendDurationMillis,
        List<ConsumeAttempt> consumeHistory,
        int retryCount,
        MessageStatus finalStatus,
        List<String> routePath,
        String bodyType,
        String bornHost) {}
