/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.diagnostics.endpoint;

/**
 * 诊断 REST 端点常量定义：基础路径、鉴权资源名与健康概览响应 key。
 *
 * @author StreamMQ Contributors
 * @since 1.1.0
 */
public final class StreamMQDiagnosticsEndpointConstants {

    /** 端点基础路径 */
    public static final String BASE_PATH = "/streammq/diagnostics";

    // ==================== 鉴权资源名 ====================
    /** 鉴权资源前缀：消息画像（profile:{messageId}） */
    public static final String RES_PROFILE_PREFIX = "profile:";

    /** 鉴权资源前缀：慢消费诊断（slow-consume:{topic}） */
    public static final String RES_SLOW_CONSUME_PREFIX = "slow-consume:";

    /** 鉴权资源前缀：积压诊断（backlog:{topic}） */
    public static final String RES_BACKLOG_PREFIX = "backlog:";

    /** 鉴权资源前缀：DLQ 诊断（dlq:{group}） */
    public static final String RES_DLQ_PREFIX = "dlq:";

    /** 鉴权资源：慢消费者列表（复用 starter DLQ 资源前缀语义，此处为独立列表资源） */
    public static final String RES_SLOW_CONSUMERS = "slow-consumers";

    /** 鉴权资源：全部积压报告 */
    public static final String RES_ALL_BACKLOGS = "all-backlogs";

    /** 鉴权资源：诊断健康概览 */
    public static final String RES_HEALTH = "health";

    // ==================== 健康概览响应 Key ====================
    /** 响应 key：状态 */
    public static final String KEY_STATUS = "status";

    /** 状态值：正常 */
    public static final String STATUS_UP = "UP";

    /** 响应 key：慢消费者数量 */
    public static final String KEY_SLOW_CONSUMER_COUNT = "slowConsumerCount";

    /** 响应 key：慢消费者列表 */
    public static final String KEY_SLOW_CONSUMERS = "slowConsumers";

    /** 响应 key：总积压量 */
    public static final String KEY_TOTAL_BACKLOG = "totalBacklog";

    /** 响应 key：积压报告列表 */
    public static final String KEY_BACKLOG_REPORTS = "backlogReports";

    /** 响应 key：时间戳 */
    public static final String KEY_TIMESTAMP = "timestamp";

    private StreamMQDiagnosticsEndpointConstants() {}
}
