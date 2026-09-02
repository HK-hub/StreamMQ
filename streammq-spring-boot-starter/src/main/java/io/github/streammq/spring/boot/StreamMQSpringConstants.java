/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.spring.boot;

/**
 * StreamMQ Spring Boot Starter 常量定义。
 *
 * <p>集中管理配置属性前缀、条件装配属性名、Bean 名称与健康检查详情 key， 供自动装配类与注解（{@code @ConditionalOnProperty}）引用编译期常量。
 *
 * @author StreamMQ Contributors
 * @since 1.1.0
 */
public final class StreamMQSpringConstants {

    // ==================== 配置属性前缀 ====================
    /** 根配置前缀：streammq */
    public static final String PROP_PREFIX = "streammq";

    /** 追踪配置前缀：streammq.tracing */
    public static final String PROP_PREFIX_TRACING = "streammq.tracing";

    /** 追踪存储配置前缀：streammq.trace */
    public static final String PROP_PREFIX_TRACE = "streammq.trace";

    /** 重试配置前缀：streammq.retry */
    public static final String PROP_PREFIX_RETRY = "streammq.retry";

    /** 延时消息配置前缀：streammq.delay */
    public static final String PROP_PREFIX_DELAY = "streammq.delay";

    /** 事务消息配置前缀：streammq.transaction */
    public static final String PROP_PREFIX_TRANSACTION = "streammq.transaction";

    /** 健康检查配置前缀：streammq.health */
    public static final String PROP_PREFIX_HEALTH = "streammq.health";

    /** 生产者配置前缀：streammq.producer */
    public static final String PROP_PREFIX_PRODUCER = "streammq.producer";

    // ==================== 默认值 ====================

    /**
     * 默认消息体序列化器实现类：Apache Fury。
     *
     * <p>与 {@link io.github.streammq.core.StreamMQConstants#DEFAULT_SERIALIZER} 指向同一实现，
     * 此处以 {@code Class} 形式提供，供自动装配在配置缺失/解析为空时回退使用。
     */
    public static final Class<? extends io.github.streammq.core.serializer.MessageSerializer>
            DEFAULT_SERIALIZER_CLASS =
                    io.github.streammq.adapter.redisson.serializer.FurySerializer.class;

    // ==================== 条件装配属性名与值 ====================
    /** 开关属性名：enabled */
    public static final String PROP_NAME_ENABLED = "enabled";

    /** 开关属性值：true */
    public static final String PROP_VALUE_TRUE = "true";

    /** 追踪存储方式属性名：storage */
    public static final String PROP_NAME_STORAGE = "storage";

    /** 追踪存储方式取值：redis */
    public static final String TRACE_STORAGE_REDIS = "redis";

    // ==================== Bean 名称 ====================
    /** 调度器生命周期 Bean 名 */
    public static final String BEAN_SCHEDULER_LIFECYCLE = "streamMQSchedulerLifecycle";

    /** Listener 容器生命周期 Bean 名 */
    public static final String BEAN_LISTENER_CONTAINER_LIFECYCLE =
            "streamMQListenerContainerLifecycle";

    /** 健康检查器 Bean 名 */
    public static final String BEAN_HEALTH_INDICATOR = "streamMQHealthIndicator";

    /** 管理端点后端逻辑 Bean 名 */
    public static final String BEAN_ADMIN_ENDPOINT = "streamMQAdminEndpoint";

    /** Actuator 端点 Bean 名 */
    public static final String BEAN_ACTUATOR_ENDPOINT = "streamMQActuatorEndpoint";

    // ==================== Actuator 端点 ====================
    /** Actuator Web 端点 ID */
    public static final String ENDPOINT_ID_STREAMMQ = "streammq";

    /** 管理端点默认列表页大小 */
    public static final int DEFAULT_LIST_PAGE_SIZE = 100;

    /** 管理端点 pending 列表最大拉取条数 */
    public static final int MAX_PENDING_QUERY_SIZE = 1000;

    // ==================== 管理鉴权资源名 ====================
    /** 鉴权资源：总览 */
    public static final String RES_OVERVIEW = "overview";

    /** 鉴权资源：消费组列表 */
    public static final String RES_GROUPS = "groups";

    /** 鉴权资源：topic 列表 / topic 管理 */
    public static final String RES_TOPICS = "topics";

    /** 鉴权资源前缀：pending 查询（pending:{group}） */
    public static final String RES_PENDING_PREFIX = "pending:";

    /** 鉴权资源前缀：DLQ 管理（dlq:{group}） */
    public static final String RES_DLQ_PREFIX = "dlq:";

    /** 鉴权资源前缀：统计查询（stats:{group}） */
    public static final String RES_STATS_PREFIX = "stats:";

    /** 鉴权资源前缀：ACK 操作（ack:{group}） */
    public static final String RES_ACK_PREFIX = "ack:";

    /** 鉴权资源前缀：重平衡触发（rebalance:{group}） */
    public static final String RES_REBALANCE_PREFIX = "rebalance:";

    /** 鉴权资源前缀：组配置更新（config:{group}） */
    public static final String RES_CONFIG_PREFIX = "config:";

    // ==================== 健康检查详情 Key ====================
    /** 健康详情：Redis ping 延迟 */
    public static final String HEALTH_DETAIL_PING_LATENCY = "redis.ping.latencyMs";

    /** 健康详情：健康检查计数器值 */
    public static final String HEALTH_DETAIL_HEALTH_VALUE = "redis.health.value";

    /** 健康详情：错误信息 */
    public static final String HEALTH_DETAIL_ERROR = "error";

    /** 健康详情：容器状态 */
    public static final String HEALTH_DETAIL_LC_STATE = "listenerContainer.state";

    /** 健康详情：容器运行标志 */
    public static final String HEALTH_DETAIL_LC_RUNNING = "listenerContainer.running";

    /** 健康详情：容器监听器数量 */
    public static final String HEALTH_DETAIL_LC_COUNT = "listenerContainer.listenerCount";

    /** 健康详情：容器未配置标记值 */
    public static final String HEALTH_VALUE_NOT_CONFIGURED = "NOT_CONFIGURED";

    /** 健康详情：调度器状态快照 */
    public static final String HEALTH_DETAIL_SCHEDULER_STATUSES = "scheduler.statuses";

    /**
     * 健康详情：消费循环启动失败登记表（loopKey → 失败原因）。
     *
     * <p>非空即代表存在"已注册但实际不消费"的监听器——这类静默故障此前在健康端点完全不可见。
     */
    public static final String HEALTH_DETAIL_LC_LOOP_FAILURES =
            "listenerContainer.consumeLoopFailures";

    private StreamMQSpringConstants() {}
}
