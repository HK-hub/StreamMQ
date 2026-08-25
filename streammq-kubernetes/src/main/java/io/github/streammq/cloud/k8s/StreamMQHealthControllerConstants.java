/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.cloud.k8s;

/**
 * K8s 健康探针端点常量：路径与响应 key。
 *
 * @author StreamMQ Contributors
 * @since 1.1.0
 */
public final class StreamMQHealthControllerConstants {

    /** 端点基础路径 */
    public static final String BASE_PATH = "/streammq/health";

    /** 存活探针路径 */
    public static final String PATH_LIVENESS = "/liveness";

    /** 就绪探针路径 */
    public static final String PATH_READINESS = "/readiness";

    /** 响应 key：状态 */
    public static final String KEY_STATUS = "status";

    /** 状态值：正常 */
    public static final String STATUS_UP = "UP";

    /** 响应 key：后端类型 */
    public static final String KEY_BACKEND = "backend";

    /** 后端值：未知 */
    public static final String VALUE_UNKNOWN = "unknown";

    /** 响应 key：就绪标志 */
    public static final String KEY_READY = "ready";

    /** 响应 key：消费者数量 */
    public static final String KEY_CONSUMER_COUNT = "consumerCount";

    private StreamMQHealthControllerConstants() {}
}
