/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.broadcast;

import java.util.Objects;

/**
 * 广播实例身份申请参数（Parameter Object）。
 *
 * @param namespace 命名空间（可为 null，按空串处理）
 * @param topic 主题（必填）
 * @param group 消费者组（必填）
 * @param host 宿主标识（必填；用于同主机槽位回收匹配，通常为主机名或容器名）
 * @param pid 进程标识（不可获取时传 {@code -1}）
 * @param preferredId 期望占用的实例身份（显式配置值或本地文件值），可为 null / 空表示无偏好
 * @param nowMillis 申请时刻（毫秒），由调用方注入以保证可测试
 * @param leaseTimeoutMillis 租约超时（毫秒）
 * @param reclaimGraceMillis 回收宽限期（毫秒）
 * @author StreamMQ Contributors
 * @since 0.1.2
 */
public record BroadcastInstanceRequest(
        String namespace,
        String topic,
        String group,
        String host,
        long pid,
        String preferredId,
        long nowMillis,
        long leaseTimeoutMillis,
        long reclaimGraceMillis) {

    /** 紧凑构造：规范化空值并校验必填项。 */
    public BroadcastInstanceRequest {
        namespace = Objects.isNull(namespace) ? "" : namespace;
        Objects.requireNonNull(topic, "topic");
        Objects.requireNonNull(group, "group");
        Objects.requireNonNull(host, "host");
    }

    /**
     * 返回偏好实例身份（已 trim，可能为 null 或空串）。
     *
     * @return 偏好身份或 null
     */
    public String preferredIdOrNull() {
        if (preferredId == null || preferredId.isBlank()) {
            return null;
        }
        return preferredId.trim();
    }

    /**
     * 静态工厂。
     *
     * @param namespace 命名空间
     * @param topic 主题
     * @param group 消费者组
     * @param host 宿主标识
     * @param pid 进程标识
     * @param preferredId 偏好身份
     * @param nowMillis 当前时间
     * @param leaseTimeoutMillis 租约超时
     * @param reclaimGraceMillis 回收宽限期
     * @return 申请参数
     */
    public static BroadcastInstanceRequest of(
            String namespace,
            String topic,
            String group,
            String host,
            long pid,
            String preferredId,
            long nowMillis,
            long leaseTimeoutMillis,
            long reclaimGraceMillis) {
        return new BroadcastInstanceRequest(
                namespace,
                topic,
                group,
                host,
                pid,
                preferredId,
                nowMillis,
                leaseTimeoutMillis,
                reclaimGraceMillis);
    }
}
