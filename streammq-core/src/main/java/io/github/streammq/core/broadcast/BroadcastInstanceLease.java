/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.broadcast;

import java.util.List;
import java.util.Objects;

/**
 * 广播消费实例的<b>租约</b>（值对象，不可变）。
 *
 * <p>一个租约代表"某个广播消费实例在某个消费者组下占用了一个持久化身份槽位"。租约由 {@link BroadcastInstanceRegistry} 发放并通过 {@link
 * BroadcastInstanceRegistry#heartbeat} 续租；超过租约超时未续租的槽位可被同主机实例回收， 超过回收宽限期仍未回收的槽位会被清扫任务销毁。
 *
 * <p><b>编码约定：</b>注册中心需要把租约序列化为单个字符串存放（如 Redis Hash 的 value）。 本类提供 {@link #encode()} / {@link
 * #decode(String)} 的紧凑编码：字段以 {@code |} 分隔、主题集合以 {@code ,} 分隔， 写入前由 {@link #sanitize(String)}
 * 剔除分隔符，保证编解码可逆且不会出现字段错位。
 *
 * @param instanceId 实例身份（非空），会参与构造广播消费者组名
 * @param host 宿主标识（主机名或容器名），用于同主机槽位回收匹配
 * @param topics 主题集合：一个实例身份可同时覆盖同一消费者组下的多个主题（多 topic 同 group 场景），清扫时全部释放对应消费者组
 * @param group 消费者组
 * @param pid 进程标识（尽力而为，不可获取时为 {@code -1}）
 * @param createdAtMillis 槽位首次分配时间（毫秒）
 * @param lastHeartbeatMillis 最后心跳时间（毫秒）
 * @param reclaimed 本次获取是否属于"回收已有槽位"（true=复用历史身份与 PEL；false=新分配）
 * @author StreamMQ Contributors
 * @since 0.1.2
 */
public record BroadcastInstanceLease(
        String instanceId,
        String host,
        List<String> topics,
        String group,
        long pid,
        long createdAtMillis,
        long lastHeartbeatMillis,
        boolean reclaimed) {

    /** 编码字段分隔符 */
    private static final char SEP = '|';

    /** 主题集合分隔符（写入编码第 3 字段前由 {@link #sanitize} 净化，故解码时可安全按此切分） */
    private static final char TOPIC_SEP = ',';

    /** 编码字段数：instanceId|host|topics|group|pid|createdAt|lastHeartbeat|reclaimed */
    private static final int FIELD_COUNT = 8;

    /**
     * 紧凑构造：校验必填字段并对 host / group / 各 topic 做分隔符净化。
     *
     * @throws NullPointerException instanceId / host / topics / group 任一为 null
     * @throws IllegalArgumentException instanceId 为空白，或 topics 为空
     */
    public BroadcastInstanceLease {
        Objects.requireNonNull(instanceId, "instanceId");
        Objects.requireNonNull(host, "host");
        Objects.requireNonNull(topics, "topics");
        Objects.requireNonNull(group, "group");
        if (instanceId.isBlank()) {
            throw new IllegalArgumentException("instanceId must not be blank");
        }
        if (topics.isEmpty()) {
            throw new IllegalArgumentException("topics must not be empty");
        }
        instanceId = sanitize(instanceId);
        host = sanitize(host);
        topics = topics.stream().map(BroadcastInstanceLease::sanitize).toList();
        group = sanitize(group);
    }

    /**
     * 剔除编码分隔符与控制字符，保证 {@link #encode()} / {@link #decode(String)} 可逆。
     *
     * @param raw 原值，可为 null
     * @return 净化后的值；null 输入返回空串
     */
    public static String sanitize(String raw) {
        if (raw == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder(raw.length());
        for (int i = 0; i < raw.length(); i++) {
            char c = raw.charAt(i);
            if (c == SEP || c == TOPIC_SEP || c == '\n' || c == '\r') {
                sb.append('_');
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    /**
     * 返回续租后的新租约（心跳时间更新为 {@code nowMillis}，其余字段不变）。
     *
     * @param nowMillis 当前时间（毫秒）
     * @return 新租约实例
     */
    public BroadcastInstanceLease renewed(long nowMillis) {
        return new BroadcastInstanceLease(
                instanceId, host, topics, group, pid, createdAtMillis, nowMillis, reclaimed);
    }

    /**
     * 租约是否已过期（超过租约超时未续租）。
     *
     * @param nowMillis 当前时间（毫秒）
     * @param leaseTimeoutMillis 租约超时（毫秒）
     * @return true 表示已过期、可被同主机实例回收
     */
    public boolean isExpired(long nowMillis, long leaseTimeoutMillis) {
        return nowMillis - lastHeartbeatMillis > leaseTimeoutMillis;
    }

    /**
     * 槽位是否可被同主机实例回收：已过期，但仍在回收宽限期内。
     *
     * <p>宽限期的意义：给"重启中的同主机实例"保留历史身份与 PEL。超过宽限期才允许 {@code XGROUP DESTROY}，
     * 否则一次稍长的重启（滚动发布、节点驱逐）就会永久丢失广播消费位点。
     *
     * @param nowMillis 当前时间（毫秒）
     * @param leaseTimeoutMillis 租约超时（毫秒）
     * @param reclaimGraceMillis 回收宽限期（毫秒，从最后心跳起算的总时长）
     * @return true 表示可回收
     */
    public boolean isReclaimable(long nowMillis, long leaseTimeoutMillis, long reclaimGraceMillis) {
        long idle = nowMillis - lastHeartbeatMillis;
        return idle > leaseTimeoutMillis && idle <= reclaimGraceMillis;
    }

    /**
     * 序列化为单行紧凑字符串。
     *
     * @return 编码后的字符串
     */
    public String encode() {
        return String.join(
                String.valueOf(SEP),
                instanceId,
                host,
                String.join(String.valueOf(TOPIC_SEP), topics),
                group,
                Long.toString(pid),
                Long.toString(createdAtMillis),
                Long.toString(lastHeartbeatMillis),
                reclaimed ? "1" : "0");
    }

    /**
     * 反序列化；输入非法时返回 {@code null}（调用方按"槽位不可用"处理，绝不抛异常打断启动）。
     *
     * @param encoded 编码字符串，可为 null
     * @return 租约；输入非法时为 null
     */
    public static BroadcastInstanceLease decode(String encoded) {
        if (encoded == null || encoded.isEmpty()) {
            return null;
        }
        String[] parts = encoded.split("\\" + SEP, -1);
        if (parts.length != FIELD_COUNT) {
            return null;
        }
        try {
            List<String> topics = List.of(parts[2].split(String.valueOf(TOPIC_SEP), -1));
            return new BroadcastInstanceLease(
                    parts[0],
                    parts[1],
                    topics,
                    parts[3],
                    Long.parseLong(parts[4]),
                    Long.parseLong(parts[5]),
                    Long.parseLong(parts[6]),
                    "1".equals(parts[7]));
        } catch (RuntimeException ex) {
            return null;
        }
    }
}
