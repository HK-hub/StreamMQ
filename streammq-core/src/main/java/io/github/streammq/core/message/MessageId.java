/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.message;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;
import lombok.Getter;

/**
 * 消息 ID 包装类，对应 Redis Stream Entry ID（格式：{timestamp}-{sequence}）。
 *
 * <p>封装 Stream Entry ID 与 StreamMQ 内部 messageId（事务消息场景下两者可能不同）。 不可变，线程安全。
 *
 * <p><b>规范化与相等语义（0.1.2 起一致化）：</b>
 *
 * <ul>
 *   <li>构造器把数值部分规范化为 {@code timestamp + "-" + sequence}（例如 {@code "01-2"} → {@code "1-2"}）
 *   <li>{@link #equals(Object)} / {@link #hashCode()} 与 {@link #compareTo(MessageId)} 均基于 {@code
 *       (timestamp, sequence)}：保证 {@code compareTo == 0} 与 {@code equals == true} 等价， 使 {@code
 *       TreeSet} 与 {@code HashSet} 判定一致（此前 compareTo 用数值、equals 用原始字符串， "01-2" 与 "1-2" 会出现比较相等但
 *       equals 不等）
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Getter
public final class MessageId implements Serializable, Comparable<MessageId> {

    @Serial private static final long serialVersionUID = 1L;

    /** Redis Stream Entry ID 规范化字符串，格式：{timestamp}-{sequence} 返回规范化后的 Stream Entry ID。 */
    private final String streamEntryId;

    /** 创建时间戳（用于顺序比较与超时判断） 返回时间戳部分（毫秒，Unix 时间）。 */
    private final long timestamp;

    /** 序列号（同时间戳内递增） 返回序列号部分。 */
    private final long sequence;

    /**
     * 从 Redis Stream Entry ID 字符串构造。
     *
     * <p>数值部分会被规范化为 {@code timestamp + "-" + sequence}，因此 {@code new MessageId("01-2").equals(new
     * MessageId("1-2"))} 为 true。两个数值部分都必须非负。
     *
     * @param streamEntryId Redis Stream Entry ID 字符串，格式：{timestamp}-{sequence}
     * @throws NullPointerException 如果 streamEntryId 为 null
     * @throws IllegalArgumentException 如果 streamEntryId 格式不合法或数值为负
     */
    public MessageId(String streamEntryId) {
        String raw = Objects.requireNonNull(streamEntryId, "streamEntryId");
        String[] parts = raw.split("-", 2);
        if (parts.length != 2) {
            throw new IllegalArgumentException(
                    "Invalid stream entry id format, expected '{timestamp}-{sequence}': " + raw);
        }
        long parsedTimestamp;
        long parsedSequence;
        try {
            parsedTimestamp = Long.parseLong(parts[0]);
            parsedSequence = Long.parseLong(parts[1]);
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException("Invalid stream entry id numeric parts: " + raw, ex);
        }
        this.timestamp = requireNonNegative(parsedTimestamp, "timestamp", raw);
        this.sequence = requireNonNegative(parsedSequence, "sequence", raw);
        this.streamEntryId = this.timestamp + "-" + this.sequence;
    }

    private MessageId(long timestamp, long sequence) {
        this.timestamp = requireNonNegative(timestamp, "timestamp", timestamp + "-" + sequence);
        this.sequence = requireNonNegative(sequence, "sequence", timestamp + "-" + sequence);
        this.streamEntryId = this.timestamp + "-" + this.sequence;
    }

    /** 校验时间戳/序列号非负（负值会生成无法回解析的 ID，如 {@code "-1--1"}）。 */
    private static long requireNonNegative(long value, String part, String rawId) {
        if (value < 0) {
            throw new IllegalArgumentException(
                    "Stream entry id "
                            + part
                            + " must be >= 0, got: "
                            + value
                            + " (id="
                            + rawId
                            + ")");
        }
        return value;
    }

    // ===================== 静态工厂（按场景收口） =====================

    /** 从 Redis Stream Entry ID 字符串创建（消费端反序列化 / 从存储还原）。 等同于 {@code new MessageId(streamEntryId)}。 */
    public static MessageId fromStreamEntry(String streamEntryId) {
        return new MessageId(streamEntryId);
    }

    /**
     * 从 Redisson {@code StreamMessageId} 创建（生产端 XADD 成功后的回填）。
     *
     * <p>参数刻意声明为 {@link Object} 以避免 core 反向依赖 Redisson；实际只依赖其 {@code toString()} 返回的 {@code
     * {timestamp}-{sequence}} 文本（Redisson {@code StreamMessageId} 正是该格式）。 传入 {@code null}
     * 或其他不满足格式的对象会立即失败。
     *
     * @param streamMessageId Redisson {@code StreamMessageId}（或任何 toString() 为合法 ID 文本的对象）
     * @return 消息 ID
     * @throws NullPointerException 如果 streamMessageId 为 null
     * @throws IllegalArgumentException 如果其文本不是合法的 {@code {timestamp}-{sequence}} 格式
     */
    public static MessageId fromStreamMessageId(Object streamMessageId) {
        Objects.requireNonNull(streamMessageId, "streamMessageId");
        return new MessageId(streamMessageId.toString());
    }

    /** 生成一个表示发送失败/占位的 MessageId。 使用当前时间戳 + 序列号 0，表示该消息未经 Redis 分配真实 Entry ID。 */
    public static MessageId sentinel() {
        return new MessageId(System.currentTimeMillis(), 0);
    }

    /**
     * 从时间戳 + 序列号直接构造（延时消息 / 自定义 ID 场景）。
     *
     * @param timestamp 时间戳（毫秒），必须 &gt;= 0
     * @param sequence 序列号，必须 &gt;= 0
     * @return 消息 ID
     * @throws IllegalArgumentException 如果 timestamp 或 sequence 为负（负值会生成不可回解析的 ID，如 {@code "-1--1"}）
     */
    public static MessageId of(long timestamp, long sequence) {
        return new MessageId(timestamp, sequence);
    }

    /**
     * 按 {@code (timestamp, sequence)} 字典序比较。
     *
     * <p>与 {@link #equals(Object)} 严格一致：{@code compareTo(other) == 0} 当且仅当 {@code equals(other)} 为
     * true（构造器已规范化 ID 文本）。因此本类型在 {@code TreeSet}/{@code TreeMap} 与在 {@code HashSet}
     * 中的"去重"判定相同。{@code null} 参数抛出 NPE（自然序约定）。
     *
     * @param other 比较对象，不能为 null
     * @return 负/零/正分别表示小于/等于/大于
     */
    @Override
    public int compareTo(MessageId other) {
        Objects.requireNonNull(other, "other");
        int ts = Long.compare(this.timestamp, other.timestamp);
        return ts != 0 ? ts : Long.compare(this.sequence, other.sequence);
    }

    /**
     * 相等语义：基于 {@code (timestamp, sequence)}（与 {@link #compareTo(MessageId)} 一致）。
     *
     * @param o 比较对象
     * @return true 如果时间戳与序列号均相同
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MessageId other)) {
            return false;
        }
        return timestamp == other.timestamp && sequence == other.sequence;
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, sequence);
    }

    @Override
    public String toString() {
        return streamEntryId;
    }
}
