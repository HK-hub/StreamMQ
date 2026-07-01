package io.github.streammq.core.message;

import java.io.Serializable;
import java.util.Objects;

/**
 * 消息 ID 包装类，对应 Redis Stream Entry ID（格式：{timestamp}-{sequence}）。
 *
 * <p>封装 Stream Entry ID 与 StreamMQ 内部 messageId（事务消息场景下两者可能不同）。
 * 不可变，线程安全。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public final class MessageId implements Serializable, Comparable<MessageId> {

    private static final long serialVersionUID = 1L;

    /** Redis Stream Entry ID 原始字符串，格式：{timestamp}-{sequence} */
    private final String streamEntryId;

    /** 创建时间戳（用于顺序比较与超时判断） */
    private final long timestamp;

    /** 序列号（同时间戳内递增） */
    private final long sequence;

    /**
     * 构造 MessageId。
     *
     * @param streamEntryId Redis Stream Entry ID 字符串，格式：{timestamp}-{sequence}
     * @throws NullPointerException 如果 streamEntryId 为 null
     * @throws IllegalArgumentException 如果 streamEntryId 格式不合法
     */
    public MessageId(String streamEntryId) {
        this.streamEntryId = Objects.requireNonNull(streamEntryId, "streamEntryId");
        String[] parts = streamEntryId.split("-", 2);
        if (parts.length != 2) {
            throw new IllegalArgumentException(
                "Invalid stream entry id format, expected '{timestamp}-{sequence}': " + streamEntryId);
        }
        try {
            this.timestamp = Long.parseLong(parts[0]);
            this.sequence = Long.parseLong(parts[1]);
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException(
                "Invalid stream entry id numeric parts: " + streamEntryId, ex);
        }
    }

    /**
     * 返回原始 Redis Stream Entry ID。
     *
     * @return Stream Entry ID 字符串
     */
    public String getStreamEntryId() {
        return streamEntryId;
    }

    /**
     * 返回时间戳部分（毫秒，Unix 时间）。
     *
     * @return 时间戳
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * 返回序列号部分。
     *
     * @return 序列号
     */
    public long getSequence() {
        return sequence;
    }

    @Override
    public int compareTo(MessageId other) {
        int ts = Long.compare(this.timestamp, other.timestamp);
        return ts != 0 ? ts : Long.compare(this.sequence, other.sequence);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MessageId other)) {
            return false;
        }
        return streamEntryId.equals(other.streamEntryId);
    }

    @Override
    public int hashCode() {
        return streamEntryId.hashCode();
    }

    @Override
    public String toString() {
        return streamEntryId;
    }
}
