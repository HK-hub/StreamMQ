package io.github.streammq.core.message;

import lombok.Getter;
import lombok.NonNull;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

/**
 * 发送结果。
 *
 * <p>由 {@code StreamMQTemplate.syncSend} / {@code syncSendBatch} 返回，
 * 封装消息 ID、状态、出生时间戳、Region 等信息。
 *
 * <p><b>持久化保证（重要）：</b>
 * <ul>
 *   <li>{@link SendStatus#SEND_OK} 表示 Redis 已确认收到 XADD 命令，消息已写入 Stream</li>
 *   <li>但这<b>不等于</b>消息已持久化到磁盘——Redis 的 AOF 策略（appendfsync）决定了实际持久化级别</li>
 *   <li>默认 {@code appendfsync everysec}：最多丢失 1 秒数据（Redis 崩溃时）</li>
 *   <li>使用 {@code appendfsync always}：每次写入都同步到磁盘，等价于磁盘级持久化</li>
 *   <li>在 Redis 主从异步复制模式下，从节点可能滞后于主节点</li>
 * </ul>
 *
 * <p>如需更高级别的持久化保证，请配置 Redis 的 AOF 策略或使用 Redis WAIT 命令等待从节点确认。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Getter
public final class SendResult implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    /** 消息 ID（对应 Redis Stream Entry ID） */
    @NonNull
    private final MessageId messageId;

    /** Topic */
    @NonNull
    private final String topic;

    /** Tag（可能为 null） */
    private final String tag;

    /** 发送状态 */
    @NonNull
    private final SendStatus sendStatus;

    /** 出生时间戳（毫秒） */
    private final long bornTimestamp;

    /** Region ID（多机房场景，v1.0+） */
    private final String regionId;

    /** 错误信息（仅在 {@link SendStatus#SEND_FAILED} 时非空） */
    private final String errorMessage;

    /**
     * 构造成功的发送结果。
     *
     * @param messageId 消息 ID
     * @param topic 主题
     * @param tag 标签，可为 null
     * @param bornTimestamp 出生时间戳
     */
    public SendResult(MessageId messageId, String topic, String tag, long bornTimestamp) {
        this(messageId, topic, tag, SendStatus.SEND_OK, bornTimestamp, null, null);
    }

    /**
     * 全参构造。
     *
     * @param messageId 消息 ID
     * @param topic 主题
     * @param tag 标签，可为 null
     * @param sendStatus 发送状态
     * @param bornTimestamp 出生时间戳
     * @param regionId Region ID，可为 null
     * @param errorMessage 错误信息，可为 null
     */
    public SendResult(MessageId messageId, String topic, String tag, SendStatus sendStatus,
                      long bornTimestamp, String regionId, String errorMessage) {
        this.messageId = Objects.requireNonNull(messageId, "messageId");
        this.topic = Objects.requireNonNull(topic, "topic");
        this.tag = tag;
        this.sendStatus = Objects.requireNonNull(sendStatus, "sendStatus");
        this.bornTimestamp = bornTimestamp;
        this.regionId = regionId;
        this.errorMessage = errorMessage;
    }

    /**
     * 是否发送成功。
     *
     * @return true 如果状态为 {@link SendStatus#SEND_OK}
     */
    public boolean isSuccess() {
        return sendStatus == SendStatus.SEND_OK;
    }

    @Override
    public String toString() {
        return "SendResult{"
            + "messageId=" + messageId
            + ", topic='" + topic + '\''
            + ", tag='" + tag + '\''
            + ", sendStatus=" + sendStatus
            + ", bornTimestamp=" + bornTimestamp
            + (Objects.nonNull(regionId) ? ", regionId='" + regionId + '\'' : "")
            + (Objects.nonNull(errorMessage) ? ", errorMessage='" + errorMessage + '\'' : "")
            + '}';
    }
}
