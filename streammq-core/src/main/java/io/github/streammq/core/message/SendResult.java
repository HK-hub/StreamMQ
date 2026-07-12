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
