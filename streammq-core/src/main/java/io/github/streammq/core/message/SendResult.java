/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.message;

import io.github.streammq.core.enums.LocalTransactionState;
import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;
import lombok.Getter;
import lombok.NonNull;

/**
 * 发送结果。
 *
 * <p>由 {@code StreamMQTemplate.syncSend} / {@code syncSendBatch} 返回， 封装消息 ID、状态、出生时间戳、Region 等信息。
 *
 * <p><b>状态语义（0.1.2 定稿，发布即冻结）：</b>{@link SendStatus#SEND_OK} 是"<b>已受理</b>"而非"已投递到目标
 * Stream"的泛化语义，按路径解读：
 *
 * <ul>
 *   <li><b>普通发送</b>：Redis 已确认收到 XADD 命令，消息已写入 Stream，{@code messageId} 为真实 Entry ID
 *   <li><b>延时消息</b>：已登记延时投递（payload + 调度条目原子写入），<b>尚未</b>写入目标 Stream； 此时 {@code messageId} 为可辨识的占位
 *       ID（{@link MessageId#pending()} / {@link MessageId#isPending()}），真实 ID 在到期转投时生成
 *   <li><b>事务消息</b>：仅在 {@code COMMIT_MESSAGE} 时为 {@code SEND_OK}；{@code ROLLBACK_MESSAGE} 与 {@code
 *       UNKNOWN} 均为 {@link SendStatus#SEND_FAILED}，调用方通过 {@link #getTransactionState()} 区分 ——{@code
 *       UNKNOWN} 表示"半消息已保留、等待事务回查"（结果未知，可能最终提交）， 非 {@code UNKNOWN} 的失败才是硬失败。{@link #isSuccess()} 对
 *       {@code UNKNOWN} 返回 false（结果未知不计入成功）
 * </ul>
 *
 * <p><b>持久化保证（重要）：</b>
 *
 * <ul>
 *   <li>{@link SendStatus#SEND_OK} 表示 Redis 已确认收到 XADD 命令（普通路径），消息已写入 Stream
 *   <li>但这<b>不等于</b>消息已持久化到磁盘——Redis 的 AOF 策略（appendfsync）决定了实际持久化级别
 *   <li>默认 {@code appendfsync everysec}：最多丢失 1 秒数据（Redis 崩溃时）
 *   <li>使用 {@code appendfsync always}：每次写入都同步到磁盘，等价于磁盘级持久化
 *   <li>在 Redis 主从异步复制模式下，从节点可能滞后于主节点
 * </ul>
 *
 * <p>如需更高级别的持久化保证，请配置 Redis 的 AOF 策略或使用 Redis WAIT 命令等待从节点确认。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Getter
public final class SendResult implements Serializable {

    @Serial private static final long serialVersionUID = 1L;

    /** 消息 ID（对应 Redis Stream Entry ID；延时 / 事务 UNKNOWN 等"尚未投递"场景为占位 ID） */
    @NonNull private final MessageId messageId;

    /** Topic */
    @NonNull private final String topic;

    /** Tag（可能为 null） */
    private final String tag;

    /** 发送状态（{@link SendStatus#SEND_OK} = 已受理，详见类型 javadoc） */
    @NonNull private final SendStatus sendStatus;

    /**
     * 事务状态（仅事务消息路径非 null；null 表示非事务发送）。
     *
     * <p>{@link io.github.streammq.core.enums.LocalTransactionState#UNKNOWN} 与 {@code
     * ROLLBACK_MESSAGE} 均为 {@link SendStatus#SEND_FAILED}，调用方据本字段区分"结果未知（等待回查）"与"硬失败"。
     */
    private final LocalTransactionState transactionState;

    /** 出生时间戳（毫秒） */
    private final long bornTimestamp;

    /** Region ID（多机房场景，v1.0+） */
    private final String regionId;

    /** 错误信息（仅在 {@link SendStatus#SEND_FAILED} 时非空） */
    private final String errorMessage;

    /**
     * 构造成功的发送结果（非事务路径，{@code transactionState} 为 null）。
     *
     * @param messageId 消息 ID
     * @param topic 主题
     * @param tag 标签，可为 null
     * @param bornTimestamp 出生时间戳
     */
    public SendResult(MessageId messageId, String topic, String tag, long bornTimestamp) {
        this(messageId, topic, tag, SendStatus.SEND_OK, bornTimestamp, null, null, null);
    }

    /**
     * 全参构造（非事务路径，{@code transactionState} 为 null）。
     *
     * @param messageId 消息 ID
     * @param topic 主题
     * @param tag 标签，可为 null
     * @param sendStatus 发送状态
     * @param bornTimestamp 出生时间戳
     * @param regionId Region ID，可为 null
     * @param errorMessage 错误信息，可为 null
     */
    public SendResult(
            MessageId messageId,
            String topic,
            String tag,
            SendStatus sendStatus,
            long bornTimestamp,
            String regionId,
            String errorMessage) {
        this(messageId, topic, tag, sendStatus, bornTimestamp, regionId, errorMessage, null);
    }

    /**
     * 全参构造（含事务状态）。
     *
     * @param messageId 消息 ID
     * @param topic 主题
     * @param tag 标签，可为 null
     * @param sendStatus 发送状态
     * @param bornTimestamp 出生时间戳
     * @param regionId Region ID，可为 null
     * @param errorMessage 错误信息，可为 null
     * @param transactionState 事务状态，非事务路径为 null
     */
    public SendResult(
            MessageId messageId,
            String topic,
            String tag,
            SendStatus sendStatus,
            long bornTimestamp,
            String regionId,
            String errorMessage,
            LocalTransactionState transactionState) {
        this.messageId = Objects.requireNonNull(messageId, "messageId");
        this.topic = Objects.requireNonNull(topic, "topic");
        this.tag = tag;
        this.sendStatus = Objects.requireNonNull(sendStatus, "sendStatus");
        this.transactionState = transactionState;
        this.bornTimestamp = bornTimestamp;
        this.regionId = regionId;
        this.errorMessage = errorMessage;
    }

    /**
     * 是否发送成功。
     *
     * <p><b>0.1.2 语义：</b>仅 {@link SendStatus#SEND_OK}（普通=已写入目标 Stream；延时=已登记延时投递；事务=仅
     * COMMIT_MESSAGE）为 true；事务 {@code UNKNOWN} 虽可能最终提交，但结果未知，本方法返回 false。
     *
     * @return true 如果状态为 {@link SendStatus#SEND_OK}
     */
    public boolean isSuccess() {
        return sendStatus == SendStatus.SEND_OK;
    }

    /**
     * 事务结果是否未知（待回查）。
     *
     * <p>为 true 时 {@link #getSendStatus()} 必为 {@link SendStatus#SEND_FAILED}，但消息并未失败——半消息已保留，
     * 由事务回查决定最终提交或回滚，调用方应据此与硬失败区别处理（如避免立即重发）。
     *
     * @return true 如果 {@link #getTransactionState()} 为 {@link
     *     io.github.streammq.core.enums.LocalTransactionState#UNKNOWN}
     */
    public boolean isTransactionUnknown() {
        return transactionState == LocalTransactionState.UNKNOWN;
    }

    @Override
    public String toString() {
        return "SendResult{"
                + "messageId="
                + messageId
                + ", topic='"
                + topic
                + '\''
                + ", tag='"
                + tag
                + '\''
                + ", sendStatus="
                + sendStatus
                + (Objects.nonNull(transactionState)
                        ? ", transactionState=" + transactionState
                        : "")
                + ", bornTimestamp="
                + bornTimestamp
                + (Objects.nonNull(regionId) ? ", regionId='" + regionId + '\'' : "")
                + (Objects.nonNull(errorMessage) ? ", errorMessage='" + errorMessage + '\'' : "")
                + '}';
    }
}
