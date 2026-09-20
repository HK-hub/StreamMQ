/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.template;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.streammq.adapter.redisson.scheduler.TransactionScanner;
import io.github.streammq.core.converter.MessageConverter;
import io.github.streammq.core.enums.LocalTransactionState;
import io.github.streammq.core.exception.StreamMQException;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageBuilder;
import io.github.streammq.core.message.MessageId;
import io.github.streammq.core.message.SendResult;
import io.github.streammq.core.message.SendStatus;
import io.github.streammq.core.producer.ProducerConfig;
import io.github.streammq.core.producer.StreamMessageProducer;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.redisson.api.StreamMessageId;

/**
 * 事务 / 失败路径发送状态语义测试（第六轮红队 R3-2）。
 *
 * <p>core 0.1.2 定稿：{@link SendStatus#SEND_OK} = "已受理"，事务路径<b>仅</b> {@code COMMIT_MESSAGE} 为
 * SEND_OK；{@code ROLLBACK_MESSAGE} / {@code UNKNOWN} 一律 SEND_FAILED 且携带事务状态（UNKNOWN 表示"半消息已保留、
 * 等待回查"， 由调用方与硬失败区分）。失败路径的消息 ID 使用可辨识的占位 {@link MessageId#pending()}。
 *
 * @author StreamMQ Contributors
 * @since 0.1.2
 */
@DisplayName("事务发送状态语义（R3-2）")
class TransactionSendStatusTest {

    private static final String TX_GROUP = "tx-group";

    private TransactionScanner scanner;
    private DefaultStreamMessageTemplate template;

    @BeforeEach
    void setUp() {
        StreamMessageProducer producer = mock(StreamMessageProducer.class);
        MessageConverter converter = mock(MessageConverter.class);
        when(converter.toStreamFields(any())).thenReturn(Map.of("body", "cGF5bG9hZA=="));
        scanner = mock(TransactionScanner.class);
        when(scanner.registerHalfMessage(anyString(), anyString(), anyString(), any()))
                .thenReturn(new StreamMessageId(1_700_000_000_000L, 0L));

        template =
                new DefaultStreamMessageTemplate(
                        producer,
                        "default-group",
                        converter,
                        ProducerConfig.builder().group("default-group").build(),
                        TX_GROUP);
        template.setTransactionScanner(scanner);
    }

    @Test
    @DisplayName("COMMIT：SEND_OK + transactionState=COMMIT_MESSAGE + 真实半消息 ID")
    void commit_isSendOkWithCommitState() {
        SendResult result =
                template.executeInTransaction(
                        message(), (msg, ctx) -> LocalTransactionState.COMMIT_MESSAGE);

        assertThat(result.getSendStatus()).isEqualTo(SendStatus.SEND_OK);
        assertThat(result.isSuccess()).isTrue();
        assertThat(result.getTransactionState()).isEqualTo(LocalTransactionState.COMMIT_MESSAGE);
        assertThat(result.getMessageId().isPending()).isFalse();
        verify(scanner).markCommit(anyString(), anyString());
    }

    @Test
    @DisplayName("UNKNOWN：SEND_FAILED + transactionState=UNKNOWN（结果未知，非硬失败）（失败即红）")
    void unknown_isSendFailedWithUnknownState() {
        SendResult result =
                template.executeInTransaction(
                        message(), (msg, ctx) -> LocalTransactionState.UNKNOWN);

        assertThat(result.getSendStatus())
                .as("UNKNOWN 是非提交状态，必须为 SEND_FAILED 而非 SEND_OK")
                .isEqualTo(SendStatus.SEND_FAILED);
        assertThat(result.isSuccess()).isFalse();
        assertThat(result.getTransactionState()).isEqualTo(LocalTransactionState.UNKNOWN);
        assertThat(result.isTransactionUnknown()).isTrue();
        assertThat(result.getErrorMessage()).isNotBlank();
        // 半消息保留、等待回查：不得触发 commit/rollback
        verify(scanner, org.mockito.Mockito.never()).markCommit(anyString(), anyString());
        verify(scanner, org.mockito.Mockito.never()).markRollback(anyString(), anyString());
    }

    @Test
    @DisplayName("回调返回 null：按 UNKNOWN 处理（SEND_FAILED + UNKNOWN）")
    void nullCallbackState_isTreatedAsUnknown() {
        SendResult result = template.executeInTransaction(message(), (msg, ctx) -> null);

        assertThat(result.getSendStatus()).isEqualTo(SendStatus.SEND_FAILED);
        assertThat(result.getTransactionState()).isEqualTo(LocalTransactionState.UNKNOWN);
    }

    @Test
    @DisplayName("ROLLBACK：SEND_FAILED + transactionState=ROLLBACK_MESSAGE")
    void rollback_isSendFailedWithRollbackState() {
        SendResult result =
                template.executeInTransaction(
                        message(), (msg, ctx) -> LocalTransactionState.ROLLBACK_MESSAGE);

        assertThat(result.getSendStatus()).isEqualTo(SendStatus.SEND_FAILED);
        assertThat(result.getTransactionState()).isEqualTo(LocalTransactionState.ROLLBACK_MESSAGE);
        assertThat(result.isSuccess()).isFalse();
    }

    @Test
    @DisplayName("发送前失败结果使用可辨识占位 ID：MessageId.pending() / isPending()=true")
    void failedResultUsesPendingPlaceholderId() {
        SendResult failed =
                RetrySafetyPolicy.buildFailedResult(
                        message(), new StreamMQException("send failed"));

        assertThat(failed.getMessageId().isPending())
                .as("失败路径必须使用可辨识的占位 ID（0-0），而非与真实 Entry ID 不可区分的旧 sentinel")
                .isTrue();
        assertThat(failed.getMessageId()).isEqualTo(MessageId.pending());
        assertThat(failed.getSendStatus()).isEqualTo(SendStatus.SEND_FAILED);
    }

    private static Message<String> message() {
        return MessageBuilder.<String>withTopic("tx-topic").body("payload").build();
    }
}
