/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.message;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.streammq.core.enums.LocalTransactionState;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/** {@link SendResult} 单元测试，覆盖两个构造器、isSuccess、getter、toString 与 null 校验。 */
@DisplayName("SendResult 发送结果测试")
class SendResultTest {

    private static final MessageId MESSAGE_ID = new MessageId("100-0");

    @Nested
    @DisplayName("4 参构造（成功）")
    class FourArgConstructor {

        @Test
        @DisplayName("4 参构造默认 SEND_OK 状态")
        void fourArgDefaultsToSendOk() {
            SendResult result = new SendResult(MESSAGE_ID, "topic", "tag", 1000L);
            assertThat(result.getMessageId()).isEqualTo(MESSAGE_ID);
            assertThat(result.getTopic()).isEqualTo("topic");
            assertThat(result.getTag()).isEqualTo("tag");
            assertThat(result.getBornTimestamp()).isEqualTo(1000L);
            assertThat(result.getSendStatus()).isEqualTo(SendStatus.SEND_OK);
            assertThat(result.getRegionId()).isNull();
            assertThat(result.getErrorMessage()).isNull();
            assertThat(result.isSuccess()).isTrue();
        }

        @Test
        @DisplayName("tag 可为 null")
        void fourArgNullTag() {
            SendResult result = new SendResult(MESSAGE_ID, "topic", null, 1L);
            assertThat(result.getTag()).isNull();
        }
    }

    @Nested
    @DisplayName("7 参构造（全参）")
    class SevenArgConstructor {

        @Test
        @DisplayName("全参构造正确赋值所有字段")
        void sevenArgFull() {
            SendResult result =
                    new SendResult(
                            MESSAGE_ID,
                            "topic",
                            "tag",
                            SendStatus.SEND_FAILED,
                            2000L,
                            "region-1",
                            "boom");
            assertThat(result.getMessageId()).isEqualTo(MESSAGE_ID);
            assertThat(result.getTopic()).isEqualTo("topic");
            assertThat(result.getTag()).isEqualTo("tag");
            assertThat(result.getSendStatus()).isEqualTo(SendStatus.SEND_FAILED);
            assertThat(result.getBornTimestamp()).isEqualTo(2000L);
            assertThat(result.getRegionId()).isEqualTo("region-1");
            assertThat(result.getErrorMessage()).isEqualTo("boom");
        }
    }

    @Nested
    @DisplayName("isSuccess")
    class IsSuccess {

        @ParameterizedTest(name = "状态 {0} 时 isSuccess 为 true")
        @EnumSource(value = SendStatus.class, names = "SEND_OK")
        @DisplayName("SEND_OK 时 isSuccess 为 true")
        void successIsTrue(SendStatus status) {
            SendResult result = new SendResult(MESSAGE_ID, "topic", null, status, 0L, null, null);
            assertThat(result.isSuccess()).isTrue();
        }

        @ParameterizedTest(name = "状态 {0} 时 isSuccess 为 false")
        @EnumSource(value = SendStatus.class, names = "SEND_OK", mode = EnumSource.Mode.EXCLUDE)
        @DisplayName("非 SEND_OK 时 isSuccess 为 false")
        void successIsFalse(SendStatus status) {
            SendResult result = new SendResult(MESSAGE_ID, "topic", null, status, 0L, null, null);
            assertThat(result.isSuccess()).isFalse();
        }
    }

    @Nested
    @DisplayName("toString")
    class ToString {

        @Test
        @DisplayName("toString 包含 messageId/topic/tag/sendTime/bornTimestamp 字段")
        void toStringContainsKeyFields() {
            SendResult result =
                    new SendResult(
                            MESSAGE_ID,
                            "order-topic",
                            "created",
                            SendStatus.SEND_OK,
                            1000L,
                            null,
                            null);
            String str = result.toString();
            assertThat(str).startsWith("SendResult{");
            assertThat(str).contains("messageId=100-0");
            assertThat(str).contains("topic='order-topic'");
            assertThat(str).contains("tag='created'");
            assertThat(str).contains("sendStatus=SEND_OK");
            assertThat(str).contains("bornTimestamp=1000");
        }

        @Test
        @DisplayName("regionId 非空时 toString 包含 regionId")
        void toStringWithRegion() {
            SendResult result =
                    new SendResult(MESSAGE_ID, "t", null, SendStatus.SEND_OK, 1L, "r1", null);
            assertThat(result.toString()).contains("regionId='r1'");
        }

        @Test
        @DisplayName("errorMessage 非空时 toString 包含 errorMessage")
        void toStringWithError() {
            SendResult result =
                    new SendResult(
                            MESSAGE_ID, "t", null, SendStatus.SEND_FAILED, 1L, null, "timeout");
            assertThat(result.toString()).contains("errorMessage='timeout'");
        }
    }

    @Nested
    @DisplayName("事务状态（8 参构造）")
    class TransactionState {

        @Test
        @DisplayName("7 参构造 transactionState 为 null（非事务路径），isTransactionUnknown 为 false")
        void sevenArgHasNullTransactionState() {
            SendResult result =
                    new SendResult(MESSAGE_ID, "t", null, SendStatus.SEND_OK, 1L, null, null);
            assertThat(result.getTransactionState()).isNull();
            assertThat(result.isTransactionUnknown()).isFalse();
        }

        @Test
        @DisplayName("UNKNOWN：SEND_FAILED + transactionState=UNKNOWN，可与硬失败区分")
        void unknownIsFailedButDistinguishable() {
            SendResult unknown =
                    new SendResult(
                            MESSAGE_ID,
                            "t",
                            null,
                            SendStatus.SEND_FAILED,
                            1L,
                            null,
                            "Transaction state UNKNOWN, waiting for check-back",
                            LocalTransactionState.UNKNOWN);
            assertThat(unknown.isSuccess()).isFalse();
            assertThat(unknown.getSendStatus()).isEqualTo(SendStatus.SEND_FAILED);
            assertThat(unknown.isTransactionUnknown()).isTrue();
            assertThat(unknown.getTransactionState()).isEqualTo(LocalTransactionState.UNKNOWN);

            SendResult hardFailure =
                    new SendResult(
                            MESSAGE_ID,
                            "t",
                            null,
                            SendStatus.SEND_FAILED,
                            1L,
                            null,
                            "Transaction rolled back",
                            LocalTransactionState.ROLLBACK_MESSAGE);
            assertThat(hardFailure.isSuccess()).isFalse();
            assertThat(hardFailure.isTransactionUnknown()).isFalse();
        }

        @Test
        @DisplayName("COMMIT_MESSAGE 为 SEND_OK 且非 unknown")
        void commitIsSuccess() {
            SendResult committed =
                    new SendResult(
                            MESSAGE_ID,
                            "t",
                            null,
                            SendStatus.SEND_OK,
                            1L,
                            null,
                            null,
                            LocalTransactionState.COMMIT_MESSAGE);
            assertThat(committed.isSuccess()).isTrue();
            assertThat(committed.isTransactionUnknown()).isFalse();
        }

        @Test
        @DisplayName("toString 在事务路径包含 transactionState，非事务路径不包含")
        void toStringContainsTransactionState() {
            SendResult unknown =
                    new SendResult(
                            MESSAGE_ID,
                            "t",
                            null,
                            SendStatus.SEND_FAILED,
                            1L,
                            null,
                            "unknown",
                            LocalTransactionState.UNKNOWN);
            assertThat(unknown.toString()).contains("transactionState=UNKNOWN");

            SendResult plain =
                    new SendResult(MESSAGE_ID, "t", null, SendStatus.SEND_OK, 1L, null, null);
            assertThat(plain.toString()).doesNotContain("transactionState");
        }
    }

    @Nested
    @DisplayName("null 校验")
    class NullValidation {

        @Test
        @DisplayName("messageId 为 null 抛 NPE")
        void nullMessageId() {
            assertThatThrownBy(() -> new SendResult(null, "topic", null, 1L))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("messageId");
        }

        @Test
        @DisplayName("topic 为 null 抛 NPE")
        void nullTopic() {
            assertThatThrownBy(() -> new SendResult(MESSAGE_ID, null, null, 1L))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("topic");
        }

        @Test
        @DisplayName("sendStatus 为 null 抛 NPE")
        void nullSendStatus() {
            assertThatThrownBy(
                            () -> new SendResult(MESSAGE_ID, "topic", null, null, 1L, null, null))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("sendStatus");
        }
    }
}
