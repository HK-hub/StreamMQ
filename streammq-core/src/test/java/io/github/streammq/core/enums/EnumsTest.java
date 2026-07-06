package io.github.streammq.core.enums;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 各枚举类型完整性测试，覆盖 AcknowledgeMode / ConsumeAction / OrderlyAction / ConsumeMode / MessageModel / LocalTransactionState。
 */
@DisplayName("核心枚举完整性测试")
class EnumsTest {

    @Nested
    @DisplayName("AcknowledgeMode")
    class AcknowledgeModeTest {

        @Test
        @DisplayName("AcknowledgeMode 含 AUTO 与 MANUAL 两个值")
        void containsAutoAndManual() {
            assertThat(AcknowledgeMode.values())
                .containsExactlyInAnyOrder(AcknowledgeMode.AUTO, AcknowledgeMode.MANUAL);
        }
    }

    @Nested
    @DisplayName("ConsumeAction")
    class ConsumeActionTest {

        @Test
        @DisplayName("ConsumeAction 含 SUCCESS 与 RECONSUME_LATER")
        void containsSuccessAndReconsumeLater() {
            assertThat(ConsumeAction.values())
                .contains(ConsumeAction.SUCCESS, ConsumeAction.RECONSUME_LATER);
        }

        @Test
        @DisplayName("ConsumeAction 含全部 2 个值")
        void hasAllTwoValues() {
            assertThat(ConsumeAction.values()).hasSize(2);
            assertThat(ConsumeAction.values())
                .containsExactly(ConsumeAction.SUCCESS, ConsumeAction.RECONSUME_LATER);
        }
    }

    @Nested
    @DisplayName("OrderlyAction")
    class OrderlyActionTest {

        @Test
        @DisplayName("OrderlyAction 含 SUCCESS 与 SUSPEND_CURRENT_QUEUE_A_MOMENT")
        void containsSuccessAndSuspend() {
            assertThat(OrderlyAction.values())
                .contains(OrderlyAction.SUCCESS, OrderlyAction.SUSPEND_CURRENT_QUEUE_A_MOMENT);
        }

        @Test
        @DisplayName("OrderlyAction 含全部 2 个值")
        void hasAllTwoValues() {
            assertThat(OrderlyAction.values()).hasSize(2);
            assertThat(OrderlyAction.values())
                .containsExactly(OrderlyAction.SUCCESS, OrderlyAction.SUSPEND_CURRENT_QUEUE_A_MOMENT);
        }
    }

    @Nested
    @DisplayName("ConsumeMode")
    class ConsumeModeTest {

        @Test
        @DisplayName("ConsumeMode 含 CLUSTERING 与 BROADCASTING")
        void containsClusteringAndBroadcasting() {
            assertThat(ConsumeMode.values())
                .containsExactlyInAnyOrder(ConsumeMode.CLUSTERING, ConsumeMode.BROADCASTING);
        }
    }

    @Nested
    @DisplayName("MessageModel")
    class MessageModelTest {

        @Test
        @DisplayName("MessageModel 含 CONCURRENT 与 ORDERLY")
        void containsConcurrentAndOrderly() {
            assertThat(MessageModel.values())
                .containsExactlyInAnyOrder(MessageModel.CONCURRENT, MessageModel.ORDERLY);
        }
    }

    @Nested
    @DisplayName("LocalTransactionState")
    class LocalTransactionStateTest {

        @Test
        @DisplayName("LocalTransactionState 含 COMMIT_MESSAGE / ROLLBACK_MESSAGE / UNKNOW")
        void containsAllThreeStates() {
            assertThat(LocalTransactionState.values())
                .containsExactlyInAnyOrder(
                    LocalTransactionState.COMMIT_MESSAGE,
                    LocalTransactionState.ROLLBACK_MESSAGE,
                    LocalTransactionState.UNKNOW);
        }
    }
}
