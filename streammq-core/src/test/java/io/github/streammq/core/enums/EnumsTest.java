package io.github.streammq.core.enums;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 各枚举类型完整性测试，覆盖 AcknowledgeMode / Action / ConsumeMode / MessageModel / LocalTransactionState。
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
    @DisplayName("Action")
    class ActionTest {

        @Test
        @DisplayName("Action 含 SUCCESS 与 RECONSUME_LATER")
        void containsSuccessAndReconsumeLater() {
            assertThat(Action.values())
                .contains(Action.SUCCESS, Action.RECONSUME_LATER);
        }

        @Test
        @DisplayName("Action 含全部 5 个值")
        void hasAllFiveValues() {
            assertThat(Action.values()).hasSize(5);
            assertThat(Action.values())
                .containsExactly(Action.SUCCESS, Action.RECONSUME_LATER,
                    Action.SUSPEND_CURRENT_QUEUE_A_MOMENT, Action.COMMIT, Action.ROLLBACK);
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
