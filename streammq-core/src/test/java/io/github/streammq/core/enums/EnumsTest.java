package io.github.streammq.core.enums;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * 各枚举/动作类型完整性测试，覆盖 ConsumeAction / ConsumeMode / MessageModel / LocalTransactionState。
 */
@DisplayName("核心枚举与动作类型完整性测试")
class EnumsTest {

    @Nested
    @DisplayName("ConsumeAction")
    class ConsumeActionTest {

        @Test
        @DisplayName("SUCCESS 与 RECONSUME_LATER 为单例常量，可 == 比较")
        void successAndReconsumeLaterAreSingletons() {
            assertThat(ConsumeAction.SUCCESS).isSameAs(ConsumeAction.SUCCESS);
            assertThat(ConsumeAction.RECONSUME_LATER).isSameAs(ConsumeAction.RECONSUME_LATER);
            assertThat(ConsumeAction.SUCCESS).isNotSameAs(ConsumeAction.RECONSUME_LATER);
        }

        @Test
        @DisplayName("类型判断方法正确")
        void typePredicates() {
            assertThat(ConsumeAction.SUCCESS.isSuccess()).isTrue();
            assertThat(ConsumeAction.SUCCESS.isReconsumeLater()).isFalse();
            assertThat(ConsumeAction.RECONSUME_LATER.isReconsumeLater()).isTrue();
            assertThat(ConsumeAction.RECONSUME_LATER.isSuccess()).isFalse();
        }

        @Test
        @DisplayName("defer(Duration) 返回 DEFER 动作并携带延迟")
        void deferCarriesDelay() {
            ConsumeAction defer = ConsumeAction.defer(Duration.ofSeconds(30));
            assertThat(defer.isDefer()).isTrue();
            assertThat(defer.isSuccess()).isFalse();
            assertThat(defer.getDeferDelay()).isEqualTo(Duration.ofSeconds(30));
            assertThat(defer.type()).isEqualTo(ConsumeAction.Type.DEFER);
        }

        @Test
        @DisplayName("defer 拒绝 null/非正延迟")
        void deferRejectsInvalidDelay() {
            assertThatThrownBy(() -> ConsumeAction.defer(null))
                .isInstanceOf(NullPointerException.class);
            assertThatThrownBy(() -> ConsumeAction.defer(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> ConsumeAction.defer(Duration.ofMillis(-1)))
                .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @DisplayName("name() 返回类型名称")
        void nameReturnsTypeName() {
            assertThat(ConsumeAction.SUCCESS.name()).isEqualTo("SUCCESS");
            assertThat(ConsumeAction.RECONSUME_LATER.name()).isEqualTo("RECONSUME_LATER");
            assertThat(ConsumeAction.defer(Duration.ofSeconds(1)).name()).isEqualTo("DEFER");
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

