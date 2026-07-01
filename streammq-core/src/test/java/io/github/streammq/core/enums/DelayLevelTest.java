package io.github.streammq.core.enums;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link DelayLevel} 单元测试，覆盖枚举数量、toMillis/toSeconds/getDuration、ofIndex、closestAbove。
 */
@DisplayName("DelayLevel 延时级别测试")
class DelayLevelTest {

    @Nested
    @DisplayName("枚举数量")
    class Count {

        @Test
        @DisplayName("延时级别共 18 个")
        void countIs18() {
            assertThat(DelayLevel.values()).hasSize(18);
        }
    }

    @Nested
    @DisplayName("toMillis")
    class ToMillis {

        @Test
        @DisplayName("SECOND_1.toMillis = 1000")
        void second1() {
            assertThat(DelayLevel.SECOND_1.toMillis()).isEqualTo(1000L);
        }

        @Test
        @DisplayName("SECOND_5.toMillis = 5000")
        void second5() {
            assertThat(DelayLevel.SECOND_5.toMillis()).isEqualTo(5000L);
        }

        @Test
        @DisplayName("SECOND_30.toMillis = 30000")
        void second30() {
            assertThat(DelayLevel.SECOND_30.toMillis()).isEqualTo(30_000L);
        }

        @Test
        @DisplayName("MINUTE_1.toMillis = 60000")
        void minute1() {
            assertThat(DelayLevel.MINUTE_1.toMillis()).isEqualTo(60_000L);
        }

        @Test
        @DisplayName("HOUR_1.toMillis = 3600000")
        void hour1() {
            assertThat(DelayLevel.HOUR_1.toMillis()).isEqualTo(3600_000L);
        }

        @Test
        @DisplayName("HOUR_2.toMillis = 2 * 3600 * 1000")
        void hour2() {
            assertThat(DelayLevel.HOUR_2.toMillis()).isEqualTo(2L * 3600 * 1000);
        }
    }

    @Nested
    @DisplayName("toSeconds")
    class ToSeconds {

        @Test
        @DisplayName("SECOND_1.toSeconds = 1")
        void second1() {
            assertThat(DelayLevel.SECOND_1.toSeconds()).isEqualTo(1L);
        }

        @Test
        @DisplayName("MINUTE_10.toSeconds = 600")
        void minute10() {
            assertThat(DelayLevel.MINUTE_10.toSeconds()).isEqualTo(600L);
        }

        @Test
        @DisplayName("HOUR_2.toSeconds = 7200")
        void hour2() {
            assertThat(DelayLevel.HOUR_2.toSeconds()).isEqualTo(7200L);
        }
    }

    @Nested
    @DisplayName("getDuration")
    class GetDuration {

        @Test
        @DisplayName("SECOND_1.getDuration = Duration.ofSeconds(1)")
        void second1Duration() {
            assertThat(DelayLevel.SECOND_1.getDuration()).isEqualTo(Duration.ofSeconds(1));
        }

        @Test
        @DisplayName("HOUR_2.getDuration = Duration.ofHours(2)")
        void hour2Duration() {
            assertThat(DelayLevel.HOUR_2.getDuration()).isEqualTo(Duration.ofHours(2));
        }
    }

    @Nested
    @DisplayName("ofIndex")
    class OfIndex {

        @Test
        @DisplayName("ofIndex(0) = SECOND_1")
        void index0() {
            assertThat(DelayLevel.ofIndex(0)).isEqualTo(DelayLevel.SECOND_1);
        }

        @Test
        @DisplayName("ofIndex(17) = HOUR_2")
        void index17() {
            assertThat(DelayLevel.ofIndex(17)).isEqualTo(DelayLevel.HOUR_2);
        }

        @Test
        @DisplayName("ofIndex(-1) 抛 IndexOutOfBoundsException")
        void negativeIndex() {
            assertThatThrownBy(() -> DelayLevel.ofIndex(-1))
                .isInstanceOf(IndexOutOfBoundsException.class)
                .hasMessageContaining("out of bounds");
        }

        @Test
        @DisplayName("ofIndex(18) 越界抛 IndexOutOfBoundsException")
        void tooLargeIndex() {
            assertThatThrownBy(() -> DelayLevel.ofIndex(18))
                .isInstanceOf(IndexOutOfBoundsException.class)
                .hasMessageContaining("out of bounds");
        }
    }

    @Nested
    @DisplayName("closestAbove")
    class ClosestAbove {

        @Test
        @DisplayName("closestAbove(500) = SECOND_1（向上取整）")
        void belowSecond1() {
            assertThat(DelayLevel.closestAbove(500)).isEqualTo(DelayLevel.SECOND_1);
        }

        @Test
        @DisplayName("closestAbove(1000) = SECOND_1（刚好等于）")
        void equalSecond1() {
            assertThat(DelayLevel.closestAbove(1000)).isEqualTo(DelayLevel.SECOND_1);
        }

        @Test
        @DisplayName("closestAbove(2000) = SECOND_5")
        void betweenSecond1And5() {
            assertThat(DelayLevel.closestAbove(2000)).isEqualTo(DelayLevel.SECOND_5);
        }

        @Test
        @DisplayName("closestAbove(超长) = HOUR_2")
        void beyondMax() {
            assertThat(DelayLevel.closestAbove(10_000_000L)).isEqualTo(DelayLevel.HOUR_2);
        }

        @Test
        @DisplayName("closestAbove(0) = SECOND_1")
        void zero() {
            assertThat(DelayLevel.closestAbove(0)).isEqualTo(DelayLevel.SECOND_1);
        }
    }
}
