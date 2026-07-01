package io.github.streammq.core.message;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link MessageId} 单元测试，覆盖格式解析、compareTo、equals/hashCode、toString 与非法格式校验。
 */
@DisplayName("MessageId 消息 ID 测试")
class MessageIdTest {

    @Nested
    @DisplayName("构造与解析")
    class Construction {

        @Test
        @DisplayName("正确解析 1234567890-0 格式")
        void parseValidFormat() {
            MessageId id = new MessageId("1234567890-0");
            assertThat(id.getStreamEntryId()).isEqualTo("1234567890-0");
            assertThat(id.getTimestamp()).isEqualTo(1234567890L);
            assertThat(id.getSequence()).isEqualTo(0L);
        }

        @Test
        @DisplayName("解析带非零序列号的 ID")
        void parseWithSequence() {
            MessageId id = new MessageId("100-42");
            assertThat(id.getTimestamp()).isEqualTo(100L);
            assertThat(id.getSequence()).isEqualTo(42L);
        }

        @Test
        @DisplayName("包含多个连字符时第二段无法解析为数字抛 IllegalArgumentException")
        void parseWithMultipleDashes() {
            assertThatThrownBy(() -> new MessageId("100-42-extra"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid stream entry id numeric parts");
        }
    }

    @Nested
    @DisplayName("compareTo 比较")
    class Compare {

        @Test
        @DisplayName("时间戳不同时按时间戳比较")
        void compareDifferentTimestamp() {
            MessageId small = new MessageId("100-5");
            MessageId big = new MessageId("200-1");
            assertThat(small.compareTo(big)).isNegative();
            assertThat(big.compareTo(small)).isPositive();
        }

        @Test
        @DisplayName("时间戳相同序列号不同时按序列号比较")
        void compareSameTimestampDifferentSequence() {
            MessageId small = new MessageId("100-1");
            MessageId big = new MessageId("100-9");
            assertThat(small.compareTo(big)).isNegative();
            assertThat(big.compareTo(small)).isPositive();
        }

        @Test
        @DisplayName("完全相等时 compareTo 返回 0")
        void compareEqual() {
            MessageId a = new MessageId("100-5");
            MessageId b = new MessageId("100-5");
            assertThat(a.compareTo(b)).isZero();
        }
    }

    @Nested
    @DisplayName("equals / hashCode")
    class EqualsHashCode {

        @Test
        @DisplayName("相同 streamEntryId 相等")
        void equalByStreamEntryId() {
            MessageId a = new MessageId("100-5");
            MessageId b = new MessageId("100-5");
            assertThat(a).isEqualTo(b);
            assertThat(a.hashCode()).isEqualTo(b.hashCode());
        }

        @Test
        @DisplayName("不同 streamEntryId 不相等")
        void notEqualDifferentId() {
            MessageId a = new MessageId("100-5");
            MessageId b = new MessageId("100-6");
            assertThat(a).isNotEqualTo(b);
        }

        @Test
        @DisplayName("与 null 比较返回 false")
        void notEqualNull() {
            MessageId a = new MessageId("100-5");
            assertThat(a).isNotEqualTo(null);
        }

        @Test
        @DisplayName("与不同类型比较返回 false")
        void notEqualDifferentType() {
            MessageId a = new MessageId("100-5");
            assertThat(a).isNotEqualTo("100-5");
            assertThat(a).isNotEqualTo(100);
        }

        @Test
        @DisplayName("自反性：等于自身")
        void reflexive() {
            MessageId a = new MessageId("100-5");
            assertThat(a).isEqualTo(a);
        }

        @Test
        @DisplayName("hashCode 与 streamEntryId 的 hashCode 一致")
        void hashCodeConsistent() {
            MessageId a = new MessageId("100-5");
            assertThat(a.hashCode()).isEqualTo("100-5".hashCode());
        }
    }

    @Nested
    @DisplayName("toString")
    class ToString {

        @Test
        @DisplayName("toString 返回原始字符串")
        void toStringReturnsRaw() {
            MessageId id = new MessageId("1234567890-7");
            assertThat(id.toString()).isEqualTo("1234567890-7");
        }
    }

    @Nested
    @DisplayName("非法格式校验")
    class InvalidFormat {

        @Test
        @DisplayName("null 抛 NPE")
        void nullInput() {
            assertThatThrownBy(() -> new MessageId(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("streamEntryId");
        }

        @Test
        @DisplayName("无连字符抛 IllegalArgumentException")
        void noDash() {
            assertThatThrownBy(() -> new MessageId("1234567890"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid stream entry id format");
        }

        @Test
        @DisplayName("非数字时间戳抛 IllegalArgumentException")
        void nonNumericTimestamp() {
            assertThatThrownBy(() -> new MessageId("abc-0"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid stream entry id numeric parts");
        }

        @Test
        @DisplayName("非数字序列号抛 IllegalArgumentException")
        void nonNumericSequence() {
            assertThatThrownBy(() -> new MessageId("100-xyz"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid stream entry id numeric parts");
        }
    }
}
