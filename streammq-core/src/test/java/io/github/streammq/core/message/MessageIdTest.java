/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.message;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/** {@link MessageId} 单元测试，覆盖格式解析、compareTo、equals/hashCode、toString 与非法格式校验。 */
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
        @DisplayName("hashCode 基于 (timestamp, sequence)：与等价 ID 一致，且与 equals 自洽")
        void hashCodeConsistent() {
            MessageId a = new MessageId("100-5");
            MessageId b = MessageId.of(100L, 5L);
            assertThat(a).isEqualTo(b);
            assertThat(a.hashCode()).isEqualTo(b.hashCode());
        }
    }

    @Nested
    @DisplayName("占位 ID（pending / sentinel）")
    class Pending {

        @Test
        @DisplayName("pending() 稳定：多次调用返回同一实例与保留值 0-0")
        void pendingIsStable() {
            MessageId pending = MessageId.pending();
            assertThat(pending).isSameAs(MessageId.pending());
            assertThat(pending.getStreamEntryId()).isEqualTo(MessageId.PENDING_STREAM_ENTRY_ID);
            assertThat(pending.getStreamEntryId()).isEqualTo("0-0");
            assertThat(pending.toString()).isEqualTo("0-0");
            assertThat(pending.getTimestamp()).isZero();
            assertThat(pending.getSequence()).isZero();
        }

        @Test
        @DisplayName("isPending 判定：pending()/of(0,0) 为 true，真实 ID 为 false")
        void isPendingDetection() {
            assertThat(MessageId.pending().isPending()).isTrue();
            assertThat(MessageId.of(0L, 0L).isPending()).isTrue();
            assertThat(new MessageId("0-0").isPending()).isTrue();
            // 真实 Entry ID：时间戳恒 > 0
            assertThat(new MessageId("1234567890-0").isPending()).isFalse();
            assertThat(MessageId.of(1L, 0L).isPending()).isFalse();
            assertThat(MessageId.of(0L, 1L).isPending()).isFalse();
        }

        @Test
        @SuppressWarnings({"deprecation", "removal"})
        @DisplayName("sentinel() 为 pending() 的别名（旧名返回值域不再与真实 ID 碰撞）")
        void sentinelIsPendingAlias() {
            MessageId sentinel = MessageId.sentinel();
            assertThat(sentinel).isEqualTo(MessageId.pending());
            assertThat(sentinel.isPending()).isTrue();
            assertThat(sentinel.hashCode()).isEqualTo(MessageId.pending().hashCode());
        }

        @Test
        @DisplayName("占位值与任意真实 ID 不混淆：equals 不等、compareTo 非 0、占位排序最前")
        void pendingNeverConfusedWithRealIds() {
            MessageId pending = MessageId.pending();
            MessageId real = new MessageId("1234567890-0");
            assertThat(pending).isNotEqualTo(real);
            assertThat(pending.compareTo(real)).isNegative();
            assertThat(real.compareTo(pending)).isPositive();
            // TreeSet 与 HashSet 判定一致（compareTo == 0 ⟺ equals）
            assertThat(real.compareTo(pending)).isNotZero();
        }

        @Test
        @DisplayName("of(0,0) 与 pending() 语义等价（同一保留值），equals/compareTo 自洽")
        void ofZeroZeroEqualsPending() {
            MessageId zero = MessageId.of(0L, 0L);
            assertThat(zero).isEqualTo(MessageId.pending());
            assertThat(zero.compareTo(MessageId.pending())).isZero();
            assertThat(zero.isPending()).isTrue();
        }

        @Test
        @DisplayName("of() 非负校验与规范化规则不回归")
        void ofValidationNotRegressed() {
            assertThat(MessageId.of(100L, 5L).getStreamEntryId()).isEqualTo("100-5");
            assertThat(new MessageId("01-2")).isEqualTo(MessageId.of(1L, 2L));
            assertThatThrownBy(() -> MessageId.of(-1L, 0L))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("timestamp");
            assertThatThrownBy(() -> MessageId.of(0L, -1L))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("sequence");
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
