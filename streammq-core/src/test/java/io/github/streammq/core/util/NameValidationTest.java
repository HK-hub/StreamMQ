/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.streammq.core.listener.ListenerConfig;
import io.github.streammq.core.message.MessageBuilder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 命名校验（topic / group / namespace）单元测试。
 *
 * <p>校验规则：非 null、非空、不含 {@code ':'}、{@code '*'}、{@code '{'}、{@code '}'}、{@code '|'}、{@code ','}
 * 或空白字符，防止破坏 Redis Key 结构或被错误路由；topic / group 还禁止以保留前缀 {@code __} 开头。
 */
@DisplayName("命名校验")
class NameValidationTest {

    @Test
    @DisplayName("合法名称通过校验并去除首尾空白")
    void validNamesPass() {
        assertThat(StringUtils.requireValidTopic("order-topic")).isEqualTo("order-topic");
        assertThat(StringUtils.requireValidTopic(" topic-x ")).isEqualTo("topic-x");
        assertThat(StringUtils.requireValidGroup("order-group")).isEqualTo("order-group");
        assertThat(StringUtils.requireValidNamespace("")).isEmpty();
        assertThat(StringUtils.requireValidNamespace(null)).isEmpty();
        assertThat(StringUtils.requireValidNamespace("prod")).isEqualTo("prod");
    }

    @Test
    @DisplayName("空名称被拒绝")
    void emptyNamesRejected() {
        assertThatThrownBy(() -> StringUtils.requireValidTopic(""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not be empty");
        assertThatThrownBy(() -> StringUtils.requireValidTopic("   "))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> StringUtils.requireValidGroup(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    @DisplayName("含 ':' / '*' / '{' / '}' / '|' / ',' / 空白字符的名称被拒绝")
    void illegalCharactersRejected() {
        assertThatThrownBy(() -> StringUtils.requireValidTopic("a:b"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("'");
        assertThatThrownBy(() -> StringUtils.requireValidTopic("a*b"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> StringUtils.requireValidTopic("a b"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> StringUtils.requireValidGroup("g:r"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> StringUtils.requireValidNamespace("ns:1"))
                .isInstanceOf(IllegalArgumentException.class);
        // '{' '}' 是 Redis Cluster Hash Tag 定界符（热点风险）
        assertThatThrownBy(() -> StringUtils.requireValidTopic("a{b}"))
                .isInstanceOf(IllegalArgumentException.class);
        // '|' 与 ',' 是广播租约/注册表的内部编码分隔符：允许会让名称在编码时被静默改写，
        // 导致「实例租约保护」判定失配（真实缺陷面）
        assertThatThrownBy(() -> StringUtils.requireValidTopic("a|b"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> StringUtils.requireValidTopic("a,b"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> StringUtils.requireValidGroup("g,1"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("MessageBuilder.withTopic 对非法主题名抛出 IllegalArgumentException")
    void messageBuilderRejectsInvalidTopic() {
        assertThatCode(() -> MessageBuilder.<String>withTopic("ok-topic"))
                .doesNotThrowAnyException();
        assertThatThrownBy(() -> MessageBuilder.<String>withTopic("bad:topic"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> MessageBuilder.<String>withTopic("bad*topic"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> MessageBuilder.<String>withTopic("bad topic"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("保留名约束（不变量）：__ 开头的 topic / group 一律拒绝（含 __dlq__ 哨兵）")
    void reservedPrefixRejected() {
        for (String reserved : new String[] {"__dlq__", "__retry__", "__"}) {
            assertThatThrownBy(() -> StringUtils.requireValidTopic(reserved))
                    .as("topic '%s' 应被拒绝", reserved)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("reserved prefix");
            assertThatThrownBy(() -> StringUtils.requireValidGroup(reserved))
                    .as("group '%s' 应被拒绝", reserved)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("reserved prefix");
        }
        // 仅拒绝"前缀"，中间出现 __ 的普通名称仍合法
        assertThat(StringUtils.requireValidTopic("order__v2")).isEqualTo("order__v2");
        assertThat(StringUtils.requireValidGroup("group__v2")).isEqualTo("group__v2");
    }

    @Test
    @DisplayName("保留名约束在构造入口生效：MessageBuilder / ListenerConfig 同步拒绝")
    void reservedPrefixRejectedAtEntryPoints() {
        assertThatThrownBy(() -> MessageBuilder.<String>withTopic("__dlq__"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("reserved prefix");

        assertThatThrownBy(
                        () -> ListenerConfig.builder().topic("__dlq__").consumerGroup("g").build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("reserved prefix");
        assertThatThrownBy(() -> ListenerConfig.builder().topic("t").consumerGroup("__g").build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("reserved prefix");
    }
}
