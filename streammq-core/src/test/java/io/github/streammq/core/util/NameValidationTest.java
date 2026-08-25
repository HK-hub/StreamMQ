/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.streammq.core.message.MessageBuilder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 命名校验（topic / group / namespace）单元测试。
 *
 * <p>校验规则：非 null、非空、不含 {@code ':'}、{@code '*'} 或空白字符，防止破坏 Redis Key 结构或被错误路由。
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
    @DisplayName("含 ':' / '*' / 空白字符的名称被拒绝")
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
}
