/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.container;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.streammq.core.enums.ConsumeFromWhere;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 注解起始位点到全局配置的解析回归测试（发布前红队审查 R5）。
 *
 * <p>缺陷背景：注解 {@code consumeFromWhere} 的默认值曾是 {@code CONSUME_FROM_LAST}， 与全局默认相同，导致「未声明」与「显式声明
 * LAST」不可区分——全局设为 {@code CONSUME_FROM_FIRST} 时，想单独强制回 {@code LAST} 的消费者被静默忽略（语义反向）。现引入独立哨兵 {@link
 * ConsumeFromWhere#ANNOTATION_DEFAULT}，两条语义都可表达。
 */
@DisplayName("consumeFromWhere 注解值解析")
class ConsumeFromWhereResolutionTest {

    @Test
    @DisplayName("未声明（ANNOTATION_DEFAULT）→ 跟随全局配置")
    void unset_followsGlobal() {
        assertThat(
                        DefaultListenerRegistrar.resolveConsumeFromWhere(
                                ConsumeFromWhere.ANNOTATION_DEFAULT,
                                ConsumeFromWhere.CONSUME_FROM_FIRST))
                .isEqualTo(ConsumeFromWhere.CONSUME_FROM_FIRST);
        assertThat(
                        DefaultListenerRegistrar.resolveConsumeFromWhere(
                                ConsumeFromWhere.ANNOTATION_DEFAULT,
                                ConsumeFromWhere.CONSUME_FROM_LAST))
                .isEqualTo(ConsumeFromWhere.CONSUME_FROM_LAST);
    }

    @Test
    @DisplayName("显式 LAST 覆盖全局 FIRST（旧实现在此静默回落到 FIRST）")
    void explicitLast_overridesGlobalFirst() {
        assertThat(
                        DefaultListenerRegistrar.resolveConsumeFromWhere(
                                ConsumeFromWhere.CONSUME_FROM_LAST,
                                ConsumeFromWhere.CONSUME_FROM_FIRST))
                .isEqualTo(ConsumeFromWhere.CONSUME_FROM_LAST);
    }

    @Test
    @DisplayName("显式 FIRST 覆盖全局 LAST")
    void explicitFirst_overridesGlobalLast() {
        assertThat(
                        DefaultListenerRegistrar.resolveConsumeFromWhere(
                                ConsumeFromWhere.CONSUME_FROM_FIRST,
                                ConsumeFromWhere.CONSUME_FROM_LAST))
                .isEqualTo(ConsumeFromWhere.CONSUME_FROM_FIRST);
    }

    @Test
    @DisplayName("注解默认值必须是独立哨兵，且不得与任何策略值相等")
    void annotationDefault_isDistinctSentinel() {
        assertThat(ConsumeFromWhere.ANNOTATION_DEFAULT)
                .isNotEqualTo(ConsumeFromWhere.CONSUME_FROM_LAST)
                .isNotEqualTo(ConsumeFromWhere.CONSUME_FROM_FIRST);
        assertThat(ConsumeFromWhere.DEFAULT).isEqualTo(ConsumeFromWhere.CONSUME_FROM_LAST);
    }
}
