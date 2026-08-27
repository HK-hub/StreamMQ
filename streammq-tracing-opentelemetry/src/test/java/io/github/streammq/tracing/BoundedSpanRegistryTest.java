/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.tracing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * {@link BoundedSpanRegistry} 有界注册表单元测试。
 *
 * <p>验证：容量淘汰（结束最旧 Span）、同键覆盖（结束被替换 Span）、移除语义与清空兜底。
 */
@DisplayName("有界 Span 注册表测试")
class BoundedSpanRegistryTest {

    /** 构造一个统计 end() 调用次数的假 Span。 */
    private static Span countingSpan(AtomicInteger endCounter) {
        Span span = mock(Span.class);
        doAnswer(inv -> endCounter.incrementAndGet() > 0).when(span).end();
        return span;
    }

    @Test
    @DisplayName("容量超限时按插入序淘汰最旧条目并结束其 Span")
    void evictionEndsEldestSpan() {
        BoundedSpanRegistry registry = new BoundedSpanRegistry(2);
        AtomicInteger ends = new AtomicInteger();
        Span s1 = countingSpan(ends);
        Span s2 = countingSpan(ends);
        Span s3 = countingSpan(ends);

        registry.track("k1", s1, null);
        registry.track("k2", s2, null);
        assertThat(registry.size()).isEqualTo(2);

        registry.track("k3", s3, null);

        assertThat(registry.size()).isEqualTo(2);
        assertThat(ends.get()).isEqualTo(1);
        verify(s1, Mockito.atLeastOnce()).end();

        BoundedSpanRegistry.Entry entry = registry.remove("k3");
        assertThat(entry.span()).isSameAs(s3);
    }

    @Test
    @DisplayName("同键重复登记时替换并结束旧 Span，保持不泄漏")
    void overwriteEndsReplacedSpan() {
        BoundedSpanRegistry registry = new BoundedSpanRegistry(4);
        AtomicInteger ends = new AtomicInteger();
        Span oldSpan = countingSpan(ends);
        Scope oldScope = mock(Scope.class);
        Span newSpan = countingSpan(ends);

        registry.track("dup", oldSpan, oldScope);
        registry.track("dup", newSpan, null);

        assertThat(registry.size()).isEqualTo(1);
        assertThat(ends.get()).isEqualTo(1);
        verify(oldScope).close();
        assertThat(registry.remove("dup").span()).isSameAs(newSpan);
    }

    @Test
    @DisplayName("remove 未命中返回 null；clear 结束所有剩余 Span")
    void removeMissAndClearEndAll() {
        BoundedSpanRegistry registry = new BoundedSpanRegistry(4);
        AtomicInteger ends = new AtomicInteger();
        Span a = countingSpan(ends);
        Span b = countingSpan(ends);
        registry.track("a", a, null);
        registry.track("b", b, null);

        assertThat(registry.remove("missing")).isNull();

        registry.clear();
        assertThat(registry.size()).isZero();
        assertThat(ends.get()).isEqualTo(2);
    }

    @Test
    @DisplayName("capacity 必须为正数")
    void capacityMustBePositive() {
        assertThatThrownBy(() -> new BoundedSpanRegistry(0))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
