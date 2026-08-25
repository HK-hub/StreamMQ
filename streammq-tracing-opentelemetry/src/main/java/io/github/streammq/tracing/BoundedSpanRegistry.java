/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.tracing;

import io.opentelemetry.api.trace.Span;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;

/**
 * 有界 Span 注册表：以消息实例为键跟踪进行中的 {@link Span}。
 *
 * <p>解决跨线程异步回调场景下 ThreadLocal 配对失效导致的 Span 泄漏问题：发送与完成回调可能运行在 不同线程，因此将 Span 与消息实例绑定。注册表容量有限（默认
 * 4096），超出时按插入顺序淘汰最旧 条目，防止未正常结束的 Span 无限累积。
 *
 * <p>线程安全：所有操作在内部锁保护下执行。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
final class BoundedSpanRegistry {

    private final int capacity;

    /** 插入顺序队列，用于按插入序淘汰 */
    private final ArrayDeque<Object> order;

    /** 消息 -> Span 映射 */
    private final Map<Object, Span> spans;

    BoundedSpanRegistry(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be positive: " + capacity);
        }
        this.capacity = capacity;
        this.order = new ArrayDeque<>(capacity);
        this.spans = new HashMap<>(capacity);
    }

    /**
     * 登记消息对应的 Span；若消息已存在则覆盖，并保持其原始插入顺序位置。
     *
     * @param messageKey 消息实例（标识键）
     * @param span 进行中的 Span
     */
    synchronized void track(Object messageKey, Span span) {
        if (spans.containsKey(messageKey)) {
            spans.put(messageKey, span);
            return;
        }
        if (order.size() >= capacity) {
            Object eldest = order.pollFirst();
            if (eldest != null) {
                spans.remove(eldest);
            }
        }
        order.addLast(messageKey);
        spans.put(messageKey, span);
    }

    /**
     * 移除并返回消息对应的 Span。
     *
     * @param messageKey 消息实例（标识键）
     * @return 关联的 Span；不存在时返回 null
     */
    synchronized Span remove(Object messageKey) {
        Span span = spans.remove(messageKey);
        if (span != null) {
            order.remove(messageKey);
        }
        return span;
    }

    /**
     * 当前登记条目数。
     *
     * @return 条目数
     */
    synchronized int size() {
        return spans.size();
    }
}
