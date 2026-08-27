/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.tracing;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 有界 Span 注册表：以字符串键跟踪进行中的 {@link Span}（及可选的 {@link Scope}）。
 *
 * <p>解决跨线程异步回调场景下 ThreadLocal 配对失效导致的 Span 泄漏问题：发送与完成回调可能运行在 不同线程，因此将 Span 与稳定键绑定。注册表容量有限（默认
 * 4096），超出时按插入顺序淘汰最旧 条目并对其调用 {@link Span#end()}， 防止未正常结束的 Span 无限累积；同键重复登记时结束被替换的旧 Span，同样避免泄漏。
 *
 * <p>实现说明：内部使用 {@link LinkedHashMap} 同时承担「键 → 条目」查找（{@code remove} 为 O(1)） 与插入序淘汰队列两个职责，替代此前
 * ArrayDeque + HashMap 的组合（其 {@code remove} 需 O(n) 扫描）。
 *
 * <p>线程安全：所有操作在内部锁保护下执行。淘汰 / 替换 / 清空时仅结束 Span 本身； Scope 属于创建它的线程上下文栈，跨线程关闭不安全，因此由调用方保证 Scope
 * 在原线程关闭。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
final class BoundedSpanRegistry {

    /** 注册表条目：Span 与可选的当前作用域 */
    record Entry(Span span, Scope scope) {}

    private final int capacity;

    /** 插入序 Map：兼做查找表与淘汰队列 */
    private final Map<String, Entry> entries;

    BoundedSpanRegistry(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be positive: " + capacity);
        }
        this.capacity = capacity;
        this.entries = new LinkedHashMap<>(Math.min(1024, capacity));
    }

    /**
     * 登记键对应的 Span；若键已存在则替换并结束被替换的旧 Span（保持其原始插入位置）。
     *
     * <p>容量已满且为新键时，按插入顺序淘汰最旧条目并对其实施 {@link Span#end()}。
     *
     * @param key 稳定标识键
     * @param span 进行中的 Span
     * @param scope 可选作用域（可为 null；由调用方负责在其创建线程关闭）
     */
    synchronized void track(String key, Span span, Scope scope) {
        Entry previous = entries.get(key);
        if (previous != null) {
            endQuietly(previous.span());
            closeQuietly(previous.scope());
            entries.put(key, new Entry(span, scope));
            return;
        }
        while (entries.size() >= capacity) {
            String eldestKey = entries.keySet().iterator().next();
            Entry eldest = entries.remove(eldestKey);
            if (eldest != null) {
                endQuietly(eldest.span());
                closeQuietly(eldest.scope());
            }
        }
        entries.put(key, new Entry(span, scope));
    }

    /**
     * 移除并返回键对应的条目。
     *
     * @param key 稳定标识键
     * @return 关联条目；不存在时返回 null。移除后 Span 的结束与 Scope 的关闭由调用方负责
     */
    synchronized Entry remove(String key) {
        return entries.remove(key);
    }

    /** 清空注册表并结束其中所有 Span（用于容器销毁等兜底场景），防止进程退出前 Span 悬挂。 */
    synchronized void clear() {
        for (Entry entry : entries.values()) {
            endQuietly(entry.span());
            closeQuietly(entry.scope());
        }
        entries.clear();
    }

    /**
     * 当前登记条目数。
     *
     * @return 条目数
     */
    synchronized int size() {
        return entries.size();
    }

    private static void endQuietly(Span span) {
        try {
            if (span != null) {
                span.end();
            }
        } catch (RuntimeException ignored) {
            // 兜底清理路径失败不影响主流程
        }
    }

    private static void closeQuietly(Scope scope) {
        try {
            if (scope != null) {
                scope.close();
            }
        } catch (RuntimeException ignored) {
            // 跨线程或重复关闭场景下的安全忽略
        }
    }
}
