/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.container;

import io.github.streammq.adapter.redisson.support.MdcKeys;
import io.github.streammq.core.listener.ListenerRegistration;
import io.github.streammq.core.message.Message;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import org.slf4j.MDC;

/**
 * 消费侧 MDC 结构化日志上下文管理（策略类）。
 *
 * <p>在 {@code handleMessage} 入口注入 topic、consumerGroup、messageId、shardingKey、reconsumeTimes 到 SLF4J
 * {@link MDC}，出口清理，使消费日志可通过 MDC 占位符输出结构化上下文。
 *
 * <p><b>跨线程重放（R4-x4 修复）：</b>上述 {@code inject/clear} 只在执行 {@code inject} 的那个线程（读循环线程）内可见。消费超时保护会把
 * {@code onMessage} 提交到 <b>另一个线程</b>（{@code DefaultMessageProcessor#processWithTimeout} 的 {@code
 * executor.submit}，虚拟线程/线程池）执行——新线程看不到调用方的 MDC，超时路径里的业务日志会丢 topic/group/msgId 等上下文。为此本类提供「快照 +
 * 在新线程内重放」工具：
 *
 * <ul>
 *   <li>{@link #snapshot()}：抓取当前线程的消费侧 MDC 快照（仅本类管理的 5 个键）；
 *   <li>{@link #replay(Map)}：把快照写入当前线程；
 *   <li>{@link #wrap(Runnable)} / {@link #wrap(Callable)}：推荐入口——在提交任务的线程上抓快照，
 *       在任务实际执行的新线程内重放，并在任务结束后清理（线程池场景避免上下文串味）。
 * </ul>
 *
 * <p>典型用法（提交侧）：{@code executor.submit(ConsumerMdcTrace.wrap(() -> { ... return action; }))}。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public final class ConsumerMdcTrace {

    /** 本类管理的 MDC 键全集（顺序固定：快照/重放/清理三处共用，保证语义一致）。 */
    private static final String[] CONSUMER_MDC_KEYS = {
        MdcKeys.TOPIC,
        MdcKeys.CONSUMER_GROUP,
        MdcKeys.MSG_ID,
        MdcKeys.SHARDING_KEY,
        MdcKeys.RECONSUME_TIMES
    };

    private ConsumerMdcTrace() {}

    /**
     * 注入消费侧 MDC 上下文。
     *
     * @param message 待消费消息
     * @param reg Listener 注册信息
     */
    public static void inject(Message<?> message, ListenerRegistration<?> reg) {
        MDC.put(MdcKeys.TOPIC, reg.getTopic());
        MDC.put(MdcKeys.CONSUMER_GROUP, reg.getGroup());
        if (Objects.nonNull(message.getMessageId())) {
            MDC.put(MdcKeys.MSG_ID, String.valueOf(message.getMessageId()));
        }
        if (Objects.nonNull(message.getShardingKey())) {
            MDC.put(MdcKeys.SHARDING_KEY, message.getShardingKey());
        }
        MDC.put(MdcKeys.RECONSUME_TIMES, String.valueOf(message.getReconsumeTimes()));
    }

    /**
     * 快照当前线程的消费侧 MDC 上下文。
     *
     * <p>只抓取本类管理的 5 个键（{@code topic} / {@code consumerGroup} / {@code msgId} / {@code shardingKey}
     * / {@code reconsumeTimes}）：这些是消费上下文的所有者； 其它键（如链路追踪的 {@code
     * traceId}）生命周期归各自的组件，不做隐式搬运，避免把不属于本类的 上下文泄漏到其它任务的线程上。缺失的键不会出现在快照里。
     *
     * @return 不可变的 MDC 快照（可能为空 Map，永不为 null）
     */
    public static Map<String, String> snapshot() {
        Map<String, String> snapshot = new LinkedHashMap<>(CONSUMER_MDC_KEYS.length);
        for (String key : CONSUMER_MDC_KEYS) {
            String value = MDC.get(key);
            if (Objects.nonNull(value)) {
                snapshot.put(key, value);
            }
        }
        return snapshot.isEmpty() ? Map.of() : Collections.unmodifiableMap(snapshot);
    }

    /**
     * 在当前线程内重放消费侧 MDC 快照（{@link #snapshot()} 的产物）。
     *
     * @param snapshot 快照；{@code null} 或空表示无上下文，直接返回（no-op）
     */
    public static void replay(Map<String, String> snapshot) {
        if (Objects.isNull(snapshot) || snapshot.isEmpty()) {
            return;
        }
        snapshot.forEach(
                (key, value) -> {
                    if (Objects.nonNull(value)) {
                        MDC.put(key, value);
                    }
                });
    }

    /**
     * 包装一个无返回值任务：调用方线程抓取 MDC 快照，任务在新线程执行时重放，执行结束后清理。
     *
     * <p>快照在 <b>调用 wrap 的线程</b>上抓取（通常已执行过 {@link #inject}）， 因此必须先把本方法调用结果提交给执行器，而不是在任务体内再抓取。
     *
     * @param task 待执行任务
     * @return 已绑定 MDC 重放/清理语义的包装任务
     */
    public static Runnable wrap(Runnable task) {
        Objects.requireNonNull(task, "task");
        Map<String, String> snapshot = snapshot();
        return () -> {
            replay(snapshot);
            try {
                task.run();
            } finally {
                // 执行线程（虚拟线程/池化线程）可能被复用：必须清理，避免上下文串给下一个任务
                clear();
            }
        };
    }

    /**
     * 包装一个有返回值任务（{@link java.util.concurrent.ExecutorService#submit(Callable)} 路径， 如消费超时包装的 {@code
     * Future<ConsumeAction>}）：语义同 {@link #wrap(Runnable)}。
     *
     * @param task 待执行任务
     * @param <T> 返回值类型
     * @return 已绑定 MDC 重放/清理语义的包装任务
     */
    public static <T> Callable<T> wrap(Callable<T> task) {
        Objects.requireNonNull(task, "task");
        Map<String, String> snapshot = snapshot();
        return () -> {
            replay(snapshot);
            try {
                return task.call();
            } finally {
                clear();
            }
        };
    }

    /** 清理消费侧 MDC 上下文（本类管理的全部键）。 */
    public static void clear() {
        for (String key : CONSUMER_MDC_KEYS) {
            MDC.remove(key);
        }
    }
}
