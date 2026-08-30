/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.template;

import io.github.streammq.adapter.redisson.support.MdcKeys;
import io.github.streammq.core.enums.InvokeTiming;
import io.github.streammq.core.interceptor.ProducerInterceptor;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.SendResult;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * 生产者拦截器链与发送侧 MDC 上下文管理。
 *
 * <p>原为 {@link DefaultStreamMessageTemplate} 的拦截器字段 + 一组私有方法（before/after 链、异常
 * 通知、MDC 注入/清理），横切关注点聚合于此，模板类只保留对外的拦截器注册 API 与发送编排。
 *
 * <p>线程安全：{@link CopyOnWriteArrayList} 支持并发注册与遍历；MDC 为 {@link ThreadLocal} 语义，
 * 必须在同一线程的 finally 中配对清理。
 *
 * @author StreamMQ Contributors
 * @since 0.1.1
 */
final class ProducerInterceptorChain {

    private static final Logger LOG = LoggerFactory.getLogger(ProducerInterceptorChain.class);

    private final List<ProducerInterceptor> interceptors = new CopyOnWriteArrayList<>();
    private final String defaultGroup;

    ProducerInterceptorChain(String defaultGroup) {
        this.defaultGroup = Objects.requireNonNull(defaultGroup, "defaultGroup");
    }

    /** @return 当前拦截器不可变快照 */
    List<ProducerInterceptor> snapshot() {
        return Collections.unmodifiableList(interceptors);
    }

    /**
     * 全量替换拦截器并按 order() 升序排序。
     *
     * @param interceptors 新拦截器列表，可为 null（等价于清空）
     */
    void setAll(List<ProducerInterceptor> interceptors) {
        this.interceptors.clear();
        if (Objects.nonNull(interceptors)) {
            List<ProducerInterceptor> sorted = new ArrayList<>(interceptors);
            sorted.sort((a, b) -> Integer.compare(a.order(), b.order()));
            this.interceptors.addAll(sorted);
        }
    }

    /**
     * 按 order() 升序插入单个拦截器。
     *
     * @param interceptor 拦截器，不能为 null
     */
    void add(ProducerInterceptor interceptor) {
        Objects.requireNonNull(interceptor, "interceptor");
        // 保持按 order 升序
        int insertIndex = 0;
        for (ProducerInterceptor existing : interceptors) {
            if (existing.order() <= interceptor.order()) {
                insertIndex++;
            } else {
                break;
            }
        }
        interceptors.add(insertIndex, interceptor);
    }

    /**
     * 执行 before 拦截器链，串联各拦截器返回的派生消息。
     *
     * @param message 待发送消息
     * @param <T> body 类型
     * @return 链路末端输出的消息实例；{@code null} 表示被任一拦截器中止
     */
    @SuppressWarnings("unchecked")
    <T> Message<T> beforeSend(Message<T> message) {
        Message<?> current = message;
        for (ProducerInterceptor interceptor : interceptors) {
            try {
                current = interceptor.beforeSend(current);
                if (Objects.isNull(current)) {
                    LOG.debug(
                            "Interceptor {} aborted send: topic={}",
                            interceptor.name(),
                            message.getTopic());
                    return null;
                }
            } catch (RuntimeException ex) {
                LOG.warn(
                        "Interceptor {} beforeSend threw exception: {}",
                        interceptor.name(),
                        ex.getMessage(),
                        ex);
                notifyException(message, ex, InvokeTiming.BEFORE);
                return null;
            }
        }
        return (Message<T>) current;
    }

    /**
     * 执行 after 拦截器链。
     *
     * @param message 已发送消息
     * @param result 发送结果
     */
    void afterSend(Message<?> message, SendResult result) {
        for (ProducerInterceptor interceptor : interceptors) {
            try {
                interceptor.afterSend(message, result);
            } catch (RuntimeException ex) {
                LOG.warn(
                        "Interceptor {} afterSend threw exception: {}",
                        interceptor.name(),
                        ex.getMessage(),
                        ex);
                notifyException(message, ex, InvokeTiming.AFTER);
            }
        }
    }

    /**
     * 通知所有生产者拦截器发生异常（按 order() 升序）。
     *
     * <p>拦截器自身的 onException 异常被忽略，不影响主流程。
     *
     * @param message 消息
     * @param ex 异常
     * @param timing 触发时机（BEFORE/EXECUTING/AFTER）
     */
    void notifyException(Message<?> message, Exception ex, InvokeTiming timing) {
        for (ProducerInterceptor interceptor : interceptors) {
            try {
                interceptor.onException(message, ex, timing);
            } catch (Exception ignored) {
                // 拦截器异常不应影响主流程
                LOG.debug("Interceptor exception", ignored);
            }
        }
    }

    /**
     * 注入发送侧 MDC 结构化日志上下文。
     *
     * @param message 待发送消息
     */
    void injectMdc(Message<?> message) {
        MDC.put(MdcKeys.TOPIC, message.getTopic());
        MDC.put(MdcKeys.PRODUCER_GROUP, defaultGroup);
        if (Objects.nonNull(message.getMessageId())) {
            MDC.put(MdcKeys.MSG_ID, String.valueOf(message.getMessageId()));
        }
        if (Objects.nonNull(message.getShardingKey())) {
            MDC.put(MdcKeys.SHARDING_KEY, message.getShardingKey());
        }
    }

    /** 清理发送侧 MDC 结构化日志上下文。 */
    void clearMdc() {
        MDC.remove(MdcKeys.TOPIC);
        MDC.remove(MdcKeys.PRODUCER_GROUP);
        MDC.remove(MdcKeys.MSG_ID);
        MDC.remove(MdcKeys.SHARDING_KEY);
    }
}
