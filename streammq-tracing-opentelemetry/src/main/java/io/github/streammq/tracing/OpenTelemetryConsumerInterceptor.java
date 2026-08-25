/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.tracing;

import io.github.streammq.core.consumer.ConsumeContext;
import io.github.streammq.core.enums.ConsumeAction;
import io.github.streammq.core.enums.InvokeTiming;
import io.github.streammq.core.interceptor.ConsumerInterceptor;
import io.github.streammq.core.message.Message;
import io.opentelemetry.api.trace.Span;
import java.util.Objects;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * 基于 OpenTelemetry 的消费者拦截器，在消息消费前后创建 / 结束消费者 Span。
 *
 * <p>执行顺序 {@link #order()} = {@value #ORDER}，高优先级，确保在其他业务拦截器之前提取追踪上下文。
 *
 * <p>工作流程：
 *
 * <ul>
 *   <li>{@link #beforeConsume(Message, ConsumeContext)}：调用 {@link
 *       StreamMQTracing#startConsumerSpan(Message, ConsumeContext)} 提取远程父级上下文并启动消费者 Span， 存入线程局部变量
 *   <li>{@link #afterConsume(Message, ConsumeAction, ConsumeContext)}：根据消费结果结束 Span
 *   <li>{@link #onException(Message, Exception, InvokeTiming, ConsumeContext)}：异常时以失败状态结束 Span
 * </ul>
 *
 * <p>追踪异常不影响正常消费流程：所有追踪操作均被 try/catch 包裹，失败时仅记录日志。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Slf4j
@RequiredArgsConstructor
public class OpenTelemetryConsumerInterceptor implements ConsumerInterceptor {

    /** 拦截器执行顺序（高优先级，早于业务拦截器执行） */
    public static final int ORDER = -100;

    /** 拦截器标识名 */
    public static final String NAME = "openTelemetryConsumerInterceptor";

    private final StreamMQTracing tracing;

    /** 当前线程的消费者 Span */
    private final ThreadLocal<Span> currentSpan = new ThreadLocal<>();

    /** 当前线程的 Span 作用域（makeCurrent 返回），after/onException 时关闭 */
    private final ThreadLocal<io.opentelemetry.context.Scope> currentScope = new ThreadLocal<>();

    @Override
    public boolean beforeConsume(Message<?> message, ConsumeContext context) {
        try {
            Span span = tracing.startConsumerSpan(message, context);
            // makeCurrent：让 CONSUMER Span 成为当前上下文。否则业务代码中的
            // Span.current() / 其他 OTel 自动探针不会挂到本消费 Span 之下，链路会断裂
            currentScope.set(span.makeCurrent());
            currentSpan.set(span);
        } catch (Exception ex) {
            log.warn("消费者追踪启动失败，不影响消费: {}", ex.getMessage());
        }
        return true;
    }

    @Override
    public void afterConsume(Message<?> message, ConsumeAction action, ConsumeContext context) {
        try {
            Span span = currentSpan.get();
            if (Objects.nonNull(span)) {
                boolean success = Objects.nonNull(action) && action.isSuccess();
                tracing.endSpan(span, success);
            }
        } catch (Exception ex) {
            log.warn("消费者追踪结束失败: {}", ex.getMessage());
        } finally {
            closeScope();
            currentSpan.remove();
        }
    }

    @Override
    public void onException(
            Message<?> message, Exception exception, InvokeTiming timing, ConsumeContext context) {
        try {
            Span span = currentSpan.get();
            if (Objects.nonNull(span)) {
                tracing.endSpan(
                        span, false, Objects.nonNull(exception) ? exception.getMessage() : "消费异常");
            }
        } catch (Exception ex) {
            log.warn("消费者异常追踪结束失败: {}", ex.getMessage());
        } finally {
            closeScope();
            currentSpan.remove();
        }
    }

    private void closeScope() {
        try {
            io.opentelemetry.context.Scope scope = currentScope.get();
            if (Objects.nonNull(scope)) {
                scope.close();
                currentScope.remove();
            }
        } catch (Exception ex) {
            log.debug("关闭追踪作用域失败: {}", ex.getMessage());
        }
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public int order() {
        return ORDER;
    }
}
