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
import io.opentelemetry.context.Scope;
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
 *       StreamMQTracing#startConsumerSpan(Message, ConsumeContext)} 提取远程父级上下文并启动消费者 Span， 以「消息
 *       ID」为键登记到有界注册表，并在当前线程建立 Span 作用域
 *   <li>{@link #afterConsume(Message, ConsumeAction, ConsumeContext)}：按同一消息 ID 取回条目，关闭作用域并根据消费结果结束
 *       Span
 *   <li>{@link #onException(Message, Exception, InvokeTiming, ConsumeContext)}：异常时以失败状态结束 Span
 * </ul>
 *
 * <p><b>为何不用 ThreadLocal 配对：</b>容器在消费超时路径上会把 {@code onMessage} 调度到独立线程执行 （before / after
 * 钩子仍留在调度线程），ThreadLocal 在线程跳变场景下配对失效且会泄漏未结束的 Span。 消费回调各阶段接收的是<b>同一 Message 实例</b>，其消息 ID（broker
 * 分配后不变）是稳定键， 因此与生产者侧一致采用有界注册表配对；注册表容量有界，超限淘汰时会自动结束最旧 Span。 注意：Scope 与创建它的线程绑定，正常流程中 before / after
 * 钩子运行于同一线程，作用域在同线程内关闭； 若未来出现跨线程的 after 回调，注册表仍保证 Span 不泄漏（作用域关闭会被安全忽略）。
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

    /** 消费者 Span 注册表：以消息 ID 为键跨线程配对，容量有界防泄漏 */
    private final BoundedSpanRegistry consumerSpans =
            new BoundedSpanRegistry(StreamMQTracing.SPAN_REGISTRY_CAPACITY);

    @Override
    public boolean beforeConsume(Message<?> message, ConsumeContext context) {
        try {
            Span span = tracing.startConsumerSpan(message, context);
            // makeCurrent：让 CONSUMER Span 成为当前上下文。否则业务代码中的
            // Span.current() / 其他 OTel 自动探针不会挂到本消费 Span 之下，链路会断裂
            Scope scope = null;
            try {
                scope = span.makeCurrent();
            } catch (Exception scopeEx) {
                log.warn("消费者追踪作用域建立失败，仅保留 Span: {}", scopeEx.getMessage());
            }
            consumerSpans.track(consumerKey(message), span, scope);
        } catch (Exception ex) {
            log.warn("消费者追踪启动失败，不影响消费: {}", ex.getMessage());
        }
        return true;
    }

    @Override
    public void afterConsume(Message<?> message, ConsumeAction action, ConsumeContext context) {
        BoundedSpanRegistry.Entry entry = removeTracked(message);
        if (Objects.isNull(entry)) {
            return;
        }
        try {
            boolean success = Objects.nonNull(action) && action.isSuccess();
            tracing.endSpan(entry.span(), success);
        } catch (Exception ex) {
            log.warn("消费者追踪结束失败: {}", ex.getMessage());
        } finally {
            closeScope(entry.scope());
        }
    }

    @Override
    public void onException(
            Message<?> message, Exception exception, InvokeTiming timing, ConsumeContext context) {
        BoundedSpanRegistry.Entry entry = removeTracked(message);
        if (Objects.isNull(entry)) {
            return;
        }
        try {
            tracing.endSpan(
                    entry.span(),
                    false,
                    Objects.nonNull(exception) ? exception.getMessage() : "消费异常");
        } catch (Exception ex) {
            log.warn("消费者异常追踪结束失败: {}", ex.getMessage());
        } finally {
            closeScope(entry.scope());
        }
    }

    /** 按消息键移除已登记的追踪条目；消息为 null 或未登记时返回 null。 */
    private BoundedSpanRegistry.Entry removeTracked(Message<?> message) {
        if (Objects.isNull(message)) {
            return null;
        }
        return consumerSpans.remove(consumerKey(message));
    }

    /**
     * 计算消费者 Span 的配对键。
     *
     * <p>优先使用 broker 分配的消息 ID（同一投递周期内稳定）；无消息 ID 时退化为对象标识哈希， 保证同一实例的 before / after 仍可配对。
     */
    private static String consumerKey(Message<?> message) {
        Object messageId = message.getMessageId();
        return Objects.nonNull(messageId)
                ? "mid:" + messageId
                : "ref:" + System.identityHashCode(message);
    }

    private void closeScope(Scope scope) {
        if (Objects.isNull(scope)) {
            return;
        }
        try {
            scope.close();
        } catch (Exception ex) {
            log.debug("关闭追踪作用域失败: {}", Objects.nonNull(ex) ? ex.getMessage() : "");
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
