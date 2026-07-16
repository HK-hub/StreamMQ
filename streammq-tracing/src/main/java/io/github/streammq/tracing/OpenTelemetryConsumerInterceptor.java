package io.github.streammq.tracing;

import io.github.streammq.core.consumer.ConsumeContext;
import io.github.streammq.core.enums.ConsumeAction;
import io.github.streammq.core.enums.InvokeTiming;
import io.github.streammq.core.interceptor.ConsumerInterceptor;
import io.github.streammq.core.message.Message;
import io.opentelemetry.api.trace.Span;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Objects;

/**
 * 基于 OpenTelemetry 的消费者拦截器，在消息消费前后创建 / 结束消费者 Span。
 *
 * <p>执行顺序 {@link #order()} = {@value #ORDER}，高优先级，确保在其他业务拦截器之前提取追踪上下文。
 *
 * <p>工作流程：
 * <ul>
 *   <li>{@link #beforeConsume(Message, ConsumeContext)}：调用
 *       {@link StreamMQTracing#startConsumerSpan(Message, ConsumeContext)} 提取远程父级上下文并启动消费者 Span，
 *       存入线程局部变量</li>
 *   <li>{@link #afterConsume(Message, ConsumeAction, ConsumeContext)}：根据消费结果结束 Span</li>
 *   <li>{@link #onException(Message, Exception, InvokeTiming, ConsumeContext)}：异常时以失败状态结束 Span</li>
 * </ul>
 *
 * <p>追踪异常不影响正常消费流程：所有追踪操作均被 try/catch 包裹，失败时仅记录日志。
 *
 * @author StreamMQ Contributors
 * @since 1.0.0
 */
@Slf4j
@RequiredArgsConstructor
public class OpenTelemetryConsumerInterceptor implements ConsumerInterceptor {

    /** 拦截器执行顺序（高优先级，早于业务拦截器执行） */
    public static final int ORDER = -100;

    private final StreamMQTracing tracing;

    /** 当前线程的消费者 Span */
    private final ThreadLocal<Span> currentSpan = new ThreadLocal<>();

    @Override
    public boolean beforeConsume(Message<?> message, ConsumeContext context) {
        try {
            Span span = tracing.startConsumerSpan(message, context);
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
            currentSpan.remove();
        }
    }

    @Override
    public void onException(Message<?> message, Exception exception, InvokeTiming timing, ConsumeContext context) {
        try {
            Span span = currentSpan.get();
            if (Objects.nonNull(span)) {
                tracing.endSpan(span, false, Objects.nonNull(exception) ? exception.getMessage() : "消费异常");
            }
        } catch (Exception ex) {
            log.warn("消费者异常追踪结束失败: {}", ex.getMessage());
        } finally {
            currentSpan.remove();
        }
    }

    @Override
    public String name() {
        return "openTelemetryConsumerInterceptor";
    }

    @Override
    public int order() {
        return ORDER;
    }
}
