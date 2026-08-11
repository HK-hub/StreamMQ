package io.github.streammq.tracing;

import io.github.streammq.core.enums.InvokeTiming;
import io.github.streammq.core.interceptor.ProducerInterceptor;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.SendResult;
import io.github.streammq.core.util.StringUtils;
import io.opentelemetry.api.trace.Span;
import java.util.Objects;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * 基于 OpenTelemetry 的生产者拦截器，在消息发送前后创建 / 结束生产者 Span。
 *
 * <p>执行顺序 {@link #order()} = {@value #ORDER}，高优先级，确保在其他业务拦截器之前注入追踪上下文。
 *
 * <p>工作流程：
 *
 * <ul>
 *   <li>{@link #beforeSend(Message)}：调用 {@link StreamMQTracing#injectProducerSpan(Message)} 创建生产者
 *       Span 并将 W3C {@code traceparent} 注入消息属性
 *   <li>{@link #afterSend(Message, SendResult)}：根据发送结果结束 Span（成功 / 失败 + 错误信息）
 *   <li>{@link #onException(Message, Exception, InvokeTiming)}：异常时以失败状态结束 Span
 * </ul>
 *
 * <p>追踪异常不影响正常发送流程：所有追踪操作均被 try/catch 包裹，失败时仅记录日志。
 *
 * @author StreamMQ Contributors
 * @since 1.0.0
 */
@Slf4j
@RequiredArgsConstructor
public class OpenTelemetryProducerInterceptor implements ProducerInterceptor {

  /** 拦截器执行顺序（高优先级，早于业务拦截器执行） */
  public static final int ORDER = -100;

  private final StreamMQTracing tracing;

  @Override
  public boolean beforeSend(Message<?> message) {
    try {
      tracing.injectProducerSpan(message);
    } catch (Exception ex) {
      log.warn("生产者追踪注入失败，不影响发送: {}", ex.getMessage());
    }
    return true;
  }

  @Override
  public void afterSend(Message<?> message, SendResult result) {
    try {
      Span span = tracing.getCurrentProducerSpan();
      if (Objects.isNull(span)) {
        return;
      }
      boolean success = Objects.nonNull(result) && result.isSuccess();
      String errorMessage = Objects.nonNull(result) ? result.getErrorMessage() : null;
      tracing.endSpan(span, success, StringUtils.isNotEmpty(errorMessage) ? errorMessage : null);
    } catch (Exception ex) {
      log.warn("生产者追踪结束失败: {}", ex.getMessage());
    } finally {
      tracing.clearCurrentProducerSpan();
    }
  }

  @Override
  public void onException(Message<?> message, Exception exception, InvokeTiming timing) {
    try {
      Span span = tracing.getCurrentProducerSpan();
      if (Objects.nonNull(span)) {
        tracing.endSpan(span, false, Objects.nonNull(exception) ? exception.getMessage() : "发送异常");
      }
    } catch (Exception ex) {
      log.warn("生产者异常追踪结束失败: {}", ex.getMessage());
    } finally {
      tracing.clearCurrentProducerSpan();
    }
  }

  @Override
  public String name() {
    return "openTelemetryProducerInterceptor";
  }

  @Override
  public int order() {
    return ORDER;
  }
}
