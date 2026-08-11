package io.github.streammq.adapter.redisson.dlq;

import io.github.streammq.core.message.Message;
import io.github.streammq.core.policy.AbstractDlqFailureStrategy;
import io.github.streammq.core.policy.DlqConfig;
import io.github.streammq.core.policy.DlqFailureContext;
import io.github.streammq.core.policy.DlqFailureDecision;
import java.time.Duration;

/**
 * 策略二：有限次重试后丢弃（Limited-Retry-Drop）。
 *
 * <p>DLQ 消费失败时：
 *
 * <ul>
 *   <li>若 {@code dlqAttempts < maxDlqRetryAttempts} → 按退避延迟重试
 *   <li>否则 → 丢弃
 * </ul>
 *
 * <p>重试延迟计算：{@code min(baseDelay × multiplier^attempts, maxDelay)} 当 multiplier=1.0 时为固定延迟。
 *
 * <p>适用场景：DLQ 消息可能因临时故障（如外部服务不可用）导致消费失败， 允许有限次重试后放弃。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class LimitedRetryDlqFailureStrategy extends AbstractDlqFailureStrategy {

  public static final String STRATEGY_NAME = "limited-retry";

  public LimitedRetryDlqFailureStrategy() {
    super(DlqConfig.builder().build());
  }

  public LimitedRetryDlqFailureStrategy(DlqConfig config) {
    super(config);
  }

  @Override
  protected DlqFailureDecision doDecide(Message<?> message, DlqFailureContext context) {
    int attempts = context.dlqAttempts();
    int maxRetries = config.getMaxDlqRetryAttempts();

    if (attempts >= maxRetries) {
      log.warn(
          "DLQ retry exhausted: attempts={}/{}, dropping message (topic={})",
          attempts,
          maxRetries,
          context.originalTopic());
      return DlqFailureDecision.drop();
    }

    Duration delay = computeDelay(attempts);
    log.info(
        "DLQ retry scheduled: attempt={}/{}, delay={}ms (topic={})",
        attempts + 1,
        maxRetries,
        delay.toMillis(),
        context.originalTopic());
    return DlqFailureDecision.retry(delay);
  }

  /** 按退避计算重试延迟：{@code min(baseDelay × multiplier^attempt, maxDelay)} */
  private Duration computeDelay(int attempt) {
    long base = config.getDlqRetryDelayMs();
    double multiplier = config.getDlqRetryBackoffMultiplier();
    long delay = (long) (base * Math.pow(multiplier, attempt));
    delay = Math.min(delay, config.getDlqRetryMaxDelayMs());
    return Duration.ofMillis(Math.max(delay, 1000L)); // at least 1s
  }

  @Override
  public String name() {
    return STRATEGY_NAME;
  }
}
