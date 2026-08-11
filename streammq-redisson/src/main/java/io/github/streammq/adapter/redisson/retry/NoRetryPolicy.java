package io.github.streammq.adapter.redisson.retry;

import io.github.streammq.core.message.Message;
import io.github.streammq.core.policy.RetryPolicy;
import java.time.Duration;

/**
 * 不重试策略。
 *
 * <p>{@link #nextRetryDelay(int, Message)} 始终返回 null（不再重试）， {@link #shouldStopRetry(int, Message)}
 * 始终返回 true。
 *
 * <p>适用于消费失败后立即进入 DLQ、不希望重试的场景。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class NoRetryPolicy implements RetryPolicy {

  @Override
  public Duration nextRetryDelay(int reconsumeTimes, Message<?> message) {
    return null;
  }

  @Override
  public boolean shouldStopRetry(int reconsumeTimes, Message<?> message) {
    return true;
  }

  @Override
  public String name() {
    return "no-retry";
  }
}
