package io.github.streammq.sample.interceptor;

import io.github.streammq.core.interceptor.ProducerInterceptor;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.SendResult;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 生产者限流拦截器示例。
 *
 * <p>演示在发送前进行限流控制，每秒最多发送 100 条消息。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Component
public class RateLimitProducerInterceptor implements ProducerInterceptor {

  private static final Logger log = LoggerFactory.getLogger(RateLimitProducerInterceptor.class);

  private static final int MAX_RATE_PER_SECOND = 100;

  private final AtomicInteger counter = new AtomicInteger(0);
  private final AtomicLong lastResetTime = new AtomicLong(System.currentTimeMillis());

  @Override
  public boolean beforeSend(Message<?> message) {
    long now = System.currentTimeMillis();
    long elapsed = now - lastResetTime.get();

    if (elapsed > 1000) {
      counter.set(0);
      lastResetTime.set(now);
    }

    int currentCount = counter.incrementAndGet();
    if (currentCount > MAX_RATE_PER_SECOND) {
      log.warn(
          "Rate limit exceeded: current={}/{} for topic={}",
          currentCount,
          MAX_RATE_PER_SECOND,
          message.getTopic());
      return false;
    }

    log.debug("Rate limit check passed: current={}/{}", currentCount, MAX_RATE_PER_SECOND);
    return true;
  }

  @Override
  public void afterSend(Message<?> message, SendResult result) {}

  @Override
  public int order() {
    return 2;
  }
}
