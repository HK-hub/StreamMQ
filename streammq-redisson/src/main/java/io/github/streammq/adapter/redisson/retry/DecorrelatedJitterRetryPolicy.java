package io.github.streammq.adapter.redisson.retry;

import io.github.streammq.core.StreamMQConstants;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.policy.RetryPolicy;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 去相关抖动重试策略（AWS 推荐）。
 *
 * <p>AWS 推荐的"去相关抖动"（Decorrelated Jitter）算法可避免同步重试导致的惊群效应。 原始有状态算法为 {@code delay = min(cap,
 * random(base, prev * 3))}，其中 prev 为上一次的延迟。
 *
 * <p>由于 {@link RetryPolicy#nextRetryDelay(int, Message)} 接口为无状态调用 （仅接收
 * reconsumeTimes，无法获取上一次延迟），本实现使用公式模拟有状态行为：
 *
 * <pre>
 *   delay = min(cap, random(base, base * 3^reconsumeTimes))
 * </pre>
 *
 * 其中 {@code base * 3^reconsumeTimes} 等价于去相关抖动在 reconsumeTimes 次迭代后的期望上界。
 *
 * <p>默认参数：
 *
 * <ul>
 *   <li>{@code base = 1s}
 *   <li>{@code cap = 2h}
 *   <li>{@code maxReconsumeTimes = 16}
 * </ul>
 *
 * <p>使用 {@link ThreadLocalRandom} 生成随机数，避免多线程竞争。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class DecorrelatedJitterRetryPolicy implements RetryPolicy {

  /** 默认初始延迟 1s */
  public static final Duration DEFAULT_BASE = Duration.ofSeconds(1);

  /** 默认最大延迟 2h */
  public static final Duration DEFAULT_CAP = Duration.ofHours(2);

  /** 默认最大重试次数 16 */
  public static final int DEFAULT_MAX_RECONSUME_TIMES =
      StreamMQConstants.DEFAULT_MAX_RECONSUME_TIMES;

  private final long baseMillis;
  private final long capMillis;
  private final int maxReconsumeTimes;

  /** 使用默认参数构造（base=1s, cap=2h, maxReconsumeTimes=16）。 */
  public DecorrelatedJitterRetryPolicy() {
    this(DEFAULT_BASE.toMillis(), DEFAULT_CAP.toMillis(), DEFAULT_MAX_RECONSUME_TIMES);
  }

  /**
   * 自定义参数构造。
   *
   * @param baseMillis 初始延迟（毫秒），必须 > 0
   * @param capMillis 最大延迟（毫秒），必须 >= baseMillis
   * @param maxReconsumeTimes 最大重试次数，必须 > 0
   */
  public DecorrelatedJitterRetryPolicy(long baseMillis, long capMillis, int maxReconsumeTimes) {
    if (baseMillis <= 0) {
      throw new IllegalArgumentException("baseMillis must be positive: " + baseMillis);
    }
    if (capMillis < baseMillis) {
      throw new IllegalArgumentException("capMillis must be >= baseMillis: " + capMillis);
    }
    if (maxReconsumeTimes <= 0) {
      throw new IllegalArgumentException(
          "maxReconsumeTimes must be positive: " + maxReconsumeTimes);
    }
    this.baseMillis = baseMillis;
    this.capMillis = capMillis;
    this.maxReconsumeTimes = maxReconsumeTimes;
  }

  @Override
  public Duration nextRetryDelay(int reconsumeTimes, Message<?> message) {
    Objects.requireNonNull(message, "message");
    if (reconsumeTimes < 0) {
      reconsumeTimes = 0;
    }
    if (reconsumeTimes >= maxReconsumeTimes) {
      return null;
    }
    double upperDouble = baseMillis * Math.pow(3.0, reconsumeTimes);
    long upper = (long) Math.min(upperDouble, capMillis);
    if (upper < 0) {
      upper = capMillis;
    }

    long high = Math.min(upper, capMillis);
    high = Math.min(high, Long.MAX_VALUE - 1);
    long delay;
    if (high <= baseMillis) {
      delay = baseMillis;
    } else {
      delay = ThreadLocalRandom.current().nextLong(baseMillis, high + 1);
    }
    return Duration.ofMillis(Math.min(delay, capMillis));
  }

  @Override
  public boolean shouldStopRetry(int reconsumeTimes, Message<?> message) {
    Objects.requireNonNull(message, "message");
    return reconsumeTimes >= maxReconsumeTimes;
  }

  /**
   * 返回初始延迟（毫秒）。
   *
   * @return 初始延迟
   */
  public long getBaseMillis() {
    return baseMillis;
  }

  /**
   * 返回最大延迟（毫秒）。
   *
   * @return 最大延迟
   */
  public long getCapMillis() {
    return capMillis;
  }

  /**
   * 返回最大重试次数。
   *
   * @return 最大重试次数
   */
  public int getMaxReconsumeTimes() {
    return maxReconsumeTimes;
  }
}
