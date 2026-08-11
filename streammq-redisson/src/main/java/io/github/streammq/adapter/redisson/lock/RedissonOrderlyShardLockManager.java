package io.github.streammq.adapter.redisson.lock;

import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import io.github.streammq.core.consumer.ConsumeOrderlyContext;
import io.github.streammq.core.consumer.StreamMessageOrderlyConsumer;
import io.github.streammq.core.enums.ConsumeAction;
import io.github.streammq.core.listener.ListenerRegistration;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.policy.OrderlyShardLockManager;
import io.github.streammq.core.util.StringUtils;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;

/**
 * 顺序消费分片锁管理器默认实现（策略类，基于 Redisson）。
 *
 * <p>负责为顺序消费 Consumer 创建 shard 级分布式锁，并在消费时按 shardingKey 路由到对应 shard 加锁执行， 保证同一 shardingKey
 * 的消息串行消费，不同 shard 之间可并行。
 *
 * <p>设计模式：策略模式，将顺序消费的锁逻辑从容器中分离。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@RequiredArgsConstructor
public class RedissonOrderlyShardLockManager implements OrderlyShardLockManager {

  @NonNull private final RedissonClient redisson;

  /**
   * 为顺序消费 Consumer 创建 shard 级分布式锁数组。
   *
   * @param defaultNs 默认命名空间
   * @param topic 主题
   * @param group 消费组
   * @param ns 注解指定的命名空间（可为空）
   * @param shardCount 分片数
   * @return RLock 数组，shardCount &lt;= 0 时返回 null
   */
  @Override
  public RLock[] createShardLocks(
      String defaultNs, String topic, String group, String ns, int shardCount) {
    if (shardCount <= 0) {
      return null;
    }
    String namespace = StringUtils.isEmpty(ns) ? defaultNs : ns;
    RLock[] locks = new RLock[shardCount];
    for (int i = 0; i < shardCount; i++) {
      String lockKey = StreamMQKeys.shardLock(namespace, topic, group, i);
      locks[i] = redisson.getLock(lockKey);
    }
    return locks;
  }

  /**
   * 按 shardingKey 路由到对应 shard 加锁后执行顺序消费。
   *
   * <p>无分片锁时直接消费（shardCount &lt;= 0 场景）。
   *
   * @param message 待消费消息
   * @param reg Listener 注册信息
   * @param ctx 顺序消费上下文
   * @param orderly 顺序消费 Consumer
   * @return 消费动作
   * @throws Exception Listener 抛出的异常
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  @Override
  public ConsumeAction consumeWithShardLock(
      Message<?> message,
      ListenerRegistration reg,
      ConsumeOrderlyContext ctx,
      StreamMessageOrderlyConsumer orderly)
      throws Exception {
    if (Objects.isNull(reg.getShardLocks()) || reg.getShardCount() <= 0) {
      return orderly.onMessage(message, ctx);
    }
    String shardingKey = message.getShardingKey();
    if (Objects.isNull(shardingKey)) {
      shardingKey = "";
    }
    int shardIndex = Math.abs(shardingKey.hashCode()) % reg.getShardCount();
    RLock lock = (RLock) reg.getShardLocks().get(shardIndex);
    try {
      lock.lock(reg.getConsumeTimeoutMillis(), TimeUnit.MILLISECONDS);
      return orderly.onMessage(message, ctx);
    } finally {
      if (lock.isHeldByCurrentThread()) {
        lock.unlock();
      }
    }
  }
}
