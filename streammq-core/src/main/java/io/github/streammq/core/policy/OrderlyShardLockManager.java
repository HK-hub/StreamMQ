package io.github.streammq.core.policy;

import io.github.streammq.core.consumer.ConsumeOrderlyContext;
import io.github.streammq.core.consumer.StreamMessageOrderlyConsumer;
import io.github.streammq.core.enums.ConsumeAction;
import io.github.streammq.core.listener.ListenerRegistration;
import io.github.streammq.core.message.Message;
import org.redisson.api.RLock;

/**
 * 顺序消费分片锁管理器策略接口。
 *
 * <p>负责为顺序消费 Consumer 创建 shard 级分布式锁，并在消费时按 shardingKey 路由到对应 shard 加锁执行，
 * 保证同一 shardingKey 的消息串行消费，不同 shard 之间可并行。
 *
 * <p>设计模式：策略模式，将顺序消费的锁逻辑从容器中分离。
 * 默认实现（基于 Redisson {@link RLock}）位于 {@code streammq-redisson-adapter} 模块，
 * 可通过容器构造器注入自定义实现。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface OrderlyShardLockManager {

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
    RLock[] createShardLocks(String defaultNs, String topic, String group, String ns, int shardCount);

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
    ConsumeAction consumeWithShardLock(Message<?> message, ListenerRegistration reg,
                                       ConsumeOrderlyContext ctx, StreamMessageOrderlyConsumer orderly) throws Exception;
}
