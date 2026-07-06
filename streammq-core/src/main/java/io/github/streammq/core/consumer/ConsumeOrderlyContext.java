package io.github.streammq.core.consumer;

import io.github.streammq.core.message.MessageId;

/**
 * 顺序消费上下文，扩展 {@link ConsumeContext} 增加分片信息。
 *
 * <p>仅在顺序消费场景下使用，提供当前消息所属 shard 的详细信息。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface ConsumeOrderlyContext extends ConsumeContext {

    /**
     * 返回当前消息的分片键。
     *
     * @return 分片键
     */
    String shardingKey();

    /**
     * 返回当前消息所属的 shard ID。
     *
     * @return shard ID（0-based）
     */
    int shardId();

    /**
     * 返回当前 shard 在本消费者上的消费位点（Stream Entry ID）。
     *
     * @return 当前消费位点
     */
    MessageId queueOffset();

    /**
     * 返回当前 shard 的最大堆积量。
     *
     * @return 堆积量
     */
    long backlog();
}
