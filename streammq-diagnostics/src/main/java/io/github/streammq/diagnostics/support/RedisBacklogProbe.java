package io.github.streammq.diagnostics.support;

import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import io.github.streammq.diagnostics.StreamMQDiagnosticsDefaults;
import io.github.streammq.diagnostics.spi.BacklogProbe;
import java.util.Objects;
import org.redisson.api.RStream;
import org.redisson.api.RedissonClient;
import org.redisson.api.StreamMessageId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 基于 Redisson 的 {@link BacklogProbe} 默认实现。
 *
 * <p>使用 {@code XLEN}（Stream 当前条目总数）与 {@code XPENDING}（消费者组未确认消息数） 提供真实积压统计； 当 Stream / 消费者组不存在时返回
 * {@code null}，由调用方降级。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class RedisBacklogProbe implements BacklogProbe {

    private static final Logger log = LoggerFactory.getLogger(RedisBacklogProbe.class);

    private final RedissonClient redisson;
    private final String namespace;

    public RedisBacklogProbe(RedissonClient redisson, String namespace) {
        this.redisson = Objects.requireNonNull(redisson, "redisson");
        this.namespace = namespace == null ? "" : namespace;
    }

    @Override
    public Result probe(String topic, String group) {
        if (topic == null || topic.isEmpty() || group == null || group.isEmpty()) {
            return null;
        }
        try {
            String streamKey = StreamMQKeys.topicStream(namespace, topic);
            RStream<String, String> stream = redisson.getStream(streamKey);
            long streamSize = stream.size();
            long pendingCount = 0;
            try {
                pendingCount =
                        stream.listPending(
                                        group,
                                        StreamMessageId.MIN,
                                        StreamMessageId.MAX,
                                        StreamMQDiagnosticsDefaults.MAX_PENDING_QUERY_SIZE)
                                .size();
            } catch (RuntimeException ex) {
                // 消费者组不存在（NOGROUP）时 pending 视为 0
                log.debug(
                        "listPending failed for topic={}, group={}: {}",
                        topic,
                        group,
                        ex.getMessage());
            }
            return new Result(streamSize, pendingCount);
        } catch (RuntimeException ex) {
            log.warn(
                    "Backlog probe failed for topic={}, group={}: {}",
                    topic,
                    group,
                    ex.getMessage());
            return null;
        }
    }
}
