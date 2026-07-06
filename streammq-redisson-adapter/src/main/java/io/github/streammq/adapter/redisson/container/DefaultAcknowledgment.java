package io.github.streammq.adapter.redisson.container;

import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import io.github.streammq.core.consumer.Acknowledgment;
import io.github.streammq.core.enums.NackRetryMode;
import io.github.streammq.core.listener.ListenerRegistration;
import io.github.streammq.core.listener.StreamMQListener;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageId;
import io.github.streammq.core.policy.RetryAndDlqHandler;
import lombok.RequiredArgsConstructor;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * 默认 {@link Acknowledgment} 实现。
 *
 * <p>由 {@link DefaultConsumeContextConsume#acknowledge()} 创建，封装消息确认逻辑：
 * <ul>
 *   <li>{@link #acknowledge()} - ACK 消息（从 PEL 移除），标记 context 已 ack，清理 nack 计数</li>
 *   <li>{@link #nack()} - 根据 {@link NackRetryMode} 分支：
 *     <ul>
 *       <li>RETRY_ZSET：主动写入 retry ZSet + ACK 原消息（复用 {@link RetryAndDlqHandler#handleReconsumeLater}）</li>
 *       <li>STREAM_AUTO：不 ACK 留 PEL，用 Redis Hash 记录 nack 次数；
 *           超过 fastRetryCount 后根据 fallbackToRetryZset 决定是否转入 RETRY_ZSET</li>
 *     </ul>
 *   </li>
 *   <li>{@link #defer(Duration)} - 调用 {@link RetryAndDlqHandler#handleDefer}，用指定延迟写入 retry ZSet + ACK</li>
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@RequiredArgsConstructor
public class DefaultAcknowledgment implements Acknowledgment {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultAcknowledgment.class);

    /** nack 计数 Hash 中的计数字段名 */
    private static final String FIELD_NACK_COUNT = "count";

    private final Message<?> message;
    private final StreamMQListener listener;
    private final DefaultConsumeContextConsume context;
    private final ListenerRegistration<?> reg;
    private final RetryAndDlqHandler retryDlqHandler;
    private final RedissonClient redisson;

    @Override
    public void acknowledge() {
        MessageId messageId = message.getMessageId();
        if (messageId != null) {
            cleanupNackCount(messageId);
            listener.ack(messageId);
            context.markAcked();
        }
    }

    @Override
    public void nack() {
        MessageId messageId = message.getMessageId();
        if (messageId == null) {
            LOG.warn("nack: message has no messageId, skipping");
            return;
        }
        NackRetryMode mode = reg.getNackRetryMode();
        if (mode == null) {
            mode = NackRetryMode.RETRY_ZSET;
        }
        switch (mode) {
            case RETRY_ZSET -> {
                LOG.debug("nack (RETRY_ZSET): scheduling retry for messageId={}", messageId);
                retryDlqHandler.handleReconsumeLater(message, reg, listener, messageId);
            }
            case STREAM_AUTO -> handleStreamAutoNack(messageId);
            default -> LOG.warn("nack: unknown NackRetryMode {} for messageId={}", mode, messageId);
        }
    }

    @Override
    public void defer(Duration delay) {
        MessageId messageId = message.getMessageId();
        if (messageId == null) {
            LOG.warn("defer: message has no messageId, skipping");
            return;
        }
        LOG.debug("defer({}ms): scheduling retry with specified delay for messageId={}",
            delay.toMillis(), messageId);
        cleanupNackCount(messageId);
        retryDlqHandler.handleDefer(message, reg, listener, messageId, delay);
    }

    /**
     * STREAM_AUTO 模式下的 nack 处理：不 ACK 留 PEL，用 Redis Hash 记录 nack 次数。
     * 超过 fastRetryCount 后根据 fallbackToRetryZset 决定是否转入 RETRY_ZSET。
     *
     * @param messageId 消息 ID
     */
    private void handleStreamAutoNack(MessageId messageId) {
        String msgIdStr = messageId.getStreamEntryId();
        String nackCountKey = StreamMQKeys.nackCountHash(reg.getNamespace(), reg.getTopic(), reg.getGroup(), msgIdStr);
        RMap<String, String> nackCountMap = redisson.getMap(nackCountKey);

        String countStr = nackCountMap.get(FIELD_NACK_COUNT);
        int count = (countStr == null || countStr.isEmpty()) ? 1 : Integer.parseInt(countStr) + 1;
        nackCountMap.put(FIELD_NACK_COUNT, Integer.toString(count));

        int fastRetryCount = reg.getFastRetryCount();
        if (count > fastRetryCount) {
            if (reg.isFallbackToRetryZset()) {
                LOG.debug("nack (STREAM_AUTO): fastRetryCount={} exceeded (count={}), converting to RETRY_ZSET for messageId={}",
                    fastRetryCount, count, messageId);
                nackCountMap.delete();
                retryDlqHandler.handleReconsumeLater(message, reg, listener, messageId);
            } else {
                LOG.debug("nack (STREAM_AUTO): fastRetryCount={} exceeded (count={}), fallbackToRetryZset=false, staying in PEL for messageId={}",
                    fastRetryCount, count, messageId);
            }
        } else {
            LOG.debug("nack (STREAM_AUTO): count={}/{}, staying in PEL for messageId={}",
                count, fastRetryCount, messageId);
        }
    }

    /**
     * 清理 nack 计数 Hash（消息被 ACK 或 defer 时调用）。
     *
     * @param messageId 消息 ID
     */
    private void cleanupNackCount(MessageId messageId) {
        try {
            String msgIdStr = messageId.getStreamEntryId();
            String nackCountKey = StreamMQKeys.nackCountHash(reg.getNamespace(), reg.getTopic(), reg.getGroup(), msgIdStr);
            RMap<String, String> nackCountMap = redisson.getMap(nackCountKey);
            nackCountMap.delete();
        } catch (RuntimeException ex) {
            LOG.debug("cleanupNackCount failed (messageId={}): {}", messageId, ex.getMessage());
        }
    }
}
