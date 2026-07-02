package io.github.streammq.adapter.redisson.container;

import io.github.streammq.core.consumer.Acknowledgment;
import io.github.streammq.core.listener.StreamMQListener;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageId;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * 默认 {@link Acknowledgment} 实现。
 *
 * <p>由 {@link DefaultConsumerContext#acknowledge()} 创建，封装消息确认逻辑：
 * <ul>
 *   <li>{@link #acknowledge()} - ACK 消息（从 PEL 移除），标记 context 已 ack</li>
 *   <li>{@link #nack()} - 不 ACK，消息留在 PEL 中等待 XAUTOCLAIM 补偿</li>
 *   <li>{@link #defer(Duration)} - 不 ACK，由 RetryScheduler 按 delay 调度重投</li>
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@RequiredArgsConstructor
public class DefaultAcknowledgment implements Acknowledgment {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultAcknowledgment.class);

    private final Message<?> message;
    private final StreamMQListener listener;
    private final DefaultConsumerContext context;

    @Override
    public void acknowledge() {
        MessageId messageId = message.getMessageId();
        if (messageId != null) {
            listener.ack(messageId);
            context.markAcked();
        }
    }

    @Override
    public void nack() {
        // 简化：不 ACK，消息留在 PEL 中等待 XAUTOCLAIM 补偿
        LOG.debug("nack: message stays in PEL for re-delivery (messageId={})", message.getMessageId());
    }

    @Override
    public void defer(Duration delay) {
        // 简化：不 ACK，由 RetryScheduler 按 delay 调度重投
        LOG.debug("defer({}ms): message stays in PEL (messageId={})", delay.toMillis(), message.getMessageId());
    }
}
