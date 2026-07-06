package io.github.streammq.adapter.redisson.container;

import io.github.streammq.core.listener.ListenerRegistration;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.policy.DlqFailureHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 默认 {@link DlqFailureHandler} 实现：记录 ERROR 日志后由框架 ACK 丢弃死信消息。
 *
 * <p>对齐 RocketMQ 死信终端理念：死信消息消费失败后不再重试，仅留日志供人工介入/重放。
 * 用户可实现自定义 {@link DlqFailureHandler} 接入告警（钉钉/飞书）、持久化或转人工队列。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class LogAndDropDlqFailureHandler implements DlqFailureHandler {

    private static final Logger LOG = LoggerFactory.getLogger(LogAndDropDlqFailureHandler.class);

    @Override
    public void handleFailure(Message<?> message, ListenerRegistration<?> reg, Throwable cause) {
        if (cause != null) {
            LOG.error("DLQ message consume failed, dropping (topic={}, group={}, messageId={}, " +
                    "reconsumeTimes={}, cause={}: {})",
                reg.getTopic(), reg.getGroup(), message.getMessageId(), message.getReconsumeTimes(),
                cause.getClass().getSimpleName(), cause.getMessage(), cause);
        } else {
            LOG.error("DLQ message consume failed (returned RECONSUME_LATER/DEFER), dropping " +
                    "(topic={}, group={}, messageId={}, reconsumeTimes={})",
                reg.getTopic(), reg.getGroup(), message.getMessageId(), message.getReconsumeTimes());
        }
    }

    @Override
    public String name() {
        return "log-and-drop";
    }
}
