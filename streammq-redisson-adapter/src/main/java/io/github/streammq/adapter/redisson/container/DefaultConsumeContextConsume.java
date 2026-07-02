package io.github.streammq.adapter.redisson.container;

import io.github.streammq.core.consumer.Acknowledgment;
import io.github.streammq.core.consumer.ConsumeOrderlyContext;
import io.github.streammq.core.enums.AcknowledgeMode;
import io.github.streammq.core.listener.StreamMQListener;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageId;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 默认 {@link ConsumeOrderlyContext} 实现，同时兼容普通消费场景。
 *
 * <p>由容器在 {@code handleMessage} 中创建，传给 Consumer 的 {@code onMessage} 方法。
 * 封装当前消息、注册信息、消费者实例，提供消息元数据访问与手动 ACK 能力。
 *
 * <p>线程安全：每个 {@code handleMessage} 调用创建独立实例，无需考虑并发。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@RequiredArgsConstructor
public class DefaultConsumeContextConsume implements ConsumeOrderlyContext {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultConsumeContextConsume.class);

    /** 默认消费者实例名后缀 */
    private static final String DEFAULT_CONSUMER_NAME_SUFFIX = "-consumer";

    private final Message<?> message;
    private final ListenerRegistration<?> registration;
    private final StreamMQListener listener;
    private final AtomicBoolean acked = new AtomicBoolean(false);

    /**
     * 标记为已 ACK（由 {@link DefaultAcknowledgment#acknowledge()} 回调）。
     */
    void markAcked() {
        acked.set(true);
    }

    /**
     * 返回是否已 ACK。
     *
     * @return true 如果已通过 {@link Acknowledgment#acknowledge()} 确认
     */
    public boolean isAcked() {
        return acked.get();
    }

    @Override
    public String topic() {
        return message.getTopic();
    }

    @Override
    public String consumerGroup() {
        return registration.getGroup();
    }

    @Override
    public String consumerName() {
        return registration.getGroup() + DEFAULT_CONSUMER_NAME_SUFFIX;
    }

    @Override
    public int reconsumeTimes() {
        return message.getReconsumeTimes();
    }

    @Override
    public long bornTimestamp() {
        return message.getBornTimestamp();
    }

    @Override
    public String bornHost() {
        return message.getBornHost();
    }

    @Override
    public Map<String, String> messageTrack() {
        return message.getProperties();
    }

    @Override
    public String ext(String key) {
        return message.getProperties().get(key);
    }

    @Override
    public AcknowledgeMode ackMode() {
        return registration.getAckMode();
    }

    @Override
    public Acknowledgment acknowledge() {
        return new DefaultAcknowledgment(message, listener, this);
    }

    @Override
    public void suspend(Duration duration) {
        LOG.debug("Suspend requested (duration={}ms, messageId={})", duration.toMillis(), message.getMessageId());
    }

    @Override
    public String shardingKey() {
        return message.getShardingKey();
    }

    @Override
    public int shardId() {
        return 0;
    }

    @Override
    public MessageId queueOffset() {
        return message.getMessageId();
    }

    @Override
    public long backlog() {
        return 0;
    }
}
