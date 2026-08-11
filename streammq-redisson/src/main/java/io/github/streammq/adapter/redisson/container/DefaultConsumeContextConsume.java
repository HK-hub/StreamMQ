package io.github.streammq.adapter.redisson.container;

import io.github.streammq.core.consumer.ConsumeOrderlyContext;
import io.github.streammq.core.listener.ListenerRegistration;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageId;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 默认 {@link ConsumeOrderlyContext} 实现，同时兼容普通消费场景。
 *
 * <p>由容器在 {@code handleMessage} 中创建，传给 Consumer 的 {@code onMessage} 方法。
 * 封装当前消息与注册信息，提供消息元数据访问与顺序消费分片信息。
 *
 * <p>消费结果由 {@code onMessage} 返回值表达，本上下文不再提供手动 ACK/nack/defer 调用。
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
