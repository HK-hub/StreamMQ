package io.github.streammq.sample.diagnostics;

import io.github.streammq.core.annotation.StreamMQConsumer;
import io.github.streammq.core.consumer.ConsumeContext;
import io.github.streammq.core.consumer.StreamMessageConcurrentlyConsumer;
import io.github.streammq.core.enums.ConsumeAction;
import io.github.streammq.core.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 订单消费者，监听订单事件主题。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Component
@StreamMQConsumer(topic = SampleConstants.TOPIC, consumerGroup = SampleConstants.CONSUMER_GROUP)
public class OrderConsumer implements StreamMessageConcurrentlyConsumer<String> {

    private static final Logger log = LoggerFactory.getLogger(OrderConsumer.class);

    @Override
    public ConsumeAction onMessage(Message<String> message, ConsumeContext context) {
        log.info("Received order: keys={}, body={}", message.getKeys(), message.getBody());
        return ConsumeAction.SUCCESS;
    }
}
