/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.sample.diagnostics;

import io.github.streammq.core.message.MessageBuilder;
import io.github.streammq.core.message.SendResult;
import io.github.streammq.core.template.StreamMessageTemplate;
import org.springframework.stereotype.Component;

/**
 * 订单生产者，通过 {@link StreamMessageTemplate} 发送模拟订单消息。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Component
public class OrderProducer {

    private static final String TOPIC = SampleConstants.TOPIC;

    private final StreamMessageTemplate template;

    public OrderProducer(StreamMessageTemplate template) {
        this.template = template;
    }

    public SendResult createOrder(String orderId, String content) {
        return template.syncSend(
                MessageBuilder.<String>withTopic(TOPIC)
                        .tag(SampleConstants.TAG)
                        .keys(orderId)
                        .body(content)
                        .withUserProperty("source", "diagnostics-sample")
                        .build());
    }
}
