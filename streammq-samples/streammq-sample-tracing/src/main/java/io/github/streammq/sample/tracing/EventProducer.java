/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.sample.tracing;

import io.github.streammq.core.message.MessageBuilder;
import io.github.streammq.core.message.SendResult;
import io.github.streammq.core.template.StreamMessageTemplate;
import org.springframework.stereotype.Component;

/**
 * 事件生产者，发送带追踪上下文的事件消息。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Component
public class EventProducer {

    private static final String TOPIC = SampleConstants.TOPIC;

    private final StreamMessageTemplate template;

    public EventProducer(StreamMessageTemplate template) {
        this.template = template;
    }

    public SendResult emitEvent(String eventId, String payload) {
        return template.syncSend(
                MessageBuilder.<String>withTopic(TOPIC)
                        .tag(SampleConstants.TAG)
                        .keys(eventId)
                        .body(payload)
                        .build());
    }
}
