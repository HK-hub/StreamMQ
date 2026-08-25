/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.sample.orderly;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/** 启动后自动发送一组顺序消息，演示同一 shardingKey 分片内严格有序消费。 */
@Component
@Profile("!it")
public class DemoRunner implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(DemoRunner.class);

    private final OrderlyMessageProducer orderlyMessageProducer;

    public DemoRunner(OrderlyMessageProducer orderlyMessageProducer) {
        this.orderlyMessageProducer = orderlyMessageProducer;
    }

    @Override
    public void run(ApplicationArguments args) {
        log.info("DemoRunner: 发送顺序消息...");
        orderlyMessageProducer.sendOrderStatusFlow("demo-order-001");
        log.info("DemoRunner: 顺序消息已发送，请观察消费者日志");
    }
}
