/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.sample.quickstart;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/** 启动后自动发送几条示例消息，演示「发送 → 消费」闭环。 */
@Component
@Profile("!it")
public class DemoRunner implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(DemoRunner.class);

    private final OrderProducer orderProducer;

    public DemoRunner(OrderProducer orderProducer) {
        this.orderProducer = orderProducer;
    }

    @Override
    public void run(ApplicationArguments args) {
        log.info("DemoRunner: 发送示例消息...");
        orderProducer.createOrder("demo-001", "Hello StreamMQ");
        orderProducer.createOrderWithBuilder("demo-002", "Builder mode");
        orderProducer.createOrderAsync("demo-003", "Async send");
        log.info("DemoRunner: 示例消息已发送，请观察消费者日志");
    }
}
