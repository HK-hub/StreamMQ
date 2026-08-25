/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.sample.dlq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/** 启动后自动发送一条必然处理失败的消息，演示重试耗尽后进入 DLQ。 */
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
        log.info("DemoRunner: 发送必然失败的消息（进入 DLQ 演示）...");
        orderProducer.sendOrder("dlq-demo-001", "this message will fail and enter DLQ");
        log.info("DemoRunner: 消息已发送，请观察重试与 DLQ 日志");
    }
}
