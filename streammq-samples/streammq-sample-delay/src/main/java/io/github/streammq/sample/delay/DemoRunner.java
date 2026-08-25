/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.sample.delay;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/** 启动后自动发送延时消息，演示固定级别与自定义毫秒延时投递。 */
@Component
@Profile("!it")
public class DemoRunner implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(DemoRunner.class);

    private final DelayMessageProducer delayMessageProducer;

    public DemoRunner(DelayMessageProducer delayMessageProducer) {
        this.delayMessageProducer = delayMessageProducer;
    }

    @Override
    public void run(ApplicationArguments args) {
        log.info("DemoRunner: 发送延时消息...");
        delayMessageProducer.sendCustomDelayMessage("demo-001", "custom delay 3s", 3000L);
        delayMessageProducer.sendOrderTimeoutReminder("demo-002", "payment timeout");
        log.info("DemoRunner: 延时消息已发送，请观察消费者日志");
    }
}
