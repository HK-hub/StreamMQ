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

/**
 * 启动后自动发送一条必然处理失败的消息，演示「消费失败 → 重试耗尽 → 进入 DLQ → DLQ 消费者处理」完整闭环。
 *
 * <p>失败由 {@link OrderConsumer#setFailOrderId(String)} 注入（仅对 {@link SampleConstants#DEMO_ORDER_ID}
 * 生效）， 因此默认运行 {@code mvn spring-boot:run} 就能在日志中看到 3 次重试与死信投递，而不是一次「假装失败」的成功消费。
 */
@Component
@Profile("!it")
public class DemoRunner implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(DemoRunner.class);

    private final OrderProducer orderProducer;

    private final OrderConsumer orderConsumer;

    public DemoRunner(OrderProducer orderProducer, OrderConsumer orderConsumer) {
        this.orderProducer = orderProducer;
        this.orderConsumer = orderConsumer;
    }

    @Override
    public void run(ApplicationArguments args) {
        // 先注入失败：该订单号在消费端会持续抛异常，重试耗尽后由框架路由到 DLQ
        orderConsumer.setFailOrderId(SampleConstants.DEMO_ORDER_ID);

        log.info(
                "DemoRunner: 发送必然失败的消息（会重试 3 次后进入 DLQ）: orderId={}", SampleConstants.DEMO_ORDER_ID);
        orderProducer.sendOrder(
                SampleConstants.DEMO_ORDER_ID, "this message will fail and enter DLQ");
        log.info("DemoRunner: 消息已发送，请依次观察「重试 → DLQ → OrderDlqConsumer 消费」日志");
    }
}
