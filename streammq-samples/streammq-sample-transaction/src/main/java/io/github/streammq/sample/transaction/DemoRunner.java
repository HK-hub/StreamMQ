package io.github.streammq.sample.transaction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/** 启动后自动发送一条事务消息，演示事务消息「半消息 + 回查 → 提交」。 */
@Component
@Profile("!it")
public class DemoRunner implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(DemoRunner.class);

    private final OrderTransactionProducer orderTransactionProducer;

    public DemoRunner(OrderTransactionProducer orderTransactionProducer) {
        this.orderTransactionProducer = orderTransactionProducer;
    }

    @Override
    public void run(ApplicationArguments args) {
        log.info("DemoRunner: 发送事务消息...");
        orderTransactionProducer.sendOrderTransactionSimple("demo-transaction-order");
        log.info("DemoRunner: 事务消息已发送，请观察消费者日志");
    }
}
