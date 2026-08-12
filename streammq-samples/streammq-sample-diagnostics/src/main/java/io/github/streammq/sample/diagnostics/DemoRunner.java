package io.github.streammq.sample.diagnostics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/** 启动后自动发送消息，产生供诊断服务（画像 / 慢消费 / 积压 / DLQ）分析的数据。 */
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
        log.info("DemoRunner: 发送诊断示例消息...");
        orderProducer.createOrder("diag-demo-001", "diagnostics demo");
        log.info("DemoRunner: 消息已发送，请观察诊断端点输出");
    }
}
