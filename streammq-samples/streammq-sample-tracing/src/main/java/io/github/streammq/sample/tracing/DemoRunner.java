package io.github.streammq.sample.tracing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/** 启动后自动发送事件消息，演示 W3C traceparent 上下文透传。 */
@Component
@Profile("!it")
public class DemoRunner implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(DemoRunner.class);

    private final EventProducer eventProducer;

    public DemoRunner(EventProducer eventProducer) {
        this.eventProducer = eventProducer;
    }

    @Override
    public void run(ApplicationArguments args) {
        log.info("DemoRunner: 发送追踪示例事件...");
        eventProducer.emitEvent("trace-demo-001", "tracing demo");
        log.info("DemoRunner: 事件已发送，请观察 traceparent 透传日志");
    }
}
