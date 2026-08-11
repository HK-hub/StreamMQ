package io.github.streammq.sample.tracing;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * StreamMQ Tracing 示例启动类。
 *
 * <p>启用 OpenTelemetry 集成（{@code streammq.tracing.otel.enabled=true}）与核心追踪 （{@code
 * streammq.trace.enabled=true}），验证 W3C TraceContext 在生产者与消费者间的传播。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@SpringBootApplication
public class TracingApplication {

    public static void main(String[] args) {
        SpringApplication.run(TracingApplication.class, args);
    }
}
