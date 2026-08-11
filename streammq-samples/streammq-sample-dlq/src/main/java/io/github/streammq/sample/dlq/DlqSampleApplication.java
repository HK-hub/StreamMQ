package io.github.streammq.sample.dlq;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * StreamMQ 死信队列示例启动类。
 *
 * <p>演示死信队列机制：
 *
 * <ol>
 *   <li>正常消息发送到 order-topic
 *   <li>OrderConsumer 消费消息，模拟消费失败
 *   <li>消息重试超过 maxReconsumeTimes 后进入死信队列
 *   <li>OrderDlqConsumer 消费死信消息
 * </ol>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@SpringBootApplication
public class DlqSampleApplication {

    public static void main(String[] args) {
        SpringApplication.run(DlqSampleApplication.class, args);
    }
}
