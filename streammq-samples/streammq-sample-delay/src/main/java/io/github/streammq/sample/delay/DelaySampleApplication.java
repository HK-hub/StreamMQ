package io.github.streammq.sample.delay;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * StreamMQ 延时消息示例启动类。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@SpringBootApplication
public class DelaySampleApplication {

    /**
     * 程序入口。
     *
     * @param args 启动参数
     */
    public static void main(String[] args) {
        SpringApplication.run(DelaySampleApplication.class, args);
    }
}
