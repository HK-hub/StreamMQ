/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.sample.transaction;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * StreamMQ 事务消息示例启动类。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@SpringBootApplication
public class TransactionSampleApplication {

    /**
     * 程序入口。
     *
     * @param args 启动参数
     */
    public static void main(String[] args) {
        SpringApplication.run(TransactionSampleApplication.class, args);
    }
}
