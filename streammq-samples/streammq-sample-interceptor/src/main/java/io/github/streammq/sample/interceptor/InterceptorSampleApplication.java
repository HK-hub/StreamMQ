/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.sample.interceptor;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * StreamMQ 拦截器示例启动类。
 *
 * <p>演示拦截器机制：
 *
 * <ul>
 *   <li>TraceProducerInterceptor - 发送前注入 traceId
 *   <li>RateLimitProducerInterceptor - 发送前限流控制
 *   <li>TraceConsumerInterceptor - 消费前后记录追踪日志
 *   <li>AuditConsumerInterceptor - 消费后记录审计日志
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@SpringBootApplication
public class InterceptorSampleApplication {

    public static void main(String[] args) {
        SpringApplication.run(InterceptorSampleApplication.class, args);
    }
}
