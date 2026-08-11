package io.github.streammq.sample.diagnostics;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * StreamMQ Diagnostics 示例启动类。
 *
 * <p>启用诊断模块（{@code streammq.diagnostics.enabled=true}）和核心追踪 （{@code
 * streammq.trace.enabled=true}），自动暴露 REST 端点供查询消息画像、慢消费报告等。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@SpringBootApplication
public class DiagnosticsApplication {

  public static void main(String[] args) {
    SpringApplication.run(DiagnosticsApplication.class, args);
  }
}
