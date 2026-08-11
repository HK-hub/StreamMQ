package io.github.streammq.diagnostics;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * StreamMQ 诊断模块配置属性。
 *
 * <p>通过 {@code streammq.diagnostics.*} 前缀配置诊断模块行为。
 *
 * <p>典型配置示例：
 *
 * <pre>{@code
 * streammq:
 *   diagnostics:
 *     enabled: true
 *     slow-consume-threshold-ms: 3000
 *     backlog-warning-threshold: 500
 *     backlog-critical-threshold: 5000
 *     max-profile-query-size: 500
 * }</pre>
 *
 * @author StreamMQ Contributors
 * @since 1.0.0
 */
@Getter
@Setter
@ConfigurationProperties(prefix = "streammq.diagnostics")
public class StreamMQDiagnosticsProperties {

  /** 是否启用诊断模块，默认关闭 */
  private boolean enabled = false;

  /** 近期诊断时间窗口（毫秒），默认 5 分钟 */
  private long recentWindowMs = 5 * 60 * 1000L;

  /** DLQ 诊断时间窗口（毫秒），默认 1 小时 */
  private long dlqWindowMs = 60 * 60 * 1000L;

  /** 慢消费耗时阈值（毫秒），超过此值判定为慢消费，默认 5000ms */
  private long slowConsumeThresholdMs = 5000L;

  /** 积压警告阈值，超过此值触发 WARNING 级别，默认 1000 */
  private long backlogWarningThreshold = 1000L;

  /** 积压严重阈值，超过此值触发 CRITICAL 级别，默认 10000 */
  private long backlogCriticalThreshold = 10000L;

  /** DLQ 主题标识关键字（小写匹配），默认 "dlq" */
  private String dlqTopicMarker = "dlq";

  /** DLQ 最大重试次数阈值，超过此值判定为死信，默认 3 */
  private int dlqMaxRetryCount = 3;

  /** 单次画像查询最大消息数，防止大范围查询导致 OOM，默认 1000 */
  private int maxProfileQuerySize = 1000;
}
