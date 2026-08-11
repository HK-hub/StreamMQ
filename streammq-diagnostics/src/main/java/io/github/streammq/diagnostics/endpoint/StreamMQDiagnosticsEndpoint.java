package io.github.streammq.diagnostics.endpoint;

import io.github.streammq.diagnostics.MessageProfileService;
import io.github.streammq.diagnostics.StreamMQDiagnosticsService;
import io.github.streammq.diagnostics.model.BacklogReport;
import io.github.streammq.diagnostics.model.DlqReport;
import io.github.streammq.diagnostics.model.MessageProfile;
import io.github.streammq.diagnostics.model.SlowConsumeReport;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * StreamMQ 诊断 REST 端点，暴露 JSON 接口供仪表盘集成。
 *
 * <p>基础路径：{@code /streammq/diagnostics}
 *
 * <p>本类不使用 {@code @RestController}，而是通过 {@link
 * StreamMQDiagnosticsAutoConfiguration#streamMQDiagnosticsEndpoint} 注册为 Bean，
 * 确保仅在诊断服务与画像服务均就绪时才启用端点，避免组件扫描导致的依赖缺失问题。
 *
 * <p>提供以下端点：
 *
 * <ul>
 *   <li>{@code GET /streammq/diagnostics/profile/{messageId}} - 获取消息画像
 *   <li>{@code GET /streammq/diagnostics/slow-consume?topic=&group=} - 诊断慢消费
 *   <li>{@code GET /streammq/diagnostics/backlog?topic=&group=} - 诊断积压
 *   <li>{@code GET /streammq/diagnostics/dlq?group=} - 诊断死信队列
 *   <li>{@code GET /streammq/diagnostics/slow-consumers} - 列出所有慢消费者
 *   <li>{@code GET /streammq/diagnostics/all-backlogs} - 列出所有积压报告
 *   <li>{@code GET /streammq/diagnostics/health} - 诊断健康概览
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 1.0.0
 */
@ResponseBody
@RequestMapping("/streammq/diagnostics")
public class StreamMQDiagnosticsEndpoint {

  private static final Logger log = LoggerFactory.getLogger(StreamMQDiagnosticsEndpoint.class);

  private final StreamMQDiagnosticsService diagnosticsService;
  private final MessageProfileService profileService;

  /**
   * 构造诊断端点。
   *
   * @param diagnosticsService 诊断服务
   * @param profileService 消息画像服务
   */
  public StreamMQDiagnosticsEndpoint(
      StreamMQDiagnosticsService diagnosticsService, MessageProfileService profileService) {
    this.diagnosticsService = Objects.requireNonNull(diagnosticsService, "diagnosticsService");
    this.profileService = Objects.requireNonNull(profileService, "profileService");
  }

  /**
   * 获取消息完整生命周期画像。
   *
   * @param messageId 消息 ID
   * @return 消息画像，若不存在则返回 404
   */
  @GetMapping("/profile/{messageId}")
  public ResponseEntity<MessageProfile> getProfile(@PathVariable String messageId) {
    MessageProfile profile = profileService.getProfile(messageId);
    if (Objects.isNull(profile)) {
      return ResponseEntity.notFound().build();
    }
    return ResponseEntity.ok(profile);
  }

  /**
   * 诊断慢消费。
   *
   * @param topic 主题
   * @param group 消费者组
   * @return 慢消费诊断报告
   */
  @GetMapping("/slow-consume")
  public SlowConsumeReport diagnoseSlowConsume(
      @RequestParam String topic, @RequestParam String group) {
    return diagnosticsService.diagnoseSlowConsume(topic, group);
  }

  /**
   * 诊断消息积压。
   *
   * @param topic 主题
   * @param group 消费者组
   * @return 积压诊断报告
   */
  @GetMapping("/backlog")
  public BacklogReport diagnoseBacklog(@RequestParam String topic, @RequestParam String group) {
    return diagnosticsService.diagnoseBacklog(topic, group);
  }

  /**
   * 诊断死信队列。
   *
   * @param group 消费者组
   * @return 死信队列诊断报告
   */
  @GetMapping("/dlq")
  public DlqReport diagnoseDlq(@RequestParam String group) {
    return diagnosticsService.diagnoseDlq(group);
  }

  /**
   * 列出所有慢消费者。
   *
   * @return 慢消费者标识列表（格式：topic:group）
   */
  @GetMapping("/slow-consumers")
  public List<String> getSlowConsumers() {
    return diagnosticsService.getSlowConsumers();
  }

  /**
   * 列出所有消费者组的积压报告。
   *
   * @return 积压报告列表
   */
  @GetMapping("/all-backlogs")
  public List<BacklogReport> getAllBacklogs() {
    return diagnosticsService.getAllBacklogs();
  }

  /**
   * 诊断健康概览，聚合所有诊断数据为单一视图。
   *
   * @return 健康概览 Map
   */
  @GetMapping("/health")
  public Map<String, Object> health() {
    List<String> slowConsumers = diagnosticsService.getSlowConsumers();
    List<BacklogReport> backlogs = diagnosticsService.getAllBacklogs();
    long totalBacklog = backlogs.stream().mapToLong(BacklogReport::currentBacklog).sum();

    Map<String, Object> summary = new LinkedHashMap<>();
    summary.put("status", "UP");
    summary.put("slowConsumerCount", slowConsumers.size());
    summary.put("slowConsumers", slowConsumers);
    summary.put("totalBacklog", totalBacklog);
    summary.put("backlogReports", backlogs);
    summary.put("timestamp", System.currentTimeMillis());
    return summary;
  }
}
