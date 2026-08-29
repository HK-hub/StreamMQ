/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.diagnostics;

import io.github.streammq.core.listener.StreamMQListenerContainer;
import io.github.streammq.core.trace.StreamMQTraceService;
import io.github.streammq.core.util.CollectionUtils;
import io.github.streammq.core.util.StringUtils;
import io.github.streammq.diagnostics.model.BacklogReport;
import io.github.streammq.diagnostics.model.DlqReport;
import io.github.streammq.diagnostics.model.SlowConsumeReport;
import io.github.streammq.diagnostics.spi.BacklogProbe;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * StreamMQ 诊断服务 Facade，自动诊断慢消费、消息积压、死信队列等异常。
 *
 * <p>本类保留原有公共 API（{@code diagnoseXxx} / {@code getXxx}）作为统一的对外门面， 实际诊断能力由三个独立的 {@code Analyzer}
 * 协作组件实现：
 *
 * <ul>
 *   <li>{@link SlowConsumeAnalyzer} - 慢消费诊断
 *   <li>{@link BacklogAnalyzer} - 积压诊断
 *   <li>{@link DlqAnalyzer} - 死信队列诊断
 * </ul>
 *
 * <p>调用方（REST 端点 / 用户代码）只需依赖 {@code StreamMQDiagnosticsService} 即可获取全部诊断能力； 内部 {@code Analyzer}
 * 也可被单独注入使用，便于按需扩展与单元测试。
 *
 * <p>诊断阈值、时间窗口、DLQ 判定规则等全部通过 {@link StreamMQDiagnosticsProperties} 外部化配置， 支持用户按需调整而无需修改代码。
 *
 * <p>当追踪数据不可用时，所有诊断方法以无数据方式优雅降级，不抛出异常。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class StreamMQDiagnosticsService {

    /** topic:group 组合 key 分隔符 */
    private static final String KEY_SEPARATOR = StreamMQDiagnosticsDefaults.KEY_SEPARATOR;

    private final SlowConsumeAnalyzer slowConsumeAnalyzer;
    private final BacklogAnalyzer backlogAnalyzer;
    private final DlqAnalyzer dlqAnalyzer;
    private final StreamMQListenerContainer listenerContainer;
    private final StreamMQDiagnosticsProperties properties;

    /**
     * 构造诊断服务（不使用积压探针，回退到追踪窗口估算）。
     *
     * <p>该构造函数保留为兼容旧式测试与外部直接实例化场景； Spring 装配请使用 5 参构造函数（{@code Analyzer} 自动注册）。
     *
     * @param traceService 追踪查询服务
     * @param listenerContainer 监听器容器
     * @param properties 诊断配置属性
     */
    public StreamMQDiagnosticsService(
            StreamMQTraceService traceService,
            StreamMQListenerContainer listenerContainer,
            StreamMQDiagnosticsProperties properties) {
        this(traceService, listenerContainer, properties, null);
    }

    /**
     * 构造诊断服务。
     *
     * <p>该构造函数保留为兼容旧式测试与外部直接实例化场景； Spring 装配请使用 5 参构造函数（{@code Analyzer} 自动注册）。
     *
     * @param traceService 追踪查询服务
     * @param listenerContainer 监听器容器
     * @param properties 诊断配置属性
     * @param backlogProbe 积压探针（可为 null，此时使用追踪窗口估算）
     */
    public StreamMQDiagnosticsService(
            StreamMQTraceService traceService,
            StreamMQListenerContainer listenerContainer,
            StreamMQDiagnosticsProperties properties,
            BacklogProbe backlogProbe) {
        Objects.requireNonNull(traceService, "traceService");
        Objects.requireNonNull(listenerContainer, "listenerContainer");
        Objects.requireNonNull(properties, "properties");
        this.listenerContainer = listenerContainer;
        this.properties = properties;
        this.slowConsumeAnalyzer =
                new SlowConsumeAnalyzer(traceService, listenerContainer, properties);
        this.backlogAnalyzer = new BacklogAnalyzer(traceService, properties, backlogProbe);
        this.dlqAnalyzer = new DlqAnalyzer(traceService, properties);
    }

    /**
     * Spring 注入主构造函数。
     *
     * <p>由 {@code StreamMQDiagnosticsAutoConfiguration} 的 {@code @Bean} 工厂方法调用， 传入 3 个
     * analyzer（{@code @Component} 自动注册）+ 容器 + 属性。
     */
    public StreamMQDiagnosticsService(
            SlowConsumeAnalyzer slowConsumeAnalyzer,
            BacklogAnalyzer backlogAnalyzer,
            DlqAnalyzer dlqAnalyzer,
            StreamMQListenerContainer listenerContainer,
            StreamMQDiagnosticsProperties properties) {
        this.slowConsumeAnalyzer =
                Objects.requireNonNull(slowConsumeAnalyzer, "slowConsumeAnalyzer");
        this.backlogAnalyzer = Objects.requireNonNull(backlogAnalyzer, "backlogAnalyzer");
        this.dlqAnalyzer = Objects.requireNonNull(dlqAnalyzer, "dlqAnalyzer");
        this.listenerContainer = Objects.requireNonNull(listenerContainer, "listenerContainer");
        this.properties = Objects.requireNonNull(properties, "properties");
    }

    /**
     * 诊断慢消费，分析指定主题+消费者组的消费性能状况。委托给 {@link SlowConsumeAnalyzer}。
     *
     * @param topic 主题
     * @param group 消费者组
     * @return 慢消费诊断报告
     */
    public SlowConsumeReport diagnoseSlowConsume(String topic, String group) {
        return slowConsumeAnalyzer.diagnose(topic, group);
    }

    /**
     * 诊断消息积压，分析指定主题+消费者组的积压状况。委托给 {@link BacklogAnalyzer}。
     *
     * @param topic 主题
     * @param group 消费者组
     * @return 积压诊断报告
     */
    public BacklogReport diagnoseBacklog(String topic, String group) {
        return backlogAnalyzer.diagnose(topic, group);
    }

    /**
     * 诊断死信队列，分析指定消费者组的死信状况与失败原因分布。委托给 {@link DlqAnalyzer}。
     *
     * @param group 消费者组
     * @return 死信队列诊断报告
     */
    public DlqReport diagnoseDlq(String group) {
        return dlqAnalyzer.diagnose(group);
    }

    /**
     * 识别所有慢消费者，遍历监听器容器中注册的全部消费者。
     *
     * <p>对每个主题+消费者组组合进行慢消费诊断，返回平均消费耗时超过阈值的消费者列表。
     *
     * @return 慢消费者标识列表（格式：topic:group）
     */
    public List<String> getSlowConsumers() {
        Collection<StreamMQListenerContainer.ConsumerMetadata> consumers =
                listenerContainer.getConsumers();
        if (CollectionUtils.isEmpty(consumers)) {
            return Collections.emptyList();
        }

        Set<String> visited = new HashSet<>();
        List<String> slowConsumers = new ArrayList<>();

        for (StreamMQListenerContainer.ConsumerMetadata metadata : consumers) {
            if (Objects.isNull(metadata)
                    || StringUtils.isEmpty(metadata.topic())
                    || StringUtils.isEmpty(metadata.consumerGroup())) {
                continue;
            }
            String key = metadata.topic() + KEY_SEPARATOR + metadata.consumerGroup();
            if (visited.contains(key)) {
                continue;
            }
            visited.add(key);

            SlowConsumeReport report =
                    diagnoseSlowConsume(metadata.topic(), metadata.consumerGroup());
            if (report.avgConsumeTimeMillis() > properties.getSlowConsumeThresholdMs()) {
                slowConsumers.add(key);
            }
        }
        return slowConsumers;
    }

    /**
     * 获取所有消费者组的积压报告。
     *
     * <p>遍历监听器容器中注册的全部消费者，对每个主题+消费者组组合进行积压诊断。
     *
     * @return 积压报告列表
     */
    public List<BacklogReport> getAllBacklogs() {
        Collection<StreamMQListenerContainer.ConsumerMetadata> consumers =
                listenerContainer.getConsumers();
        if (CollectionUtils.isEmpty(consumers)) {
            return Collections.emptyList();
        }

        Set<String> visited = new HashSet<>();
        List<BacklogReport> backlogs = new ArrayList<>();

        for (StreamMQListenerContainer.ConsumerMetadata metadata : consumers) {
            if (Objects.isNull(metadata)
                    || StringUtils.isEmpty(metadata.topic())
                    || StringUtils.isEmpty(metadata.consumerGroup())) {
                continue;
            }
            String key = metadata.topic() + KEY_SEPARATOR + metadata.consumerGroup();
            if (visited.contains(key)) {
                continue;
            }
            visited.add(key);
            backlogs.add(diagnoseBacklog(metadata.topic(), metadata.consumerGroup()));
        }
        return backlogs;
    }
}
