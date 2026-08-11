/**
 * StreamMQ 云原生 K8s 增强模块。
 *
 * <p>本模块提供 Kubernetes 环境下的云原生增强能力，使 StreamMQ 能够与 K8s 原生探针、自动扩缩容、配置热更新等机制深度集成。
 *
 * <p>核心能力：
 *
 * <ul>
 *   <li>健康探针：{@link io.github.streammq.cloud.k8s.StreamMQHealthController} 暴露 K8s
 *       liveness/readiness 探针端点；{@link io.github.streammq.cloud.k8s.StreamMQHealthIndicator} 集成
 *       Spring Boot Actuator 健康指标
 *   <li>优雅上下线：{@link io.github.streammq.cloud.k8s.GracefulShutdownHandler} 在 Pod 终止时
 *       逐步暂停拉取、等待处理中消息完成、停止容器，避免消息丢失或重复消费
 *   <li>配置热更新：{@link io.github.streammq.cloud.k8s.StreamMQConfigRefresher} 接口支持
 *       运行时刷新重试策略、消费线程数、扫描间隔，{@link io.github.streammq.cloud.k8s.NoopConfigRefresher} 提供空操作默认实现
 *   <li>HPA 指标：{@link io.github.streammq.cloud.k8s.HpaMetricsProvider} 暴露消费延迟与速率指标， 供 Kubernetes
 *       Horizontal Pod Autoscaler 自动扩缩容使用
 * </ul>
 *
 * <p>启用方式：在 {@code application.yml} 中配置：
 *
 * <pre>{@code
 * streammq:
 *   cloud:
 *     k8s:
 *       enabled: true
 *       graceful-shutdown-timeout-ms: 30000
 *       health-endpoint-enabled: true
 *       config-refresh-enabled: false
 * }</pre>
 *
 * <p>当 {@link io.github.streammq.core.listener.StreamMQListenerContainer} 不存在时，
 * 所有探针与指标方法将以降级方式安全返回，不抛出异常。
 *
 * @author StreamMQ Contributors
 * @since 2.0.0
 */
package io.github.streammq.cloud.k8s;
