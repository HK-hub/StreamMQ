package io.github.streammq.cloud.k8s;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * HPA（Horizontal Pod Autoscaler）指标提供者。
 *
 * <p>为 Kubernetes 水平 Pod 自动扩缩容提供消费延迟与消费速率指标。 默认实现使用内存数据结构（{@link ConcurrentHashMap}）存储指标值，
 * 可被子类覆盖以接入真实指标源（如 {@code streammq-redisson} 的 StreamMQMetrics）。
 *
 * <p>指标维度：
 *
 * <ul>
 *   <li>消费延迟（lag）：未消费的积压消息数，用于触发扩容
 *   <li>消费速率（rate）：每秒处理消息数，用于评估扩缩容效果
 * </ul>
 *
 * <p>外部组件可通过 {@link #recordLag} 与 {@link #recordConsumeRate} 写入指标值， K8s 自定义指标适配器通过 getter 方法读取并暴露给
 * HPA 控制器。
 *
 * @author StreamMQ Contributors
 * @since 2.0.0
 */
public class HpaMetricsProvider {

    private final ConcurrentHashMap<String, AtomicLong> lagMap = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<String, Double> rateMap = new ConcurrentHashMap<>();

    /**
     * 获取指定主题与消费者组的消费延迟（积压消息数）。
     *
     * @param topic 主题
     * @param group 消费者组
     * @return 积压消息数，未记录时返回 0
     */
    public long getConsumerLag(String topic, String group) {
        Objects.requireNonNull(topic, "topic");
        Objects.requireNonNull(group, "group");
        AtomicLong lag = lagMap.get(key(topic, group));
        return Objects.isNull(lag) ? 0L : lag.get();
    }

    /**
     * 获取指定主题与消费者组的消费速率（消息/秒）。
     *
     * @param topic 主题
     * @param group 消费者组
     * @return 消费速率，未记录时返回 0.0
     */
    public double getConsumeRate(String topic, String group) {
        Objects.requireNonNull(topic, "topic");
        Objects.requireNonNull(group, "group");
        Double rate = rateMap.get(key(topic, group));
        return Objects.isNull(rate) ? 0.0 : rate;
    }

    /**
     * 返回所有已记录 consumer 的指标快照。
     *
     * <p>返回的 Map 以指标名作为键（格式：{@code metricType.topic:group}），
     * 包含消费延迟与消费速率两类指标。返回的是当前快照的不可变副本，调用方修改不影响内部状态。
     *
     * @return 不可变的指标 Map，键为指标名，值为指标值
     */
    public Map<String, Double> getConsumerMetrics() {
        Map<String, Double> metrics = new LinkedHashMap<>();
        lagMap.forEach((k, lagRef) -> metrics.put("consumer.lag." + k, (double) lagRef.get()));
        rateMap.forEach((k, rate) -> metrics.put("consume.rate." + k, rate));
        return Collections.unmodifiableMap(metrics);
    }

    /**
     * 记录指定主题与消费者组的消费延迟。
     *
     * @param topic 主题
     * @param group 消费者组
     * @param lag 积压消息数
     */
    public void recordLag(String topic, String group, long lag) {
        lagMap.computeIfAbsent(key(topic, group), k -> new AtomicLong()).set(lag);
    }

    /**
     * 记录指定主题与消费者组的消费速率。
     *
     * @param topic 主题
     * @param group 消费者组
     * @param rate 消费速率（消息/秒）
     */
    public void recordConsumeRate(String topic, String group, double rate) {
        rateMap.put(key(topic, group), rate);
    }

    private static String key(String topic, String group) {
        return topic + ":" + group;
    }
}
